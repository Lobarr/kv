package hashindex

import (
	"context"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru"
	"github.com/vmihailenco/msgpack/v5"
	"io/ioutil"
	"os"
	"path"
	"storage-engines/core"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrKeyNotFound = errors.New("key not found")

type logEntryIndexByKey map[string]*core.LogEntryIndex

type Engine struct {
	logEntryIndexesBySegmentID map[string]logEntryIndexByKey
	segments                   []string // historical list of segments id's
	lruSegments                *lru.Cache
	segment                    *dataSegment // current segment
	segmentMaxSize             int
	compactorInterval          time.Duration
	mu                         *sync.RWMutex
	ctx                        context.Context
}

type EngineConfig struct {
	SegmentMaxSize             int
	SnapshotInterval           time.Duration
	TolerableSnapshotFailCount int
	CacheSize                  int
	CompactorInterval          time.Duration
}

func (engine *Engine) captureSnapshots(ctx context.Context, interval time.Duration, tolerableFailCount int) {
	failCounts := 0
	for {
		if failCounts >= tolerableFailCount {
			panic("snapshotting failure")
		}

		select {
		case <-time.Tick(interval):
			engine.mu.RLock()
			err := engine.snapshot()
			if err != nil {
				fmt.Printf("error %s", err)
				failCounts++
			}
			engine.mu.RUnlock()
		case <-ctx.Done():
			return
		}
	}
}

// expected to be called within a writable thread safe method
func (engine *Engine) checkDataSegment() error {
	if engine.segment.entriesCount >= engine.segmentMaxSize {
		// switch to new segment
		if err := engine.segment.close(); err != nil {
			return err
		}

		if err = engine.snapshot(); err != nil {
			return err
		}

		newDataSegment, err := newDataSegment()

		if err != nil {
			return err
		}

		engine.segment = newDataSegment
		engine.segments = append(engine.segments, newDataSegment.id)

		fmt.Println("switched to new segment")
	}
	return nil
}

func (engine *Engine) addLogEntryIndex(key string, logEntryIndex *core.LogEntryIndex) {
	_, ok := engine.logEntryIndexesBySegmentID[engine.segment.id]

	if !ok {
		engine.logEntryIndexesBySegmentID[engine.segment.id] = make(logEntryIndexByKey)
	}

	engine.logEntryIndexesBySegmentID[engine.segment.id][key] = logEntryIndex
}

func (engine *Engine) Set(key, value string) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	err := engine.checkDataSegment()

	if err != nil {
		return err
	}

	logEntry := core.NewLogEntry(key, value)
	logEntryIndex, err := engine.segment.addLogEntry(logEntry)

	if err != nil {
		return nil
	}

	engine.addLogEntryIndex(key, logEntryIndex)

	return nil
}

func (engine *Engine) loadSegment(segmentID string) (*dataSegment, error) {
	var segment *dataSegment
	cacheHit, ok := engine.lruSegments.Get(segmentID)

	if !ok {
		loadedSegment, err := loadDataSegment(segmentID)
		if err != nil {
			return nil, err
		}

		segment = loadedSegment
		engine.lruSegments.Add(segmentID, segment)
	} else {
		segment = cacheHit.(*dataSegment)
	}

	return segment, nil
}

func (engine *Engine) findLogEntryByKey(key string) (*core.LogEntry, error) {
	var segment *dataSegment
	cursor := len(engine.segments) - 1

	for cursor >= 0 {
		segmentID := engine.segments[cursor]
		logEntryIndexesByKey, _ := engine.logEntryIndexesBySegmentID[segmentID]
		logEntryIndex, ok := logEntryIndexesByKey[key]
		cursor--

		if !ok {
			continue
		}

		if segmentID != engine.segment.id {
			var err error
			segment, err = engine.loadSegment(segmentID)

			if err != nil {
				return nil, err
			}
		} else {
			segment = engine.segment
		}

		return segment.getLogEntry(logEntryIndex)
	}

	return nil, ErrKeyNotFound
}

func (engine *Engine) Get(key string) (string, error) {
	engine.mu.RLock()
	defer engine.mu.RUnlock()

	logEntry, err := engine.findLogEntryByKey(key)
	if err != nil {
		return "", err
	}

	if logEntry.IsDeleted {
		return "", ErrKeyNotFound
	}

	return logEntry.Value, nil
}

func (engine *Engine) Delete(key string) error {
	engine.checkDataSegment()
	logEntry, err := engine.findLogEntryByKey(key)

	if err != nil {
		return err
	}

	logEntry.IsDeleted = true
	logEntry.Value = ""
	logEntryIndex, err := engine.segment.addLogEntry(logEntry)

	if err != nil {
		return err
	}

	engine.addLogEntryIndex(key, logEntryIndex)

	return nil
}

func (engine *Engine) Close() error {
	err := engine.segment.close()
	if err != nil {
		return err
	}

	return nil
}

func (engine *Engine) snapshot() error {
	snapshotEntry, err := newSnapshotEntry(engine.logEntryIndexesBySegmentID)
	if err != nil {
		return err
	}

	file, err := os.Create(snapshotEntry.ComputeFilename())
	if err != nil {
		return err
	}

	snapshotEntryBytes, err := snapshotEntry.Encode()
	if err != nil {
		return err
	}

	if _, err := file.Write(snapshotEntryBytes); err != nil {
		return err
	}

	if err = file.Close(); err != nil {
		return err
	}

	return nil
}

func (engine *Engine) loadSnapshot(fileName string) error {
	snapshotBytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}

	snapshot := new(SnapshotEntry)
	err = snapshot.Decode(snapshotBytes)

	if err != nil {
		return err
	}

	err = msgpack.Unmarshal(snapshot.Snapshot, &engine.logEntryIndexesBySegmentID)
	if err != nil {
		return err
	}

	for segmentID := range engine.logEntryIndexesBySegmentID {
		engine.segments = append(engine.segments, segmentID)
	}

	return nil
}

func (engine *Engine) isRecoverable() (bool, error) {
	files, err := ioutil.ReadDir(getSnapshotsPath())
	if err != nil {
		return false, err
	}

	for _, file := range files {
		if strings.Contains(file.Name(), ".snapshot") {
			return true, nil
		}
	}

	return false, nil
}

func (engine *Engine) compactSegments() error {
	files, err := ioutil.ReadDir(getSegmentsPath())
	if err != nil {
		return err
	}

	for _, file := range files {
		segmentID := file.Name()
		segmentLogEntries, ok := engine.logEntryIndexesBySegmentID[segmentID]

		if !ok {
			continue
		}

		var entriesByKey map[string]*core.LogEntry
		for _, logEntries := range segmentLogEntries {

		}
	}

	return nil
}

func (engine *Engine) compactSnapshots() error {

	return nil
}

func (engine *Engine) startCompactor(ctx context.Context) error {
	errChan := make(chan error, 1)

	// segment compactor
	go func() {
		for {
			select {
			case <-time.Tick(engine.compactorInterval):
				engine.mu.Lock()
				if err := engine.compactSegments(); err != nil {
					errChan <- err
				}
				engine.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()

	// snapshot compactor
	go func() {
		for {
			select {
			case <-time.Tick(engine.compactorInterval):
				if err := engine.compactSnapshots(); err != nil {
					errChan <- err
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case err := <-errChan:
		panic(fmt.Sprintf("compactor error %s", err))
	case <-ctx.Done():
		return nil
	}
}

func (engine *Engine) recover() error {
	files, err := ioutil.ReadDir(getSnapshotsPath())
	if err != nil {
		return err
	}

	latestSnapshotFilename := ""

	for _, file := range files {
		if latestSnapshotFilename == "" {
			latestSnapshotFilename = file.Name()
			continue
		}

		fileTimeStamp, err := strconv.Atoi(strings.Split(file.Name(), "-")[0])
		if err != nil {
			return err
		}

		latestSnapshotTimestamp, err := strconv.Atoi(strings.Split(latestSnapshotFilename, "-")[0])
		if err != nil {
			return err
		}

		if fileTimeStamp >= latestSnapshotTimestamp {
			latestSnapshotFilename = file.Name()
		}
	}

	err = engine.loadSnapshot(path.Join(getSnapshotsPath(), latestSnapshotFilename))
	if err != nil {
		return err
	}

	return nil
}

func NewEngine(config *EngineConfig) (*Engine, error) {
	if _, err := os.Stat(getDataPath()); os.IsNotExist(err) {
		err = os.MkdirAll(getDataPath(), 0644)
		if err != nil {
			return nil, err
		}
	}

	if _, err := os.Stat(getSegmentsPath()); os.IsNotExist(err) {
		err = os.MkdirAll(getSegmentsPath(), 0644)
		if err != nil {
			return nil, err
		}
	}

	if _, err := os.Stat(getSnapshotsPath()); os.IsNotExist(err) {
		err = os.MkdirAll(getSnapshotsPath(), 0644)
		if err != nil {
			return nil, err
		}
	}

	segment, err := newDataSegment()
	if err != nil {
		return nil, err
	}

	lruCache, err := lru.New(config.CacheSize)
	if err != nil {
		return nil, err
	}

	engine := &Engine{
		logEntryIndexesBySegmentID: make(map[string]logEntryIndexByKey),
		segmentMaxSize:             config.SegmentMaxSize,
		mu:                         new(sync.RWMutex),
		segment:                    segment,
		lruSegments:                lruCache,
		segments:                   []string{segment.id},
		ctx:                        context.Background(),
		compactorInterval:          config.CompactorInterval,
	}

	recoverable, err := engine.isRecoverable()
	if err != nil {
		return nil, err
	}

	if recoverable {
		fmt.Println("recovering database")
		if err = engine.recover(); err != nil {
			return nil, err
		}
		fmt.Println("recovered database")
	}

	go engine.captureSnapshots(engine.ctx, config.SnapshotInterval, config.TolerableSnapshotFailCount)

	return engine, nil
}
