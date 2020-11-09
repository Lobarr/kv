package hashindex

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"storage-engines/core"
	"strconv"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/vmihailenco/msgpack/v5"
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
	compactorWorkerCount       int
}

type EngineConfig struct {
	SegmentMaxSize             int
	SnapshotInterval           time.Duration
	TolerableSnapshotFailCount int
	CacheSize                  int
	CompactorInterval          time.Duration
	CompactorWorkerCount       int
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

		if err := engine.snapshot(); err != nil {
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
	fmt.Println("starting segments compaction")

	files, err := ioutil.ReadDir(getSegmentsPath())
	if err != nil {
		return err
	}

	type compactedSegmentEntriesContext struct {
		compactedEntries map[string]*core.LogEntry // key to log entry
		timestamp        int64
	}

	type jobContext struct {
		timestamp       int64
		segmentID       string
		logEntriesBytes [][]byte
	}

	compactedSegmentEntries := make([]compactedSegmentEntriesContext, 0)
	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)
	jobChan := make(chan *jobContext)
	jobCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	segmentsToDelete := make([]string, 0)

	for i := 1; i <= engine.compactorWorkerCount; i++ {
		go func(ctx context.Context) {
			for {
				select {
				case jobData := <-jobChan:
					fmt.Println(fmt.Sprintf("received job for %s", jobData.segmentID))
					latestLogEntries := make(map[string]*core.LogEntry)

					for _, logEntryBytes := range jobData.logEntriesBytes {
						if len(logEntryBytes) <= 0 {
							continue
						}

						logEntry := &core.LogEntry{}
						err := logEntry.Decode(logEntryBytes)

						if err != nil {
							continue
						}

						latestLogEntries[logEntry.Key] = logEntry
					}

					mu.Lock()
					compactedSegmentEntries = append(compactedSegmentEntries, compactedSegmentEntriesContext{
						compactedEntries: latestLogEntries,
						timestamp:        jobData.timestamp,
					})
					mu.Unlock()

					fmt.Println(fmt.Sprintf("completed job for %s", jobData.segmentID))
					wg.Done()

				case <-ctx.Done():
					return
				}
			}
		}(jobCtx)
	}

	for _, file := range files {
		if !strings.Contains(file.Name(), ".segment") {
			continue
		}

		segmentID := strings.Split(file.Name(), ".")[0]
		segmentContentBytes, err := ioutil.ReadFile(path.Join(getSegmentsPath(), file.Name()))

		if err != nil {
			return err
		}

		logEntriesBytes := bytes.Split(segmentContentBytes, LogEntrySeperator)

		wg.Add(1)
		jobChan <- &jobContext{
			timestamp:       file.ModTime().Unix(),
			logEntriesBytes: logEntriesBytes,
			segmentID:       segmentID,
		}
		segmentsToDelete = append(segmentsToDelete, file.Name())
	}

	fmt.Println("waiting for jobs to complete")
	wg.Wait()
	fmt.Println("jobs completed")

	sort.Slice(compactedSegmentEntries, func(a, b int) bool {
		return compactedSegmentEntries[a].timestamp < compactedSegmentEntries[b].timestamp
	})

	compactedLogEntries := make(map[string]*core.LogEntry)

	for _, compactedSegmentEntry := range compactedSegmentEntries {
		for key, logEntry := range compactedSegmentEntry.compactedEntries {
			compactedLogEntries[key] = logEntry
		}
	}

	compactedSegment, err := newDataSegment()
	if err != nil {
		return err
	}

	fmt.Println(compactedLogEntries)

	for _, logEntry := range compactedLogEntries {
		logEntryIndex, err := compactedSegment.addLogEntry(logEntry)
		if err != nil {
			return err
		}

		engine.addLogEntryIndex(logEntry.Key, logEntryIndex)
	}

	engine.segments = append(engine.segments, compactedSegment.id)
	if err := compactedSegment.close(); err != nil {
		return err
	}

	for _, segmentName := range segmentsToDelete {
		os.Remove(path.Join(getSegmentsPath(), segmentName))
	}

	fmt.Println("completed segment compaction")

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
	for _, dataPath := range []string{getDataPath(), getSegmentsPath(), getSnapshotsPath()} {
		if _, err := os.Stat(dataPath); os.IsNotExist(err) {
			err = os.MkdirAll(dataPath, 0644)
			if err != nil {
				return nil, err
			}
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
		compactorWorkerCount:       config.CompactorWorkerCount,
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
	go engine.startCompactor(engine.ctx)

	return engine, nil
}
