package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	EngineCaptureSnapshotDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_capture_snapshot_duration_nanoseconds",
		Help: "how long it takes to capture a snapshot",
	}, []string{"log_entry_indexes_length", "snapshot_entry_size", "compressed_snapshot_entry_size"})

	EngineCaptureSnapshotDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_capture_snapshot_duration_milliseconds",
		Help: "how long it takes to capture a snapshot",
	}, []string{"log_entry_indexes_length", "snapshot_entry_size", "compressed_snapshot_entry_size"})

	EngineLoadSnapshotDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_load_snapshot_duration_nanoseconds",
		Help: "how long it takes to load a snapshot",
	}, []string{"log_entry_indexes_length", "snapshot_entry_size", "compressed_snapshot_entry_size"})

	EngineLoadSnapshotDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_load_snapshot_duration_nanoseconds",
		Help: "how long it takes to load a snapshot",
	}, []string{"log_entry_indexes_length", "snapshot_entry_size", "compressed_snapshot_entry_size"})

	EngineRolloverSegmentDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_rollover_segment_duration_nanoseconds",
		Help: "how long it takes to switch to a new data segment when current gets full",
	}, []string{"segment_id", "segment_entries_count"})

	EngineRolloverSegmentDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_rollover_segment_duration_milliseconds",
		Help: "how long it takes to switch to a new data segment when current gets full",
	}, []string{"segment_id", "segment_entries_count"})

	EngineSetDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_set_duration_nanoseconds",
		Help: "how long it takes to set a key value pair",
	}, []string{"segment_id", "segment_entries_count", "key", "value_size"})

	EngineSetDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_set_duration_milliseconds",
		Help: "how long it takes to set a key value pair",
	}, []string{"segment_id", "segment_entries_count", "key", "value_size"})

	EngineLoadDataSegmentDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_load_data_segment_duration_nanoseconds",
		Help: "how long it takes to load a data segment from disk",
	}, []string{"segment_id", "segment_entries_count", "cache_hit"})

	EngineLoadDataSegmentDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_load_data_segment_duration_milliseconds",
		Help: "how long it takes to load a data segment from disk",
	}, []string{"segment_id", "segment_size", "cache_hit"})

	EngineFindLogEntryByKeyDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_find_log_entry_by_key_duration_nanoseconds",
		Help: "how long it took to find a log entry belonging to a key",
	}, []string{"key", "found", "searched_segments_count"})

	EngineFindLogEntryByKeyDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_find_log_entry_by_key_duration_milliseconds",
		Help: "how long it took to find a log entry belonging to a key",
	}, []string{"key", "found", "searched_segments_count"})

	EngineGetDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_get_duration_nanoseconds",
		Help: "how long it takes to get a key's value",
	}, []string{"key", "found", "deleted"})

	EngineGetDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_get_duration_milliseconds",
		Help: "how long it takes to get a key's value",
	}, []string{"key", "found", "deleted"})

	EngineDeleteDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_delete_duration_nanoseconds",
		Help: "how long it takes to delete a key's value",
	}, []string{"key", "found"})

	EngineDeleteDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_delete_duration_milliseconds",
		Help: "how long it takes to delete a key's value",
	}, []string{"key", "found"})

	EngineCloseDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_close_duration_nanoseconds",
		Help: "how long it takes to close the engine",
	}, []string{"segment_id", "segment_entries_count", "historical_segment_ids_length"})

	EngineCloseDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_close_duration_milliseconds",
		Help: "how long it takes to close the engine",
	}, []string{"segment_id", "segment_entries_count", "historical_segment_ids_length"})

	EngineCompactSegmentsDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_compact_segments_duration_nanoseconds",
		Help: "how long it takes to run a segment compaction job",
	}, []string{"files_to_compact", "segments_to_delete", "workers_count", "segments_max_size"})

	EngineCompactSegmentsDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_compact_segments_duration_nanoseconds",
		Help: "how long it takes to run a segment compaction job",
	}, []string{"files_to_compact", "segments_to_delete", "workers_count", "segments_max_size"})

	EngineCompactSnapshotsDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_compact_segments_duration_nanoseconds",
		Help: "how long it takes to run a snapshot compaction job",
	}, []string{"files_to_compact", "deleted_count"})

	EngineCompactSnapshotsDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_compact_segments_duration_nanoseconds",
		Help: "how long it takes to run a snapshot compaction job",
	}, []string{"files_to_compact", "deleted_count"})

	EngineSegmentsCompactionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "engine_segments_compaction_count",
		Help: "how many times the segments compaction job has run",
	})

	EngineSnapshotsCompactionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "engine_snapshots_compaction_count",
		Help: "how many times the snapshots compaction job has run",
	})
)

// ErrKeyNotFound occurs when a key is not found in the data store
var ErrKeyNotFound = errors.New("key not found")

// logEntryIndexByKey map that holds log entries by the associated key
type logEntryIndexByKey map[string]*LogEntryIndex

// Engine thread safe storage engine that uses the hash index strategy for keeping track
// where data is located on disk
type Engine struct {
	logEntryIndexesBySegmentID map[string]logEntryIndexByKey // map that holds log entry indexes by segment id
	segmentsMetadataList       *SegmentMetadataList          // historical list of segments id's
	lruSegments                *lru.Cache                    // cache that holds the most recently used data segments
	segment                    *dataSegment                  // current data segment
	segmentMaxSize             int                           // max size of entries stored in a data segment
	compactorInterval          time.Duration                 // intervals that compaction process occurs
	mu                         *sync.RWMutex                 // mutex that synchronizes access to properties
	compactorWorkerCount       int                           // number of workers compaction process uses
	snapshotTTLDuration        time.Duration                 // snapshot files time to live duration
	isCompactingSegments       bool                          // flag that ensures only one segments compaction process is running at a time
	isCompactingSnapshots      bool                          // flag that ensures only one snapshots compaction process is running at a time

	logger        log.FieldLogger
	ctx           context.Context
	ctxCancelFunc context.CancelFunc
}

// Store instance of a storage engine
type Store interface {
	Get(string) (string, error)
	Set(string, string) error
	Delete(string) error
	Close() error
}

// EngineConfig configuration properties utilized when initializing an engine
type EngineConfig struct {
	SegmentMaxSize             int           // max size of entries stored in a data segment
	SnapshotInterval           time.Duration // intervals that snapshots are captured
	TolerableSnapshotFailCount int           // max number of acceptable failures during the snapshotting
	CacheSize                  int           // max number of data segments to hold in memory
	CompactorInterval          time.Duration // intervals that compaction process occurs
	CompactorWorkerCount       int           // number of workers compaction process uses
	SnapshotTTLDuration        time.Duration // snapshot files time to live duration
	DataPath                   string        // path where store data is persisted on disk
}

// captureSnapshots captures snapshots at an interval
func (engine *Engine) captureSnapshots(ctx context.Context, interval time.Duration, tolerableFailCount int) {
	engine.logger.Debugf("starting snapshots taker process with interval %v and tolerableFailCount %d", interval, tolerableFailCount)

	failCounts := 0
	ticker := time.NewTicker(interval)

	for {
		if failCounts >= tolerableFailCount {
			panic("snapshotting failure")
		}

		select {
		case <-ticker.C:
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

// checkDataSegment verifies data segments and performs handoff between old and
// new data segments
// this method is expected to be used within a writable thread safe method
func (engine *Engine) checkDataSegment() error {
	if engine.segment.entriesCount >= engine.segmentMaxSize {
		start := time.Now()
		oldSegment := engine.segment

		defer func() {
			EngineRolloverSegmentDurationNanoseconds.WithLabelValues(
				oldSegment.id,
				fmt.Sprint(oldSegment.entriesCount),
			).Observe(
				float64(time.Since(start).Nanoseconds()),
			)
			EngineRolloverSegmentDurationMilliseconds.WithLabelValues(
				oldSegment.id,
				fmt.Sprint(oldSegment.entriesCount),
			).Observe(
				float64(time.Since(start).Milliseconds()),
			)
		}()

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
		engine.segmentsMetadataList.Add(newDataSegment.id)

		engine.logger.Debugf("switched to new data segment with id %s", newDataSegment.id)
	}
	return nil
}

// addLogEntryIndex stores log entry index in memory
func (engine *Engine) addLogEntryIndex(key string, logEntryIndex *LogEntryIndex) {
	engine.logger.Debugf("adding log entry index for key %s", key)

	_, ok := engine.logEntryIndexesBySegmentID[engine.segment.id]

	if !ok {
		engine.logEntryIndexesBySegmentID[engine.segment.id] = make(logEntryIndexByKey)
	}

	engine.logEntryIndexesBySegmentID[engine.segment.id][key] = logEntryIndex
}

// Set stores a key and it's associated value
func (engine *Engine) Set(key, value string) error {
	start := time.Now()
	defer func() {
		EngineSetDurationNanoseconds.WithLabelValues(
			engine.segment.id,
			fmt.Sprint(engine.segment.entriesCount),
			key,
			fmt.Sprint(len(value)),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)
		EngineSetDurationMilliseconds.WithLabelValues(
			engine.segment.id,
			fmt.Sprint(engine.segment.entriesCount),
			key,
			fmt.Sprint(len(value)),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	engine.mu.Lock()
	defer engine.mu.Unlock()

	engine.logger.Debugf("setting key %s and value %s", key, value)

	err := engine.checkDataSegment()

	if err != nil {
		return err
	}

	logEntry := NewLogEntry(key, value)
	logEntryIndex, err := engine.segment.addLogEntry(logEntry)

	if err != nil {
		return nil
	}

	engine.addLogEntryIndex(key, logEntryIndex)

	return nil
}

// loadSegment loads a data segment attempting to hit the cache first
func (engine *Engine) loadSegment(segmentID string) (*dataSegment, error) {
	start := time.Now()
	var segment *dataSegment
	var stat fs.FileInfo
	var ok bool

	defer func() {
		EngineLoadDataSegmentDurationNanoseconds.WithLabelValues(
			segmentID,
			fmt.Sprint(stat.Size()),
			strings.ToLower(strconv.FormatBool(ok)),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)
		EngineLoadDataSegmentDurationMilliseconds.WithLabelValues(
			segmentID,
			fmt.Sprint(stat.Size()),
			strings.ToLower(strconv.FormatBool(ok)),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	cacheHit, ok := engine.lruSegments.Get(segmentID)

	if !ok {
		loadedSegment, err := loadDataSegment(segmentID)
		if err != nil {
			return nil, err
		}

		segment = loadedSegment
		engine.lruSegments.Add(segmentID, segment)

		engine.logger.Debugf("loaded data segment %s from disk and added to cache", segmentID)
	} else {
		segment = cacheHit.(*dataSegment)

		engine.logger.Debugf("loaded data segment %s from cache", segmentID)
	}

	stat, err := segment.file.Stat()
	if err != nil {
		return nil, err
	}

	return segment, nil
}

// findLogEntryByKey locates the log entry of provided key by locating
// the latest data segment containing that key
func (engine *Engine) findLogEntryByKey(key string) (*LogEntry, error) {
	start := time.Now()
	engine.logger.Debugf("searching log entry for key %s", key)

	var segment *dataSegment
	segments := engine.segmentsMetadataList.GetSegmentIDs()
	cursor := len(segments) - 1

	defer func() {
		searchedSegmentsCount := 0
		found := false

		if segment != nil {
			searchedSegmentsCount = (len(segments) - cursor) + 1
			found = true
		}

		EngineFindLogEntryByKeyDurationNanoseconds.WithLabelValues(
			key,
			strings.ToLower(strconv.FormatBool(found)),
			fmt.Sprint(searchedSegmentsCount),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)

		EngineFindLogEntryByKeyDurationMilliseconds.WithLabelValues(
			key,
			strings.ToLower(strconv.FormatBool(found)),
			fmt.Sprint(searchedSegmentsCount),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)
	}()

	for cursor >= 0 {
		segmentID := segments[cursor]
		logEntryIndexesByKey, logEntryIndexExists := engine.logEntryIndexesBySegmentID[segmentID]

		cursor--

		if !logEntryIndexExists {
			continue
		}

		logEntryIndex, logEntryExist := logEntryIndexesByKey[key]

		if !logEntryExist {
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

// Get retrieves stored value for associated key
func (engine *Engine) Get(key string) (string, error) {
	engine.logger.Debugf("getting key %s", key)

	start := time.Now()
	var logEntry *LogEntry
	var err error

	defer func() {
		EngineGetDurationNanoseconds.WithLabelValues(
			key,
			strings.ToLower(
				strconv.FormatBool(logEntry != nil),
			),
			strings.ToLower(
				strconv.FormatBool(logEntry != nil && logEntry.IsDeleted),
			),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)

		EngineGetDurationMilliseconds.WithLabelValues(
			key,
			strings.ToLower(
				strconv.FormatBool(logEntry != nil),
			),
			strings.ToLower(
				strconv.FormatBool(logEntry != nil && logEntry.IsDeleted),
			),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	logEntry, err = engine.findLogEntryByKey(key)
	if err != nil {
		return "", err
	}

	if logEntry.IsDeleted {
		return "", ErrKeyNotFound
	}

	return logEntry.Value, nil
}

// Delete deletes a key by appending a tombstone log entry to the latest data
// segment
func (engine *Engine) Delete(key string) error {
	engine.logger.Debugf("deleting key %s", key)
	var logEntry *LogEntry
	var err error
	start := time.Now()

	defer func() {
		EngineDeleteDurationNanoseconds.WithLabelValues(
			key,
			strings.ToLower(
				strconv.FormatBool(err == ErrKeyNotFound),
			),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)

		EngineDeleteDurationMilliseconds.WithLabelValues(
			key,
			strings.ToLower(
				strconv.FormatBool(err == ErrKeyNotFound),
			),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	engine.mu.Lock()
	defer engine.mu.Unlock()

	engine.checkDataSegment()
	logEntry, err = engine.findLogEntryByKey(key)

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

// Close closes the storage engine
func (engine *Engine) Close() error {
	start := time.Now()
	defer func() {
		EngineCloseDurationNanoseconds.WithLabelValues(
			engine.segment.id,
			fmt.Sprint(engine.segment.entriesCount),
			fmt.Sprint(len(engine.segmentsMetadataList.GetSegmentIDs())),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)

		EngineCloseDurationMilliseconds.WithLabelValues(
			engine.segment.id,
			fmt.Sprint(engine.segment.entriesCount),
			fmt.Sprint(len(engine.segmentsMetadataList.GetSegmentIDs())),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	engine.logger.Debug("closing database")
	engine.ctxCancelFunc()

	if err := engine.snapshot(); err != nil {
		return err
	}

	if err := engine.segment.close(); err != nil {
		return err
	}

	return nil
}

// func (engine *Engine) compressSnapshot

// snapshot writes a snapshot of log entry indexes by segment id to disk
func (engine *Engine) snapshot() error {
	engine.logger.Debug("snapshotting database state")
	start := time.Now()
	var snapshotEntryBytes []byte
	var compressedSnapshotEntryBytes []byte

	defer func() {
		EngineCaptureSnapshotDurationNanoseconds.WithLabelValues(
			fmt.Sprint(len(engine.logEntryIndexesBySegmentID)),
			fmt.Sprint(len(snapshotEntryBytes)),
			fmt.Sprint(len(compressedSnapshotEntryBytes)),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)
		EngineCaptureSnapshotDurationMilliseconds.WithLabelValues(
			fmt.Sprint(len(engine.logEntryIndexesBySegmentID)),
			fmt.Sprint(len(snapshotEntryBytes)),
			fmt.Sprint(len(compressedSnapshotEntryBytes)),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	snapshotEntry, err := newSnapshotEntry(engine.logEntryIndexesBySegmentID)
	if err != nil {
		return err
	}

	file, err := os.Create(snapshotEntry.ComputeFilename())
	if err != nil {
		return err
	}

	snapshotEntryBytes, err = snapshotEntry.Encode()
	if err != nil {
		return err
	}

	compressedSnapshotEntryBytes, err = compressBytes(snapshotEntryBytes)
	if err != nil {
		return err
	}

	if _, err := file.Write(compressedSnapshotEntryBytes); err != nil {
		return err
	}

	if err = file.Close(); err != nil {
		return err
	}

	return nil
}

// loadSnapshot loads snapshot from disk to memory
func (engine *Engine) loadSnapshot(fileName string) error {
	engine.logger.Debugf("loading snapshot %s", fileName)
	start := time.Now()
	var snapshotBytes []byte
	var compressedSnapshotEntryBytes []byte

	defer func() {
		EngineLoadSnapshotDurationNanoseconds.WithLabelValues(
			fmt.Sprint(len(engine.logEntryIndexesBySegmentID)),
			fmt.Sprint(len(snapshotBytes)),
			fmt.Sprint(len(compressedSnapshotEntryBytes)),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)
		EngineLoadSnapshotDurationMilliseconds.WithLabelValues(
			fmt.Sprint(len(engine.logEntryIndexesBySegmentID)),
			fmt.Sprint(len(snapshotBytes)),
			fmt.Sprint(len(compressedSnapshotEntryBytes)),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	compressedSnapshotEntryBytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}

	snapshotBytes, err = uncompressBytes(compressedSnapshotEntryBytes)
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
		engine.segmentsMetadataList.Add(segmentID)
	}

	return nil
}

// isRecoverable checkes if storage engine contains existing data of which's logn
// entry indexes can be recovered
func (engine *Engine) isRecoverable() (bool, error) {
	engine.logger.Debug("checking if databasae is reecoverable")

	files, err := ioutil.ReadDir(getSnapshotsPath())
	if err != nil {
		return false, err
	}

	if _, err := ioutil.ReadDir(getSegmentsPath()); err != nil {
		return false, err
	}

	for _, file := range files {
		if strings.Contains(file.Name(), ".snapshot") {
			return true, nil
		}
	}

	return false, nil
}

type compactedSegmentEntriesContext struct {
	compactedEntries map[string]*LogEntry // key to log entry
	timestamp        int64
}

type jobContext struct {
	timestamp       int64
	segmentID       string
	logEntriesBytes [][]byte
}

type segmentContext struct {
	fileName string
	id       string
}

type segmentJobContext struct {
	ctx                         context.Context
	mu                          *sync.Mutex
	wg                          *sync.WaitGroup
	jobChan                     chan *jobContext
	compactedSegmentEntriesChan chan compactedSegmentEntriesContext
}

func (engine Engine) startCompactionWorkers(s *segmentJobContext) {
	for i := 1; i <= engine.compactorWorkerCount; i++ {
		engine.logger.Debugf("starting compaction worker %d", i)
		go engine.processSegmentJobs(i, s)
	}
}

func (engine Engine) processSegmentJobs(workerId int, s *segmentJobContext) {
	for {
		select {
		case <-s.ctx.Done():
			return

		case jobData := <-s.jobChan:
			engine.logger.Debugf("worker %d received job for segment %s containing %d log entries", workerId, jobData.segmentID, len(jobData.logEntriesBytes))

			latestLogEntries := make(map[string]*LogEntry)

			for _, compresedLogEntryBytes := range jobData.logEntriesBytes {
				if len(compresedLogEntryBytes) <= 0 {
					continue
				}

				logEntryBytes, err := uncompressBytes(compresedLogEntryBytes)
				if err != nil {
					continue
				}

				logEntry := &LogEntry{}
				err = logEntry.Decode(logEntryBytes)

				if err != nil {
					engine.logger.Errorf("unable to decode log entry %s", logEntry.Key)
					continue
				}

				latestLogEntries[logEntry.Key] = logEntry
			}

			s.mu.Lock()
			s.compactedSegmentEntriesChan <- compactedSegmentEntriesContext{
				compactedEntries: latestLogEntries,
				timestamp:        jobData.timestamp,
			}
			s.mu.Unlock()
			s.wg.Done()
		}
	}
}

func (engine Engine) dispatcSegmentJobs(
	files []fs.FileInfo,
	wg *sync.WaitGroup,
	jobChan chan *jobContext,
) ([]segmentContext, error) {
	segmentsToDelete := make([]segmentContext, 0)

	for _, file := range files {
		if !strings.Contains(file.Name(), ".segment") || engine.segment.fileName == file.Name() {
			continue
		}

		segmentID := strings.Split(file.Name(), ".")[0]
		segmentContentBytes, err := ioutil.ReadFile(path.Join(getSegmentsPath(), file.Name()))

		if err != nil {
			return []segmentContext{}, err
		}

		wg.Add(1)

		logEntriesBytes := bytes.Split(segmentContentBytes, LogEntrySeperator)
		jobChan <- &jobContext{
			timestamp:       file.ModTime().Unix(),
			logEntriesBytes: logEntriesBytes,
			segmentID:       segmentID,
		}
		segmentsToDelete = append(segmentsToDelete, segmentContext{
			fileName: file.Name(),
			id:       segmentID,
		})
	}

	return segmentsToDelete, nil
}

func (engine Engine) persistCompectedSegment(compactedLogEntries map[string]*LogEntry, compactedSegment *dataSegment) error {
	for _, logEntry := range compactedLogEntries {
		engine.logger.Debugf("compacting log entry %s", logEntry.Key)

		logEntryIndex, err := compactedSegment.addLogEntry(logEntry)
		if err != nil {
			return err
		}

		engine.addLogEntryIndex(logEntry.Key, logEntryIndex)
	}

	if err := compactedSegment.close(); err != nil {
		return err
	}

	return nil
}

func (engine Engine) cleanUpStaleSegments(segmentsToDelete []segmentContext, compactedLogEntries map[string]*LogEntry) error {
	for _, segmentCtx := range segmentsToDelete {
		engine.lruSegments.Remove(segmentCtx.id)
		delete(engine.logEntryIndexesBySegmentID, segmentCtx.id)
		err := engine.segmentsMetadataList.Remove(segmentCtx.id)
		if err != nil {
			return err
		}
		os.Remove(path.Join(getSegmentsPath(), segmentCtx.fileName))
	}

	return nil
}

// compactSegments compacts data segments by joining closed segments together
// and getting rid of duplicaate log engtries by keys
func (engine *Engine) compactSegments() error {
	engine.logger.Debug("compacting segments")
	start := time.Now()
	segmentsToDelete := make([]segmentContext, 0)
	files := make([]fs.FileInfo, 0)

	defer func() {
		EngineCompactSegmentsDurationNanoseconds.WithLabelValues(
			fmt.Sprint(len(files)),
			fmt.Sprint(len(segmentsToDelete)),
			fmt.Sprint(engine.compactorWorkerCount),
			fmt.Sprint(engine.segmentMaxSize),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)

		EngineCompactSegmentsDurationMilliseconds.WithLabelValues(
			fmt.Sprint(len(files)),
			fmt.Sprint(len(segmentsToDelete)),
			fmt.Sprint(engine.compactorWorkerCount),
			fmt.Sprint(engine.segmentMaxSize),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	files, err := ioutil.ReadDir(getSegmentsPath())
	if err != nil {
		return err
	}

	compactedSegmentEntriesChan := make(chan compactedSegmentEntriesContext, len(files))
	compactedSegmentEntries := make([]compactedSegmentEntriesContext, len(files))
	compactedLogEntries := make(map[string]*LogEntry)
	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)
	jobChan := make(chan *jobContext, 10)
	jobCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start compaction workers
	engine.startCompactionWorkers(&segmentJobContext{
		ctx:                         jobCtx,
		mu:                          mu,
		wg:                          wg,
		jobChan:                     jobChan,
		compactedSegmentEntriesChan: compactedSegmentEntriesChan,
	})

	// send compaction jobs to workers through shared channel
	segmentsToDelete, err = engine.dispatcSegmentJobs(
		files,
		wg,
		jobChan,
	)
	if err != nil {
		return err
	}

	wg.Wait()
	close(compactedSegmentEntriesChan)

	for compactedSegmentEntry := range compactedSegmentEntriesChan {
		compactedSegmentEntries = append(compactedSegmentEntries, compactedSegmentEntry)
	}

	// sorts compacted segment entries by timestamp in order to have the latest
	// keys updated last
	sort.Slice(compactedSegmentEntries, func(a, b int) bool {
		return compactedSegmentEntries[a].timestamp < compactedSegmentEntries[b].timestamp
	})

	for _, compactedSegmentEntry := range compactedSegmentEntries {
		engine.logger.Debugf("processing compacted segment entry containing %d log entries", len(compactedSegmentEntry.compactedEntries))
		for key, logEntry := range compactedSegmentEntry.compactedEntries {
			compactedLogEntries[key] = logEntry
		}
	}

	compactedSegment, err := newDataSegment()
	if err != nil {
		return err
	}

	// writes log entries to to compacted segment and index it in memory
	engine.persistCompectedSegment(
		compactedLogEntries,
		compactedSegment,
	)
	engine.segmentsMetadataList.Add(compactedSegment.id)

	engine.logger.Debugf("processed %d segments", len(compactedSegmentEntries))
	engine.logger.Debugf("wrote %d compacted log entries", len(compactedLogEntries))

	// clean up segment references from memory
	engine.cleanUpStaleSegments(segmentsToDelete, compactedLogEntries)
	engine.logger.Debugf("cleaned up %d segments", len(segmentsToDelete))

	return nil
}

// compactSnapshots compacts snapshots
func (engine *Engine) compactSnapshots() error {
	engine.logger.Debug("compacting snapshots")

	var files []fs.FileInfo
	now := time.Now()
	deletedCount := 0

	defer func() {
		EngineCompactSnapshotsDurationNanoseconds.WithLabelValues(
			fmt.Sprintln(len(files)),
			fmt.Sprint(deletedCount),
		).Observe(
			float64(time.Since(now).Nanoseconds()),
		)

		EngineCompactSnapshotsDurationMilliseconds.WithLabelValues(
			fmt.Sprintln(len(files)),
			fmt.Sprint(deletedCount),
		).Observe(
			float64(time.Since(now).Milliseconds()),
		)
	}()

	files, err := ioutil.ReadDir(getSnapshotsPath())
	if err != nil {
		return err
	}

	for _, file := range files {
		if now.Sub(file.ModTime()) > engine.snapshotTTLDuration {
			deletedCount++
			os.Remove(path.Join(getSnapshotsPath(), file.Name()))
		}
	}

	engine.logger.Debugf("deleted %d snapshots \n", deletedCount)

	return nil
}

// startCompactor start segment and snspshot compaaction go routines
func (engine *Engine) startCompactors(ctx context.Context) error {
	engine.logger.Debug("starting compactor processes")

	ticker := time.NewTicker(engine.compactorInterval)
	errChan := make(chan error, 1)

	// segment compactor
	go func() {
		for {
			select {
			case <-ctx.Done():
				engine.logger.Debug("stopping segments compactor process")
				return

			case <-ticker.C:
				engine.mu.Lock()
				if !engine.isCompactingSegments && engine.lruSegments.Len() > 1 {
					engine.isCompactingSegments = true

					if err := engine.compactSegments(); err != nil {
						errChan <- err
					}

					engine.isCompactingSegments = false
				}

				EngineSegmentsCompactionCount.Inc()

				engine.mu.Unlock()
			}
		}
	}()

	// snapshot compactor
	go func() {
		for {
			select {
			case <-ctx.Done():
				engine.logger.Debug("stopping snapshots compactor process")
				return

			case <-ticker.C:
				if !engine.isCompactingSnapshots {
					engine.isCompactingSnapshots = true

					if err := engine.compactSnapshots(); err != nil {
						errChan <- err
					}

					engine.isCompactingSnapshots = false

					EngineSnapshotsCompactionCount.Inc()
				}
			}
		}
	}()

	select {
	case <-ctx.Done():
		return nil

	case err := <-errChan:
		panic(fmt.Sprintf("compactor error %s", err))
	}
}

// recover recovers database indexes from snapshots
func (engine *Engine) recover() error {
	engine.logger.Debug("recovering database state from snapshots")

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

//NewEngine creates a new engine
func NewEngine(config *EngineConfig) (*Engine, error) {
	setDataPath(config.DataPath)

	for _, dataPath := range []string{getDataPath(), getSegmentsPath(), getSnapshotsPath()} {
		if _, err := os.Stat(dataPath); os.IsNotExist(err) {
			err = os.MkdirAll(dataPath, 0777)
			if err != nil {
				return nil, err
			}
		}
	}

	segmentMetadataList := NewSegmentMetadataList()

	segment, err := newDataSegment()
	if err != nil {
		return nil, err
	}

	lruCache, err := lru.New(config.CacheSize)
	if err != nil {
		return nil, err
	}

	segmentMetadataList.Add(segment.id)

	ctx, cancel := context.WithCancel(context.Background())
	engine := &Engine{
		logEntryIndexesBySegmentID: make(map[string]logEntryIndexByKey),
		segmentMaxSize:             config.SegmentMaxSize,
		mu:                         new(sync.RWMutex),
		segment:                    segment,
		lruSegments:                lruCache,
		segmentsMetadataList:       segmentMetadataList,
		ctx:                        ctx,
		compactorInterval:          config.CompactorInterval,
		compactorWorkerCount:       config.CompactorWorkerCount,
		snapshotTTLDuration:        config.SnapshotTTLDuration,
		isCompactingSegments:       false,
		isCompactingSnapshots:      false,
		logger:                     log.WithField("storage_engine", "hash_index"),
		ctxCancelFunc:              cancel,
	}

	engine.logger.Info("attempting to recover database")

	recoverable, err := engine.isRecoverable()
	if err != nil {
		return nil, err
	}

	if recoverable {
		if err = engine.recover(); err != nil {
			return nil, err
		}

		engine.logger.Info("successfully recovered database")
	}

	go engine.captureSnapshots(engine.ctx, config.SnapshotInterval, config.TolerableSnapshotFailCount)
	go engine.startCompactors(engine.ctx)

	return engine, nil
}
