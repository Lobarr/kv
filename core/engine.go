package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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
	"golang.org/x/sync/errgroup"
)

const (
	CaptureSnapshotOperation     = "capture_snapshot"
	LoadSnapshotOperation        = "load_snapshot"
	LoadDataSegment              = "load_data_segment"
	FindLogEntryOperation        = "find_log_entry"
	RolloverSegmentOperation     = "rollover_segment"
	SetOperation                 = "set"
	GetOperation                 = "get"
	DeleteOperation              = "delete"
	CompactSegmentsOperation     = "compact_segments"
	CompactSnapshotsOperation    = "compact_snpshots"
	CloseEngineOperation         = "close_engine"
	CheckRecoveryStatusOperation = "check_recovery_status"
	SnapCompactionOperation      = "compaction"
)

var (
	EngineOperationDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_operation_duration_ns",
		Help: "how long it takes to perform an engine operation in nanoseconds",
	}, []string{"operation"})

	EngineOperationDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "engine_operation_duration_ms",
		Help: "how long it takes to perform an engine operation in milliseconds",
	}, []string{"operation"})

	EngineSnapshotLogEntrySizes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "engine_snapshot_log_entry_sizes",
		Help: "size of snapshot entries in bytes",
	})

	EngineCompressedSnapshotLogEntrySizes = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "engine_snapshot_compressed_log_entry_sizes",
		Help: "size of compressed snapshot entries in bytes",
	})

	EngineCacheHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "engine_query_cache_hit_count",
		Help: "number of hits on the cache",
	})

	EngineDiskHits = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "engine_query_disk_hit_count",
		Help: "number of hits on the disk",
	})

	EngineDeletedKeysCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "engine_deleted_keys_count",
		Help: "nubmer of deleted keys",
	})

	EngineKeysCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "engine_keys_count",
		Help: "nubmer of keys in the storage engine",
	})

	EngineLogEntryIndexCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "engine_log_entry_index_count",
		Help: "number of log entry indexes",
	})

	EngineSearchedDataSegments = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "engine_query_searched_data_segments",
		Help: "number of data segments searched",
	})

	EngineQueryCount = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "engine_query_count",
		Help: "number of queries on the storage engine",
	}, []string{"status", "found", "deleted"})

	EngineFilesToCompact = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "engine_files_to_compact",
		Help: "number of files to compact",
	}, []string{"operation"})

	EngineSegmentsToDelete = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "engine_segments_to_delete",
		Help: "number of segments to delete",
	})

	EngineActiveWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "engine_active_workers",
	}, []string{"operation"})

	EngineSnapshotsCompactionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "engine_snapshot_compaction_count",
		Help: "how many times the snapshots compaction job has run",
	})

	EngineSegmentsCompactionCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "engine_data_segment_compaction_count",
		Help: "how many times the segments compaction job has run",
	})
)

func init() {
	prometheus.Register(EngineOperationDurationMilliseconds)
	prometheus.Register(EngineOperationDurationNanoseconds)
	prometheus.Register(EngineSnapshotLogEntrySizes)
	prometheus.Register(EngineCompressedSnapshotLogEntrySizes)
	prometheus.Register(EngineCacheHits)
	prometheus.Register(EngineDiskHits)
	prometheus.Register(EngineDeletedKeysCount)
	prometheus.Register(EngineKeysCount)
	prometheus.Register(EngineLogEntryIndexCount)
	prometheus.Register(EngineSearchedDataSegments)
	prometheus.Register(EngineQueryCount)
	prometheus.Register(EngineFilesToCompact)
	prometheus.Register(EngineSegmentsToDelete)
	prometheus.Register(EngineActiveWorkers)
	prometheus.Register(EngineSnapshotsCompactionCount)
	prometheus.Register(EngineSegmentsCompactionCount)
}

// ErrKeyNotFound occurs when a key is not found in the data store
var ErrKeyNotFound = errors.New("key not found")

// logEntryIndexByKey map that holds log entries by the associated key
type logEntryIndexByKey map[string]LogEntryIndex

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
			engine.mu.Lock()
			err := engine.snapshot()
			engine.mu.Unlock()

			if err != nil {
				fmt.Printf("error %s", err)
				failCounts++
			}
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
		var err error

		defer func() {
			EngineOperationDurationNanoseconds.WithLabelValues(RolloverSegmentOperation).Observe(
				float64(time.Since(start).Nanoseconds()))
			EngineOperationDurationMilliseconds.WithLabelValues(RolloverSegmentOperation).Observe(
				float64(time.Since(start).Milliseconds()))
		}()

		if err = engine.snapshot(); err != nil {
			return err
		}

		// switch to new segment
		if err = engine.segment.close(); err != nil {
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

// addLogEntryIndexToSegment stores log entry index in memory withing specific segment
func (engine *Engine) addLogEntryIndexToSegment(segmentID string, key string, logEntryIndex LogEntryIndex) {
	engine.logger.Debugf("adding log entry index for key %s into segment %s", key, segmentID)

	_, ok := engine.logEntryIndexesBySegmentID[segmentID]

	if !ok {
		engine.logEntryIndexesBySegmentID[segmentID] = make(logEntryIndexByKey)
	}

	engine.logEntryIndexesBySegmentID[segmentID][key] = logEntryIndex
}

// addLogEntryIndex stores log entry index in memory
func (engine *Engine) addLogEntryIndex(key string, logEntryIndex LogEntryIndex) {
	engine.addLogEntryIndexToSegment(engine.segment.id, key, logEntryIndex)
	EngineLogEntryIndexCount.Inc()
}

// Set stores a key and it's associated value
func (engine *Engine) Set(key, value string) error {
	start := time.Now()
	var err error

	defer func() {
		EngineOperationDurationNanoseconds.WithLabelValues(SetOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		EngineOperationDurationMilliseconds.WithLabelValues(SetOperation).Observe(
			float64(time.Since(start).Milliseconds()))
	}()

	engine.logger.Debugf("setting key %s and value %s", key, value)

	engine.mu.Lock()
	err = engine.checkDataSegment()
	engine.mu.Unlock()

	if err != nil {
		return err
	}

	logEntry := NewLogEntry(key, value)
	logEntryIndex, err := engine.segment.addLogEntry(logEntry)
	if err != nil {
		return nil
	}

	engine.mu.Lock()
	engine.addLogEntryIndex(key, *logEntryIndex)
	engine.mu.Unlock()

	EngineKeysCount.Inc()

	return nil
}

// loadSegment loads a data segment attempting to hit the cache first
func (engine *Engine) loadSegment(segmentID string) (*dataSegment, error) {
	var segment *dataSegment
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(LoadDataSegmentOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(LoadDataSegmentOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	cacheHit, ok := engine.lruSegments.Get(segmentID)

	if !ok {
		loadedSegment, err := loadDataSegment(segmentID)
		if err != nil {
			return nil, err
		}

		segment := loadedSegment
		engine.lruSegments.Add(segmentID, segment)
		EngineDiskHits.Inc()
		engine.logger.Debugf("loaded data segment %s from disk and added to cache", segmentID)
	} else {
		segment = cacheHit.(*dataSegment)
		EngineCacheHits.Inc()
		engine.logger.Debugf("loaded data segment %s from cache", segmentID)
	}

	return segment, nil
}

// findLogEntryByKey locates the log entry of provided key by locating
// the latest data segment containing that key
func (engine *Engine) findLogEntryByKey(key string) (*LogEntry, error) {
	var segment *dataSegment
	var err error
	start := time.Now()
	engine.logger.Debugf("searching log entry for key %s", key)

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(FindLogEntryOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(FindLogEntryOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	segments := engine.segmentsMetadataList.GetSegmentIDs()
	cursor := len(segments) - 1

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
			if segment, err = engine.loadSegment(segmentID); err != nil {
				return nil, err
			}
		} else {
			segment = engine.segment
		}

		searchedSegmentsCount := (len(segments) - cursor) + 1
		EngineSearchedDataSegments.Observe(float64(searchedSegmentsCount))

		return segment.getLogEntry(&logEntryIndex)
	}

	return nil, ErrKeyNotFound
}

// Get retrieves stored value for associated key
func (engine *Engine) Get(key string) (string, error) {
	engine.logger.Debugf("getting key %s", key)

	start := time.Now()
	var logEntry *LogEntry
	var err error
	status := "ok"

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(GetOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(GetOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		EngineQueryCount.WithLabelValues(
			status,
			strings.ToLower(strconv.FormatBool(logEntry != nil)),
			strings.ToLower(strconv.FormatBool(logEntry != nil && logEntry.IsDeleted)))
	}()

	engine.mu.RLock()
	defer engine.mu.RUnlock()

	logEntry, err = engine.findLogEntryByKey(key)
	if err != nil {
		status = "cant_find_log_entry"
		return "", err
	}

	if logEntry.IsDeleted {
		status = "log_entry_deleted"
		return "", ErrKeyNotFound
	}

	return logEntry.Value, nil
}

// Delete deletes a key by appending a tombstone log entry to the latest data
// segment
func (engine *Engine) Delete(key string) error {
	engine.logger.Debugf("deleting key %s", key)
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(DeleteOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(DeleteOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	engine.mu.Lock()
	defer engine.mu.Unlock()

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

	engine.addLogEntryIndex(key, *logEntryIndex)
	EngineDeletedKeysCount.Inc()

	return nil
}

// Close closes the storage engine
func (engine *Engine) Close() error {
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CloseEngineOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CloseEngineOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	engine.logger.Debug("closing database")

	engine.mu.Lock()
	defer engine.mu.Unlock()
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
	var err error

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CaptureSnapshotOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CaptureSnapshotOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
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

	EngineSnapshotLogEntrySizes.Observe(float64(len(snapshotEntryBytes)))
	EngineCompressedSnapshotLogEntrySizes.Observe(float64(len(compressedSnapshotEntryBytes)))

	return nil
}

// loadSnapshot loads snapshot from disk to memory
func (engine *Engine) loadSnapshot(fileName string) error {
	engine.logger.Debugf("loading snapshot %s", fileName)
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(LoadSnapshotOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(LoadSnapshotOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	compressedSnapshotEntryBytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return err
	}

	snapshotBytes, err := uncompressBytes(compressedSnapshotEntryBytes)
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

	EngineSnapshotLogEntrySizes.Observe(float64(len(snapshotBytes)))
	EngineCompressedSnapshotLogEntrySizes.Observe(float64(len(compressedSnapshotEntryBytes)))

	return nil
}

// isRecoverable checkes if storage engine contains existing data of which's logn
// entry indexes can be recovered
func (engine *Engine) isRecoverable() (bool, error) {
	engine.logger.Debug("checking if databasae is reecoverable")
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CheckRecoveryStatusOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CheckRecoveryStatusOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

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
	timestamp                 int64
	segmentID                 string
	compressedLogEntriesBytes []byte
	logEntryIndexes           []LogEntryIndex
}

type segmentContext struct {
	fileName string
	id       string
}

func (engine Engine) processSegmentJob(compactedSegmentEntriesChan chan compactedSegmentEntriesContext, jCtx *jobContext) {
	engine.logger.Debugf("received job for segment %s containing %d log entries", jCtx.segmentID, len(jCtx.logEntryIndexes))

	latestLogEntries := make(map[string]*LogEntry)

	for _, logEntryIndex := range jCtx.logEntryIndexes {
		compressedLogEntryBytes := make([]byte, logEntryIndex.CompressedEntrySize)
		logEntryReader := io.NewSectionReader(
			bytes.NewReader(jCtx.compressedLogEntriesBytes),
			logEntryIndex.OffSet,
			int64(logEntryIndex.CompressedEntrySize),
		)

		_, err := logEntryReader.Read(compressedLogEntryBytes)
		if err != nil {
			engine.logger.Debugf("unable to read log entry %s from segment %s", logEntryIndex.Key, jCtx.segmentID)
			continue
		}

		logEntryBytes, err := uncompressBytes(compressedLogEntryBytes)
		if err != nil {
			engine.logger.Debugf("unable to uncompress log entry of size %d due to %v", len(compressedLogEntryBytes), err)
			continue
		}

		logEntry := &LogEntry{}
		err = logEntry.Decode(logEntryBytes)

		if err != nil {
			engine.logger.Errorf("unable to decode log entry %s", logEntry.Key)
			continue
		}

		engine.logger.Debugf("processed key %s in segment %s", logEntry.Key, jCtx.segmentID)

		latestLogEntries[logEntry.Key] = logEntry
	}

	compactedSegmentEntriesChan <- compactedSegmentEntriesContext{
		compactedEntries: latestLogEntries,
		timestamp:        jCtx.timestamp,
	}
}

func (engine Engine) persistCompactedSegment(compactedLogEntries map[string]*LogEntry) error {
	var compactedSegment *dataSegment
	for _, logEntry := range compactedLogEntries {
		if compactedSegment == nil || compactedSegment.isClosed {
			engine.logger.Debugf("compacted segment full, creating new one")
			newSegment, err := newDataSegment()
			if err != nil {
				return err
			}
			compactedSegment = newSegment
		}

		engine.logger.Debugf("compacting log entry %s into segment %s", logEntry.Key, compactedSegment.id)

		logEntryIndex, err := compactedSegment.addLogEntry(logEntry)
		if err != nil {
			return err
		}

		engine.addLogEntryIndexToSegment(compactedSegment.id, logEntry.Key, *logEntryIndex)
		engine.segmentsMetadataList.Add(compactedSegment.id)
	}

	if !compactedSegment.isClosed {
		if err := compactedSegment.close(); err != nil {
			return err
		}
	}

	if err := engine.snapshot(); err != nil {
		return err
	}

	return nil
}

func (engine Engine) cleanUpStaleSegments(segmentsToDelete []segmentContext, compactedLogEntries map[string]*LogEntry) error {
	for _, segmentCtx := range segmentsToDelete {
		engine.lruSegments.Remove(segmentCtx.id)
		delete(engine.logEntryIndexesBySegmentID, segmentCtx.id)

		if err := engine.segmentsMetadataList.Remove(segmentCtx.id); err != nil {
			return err
		}
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
	var err error

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CompactSegmentsOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CompactSegmentsOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		EngineSegmentsToDelete.Set(float64(len(segmentsToDelete)))
		EngineFilesToCompact.WithLabelValues(CompactSegmentsOperation).Set(float64(len(files)))
		EngineSegmentsCompactionCount.Inc()
	}()

	files, err = ioutil.ReadDir(getSegmentsPath())
	if err != nil {
		return err
	}

	compactedSegmentEntriesChan := make(chan compactedSegmentEntriesContext, len(files))
	compactedSegmentEntries := make([]compactedSegmentEntriesContext, len(files))
	compactedLogEntries := make(map[string]*LogEntry)
	jobCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultsGroup, resultsGroupCtx := errgroup.WithContext(jobCtx)
	resultsGroup.SetLimit(2)

	// collect results from worker routines
	resultsGroup.Go(func() error {
		for compactedSegmentEntry := range compactedSegmentEntriesChan {
			compactedSegmentEntries = append(compactedSegmentEntries, compactedSegmentEntry)
		}
		return nil
	})

	resultsGroup.Go(func() error {
		defer close(compactedSegmentEntriesChan)
		workerGroup, _ := errgroup.WithContext(resultsGroupCtx)

		concurrencyLimit := engine.compactorWorkerCount
		if concurrencyLimit < 1 {
			concurrencyLimit = 5 // default concurrency limit
		}

		workerGroup.SetLimit(concurrencyLimit)

		// concurrently compact segments
		for _, file := range files {
			workerGroup.Go(func() error {
				EngineActiveWorkers.WithLabelValues(CompactSegmentsOperation).Inc()
				defer EngineActiveWorkers.WithLabelValues(CompactSegmentsOperation).Dec()

				if !strings.Contains(file.Name(), ".segment") || engine.segment.fileName == file.Name() {
					engine.logger.Debugf("skipping dispatching of file %s - cur segment %s", file.Name(), engine.segment.fileName)
					return nil
				}

				segmentID := strings.Split(file.Name(), ".")[0]
				segmentContentBytes, err := ioutil.ReadFile(path.Join(getSegmentsPath(), file.Name()))

				if err != nil {
					return err
				}

				segmentLogEntryIndexes, ok := engine.logEntryIndexesBySegmentID[segmentID]
				if !ok {
					engine.logger.Debugf("unable to find log entry indexes of segment %s", segmentID)
					return nil
				}

				logEntryIndexes := make([]LogEntryIndex, len(segmentContentBytes))
				for _, logEntryIndex := range segmentLogEntryIndexes {
					logEntryIndexes = append(logEntryIndexes, logEntryIndex)
				}

				engine.processSegmentJob(compactedSegmentEntriesChan, &jobContext{
					timestamp:                 file.ModTime().Unix(),
					compressedLogEntriesBytes: segmentContentBytes,
					segmentID:                 segmentID,
					logEntryIndexes:           logEntryIndexes,
				})

				segmentsToDelete = append(segmentsToDelete, segmentContext{
					fileName: file.Name(),
					id:       segmentID,
				})

				engine.logger.Debugf("dispatched job for segment %s containing %d log entries",
					segmentID, len(engine.logEntryIndexesBySegmentID[segmentID]))

				return nil
			})
		}

		return workerGroup.Wait()
	})

	if err = resultsGroup.Wait(); err != nil {
		return err
	}

	// sorts compacted segment entries by timestamp in order to have the latest
	// keys updated last
	sort.Slice(compactedSegmentEntries, func(a, b int) bool {
		return compactedSegmentEntries[a].timestamp < compactedSegmentEntries[b].timestamp
	})

	for i, compactedSegmentEntry := range compactedSegmentEntries {
		engine.logger.Debugf("processing compacted segment entry %d containing %d log entries", i, len(compactedSegmentEntry.compactedEntries))
		for key, logEntry := range compactedSegmentEntry.compactedEntries {
			engine.logger.Debugf("processing log entry %s in compacted segment %d", key, i)
			compactedLogEntries[key] = logEntry
		}
	}
	// writes log entries to to compacted segment and index it in memory
	engine.mu.Lock()
	err = engine.persistCompactedSegment(
		compactedLogEntries,
	)
	engine.mu.Unlock()

	if err != nil {
		engine.logger.Errorf("error occurred when persisting compacted segments %v", err)
	}

	engine.logger.Debugf("processed %d segments", len(compactedSegmentEntries))
	engine.logger.Debugf("wrote %d compacted log entries", len(compactedLogEntries))

	// clean up segment references from memory
	engine.mu.Lock()
	engine.cleanUpStaleSegments(segmentsToDelete, compactedLogEntries)
	engine.mu.Unlock()

	engine.logger.Debugf("cleaned up %d segments", len(segmentsToDelete))

	for _, segmentCtx := range segmentsToDelete {
		os.Remove(path.Join(getSegmentsPath(), segmentCtx.fileName))
	}

	return nil
}

// compactSnapshots compacts snapshots
func (engine *Engine) compactSnapshots() error {
	EngineActiveWorkers.WithLabelValues(CompactSnapshotsOperation).Inc()
	defer EngineActiveWorkers.WithLabelValues(CompactSnapshotsOperation).Dec()

	start := time.Now()
	engine.logger.Debug("compacting snapshots")

	var files []fs.FileInfo
	now := time.Now()
	deletedCount := 0

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CompactSnapshotsOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CompactSnapshotsOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		EngineFilesToCompact.WithLabelValues(CompactSnapshotsOperation).Set(float64(len(files)))
		EngineSnapshotsCompactionCount.Inc()
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
				if !engine.isCompactingSegments && engine.lruSegments.Len() > 1 {
					engine.isCompactingSegments = true

					if err := engine.compactSegments(); err != nil {
						errChan <- err
					}

					engine.isCompactingSegments = false
					EngineSegmentsCompactionCount.Inc()
				}
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
	engine.mu.Lock()
	defer engine.mu.Unlock()

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

// NewEngine creates a new engine
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
