package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"kv/protos"
	"maps"
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

type LogEntryIndexKey struct {
	Key     string
	Segment string
}

func (e LogEntryIndexKey) String() string {
	return e.Key + "::" + e.Segment
}

func (e *LogEntryIndexKey) FromString(s string) {
	parts := strings.Split(s, "::")
	e.Key = parts[0]
	e.Segment = parts[1]
}

// Represents the state of the storage engine.
type EngineState struct {
	mu                   sync.RWMutex
	logEntryIndexesByKey map[string]*protos.LogEntryIndex // map that holds log entry indexes by segment id
}

func (state *EngineState) Set(key *LogEntryIndexKey, value *protos.LogEntryIndex) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.logEntryIndexesByKey[key.String()] = value
}

func (state *EngineState) Get(key *LogEntryIndexKey) *protos.LogEntryIndex {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.logEntryIndexesByKey[key.String()]
}

func (state *EngineState) Remove(key *LogEntryIndexKey) {
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.logEntryIndexesByKey, key.String())
}

func (state *EngineState) Snapshot() map[string]*protos.LogEntryIndex {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return maps.Clone(state.logEntryIndexesByKey)
}

// Engine thread safe storage engine that uses the hash index strategy for keeping track
// where data is located on disk
type Engine struct {
	state                      *EngineState
	segmentsMetadataList       *SegmentMetadataList // historical list of segments id's
	lruSegments                *lru.Cache           // cache that holds the most recently used data segments
	segment                    *dataSegment         // current data segment
	segmentMaxSize             int                  // max size of entries stored in a data segment
	compactorInterval          time.Duration        // intervals that compaction process occurs
	compactorWorkerCount       int                  // number of workers compaction process uses
	snapshotTTLDuration        time.Duration        // snapshot files time to live duration
	isCompactingSegments       bool                 // flag that ensures only one segments compaction process is running at a time
	isCompactingSegmentsMutex  sync.RWMutex
	isCompactingSnapshots      bool // flag that ensures only one snapshots compaction process is running at a time
	isCompactingSnapshotsMutex sync.RWMutex
	logger                     log.FieldLogger
	ctx                        context.Context
	ctxCancelFunc              context.CancelFunc
}

// Store instance of a storage engine
type Store interface {
	Get(string) (string, error)
	Set(string, string) error
	Delete(string) error
	Close() error
}

type ProfilerConfig struct {
	ApplicationName string
	ServerAddress   string
	EnableLogging   bool
}

// EngineConfig configuration properties utilized when initializing an engine
type EngineConfig struct {
	SegmentMaxSize             int             // max size of entries stored in a data segment
	SnapshotInterval           time.Duration   // intervals that snapshots are captured
	TolerableSnapshotFailCount int             // max number of acceptable failures during the snapshotting
	CacheSize                  int             // max number of data segments to hold in memory
	CompactorInterval          time.Duration   // intervals that compaction process occurs
	CompactorWorkerCount       int             // number of workers compaction process uses
	SnapshotTTLDuration        time.Duration   // snapshot files time to live duration
	DataPath                   string          // path where store data is persisted on disk
	ShouldCompact              bool            // should the data be compacted at the specified interval
	ProfilerConfig             *ProfilerConfig // config used to initialize the profiler
}

// captureSnapshots captures snapshots at an interval
func (engine *Engine) captureSnapshots(
	ctx context.Context, interval time.Duration, tolerableFailCount int,
) {
	engine.logger.Debugf("starting snapshots taker process with interval %v and tolerableFailCount %d", interval, tolerableFailCount)

	failCounts := 0
	ticker := time.NewTicker(interval)

	for {
		if failCounts >= tolerableFailCount {
			panic("snapshotting failure")
		}

		select {
		case <-ticker.C:
			if err := engine.snapshot(); err != nil {
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
	segmentEntriesCount := engine.segment.entriesCount
	if segmentEntriesCount >= engine.segmentMaxSize {
		start := time.Now()
		var err error

		defer func() {
			EngineOperationDurationNanoseconds.WithLabelValues(RolloverSegmentOperation).Observe(
				float64(time.Since(start).Nanoseconds()))
			EngineOperationDurationMilliseconds.WithLabelValues(RolloverSegmentOperation).Observe(
				float64(time.Since(start).Milliseconds()))
		}()

		// create new segment
		newDataSegment, err := newDataSegment()
		if err != nil {
			return err
		}

		prevSegment := engine.segment
		engine.segment = newDataSegment
		engine.segmentsMetadataList.Add(newDataSegment.id)

		engine.logger.Debugf("switched to new data segment with id %s", newDataSegment.id)

		// clean up old segment
		if err = engine.snapshot(); err != nil {
			return err
		}

		// switch to new segment
		if err = prevSegment.close(); err != nil {
			return err
		}
	}
	return nil
}

// addLogEntryIndexToSegment stores log entry index in memory within specific segment
func (engine *Engine) addLogEntryIndexToSegment(segmentID string, logEntryIndex *protos.LogEntryIndex) {
	key := &LogEntryIndexKey{logEntryIndex.Key, segmentID}
	engine.state.Set(key, logEntryIndex)
	// state := strings.Builder{}
	// for k, v := range engine.state.Snapshot() {
	// 	state.WriteString(fmt.Sprintf("key: %v, value: %v\n", k, v))
	// }
	// engine.logger.Debugf("[%s] added log entry index (%s, %v) into segment %s. new state is \n%v",
	// 	logEntryIndex.Key, key, logEntryIndex, segmentID, state.String())
}

// addLogEntryIndex stores log entry index in memory
func (engine *Engine) addLogEntryIndex(logEntryIndex *protos.LogEntryIndex) {
	engine.addLogEntryIndexToSegment(engine.segment.id, logEntryIndex)
	EngineLogEntryIndexCount.Inc()
}

// Set stores a key and it's associated value
func (engine *Engine) Set(key, value string) error {
	engine.logger.Debugf("[%s] setting key", key)

	start := time.Now()
	var err error

	defer func() {
		EngineOperationDurationNanoseconds.WithLabelValues(SetOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		EngineOperationDurationMilliseconds.WithLabelValues(SetOperation).Observe(
			float64(time.Since(start).Milliseconds()))
	}()

	// engine.logger.Debugf("setting key %s with value of size %d", key, len(value))

	if err = engine.checkDataSegment(); err != nil {
		return err
	}

	logEntry := newLogEntry(key, value)
	logEntryIndex, err := engine.segment.addLogEntry(logEntry)
	if err != nil {
		return err
	}

	engine.addLogEntryIndex(logEntryIndex)
	EngineKeysCount.Inc()
	return nil
}

// loadSegment loads a data segment attempting to hit the cache first
func (engine *Engine) loadSegment(segmentID string) (*dataSegment, error) {
	var segment *dataSegment
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(loadDataSegmentOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(loadDataSegmentOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	cacheHit, ok := engine.lruSegments.Get(segmentID)

	if !ok {
		loadedSegment, err := loadDataSegment(segmentID)
		if err != nil {
			return nil, err
		}

		segment = loadedSegment
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
func (engine *Engine) findLogEntryByKey(key string) (*protos.LogEntry, error) {
	var err error
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(FindLogEntryOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(FindLogEntryOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	segments := engine.segmentsMetadataList.GetSegmentIDs()

	cursor := len(segments) - 1
	searchedSegmentsCount := 0

	for cursor >= 0 {
		segmentID := segments[cursor]
		engine.logger.Debugf("[%s] checking segment %s", key, segmentID)
		indexKey := LogEntryIndexKey{key, segmentID}
		state := strings.Builder{}
		for key, val := range engine.state.Snapshot() {
			state.WriteString(fmt.Sprintf("key: %v, value: %v\n", key, val))
		}
		logEntryIndex := engine.state.Get(&indexKey)
		logEntryExists := logEntryIndex != nil
		cursor--
		searchedSegmentsCount++

		// engine.logger.Debugf("checking segment %s with state %v",
		//	segmentID, logEntryIndexesByKey)

		if !logEntryExists {
			engine.logger.Debugf("[%s] didn't find log entry for key %s using index key %v in segment in %s",
				key, key, indexKey, segmentID)
			continue
		}

		var segment *dataSegment
		if segmentID != engine.segment.id {
			if segment, err = engine.loadSegment(segmentID); err != nil {
				return nil, err
			}
		} else {
			segment = engine.segment
			EngineCacheHits.Inc()
		}

		logEntry, err := segment.getLogEntry(logEntryIndex)
		EngineSearchedDataSegments.Observe(float64(searchedSegmentsCount))

		return logEntry, err
	}
	return nil, ErrKeyNotFound
}

// Get retrieves stored value for associated key
func (engine *Engine) Get(key string) (string, error) {
	engine.logger.Debugf("[%s] getting key", key)

	start := time.Now()
	var logEntry *protos.LogEntry
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
			strings.ToLower(strconv.FormatBool(logEntry != nil && logEntry.IsDeleted))).Inc()
	}()

	logEntry, err = engine.findLogEntryByKey(key)
	if err != nil {
		status = "cant_find_log_entry"
		return "", err
	}

	// engine.logger.Debugf("getting key %s with entry %v", key, logEntry)

	if logEntry.IsDeleted {
		status = "log_entry_deleted"
		return "", ErrKeyNotFound
	}

	return logEntry.Value, nil
}

// Delete deletes a key by appending a tombstone log entry to the latest data
// segment
func (engine *Engine) Delete(key string) error {
	engine.logger.Debugf("[%s] deleting key", key)

	start := time.Now()
	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(DeleteOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(DeleteOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

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

	engine.addLogEntryIndex(logEntryIndex)
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

	engine.ctxCancelFunc()

	if err := engine.snapshot(); err != nil {
		return err
	}

	if err := engine.segment.close(); err != nil {
		return err
	}

	engine.logger.Debug("closing database")

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

	snapshotState := &protos.SnapshotState{
		LogEntryIndexesByKey: engine.state.Snapshot(),
	}

	snapshotEntry, err := newSnapshotEntry(snapshotState)
	if err != nil {
		return err
	}

	file, err := os.Create(snapshotEntryFileName(snapshotEntry))
	if err != nil {
		return err
	}

	snapshotEntryBytes, err = encodeSnapshotEntry(snapshotEntry)
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

	compressedSnapshotEntryBytes, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}

	snapshotBytes, err := uncompressBytes(compressedSnapshotEntryBytes)
	if err != nil {
		return err
	}

	snapshot, err := decodeSnapshotEntry(snapshotBytes)
	if err != nil {
		return err
	}

	for key, index := range snapshot.Snapshot.LogEntryIndexesByKey {
		keyParts := strings.Split(key, "::")
		key := keyParts[0]
		segment := keyParts[1]
		engine.segmentsMetadataList.Add(segment)
		engine.state.Set(&LogEntryIndexKey{key, segment}, index)
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

	files, err := os.ReadDir(getSnapshotsPath())
	if err != nil {
		return false, err
	}

	if _, err := os.ReadDir(getSegmentsPath()); err != nil {
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
	compactedEntries map[string]*protos.LogEntry // key to log entry
	timestamp        int64
}

type jobContext struct {
	timestamp                 int64
	segmentID                 string
	compressedLogEntriesBytes []byte
	logEntryIndexes           []*protos.LogEntryIndex
}

type segmentContext struct {
	fileName string
	id       string
}

func (engine *Engine) processSegmentJob(compactedSegmentEntriesChan chan compactedSegmentEntriesContext, jCtx *jobContext) {
	engine.logger.Debugf("received job for segment %s containing %d log entries", jCtx.segmentID, len(jCtx.logEntryIndexes))

	latestLogEntries := make(map[string]*protos.LogEntry)

	for _, logEntryIndex := range jCtx.logEntryIndexes {
		compressedLogEntryBytes := make([]byte, logEntryIndex.CompressedEntrySize)
		logEntryReader := io.NewSectionReader(
			bytes.NewReader(jCtx.compressedLogEntriesBytes),
			logEntryIndex.Offset,
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

		logEntry, err := decodeLogEntry(logEntryBytes)

		if err != nil {
			engine.logger.Errorf("unable to decode log entry %s", logEntryIndex.Key)
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

func (engine *Engine) persistCompactedSegment(compactedLogEntries map[string]*protos.LogEntry) error {
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

		engine.addLogEntryIndexToSegment(compactedSegment.id, logEntryIndex)
	}

	engine.segmentsMetadataList.Add(compactedSegment.id)

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

func (engine *Engine) cleanUpStaleSegments(
	segmentsToDelete []segmentContext, compactedLogEntries map[string]*protos.LogEntry,
) error {
	for _, segmentCtx := range segmentsToDelete {
		engine.lruSegments.Remove(segmentCtx.id)
		for key := range engine.state.Snapshot() {
			indexKey := &LogEntryIndexKey{}
			indexKey.FromString(key)
			if indexKey.Segment == segmentCtx.id {
				engine.state.Remove(indexKey)
			}
		}
		err := engine.segmentsMetadataList.Remove(segmentCtx.id)
		if err != nil {
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
	var segmentsToDeleteMutex sync.Mutex
	files := make([]fs.DirEntry, 0)
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

	files, err = os.ReadDir(getSegmentsPath())
	if err != nil {
		return err
	}

	compactedSegmentEntriesChan := make(chan compactedSegmentEntriesContext, len(files))
	compactedSegmentEntries := make([]compactedSegmentEntriesContext, len(files))
	compactedLogEntries := make(map[string]*protos.LogEntry)
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
			f := file
			workerGroup.Go(func() error {
				EngineActiveWorkers.WithLabelValues(CompactSegmentsOperation).Inc()
				defer EngineActiveWorkers.WithLabelValues(CompactSegmentsOperation).Dec()

				if !strings.Contains(f.Name(), ".segment") || engine.segment.fileName == f.Name() {
					engine.logger.Debugf("skipping dispatching of file %s - cur segment %s", f.Name(), engine.segment.fileName)
					return nil
				}

				segmentID := strings.Split(f.Name(), ".")[0]
				segmentContentBytes, err := os.ReadFile(path.Join(getSegmentsPath(), f.Name()))

				if err != nil {
					return err
				}

				logEntryIndexes := []*protos.LogEntryIndex{}
				for key, logEntryIndex := range engine.state.Snapshot() {
					indexKey := &LogEntryIndexKey{}
					indexKey.FromString(key)
					if indexKey.Segment == segmentID {
						logEntryIndexes = append(logEntryIndexes, logEntryIndex)
					}
				}

				info, err := f.Info()
				if err != nil {
					engine.logger.Errorf("unable to get file info for file %s", f.Name())
				}

				engine.processSegmentJob(compactedSegmentEntriesChan, &jobContext{
					timestamp:                 info.ModTime().Unix(),
					compressedLogEntriesBytes: segmentContentBytes,
					segmentID:                 segmentID,
					logEntryIndexes:           logEntryIndexes,
				})

				segmentsToDeleteMutex.Lock()
				engine.logger.Debugf("[%s] segmentsToDeleteMutex", f.Name())
				segmentsToDelete = append(segmentsToDelete, segmentContext{
					fileName: f.Name(),
					id:       segmentID,
				})
				segmentsToDeleteMutex.Unlock()
				engine.logger.Debugf("[%s] released segmentsToDeleteMutex", f.Name())

				engine.logger.Debugf("dispatched job for segment %s containing %d log entries",
					segmentID, len(logEntryIndexes))

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
	if err = engine.persistCompactedSegment(compactedLogEntries); err != nil {
		engine.logger.Errorf("error occurred when persisting compacted segments %v", err)
	}

	engine.logger.Debugf("processed %d segments", len(compactedSegmentEntries))
	engine.logger.Debugf("wrote %d compacted log entries", len(compactedLogEntries))

	// clean up segment references from memory
	if err = engine.cleanUpStaleSegments(segmentsToDelete, compactedLogEntries); err != nil {
		return err
	}

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
	var files []fs.DirEntry
	now := time.Now()
	deletedCount := 0

	engine.logger.Debug("compacting snapshots")

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CompactSnapshotsOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CompactSnapshotsOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		EngineFilesToCompact.WithLabelValues(CompactSnapshotsOperation).Set(float64(len(files)))
		EngineSnapshotsCompactionCount.Inc()
	}()

	files, err := os.ReadDir(getSnapshotsPath())
	if err != nil {
		return err
	}

	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			engine.logger.Errorf("unable to get file info for file %s", file.Name())
		}

		if now.Sub(info.ModTime()) > engine.snapshotTTLDuration {
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
	wg := new(errgroup.Group)
	wg.SetLimit(2)

	// segment compactor
	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				engine.logger.Debug("stopping segments compactor process")
				return nil

			case <-ticker.C:
				logPrefix := "segment compactor"
				engine.isCompactingSegmentsMutex.Lock()
				engine.logger.Debugf("[%s] acquired engine.isCompactingSegmentsMutex", logPrefix)

				isCompactingSegments := engine.isCompactingSegments

				hasLruSegments := engine.lruSegments.Len() > 1

				if !isCompactingSegments && hasLruSegments {
					engine.isCompactingSegments = true

					if err := engine.compactSegments(); err != nil {
						panic(fmt.Sprintf("error compacting segments: %v", err))
					}

					engine.isCompactingSegments = false

					EngineSegmentsCompactionCount.Inc()
				}

				engine.isCompactingSegmentsMutex.Unlock()
				engine.logger.Debugf("[%s] engine.isCompactingSegmentsMutex", logPrefix)

			}
		}
	})

	// snapshot compactor
	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				engine.logger.Debug("stopping snapshots compactor process")
				return nil

			case <-ticker.C:
				logPrefix := "snapshot compactor"
				engine.isCompactingSnapshotsMutex.Lock()
				engine.logger.Debugf("[%s] engine.isCompactingSnapshotsMutex", logPrefix)

				if !engine.isCompactingSnapshots {
					engine.isCompactingSnapshots = true

					if err := engine.compactSnapshots(); err != nil {
						panic(fmt.Sprintf("error compacting snapshot: %v", err))
					}

					engine.isCompactingSnapshots = false
					EngineSnapshotsCompactionCount.Inc()
				}

				engine.isCompactingSnapshotsMutex.Unlock()
				engine.logger.Debugf("[%s] released engine.isCompactingSnapshotsMutex", logPrefix)
			}
		}
	})

	wg.Wait()

	return nil
}

// recover recovers database indexes from snapshots
func (engine *Engine) recover() error {
	engine.logger.Debug("recovering database state from snapshots")

	files, err := os.ReadDir(getSnapshotsPath())
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

	for _, p := range []string{getDataPath(), getSegmentsPath(), getSnapshotsPath()} {
		if _, err := os.Stat(p); os.IsNotExist(err) {
			err = os.MkdirAll(p, 0777)
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
		state: &EngineState{
			logEntryIndexesByKey: make(map[string]*protos.LogEntryIndex),
		},
		segmentMaxSize:        config.SegmentMaxSize,
		segment:               segment,
		lruSegments:           lruCache,
		segmentsMetadataList:  segmentMetadataList,
		ctx:                   ctx,
		compactorInterval:     config.CompactorInterval,
		compactorWorkerCount:  config.CompactorWorkerCount,
		snapshotTTLDuration:   config.SnapshotTTLDuration,
		isCompactingSegments:  false,
		isCompactingSnapshots: false,
		logger:                log.WithField("storage_engine", "hash_index"),
		ctxCancelFunc:         cancel,
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

	if config.ShouldCompact {
		go engine.startCompactors(engine.ctx)
	}

	return engine, nil
}
