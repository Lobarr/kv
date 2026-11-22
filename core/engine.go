package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"kv/protos"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

const (
	engineStateShardCount = 64
	defaultSegmentMaxSize = 1000000
	defaultCacheSize      = 256
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

// ErrEngineClosed occurs when an operation is attempted on a closed engine
var ErrEngineClosed = errors.New("engine is closed")

// errSegmentUnavailable occurs when a segment cannot be pinned for reading
// (e.g. it is being closed or was removed after compaction). Reads retry
// with a fresh state lookup when this happens.
var errSegmentUnavailable = errors.New("segment unavailable")

// indexEntry is the compact in-memory index record stored per key.
// Storing a value struct avoids per-key protobuf heap allocations and
// eliminates filepath parsing on every Get.
type indexEntry struct {
	segmentID string
	offset    int64
	entrySize int64
}

// engineStateShard is a single shard of the engine state. Locks are sharded
// by key hash to eliminate global mutex contention under high concurrency.
type engineStateShard struct {
	mu      sync.RWMutex
	indexes map[string]indexEntry
	_       [40]byte // cache-line padding to prevent false sharing between shards
}

// EngineState holds the latest log entry index per key, sharded by key hash.
// Only the latest index per key is tracked: historical per-segment indexes
// are not needed (compaction only ever rewrites the latest entry of a key),
// which also makes recovery deterministic and keys containing "::" safe.
type EngineState struct {
	shards []engineStateShard
	layout *dataLayout
}

func stateShardForKey(key string) uint32 {
	// FNV-1a, allocation free
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h & (engineStateShardCount - 1)
}

func (state *EngineState) shardFor(key string) *engineStateShard {
	return &state.shards[stateShardForKey(key)]
}

func (state *EngineState) toProto(key string, e indexEntry) *protos.LogEntryIndex {
	fileName := e.segmentID
	if state.layout != nil {
		fileName = state.layout.segmentFileName(e.segmentID)
	}
	return &protos.LogEntryIndex{
		Key:             key,
		EntrySize:       e.entrySize,
		SegmentFilename: fileName,
		Offset:          e.offset,
	}
}

// lockShard exclusively locks the shard owning key and returns it. The
// caller must eventually call the matching Unlock.
func (state *EngineState) lockShard(key string) *engineStateShard {
	shard := state.shardFor(key)
	shard.mu.Lock()
	return shard
}

// setLockedFast stores a compact indexEntry in an already-locked shard.
func (state *EngineState) setLockedFast(shard *engineStateShard, key string, entry indexEntry) {
	shard.indexes[key] = entry
}

// setLocked stores an index in an already-locked shard.
func (state *EngineState) setLocked(shard *engineStateShard, key string, value *protos.LogEntryIndex) {
	shard.indexes[key] = indexEntry{
		segmentID: segmentIDOfIndex(value),
		offset:    value.Offset,
		entrySize: value.EntrySize,
	}
}

// Set stores the latest index for a key
func (state *EngineState) Set(key string, value *protos.LogEntryIndex) {
	shard := state.shardFor(key)
	shard.mu.Lock()
	shard.indexes[key] = indexEntry{
		segmentID: segmentIDOfIndex(value),
		offset:    value.Offset,
		entrySize: value.EntrySize,
	}
	shard.mu.Unlock()
}

// GetFast returns the compact indexEntry for a key without allocation.
func (state *EngineState) GetFast(key string) (indexEntry, bool) {
	shard := state.shardFor(key)
	shard.mu.RLock()
	e, ok := shard.indexes[key]
	shard.mu.RUnlock()
	return e, ok
}

// Get returns the latest index for a key, or nil
func (state *EngineState) Get(key string) *protos.LogEntryIndex {
	e, ok := state.GetFast(key)
	if !ok {
		return nil
	}
	return state.toProto(key, e)
}

// getLocked returns the index for a key in an already-locked shard.
func (state *EngineState) getLocked(shard *engineStateShard, key string) *protos.LogEntryIndex {
	e, ok := shard.indexes[key]
	if !ok {
		return nil
	}
	return state.toProto(key, e)
}

// removeLocked deletes a key from an already-locked shard.
func (state *EngineState) removeLocked(shard *engineStateShard, key string) {
	delete(shard.indexes, key)
}

// setIfSegmentUnchanged stores index for key, but only if the currently
// stored index for the key (if any) belongs to one of the staleSegments.
// This prevents a compaction from clobbering a newer live entry written by a
// concurrent Set. Returns true if the update was applied.
func (state *EngineState) setIfSegmentUnchanged(key string, index *protos.LogEntryIndex, staleSegments map[string]struct{}) bool {
	shard := state.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if current, ok := shard.indexes[key]; ok {
		if _, stale := staleSegments[current.segmentID]; !stale {
			return false
		}
	}
	shard.indexes[key] = indexEntry{
		segmentID: segmentIDOfIndex(index),
		offset:    index.Offset,
		entrySize: index.EntrySize,
	}
	return true
}

// removeIfSegmentUnchanged deletes key, but only if the currently stored
// index (if any) belongs to one of the staleSegments. Used to drop tombstone
// entries during compaction without clobbering concurrent writes. Returns
// true if the key was removed.
func (state *EngineState) removeIfSegmentUnchanged(key string, staleSegments map[string]struct{}) bool {
	shard := state.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if current, ok := shard.indexes[key]; ok {
		if _, stale := staleSegments[current.segmentID]; stale {
			delete(shard.indexes, key)
			return true
		}
	}
	return false
}

// Remove deletes the index for a key
func (state *EngineState) Remove(key string) {
	shard := state.shardFor(key)
	shard.mu.Lock()
	delete(shard.indexes, key)
	shard.mu.Unlock()
}

// RemoveIfSegmentIn removes every index whose segment is in staleSegments.
// Returns the number of removed entries.
func (state *EngineState) RemoveIfSegmentIn(staleSegments map[string]struct{}) int {
	removed := 0
	for i := range state.shards {
		shard := &state.shards[i]
		shard.mu.Lock()
		for key, entry := range shard.indexes {
			if _, stale := staleSegments[entry.segmentID]; stale {
				delete(shard.indexes, key)
				removed++
			}
		}
		shard.mu.Unlock()
	}
	return removed
}

// Snapshot returns a copy of the whole state (key -> latest index)
func (state *EngineState) Snapshot() map[string]*protos.LogEntryIndex {
	result := make(map[string]*protos.LogEntryIndex)
	for i := range state.shards {
		shard := &state.shards[i]
		shard.mu.RLock()
		for k, v := range shard.indexes {
			result[k] = state.toProto(k, v)
		}
		shard.mu.RUnlock()
	}
	return result
}

// Len returns the number of tracked keys
func (state *EngineState) Len() int {
	total := 0
	for i := range state.shards {
		shard := &state.shards[i]
		shard.mu.RLock()
		total += len(shard.indexes)
		shard.mu.RUnlock()
	}
	return total
}

// segmentIDOfIndex extracts the segment ID from a log entry index
func segmentIDOfIndex(index *protos.LogEntryIndex) string {
	if index == nil {
		return ""
	}
	return segmentIDFromFileName(index.SegmentFilename)
}

// Engine thread safe storage engine that uses the hash index strategy for keeping track
// where data is located on disk
type Engine struct {
	state                      *EngineState
	layout                     *dataLayout                 // per-engine on-disk layout (multiple engines can coexist)
	segmentsMetadataList       *SegmentMetadataList        // historical list of segments id's
	lruSegments                *lru.Cache                  // cache that holds the most recently used (sealed) data segments
	segment                    *dataSegment                // current active data segment (never evicted from the LRU)
	activeSegment              atomic.Pointer[dataSegment] // lock-free mirror of segment for hot-path reads/writes
	segmentMutex               sync.RWMutex
	segmentMaxSize             int // max number of entries stored in a data segment
	segmentMaxSizeMutex        sync.RWMutex
	compactorInterval          time.Duration // intervals that compaction process occurs
	compactorWorkerCount       int           // number of workers compaction process uses
	snapshotTTLDuration        time.Duration // snapshot files time to live duration
	enableCompression          bool          // whether log entries are compressed on disk
	isCompactingSegments       bool          // flag that ensures only one segments compaction process is running at a time
	isCompactingSegmentsMutex  sync.RWMutex
	isCompactingSnapshots      bool // flag that ensures only one snapshots compaction process is running at a time
	isCompactingSnapshotsMutex sync.RWMutex
	loadMutex                  sync.Mutex // serializes disk segment loads (singleflight)
	closed                     atomic.Bool
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
	EnableCompression          bool            // whether log entries are compressed on disk
	ProfilerConfig             *ProfilerConfig // config used to initialize the profiler
}

// captureSnapshots captures snapshots at an interval
func (engine *Engine) captureSnapshots(
	ctx context.Context, interval time.Duration, tolerableFailCount int,
) {
	engine.logger.Debugf("starting snapshots taker process with interval %v and tolerableFailCount %d", interval, tolerableFailCount)

	failCounts := 0
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if failCounts >= tolerableFailCount {
			panic("snapshotting failure")
		}

		select {
		case <-ticker.C:
			if err := engine.snapshot(); err != nil {
				failCounts++
			}
		case <-ctx.Done():
			return
		}
	}
}

// checkDataSegment verifies data segments and performs handoff between old
// and new data segments. On rollover the old segment is flushed and sealed
// (so it never has unflushed bufio data) before the new segment becomes
// active.
func (engine *Engine) checkDataSegment() error {
	segment := engine.activeSegment.Load()

	// Fast path: lock-free check, no rollover needed
	if segment.getEntriesCount() < engine.segmentMaxSize {
		return nil
	}

	// Slow path: need to rollover, acquire exclusive lock
	engine.segmentMutex.Lock()
	defer engine.segmentMutex.Unlock()

	if engine.closed.Load() {
		return ErrEngineClosed
	}

	// Double-check after acquiring lock
	if engine.segment.getEntriesCount() < engine.segmentMaxSize {
		return nil
	}

	// Create the new segment first so a failure leaves the old one usable.
	newDataSegment, err := newDataSegment(engine.layout)
	if err != nil {
		return err
	}

	// Flush + seal the old segment: its buffered writes are persisted and it
	// is memory-mapped for reads. In-flight writers finish first (refcount
	// drain), and writers that arrive after this point are handed the new
	// segment instead.
	oldSegment := engine.segment
	if err := oldSegment.sealAndMap(); err != nil {
		newDataSegment.close()
		os.Remove(newDataSegment.fileName)
		return err
	}

	engine.segment = newDataSegment
	engine.activeSegment.Store(newDataSegment)
	engine.segmentsMetadataList.Add(newDataSegment.id)

	// Only now that the old segment is sealed is it safe to put it in the
	// LRU: the active write segment is never kept in the LRU so it can never
	// be evicted while still accepting writes.
	engine.lruSegments.Add(oldSegment.id, oldSegment)

	go func() {
		if err := engine.snapshot(); err != nil {
			engine.logger.Errorf("failed to snapshot after rollover: %v", err)
		}
	}()

	return nil
}

// addLogEntryIndexToSegment stores log entry index in memory for a key
func (engine *Engine) addLogEntryIndexToSegment(key string, logEntryIndex *protos.LogEntryIndex) {
	engine.state.Set(key, logEntryIndex)
	EngineLogEntryIndexCount.Inc()
}

// acquireSegmentForWrite pins the current active segment for a write.
// Lock-free on the fast path; if a rollover is in progress it waits for
// rollover to complete and retries on the new active segment.
func (engine *Engine) acquireSegmentForWrite() (*dataSegment, error) {
	for attempt := 0; attempt < 8; attempt++ {
		if segment := engine.activeSegment.Load(); segment.acquire() {
			return segment, nil
		}

		if engine.closed.Load() {
			return nil, ErrEngineClosed
		}

		// Slow path: a rollover is in progress or needed. Synchronize with
		// segmentMutex so we don't spin while sealAndMap finishes.
		engine.segmentMutex.RLock()
		segment := engine.segment
		ok := segment.acquire()
		engine.segmentMutex.RUnlock()
		if ok {
			return segment, nil
		}

		if err := engine.checkDataSegment(); err != nil {
			return nil, err
		}
	}
	return nil, ErrClosedDataSegment
}

// acquireSegmentForRead pins the segment owning segmentID for a read,
// flushing it first when it is still the active segment (so buffered writes
// become visible to ReadAt). The caller must release the returned segment.
func (engine *Engine) acquireSegmentForRead(segmentID string) (*dataSegment, error) {
	if segment := engine.activeSegment.Load(); segment.id == segmentID {
		if segment.acquireWait() {
			// If still active, flush buffered writes before ReadAt.
			if segment.state.Load() == segmentStateActive {
				if err := segment.flush(); err != nil {
					segment.release()
					return nil, err
				}
			}
			return segment, nil
		}
	}

	return engine.loadSegment(segmentID)
}

// loadSegment loads a data segment attempting to hit the cache first. The
// returned segment is pinned (caller must release).
func (engine *Engine) loadSegment(segmentID string) (*dataSegment, error) {
	for attempt := 0; attempt < 3; attempt++ {
		if cached, ok := engine.lruSegments.Get(segmentID); ok {
			segment := cached.(*dataSegment)
			if segment.acquireWait() {
				return segment, nil
			}
			// Segment was closed (e.g. by LRU eviction); drop the stale
			// entry and fall through to a disk load.
			engine.lruSegments.Remove(segmentID)
		}

		// Serialize disk loads (singleflight): without this, concurrent
		// loaders of the same segment would stampede, each caching its own
		// instance and invalidating the others in a loop.
		engine.loadMutex.Lock()

		// Double-check the cache: another loader may have completed while we
		// waited for the lock.
		if cached, ok := engine.lruSegments.Get(segmentID); ok {
			segment := cached.(*dataSegment)
			if segment.acquireWait() {
				engine.loadMutex.Unlock()
				return segment, nil
			}
			engine.lruSegments.Remove(segmentID)
		}

		loadedSegment, err := loadDataSegment(engine.layout, segmentID)
		if err != nil {
			engine.loadMutex.Unlock()
			return nil, err
		}

		// Pin before exposing to the cache so eviction cannot close it
		// while we hand it back to the caller.
		loadedSegment.acquireWait()
		engine.lruSegments.Add(segmentID, loadedSegment)

		// We hold the load mutex, so this instance owns the cache slot.
		engine.loadMutex.Unlock()

		EngineDiskHits.Inc()
		engine.logger.Debugf("loaded data segment %s from disk and added to cache", segmentID)
		return loadedSegment, nil
	}

	return nil, errSegmentUnavailable
}

// findLogEntryByKey locates the log entry of the provided key by locating the
// latest data segment containing that key. Retries once with a fresh state
// lookup to tolerate segments swapped by a concurrent compaction.
func (engine *Engine) findLogEntryByKey(key string) (*protos.LogEntry, error) {
	for attempt := 0; attempt < 2; attempt++ {
		entry, ok := engine.state.GetFast(key)
		if !ok || entry.segmentID == "" {
			return nil, ErrKeyNotFound
		}

		segment, err := engine.acquireSegmentForRead(entry.segmentID)
		if err != nil {
			if attempt == 0 {
				continue
			}
			return nil, err
		}

		logEntry, err := segment.getLogEntryAt(key, entry.offset, entry.entrySize, engine.enableCompression)
		segment.release()
		if err != nil {
			if attempt == 0 {
				continue
			}
			return nil, err
		}
		return logEntry, nil
	}
	return nil, ErrKeyNotFound
}

// findLogEntryValueByKey locates the log entry value only
func (engine *Engine) findLogEntryValueByKey(key string) (string, bool, error) {
	for attempt := 0; attempt < 2; attempt++ {
		entry, ok := engine.state.GetFast(key)
		if !ok || entry.segmentID == "" {
			return "", false, ErrKeyNotFound
		}

		segment, err := engine.acquireSegmentForRead(entry.segmentID)
		if err != nil {
			if attempt == 0 {
				continue
			}
			return "", false, err
		}

		value, isDeleted, err := segment.getLogEntryValueAt(entry.offset, entry.entrySize, engine.enableCompression)
		segment.release()
		if err != nil {
			if attempt == 0 {
				continue
			}
			return "", false, err
		}
		return value, isDeleted, nil
	}
	return "", false, ErrKeyNotFound
}

// Get retrieves stored value for associated key
func (engine *Engine) Get(key string) (string, error) {
	if engine.closed.Load() {
		return "", ErrEngineClosed
	}

	value, isDeleted, err := engine.findLogEntryValueByKey(key)
	if err != nil {
		return "", err
	}

	if isDeleted {
		return "", ErrKeyNotFound
	}

	return value, nil
}

// Set stores a key and its associated value.
// The write (disk append + state update) is serialized per key via the
// engine state shard lock so concurrent writes to the same key can never
// update EngineState out of disk order.
func (engine *Engine) Set(key, value string) error {
	if err := engine.checkDataSegment(); err != nil {
		return err
	}

	// Hold the shard lock across append+index so same-key writes are totally
	// ordered: the last writer to reach the disk also updates the state last.
	shard := engine.state.lockShard(key)

	segment, err := engine.acquireSegmentForWrite()
	if err != nil {
		shard.mu.Unlock()
		return err
	}

	segID, offset, entrySize, err := segment.addKeyValue(key, value, false, engine.enableCompression)
	segment.release()

	if err != nil {
		shard.mu.Unlock()
		return err
	}

	engine.state.setLockedFast(shard, key, indexEntry{
		segmentID: segID,
		offset:    offset,
		entrySize: entrySize,
	})
	shard.mu.Unlock()

	return nil
}

// Sync flushes the write buffer to disk, ensuring all writes are persisted
func (engine *Engine) Sync() error {
	engine.segmentMutex.RLock()
	defer engine.segmentMutex.RUnlock()
	return engine.segment.flush()
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

	if err := engine.checkDataSegment(); err != nil {
		return err
	}

	logEntry, err := engine.findLogEntryByKey(key)
	if err != nil {
		return err
	}

	if logEntry.IsDeleted {
		return nil // already deleted
	}

	logEntry.IsDeleted = true
	logEntry.Value = ""
	// Recompute the checksum after mutating the entry, otherwise the
	// tombstone would fail checksum verification when read back.
	logEntry.Checksum = computeCheckSum(logEntry.Key, logEntry.Value)

	// Same per-key serialization as Set so tombstones are ordered correctly
	// against concurrent writes to the same key.
	shard := engine.state.lockShard(key)

	segment, err := engine.acquireSegmentForWrite()
	if err != nil {
		shard.mu.Unlock()
		return err
	}

	_, logEntryIndex, err := segment.addLogEntry(logEntry, engine.enableCompression)
	segment.release()

	if err != nil {
		shard.mu.Unlock()
		return err
	}

	engine.state.setLocked(shard, key, logEntryIndex)
	shard.mu.Unlock()

	EngineDeletedKeysCount.Inc()

	return nil
}

// Close closes the storage engine. It is safe to call multiple times.
func (engine *Engine) Close() error {
	start := time.Now()
	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CloseEngineOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CloseEngineOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	if !engine.closed.CompareAndSwap(false, true) {
		return nil // already closed
	}

	engine.ctxCancelFunc()

	snapshotErr := engine.snapshot()
	if snapshotErr != nil {
		engine.logger.Errorf("failed to write final snapshot during close: %v", snapshotErr)
	}

	engine.segmentMutex.Lock()
	defer engine.segmentMutex.Unlock()

	// close open segments in the cache
	for _, key := range engine.lruSegments.Keys() {
		if v, ok := engine.lruSegments.Get(key); ok {
			segment, ok := v.(*dataSegment)
			if !ok {
				continue
			}

			if err := segment.close(); err != nil {
				engine.logger.Errorf("failed to close cached segment: %v", err)
			}
		}
	}
	engine.lruSegments.Purge()

	if err := engine.segment.close(); err != nil {
		engine.logger.Errorf("failed to close active segment: %v", err)
	}

	engine.logger.Debug("closing database")

	return snapshotErr
}

// snapshot writes a snapshot of the log entry index state to disk atomically:
// it writes to a temp file, syncs, closes it and renames over the final name
// so readers can never observe a partially-written snapshot.
func (engine *Engine) snapshot() error {
	engine.logger.Debug("snapshotting database state")
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CaptureSnapshotOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CaptureSnapshotOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	rawBytes := encodeFastSnapshot(engine.state)
	writeBytes := rawBytes
	if engine.enableCompression {
		var err error
		writeBytes, err = compressBytes(rawBytes)
		if err != nil {
			return err
		}
	}

	finalPath := newSnapshotFileName(engine.layout)
	tempPath := finalPath + ".tmp"

	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}

	if _, err := file.Write(writeBytes); err != nil {
		file.Close()
		os.Remove(tempPath)
		return err
	}

	if err := file.Sync(); err != nil {
		file.Close()
		os.Remove(tempPath)
		return err
	}

	if err := file.Close(); err != nil {
		os.Remove(tempPath)
		return err
	}

	if err := os.Rename(tempPath, finalPath); err != nil {
		os.Remove(tempPath)
		return err
	}

	EngineSnapshotLogEntrySizes.Observe(float64(len(rawBytes)))
	EngineCompressedSnapshotLogEntrySizes.Observe(float64(len(writeBytes)))

	return nil
}

// loadSnapshot loads a snapshot from disk to memory. Supports both the fast
// KVSN binary format (compressed or uncompressed) and legacy compressed
// protobuf/prototext snapshots.
func (engine *Engine) loadSnapshot(fileName string) error {
	engine.logger.Debugf("loading snapshot %s", fileName)
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(LoadSnapshotOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(LoadSnapshotOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	fileBytes, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}

	// Fast path: uncompressed KVSN binary snapshot.
	if handled, err := decodeFastSnapshot(fileBytes, func(key string, entry indexEntry) {
		if entry.segmentID == "" {
			return
		}
		engine.segmentsMetadataList.Add(entry.segmentID)
		shard := engine.state.lockShard(key)
		engine.state.setLockedFast(shard, key, entry)
		shard.mu.Unlock()
	}); handled {
		if err != nil {
			return err
		}
		EngineSnapshotLogEntrySizes.Observe(float64(len(fileBytes)))
		EngineCompressedSnapshotLogEntrySizes.Observe(float64(len(fileBytes)))
		return nil
	}

	snapshotBytes, err := uncompressBytes(fileBytes)
	if err != nil {
		return err
	}

	// Compressed KVSN binary snapshot.
	if handled, err := decodeFastSnapshot(snapshotBytes, func(key string, entry indexEntry) {
		if entry.segmentID == "" {
			return
		}
		engine.segmentsMetadataList.Add(entry.segmentID)
		shard := engine.state.lockShard(key)
		engine.state.setLockedFast(shard, key, entry)
		shard.mu.Unlock()
	}); handled {
		if err != nil {
			return err
		}
		EngineSnapshotLogEntrySizes.Observe(float64(len(snapshotBytes)))
		EngineCompressedSnapshotLogEntrySizes.Observe(float64(len(fileBytes)))
		return nil
	}

	// Legacy compressed protobuf / prototext snapshot.
	snapshot, err := decodeSnapshotEntry(snapshotBytes)
	if err != nil {
		return err
	}

	for key, index := range snapshot.Snapshot.LogEntryIndexesByKey {
		segmentID := segmentIDOfIndex(index)
		if segmentID == "" {
			engine.logger.Warnf("skipping snapshot index for key %s with unknown segment file %q", key, index.SegmentFilename)
			continue
		}
		engine.segmentsMetadataList.Add(segmentID)
		engine.state.Set(key, index)
	}

	EngineSnapshotLogEntrySizes.Observe(float64(len(snapshotBytes)))
	EngineCompressedSnapshotLogEntrySizes.Observe(float64(len(fileBytes)))

	return nil
}

// isRecoverable checks if storage engine contains existing data whose log
// entry indexes can be recovered
func (engine *Engine) isRecoverable() (bool, error) {
	engine.logger.Debug("checking if database is recoverable")
	start := time.Now()

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CheckRecoveryStatusOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CheckRecoveryStatusOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
	}()

	files, err := os.ReadDir(engine.layout.snapshotsPath())
	if err != nil {
		return false, err
	}

	if _, err := os.ReadDir(engine.layout.segmentsPath()); err != nil {
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
	timestamp       int64
	segmentID       string
	logEntriesBytes []byte
	logEntryIndexes []*protos.LogEntryIndex
}

type segmentContext struct {
	fileName string
	id       string
}

func (engine *Engine) processSegmentJob(compactedSegmentEntriesChan chan compactedSegmentEntriesContext, jCtx *jobContext) {
	engine.logger.Debugf("received job for segment %s containing %d log entries", jCtx.segmentID, len(jCtx.logEntryIndexes))

	latestLogEntries := make(map[string]*protos.LogEntry, len(jCtx.logEntryIndexes))

	for _, logEntryIndex := range jCtx.logEntryIndexes {
		if logEntryIndex == nil || logEntryIndex.EntrySize <= 0 {
			continue
		}

		logEntryBytes := make([]byte, logEntryIndex.EntrySize)
		logEntryReader := io.NewSectionReader(
			bytes.NewReader(jCtx.logEntriesBytes),
			logEntryIndex.Offset,
			int64(logEntryIndex.EntrySize),
		)

		// ReadFull guards against short reads on large entries.
		if _, err := io.ReadFull(logEntryReader, logEntryBytes); err != nil {
			engine.logger.Errorf("unable to read log entry %s from segment %s: %v", logEntryIndex.Key, jCtx.segmentID, err)
			continue
		}

		if engine.enableCompression {
			var err error
			logEntryBytes, err = uncompressBytes(logEntryBytes)
			if err != nil {
				engine.logger.Errorf("unable to uncompress log entry of size %d due to %v", len(logEntryBytes), err)
				continue
			}
		}

		logEntry, err := decodeLogEntry(logEntryBytes, logEntryIndex.Key)
		if err != nil {
			engine.logger.Errorf("unable to decode log entry %s: %v", logEntryIndex.Key, err)
			continue
		}

		latestLogEntries[logEntry.Key] = logEntry
	}

	compactedSegmentEntriesChan <- compactedSegmentEntriesContext{
		compactedEntries: latestLogEntries,
		timestamp:        jCtx.timestamp,
	}
}

// persistCompactedSegment writes the compacted log entries into fresh
// segment(s), enforcing the configured segment max size, and updates the
// in-memory index. Tombstones are dropped entirely: every older live value
// of the key lives in the compacted set, so removing the key from the index
// is safe. Returns an error without registering anything if writing fails.
func (engine *Engine) persistCompactedSegment(compactedLogEntries map[string]*protos.LogEntry, compactedSegments map[string]struct{}) error {
	// Nothing to persist: do not create a segment (previously panicked here
	// on a nil active segment when the map was empty).
	if len(compactedLogEntries) == 0 {
		return nil
	}

	engine.segmentMaxSizeMutex.RLock()
	maxSize := engine.segmentMaxSize
	engine.segmentMaxSizeMutex.RUnlock()
	if maxSize <= 0 {
		maxSize = defaultSegmentMaxSize
	}

	var compactedSegment *dataSegment

	// registerSealed seals the current compacted segment and registers it in
	// the metadata list and the LRU so it stays readable/loadable.
	registerSealed := func(segment *dataSegment) error {
		if err := segment.sealAndMap(); err != nil {
			return err
		}
		engine.segmentsMetadataList.Add(segment.id)
		engine.lruSegments.Add(segment.id, segment)
		return nil
	}

	fail := func(err error) error {
		// Flush whatever was written so far to disk, then abort. The caller
		// must not delete any stale segment files after an error.
		if compactedSegment != nil {
			if regErr := registerSealed(compactedSegment); regErr != nil {
				engine.logger.Errorf("failed to seal partially written compacted segment: %v", regErr)
			}
		}
		return err
	}

	for key, logEntry := range compactedLogEntries {
		// Skip entries that were overwritten by a concurrent write to a
		// segment outside the compacted set (a newer live entry exists).
		if current := engine.state.Get(key); current != nil {
			if _, stale := compactedSegments[segmentIDOfIndex(current)]; !stale {
				continue
			}
		}

		// Enforce the segment max size when creating compacted segments.
		if compactedSegment == nil || compactedSegment.getEntriesCount() >= maxSize {
			if compactedSegment != nil {
				if err := registerSealed(compactedSegment); err != nil {
					return fail(err)
				}
			}

			newSegment, err := newDataSegment(engine.layout)
			if err != nil {
				return fail(err)
			}
			compactedSegment = newSegment
		}

		_, logEntryIndex, err := compactedSegment.addLogEntry(logEntry, engine.enableCompression)
		if err != nil {
			return fail(err)
		}

		if logEntry.IsDeleted {
			// Drop tombstones: all older values for this key were in the
			// compacted segments and are not rewritten, so the key can be
			// removed from the index entirely.
			engine.state.removeIfSegmentUnchanged(key, compactedSegments)
		} else {
			engine.state.setIfSegmentUnchanged(key, logEntryIndex, compactedSegments)
		}
	}

	if compactedSegment != nil {
		if err := registerSealed(compactedSegment); err != nil {
			return fail(err)
		}
	}

	if err := engine.snapshot(); err != nil {
		return err
	}

	return nil
}

func (engine *Engine) cleanUpStaleSegments(segmentsToDelete []segmentContext) error {
	staleSegments := make(map[string]struct{}, len(segmentsToDelete))
	for _, segmentCtx := range segmentsToDelete {
		staleSegments[segmentCtx.id] = struct{}{}
	}

	// Remove any state entries still referencing the stale segments.
	engine.state.RemoveIfSegmentIn(staleSegments)

	for _, segmentCtx := range segmentsToDelete {
		// Dropping from the LRU triggers the evict callback which closes the
		// segment (waiting for in-flight readers first).
		engine.lruSegments.Remove(segmentCtx.id)
		if err := engine.segmentsMetadataList.Remove(segmentCtx.id); err != nil {
			// Segment may not be tracked (e.g. never referenced); continue
			// with the remaining ones.
			engine.logger.Debugf("segment %s not found in metadata list: %v", segmentCtx.id, err)
		}
	}

	return nil
}

// compactSegments compacts data segments by joining closed segments together
// and getting rid of duplicate log entries by keys. The active segment is
// never compacted. On any persist error it returns immediately without
// deleting any segment files.
func (engine *Engine) compactSegments() error {
	engine.logger.Debug("compacting segments")

	start := time.Now()
	segmentsToDelete := make([]segmentContext, 0)
	files := make([]fs.DirEntry, 0)

	defer func() {
		EngineOperationDurationMilliseconds.WithLabelValues(CompactSegmentsOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		EngineOperationDurationNanoseconds.WithLabelValues(CompactSegmentsOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		EngineSegmentsToDelete.Set(float64(len(segmentsToDelete)))
		EngineFilesToCompact.WithLabelValues(CompactSegmentsOperation).Set(float64(len(files)))
		EngineSegmentsCompactionCount.Inc()
	}()

	dirFiles, err := os.ReadDir(engine.layout.segmentsPath())
	if err != nil {
		return err
	}
	files = dirFiles

	// The active segment must never be compacted: compare by segment ID
	// under the segment mutex (not by file name).
	engine.segmentMutex.RLock()
	activeSegmentID := engine.segment.id
	engine.segmentMutex.RUnlock()

	// One consistent state snapshot grouped per segment.
	stateSnapshot := engine.state.Snapshot()
	indexesPerSegment := make(map[string][]*protos.LogEntryIndex)
	for _, logEntryIndex := range stateSnapshot {
		segmentID := segmentIDOfIndex(logEntryIndex)
		if segmentID == "" {
			continue
		}
		indexesPerSegment[segmentID] = append(indexesPerSegment[segmentID], logEntryIndex)
	}

	segmentsToDeleteChan := make(chan segmentContext, len(files))
	compactedSegmentEntriesChan := make(chan compactedSegmentEntriesContext, len(files))
	compactedSegmentEntries := make([]compactedSegmentEntriesContext, 0, len(files))
	jobCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultsGroup, resultsGroupCtx := errgroup.WithContext(jobCtx)
	resultsGroup.SetLimit(2)

	// collect results from worker routines
	resultsGroup.Go(func() error {
		for s := range compactedSegmentEntriesChan {
			compactedSegmentEntries = append(compactedSegmentEntries, s)
		}
		for s := range segmentsToDeleteChan {
			segmentsToDelete = append(segmentsToDelete, s)
		}
		return nil
	})

	resultsGroup.Go(func() error {
		defer close(compactedSegmentEntriesChan)
		defer close(segmentsToDeleteChan)

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

				segmentID := segmentIDFromFileName(f.Name())
				if segmentID == "" || segmentID == activeSegmentID {
					engine.logger.Debugf("skipping dispatching of file %s - cur segment %s", f.Name(), activeSegmentID)
					return nil
				}

				segmentContentBytes, err := os.ReadFile(filepath.Join(engine.layout.segmentsPath(), f.Name()))
				if err != nil {
					engine.logger.Errorf("failed to read segment %s: %v", f.Name(), err)
					return err
				}

				logEntryIndexes := indexesPerSegment[segmentID]

				info, err := f.Info()
				if err != nil {
					engine.logger.Errorf("unable to get file info for file %s: %v", f.Name(), err)
					return err
				}

				engine.processSegmentJob(compactedSegmentEntriesChan, &jobContext{
					timestamp:       info.ModTime().Unix(),
					logEntriesBytes: segmentContentBytes,
					segmentID:       segmentID,
					logEntryIndexes: logEntryIndexes,
				})

				segmentsToDeleteChan <- segmentContext{
					fileName: f.Name(),
					id:       segmentID,
				}

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

	compactedLogEntries := make(map[string]*protos.LogEntry)
	for _, compactedSegmentEntry := range compactedSegmentEntries {
		for key, logEntry := range compactedSegmentEntry.compactedEntries {
			compactedLogEntries[key] = logEntry
		}
	}

	// The set of segment IDs being compacted (used to detect newer live
	// entries written concurrently and to drop tombstones safely).
	compactedSegments := make(map[string]struct{}, len(segmentsToDelete))
	for _, segmentCtx := range segmentsToDelete {
		compactedSegments[segmentCtx.id] = struct{}{}
	}

	// writes log entries to the compacted segment and indexes it in memory.
	// On error, return immediately: no stale segment may be deleted.
	if err = engine.persistCompactedSegment(compactedLogEntries, compactedSegments); err != nil {
		engine.logger.Errorf("error occurred when persisting compacted segments %v", err)
		return err
	}

	engine.logger.Debugf("processed %d segments", len(compactedSegmentEntries))
	engine.logger.Debugf("wrote %d compacted log entries", len(compactedLogEntries))

	// clean up segment references from memory
	if err = engine.cleanUpStaleSegments(segmentsToDelete); err != nil {
		return err
	}

	engine.logger.Debugf("cleaned up %d segments", len(segmentsToDelete))

	for _, segmentCtx := range segmentsToDelete {
		if err := os.Remove(filepath.Join(engine.layout.segmentsPath(), segmentCtx.fileName)); err != nil {
			engine.logger.Errorf("failed to remove segment file %s: %v", segmentCtx.fileName, err)
		}
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

	dirFiles, err := os.ReadDir(engine.layout.snapshotsPath())
	if err != nil {
		return err
	}
	files = dirFiles

	for _, file := range files {
		info, err := file.Info()
		if err != nil {
			engine.logger.Errorf("unable to get file info for file %s: %v", file.Name(), err)
			continue // skip this file instead of dereferencing nil info
		}

		if now.Sub(info.ModTime()) > engine.snapshotTTLDuration {
			deletedCount++
			if err := os.Remove(filepath.Join(engine.layout.snapshotsPath(), file.Name())); err != nil {
				engine.logger.Errorf("failed to remove snapshot %s: %v", file.Name(), err)
			}
		}
	}

	engine.logger.Debugf("deleted %d snapshots", deletedCount)

	return nil
}

// startCompactors starts the segment and snapshot compaction goroutines.
// Each compactor uses its own ticker so they cannot steal each other's
// ticks. Compaction failures are logged, never panic.
func (engine *Engine) startCompactors(ctx context.Context) error {
	engine.logger.Debug("starting compactor processes")
	wg := new(errgroup.Group)

	// segment compactor
	segmentTicker := time.NewTicker(engine.compactorInterval)
	wg.Go(func() error {
		defer segmentTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				engine.logger.Debug("stopping segments compactor process")
				return nil

			case <-segmentTicker.C:
				logPrefix := "segment compactor"
				engine.isCompactingSegmentsMutex.Lock()
				engine.logger.Debugf("[%s] acquired engine.isCompactingSegmentsMutex", logPrefix)

				isCompactingSegments := engine.isCompactingSegments
				hasSealedSegments := engine.lruSegments.Len() > 0

				if !isCompactingSegments && hasSealedSegments {
					engine.isCompactingSegments = true
					engine.isCompactingSegmentsMutex.Unlock()

					if err := engine.compactSegments(); err != nil {
						engine.logger.Errorf("error compacting segments: %v", err)
					}

					engine.isCompactingSegmentsMutex.Lock()
					engine.isCompactingSegments = false
				}

				engine.isCompactingSegmentsMutex.Unlock()
				engine.logger.Debugf("[%s] engine.isCompactingSegmentsMutex", logPrefix)
			}
		}
	})

	// snapshot compactor
	snapshotTicker := time.NewTicker(engine.compactorInterval)
	wg.Go(func() error {
		defer snapshotTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				engine.logger.Debug("stopping snapshots compactor process")
				return nil

			case <-snapshotTicker.C:
				logPrefix := "snapshot compactor"
				engine.isCompactingSnapshotsMutex.Lock()
				engine.logger.Debugf("[%s] engine.isCompactingSnapshotsMutex", logPrefix)

				if !engine.isCompactingSnapshots {
					engine.isCompactingSnapshots = true
					engine.isCompactingSnapshotsMutex.Unlock()

					if err := engine.compactSnapshots(); err != nil {
						engine.logger.Errorf("error compacting snapshots: %v", err)
					}

					engine.isCompactingSnapshotsMutex.Lock()
					engine.isCompactingSnapshots = false
				}

				engine.isCompactingSnapshotsMutex.Unlock()
				engine.logger.Debugf("[%s] released engine.isCompactingSnapshotsMutex", logPrefix)
			}
		}
	})

	wg.Wait()

	return nil
}

// recover recovers database indexes from the latest snapshot
func (engine *Engine) recover() error {
	engine.logger.Debug("recovering database state from snapshots")

	files, err := os.ReadDir(engine.layout.snapshotsPath())
	if err != nil {
		return err
	}

	latestSnapshotFilename := ""
	var latestTimestamp int64

	for _, file := range files {
		if !strings.Contains(file.Name(), ".snapshot") {
			continue
		}

		timestamp, err := strconv.ParseInt(strings.SplitN(file.Name(), "-", 2)[0], 10, 64)
		if err != nil {
			engine.logger.Warnf("skipping unparseable snapshot file %s: %v", file.Name(), err)
			continue
		}

		// Strictly newer wins; ties (same nanosecond) keep the first file
		// seen in directory order, which is deterministic.
		if latestSnapshotFilename == "" || timestamp > latestTimestamp {
			latestSnapshotFilename = file.Name()
			latestTimestamp = timestamp
		}
	}

	// Nothing to recover from (fresh directory)
	if latestSnapshotFilename == "" {
		return nil
	}

	if err := engine.loadSnapshot(filepath.Join(engine.layout.snapshotsPath(), latestSnapshotFilename)); err != nil {
		return err
	}

	return nil
}

// NewEngine creates a new engine
func NewEngine(config *EngineConfig) (*Engine, error) {
	if config == nil {
		return nil, fmt.Errorf("engine config must not be nil")
	}
	if strings.TrimSpace(config.DataPath) == "" {
		return nil, fmt.Errorf("data path must not be empty")
	}

	segmentMaxSize := config.SegmentMaxSize
	if segmentMaxSize <= 0 {
		segmentMaxSize = defaultSegmentMaxSize
	}

	cacheSize := config.CacheSize
	if cacheSize <= 0 {
		cacheSize = defaultCacheSize
	}

	layout := newDataLayout(config.DataPath)

	for _, dir := range []string{layout.dataDir(), layout.segmentsPath(), layout.snapshotsPath()} {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0777); err != nil {
				return nil, err
			}
		}
	}

	segmentMetadataList := NewSegmentMetadataList()

	segment, err := newDataSegment(layout)
	if err != nil {
		return nil, err
	}

	segmentMetadataList.Add(segment.id)

	ctx, cancel := context.WithCancel(context.Background())
	engine := &Engine{
		state: &EngineState{
			shards: newEngineStateShards(),
			layout: layout,
		},
		layout:                layout,
		segmentMaxSize:        segmentMaxSize,
		segment:               segment,
		segmentsMetadataList:  segmentMetadataList,
		ctx:                   ctx,
		ctxCancelFunc:         cancel,
		compactorInterval:     config.CompactorInterval,
		compactorWorkerCount:  config.CompactorWorkerCount,
		snapshotTTLDuration:   config.SnapshotTTLDuration,
		enableCompression:     config.EnableCompression,
		isCompactingSegments:  false,
		isCompactingSnapshots: false,
		logger:                log.WithField("storage_engine", "hash_index"),
	}
	engine.activeSegment.Store(segment)

	lruCache, err := lru.NewWithEvict(cacheSize, func(k, v interface{}) {
		segment, ok := v.(*dataSegment)
		if !ok {
			return
		}
		engine.logger.Infof("evicting segment %s from the cache", segment.id)
		if err := segment.close(); err != nil {
			engine.logger.Errorf("failed to close segment %s with id %s: %v", segment.fileName, segment.id, err)
		}
	})
	if err != nil {
		cancel()
		segment.close()
		return nil, err
	}
	engine.lruSegments = lruCache
	// Note: the active write segment is intentionally NOT added to the LRU.
	// It is registered once it is sealed during rollover, so it can never be
	// evicted (and closed) while still accepting writes.

	engine.logger.Info("attempting to recover database")

	recoverable, err := engine.isRecoverable()
	if err != nil {
		cancel()
		segment.close()
		return nil, err
	}

	if recoverable {
		if err = engine.recover(); err != nil {
			cancel()
			segment.close()
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

func newEngineStateShards() []engineStateShard {
	shards := make([]engineStateShard, engineStateShardCount)
	for i := range shards {
		shards[i].indexes = make(map[string]indexEntry)
	}
	return shards
}
