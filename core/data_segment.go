package core

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"kv/protos"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

const (
	addLogEntryOperation       = "add_log_entry"
	getLogEntryOperation       = "get_log_entry"
	closeDataSegmentOperation  = "close_data_segment"
	createDataSegmentOperation = "create_data_segment"
	loadDataSegmentOperation   = "load_data_segment"
	sealDataSegmentOperation   = "seal_data_segment"
)

// Buffer pools (storing *[]byte avoids interface-boxing allocations on Put/Get)
var readBufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 4096)
		return &b
	},
}

var writeBufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 4096)
		return &b
	},
}

// Segment metrics intentionally carry no per-segment (UUID) label to avoid
// unbounded Prometheus cardinality. promauto registers these automatically;
// do not re-register them.
var (
	DataSegmentOperationDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_operation_duration_ns",
		Help: "how long it takes to perform a data segment operation in nanoseconds",
	}, []string{"operation"})

	DataSegmentOperationDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_operation_duration_ms",
		Help: "how long it takes to perform a data segment operation in milliseconds",
	}, []string{"operation"})

	DataSegmentFileSizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "data_segment_file_sizes",
		Help: "size of data segment files in bytes",
	})

	DataSegmentLogEntryKeySizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "data_segment_log_entry_key_sizes",
		Help: "size of data segment keys in bytes",
	})

	DataSegmentLogEntryValueSizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "data_segment_log_entry_value_sizes",
		Help: "size of data segment values in bytes",
	})

	DataSegmentLogEntryCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "data_segment_log_entry_count",
		Help: "number of log entries appended across all data segments",
	})

	DataSegmentLogEntrySizes = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "data_segment_log_entry_sizes",
		Help: "size of log entries appended to data segments",
	})
)

// ErrClosedDataSegment occurs when an attempt to write to a closed data segment is made
var ErrClosedDataSegment = errors.New("data segment closed")

// segment lifecycle states
const (
	segmentStateActive = int32(iota) // accepting writes
	segmentStateSealed               // read-only, flushed, possibly mmapped, file still open
	segmentStateClosed               // file closed, mapping released
)

// segmentDrainBit is set in ds.refs while sealAndMap or close is draining
// in-flight operations and mutating file/writer/mapped fields.
const segmentDrainBit = int64(1) << 62

// dataSegment represents a portion of the data stored by the data store that
// is bounded by an upper limit of number of entries.
//
// Lifecycle: active -> sealed -> closed. Readers and writers pin a segment
// with lock-free atomic refcounting (acquire/acquireWait/release); seal/close
// set segmentDrainBit and wait for refs to reach zero before mutating
// file/writer/mapped, so readers holding a ref need no lock on mapped or ReadAt.
type dataSegment struct {
	mu           sync.Mutex    // guards writer/flush/offset appends while active
	opsMu        sync.Mutex    // slow-path mutex for seal/close drain + Cond
	refsCond     *sync.Cond    // signaled when refs drop to zero or drain finishes
	refs         atomic.Int64  // low bits: active refcount; bit 62: draining
	state        atomic.Int32  // segmentStateActive / Sealed / Closed
	entriesCount atomic.Int64  // number of entries stored in the segment
	file         *os.File      // open file descriptor of segment
	writer       *bufio.Writer // buffered writer for async writes (active only)
	fileName     string        // filename of segment on disk
	id           string        // unique identifier of the segment
	offset       atomic.Int64  // current latest offset to write new log entries
	dirty        atomic.Bool   // true if buffer has unflushed data
	mapped       []byte        // memory-mapped content (read-only), immutable after seal
	logger       log.FieldLogger
}

func newDataSegmentInstance(fileName, id string) *dataSegment {
	ds := &dataSegment{
		fileName: fileName,
		id:       id,
		logger: log.WithFields(log.Fields{
			"fileName": fileName,
			"id":       id,
		}),
	}
	ds.refsCond = sync.NewCond(&ds.opsMu)
	return ds
}

// acquire pins an active segment for a write operation without waiting.
// Lock-free on the hot path; returns false if the segment is no longer active
// or is currently draining.
func (ds *dataSegment) acquire() bool {
	for {
		v := ds.refs.Load()
		if v&segmentDrainBit != 0 || ds.state.Load() != segmentStateActive {
			return false
		}
		if ds.refs.CompareAndSwap(v, v+1) {
			return true
		}
	}
}

// acquireWait pins the segment for a read operation, waiting for any
// in-flight seal/close to complete first. Lock-free on the hot path.
func (ds *dataSegment) acquireWait() bool {
	for {
		v := ds.refs.Load()
		if v&segmentDrainBit == 0 {
			if ds.state.Load() == segmentStateClosed {
				return false
			}
			if ds.refs.CompareAndSwap(v, v+1) {
				return true
			}
			continue
		}
		ds.opsMu.Lock()
		for ds.refs.Load()&segmentDrainBit != 0 {
			ds.refsCond.Wait()
		}
		ds.opsMu.Unlock()
	}
}

// release unpins the segment after an acquire. Lock-free on the hot path.
func (ds *dataSegment) release() {
	newV := ds.refs.Add(-1)
	if newV&segmentDrainBit != 0 && (newV&^segmentDrainBit) == 0 {
		ds.opsMu.Lock()
		ds.refsCond.Broadcast()
		ds.opsMu.Unlock()
	}
}

// setDrainAndWait sets segmentDrainBit and waits (holding opsMu) until all
// in-flight readers/writers have released the segment.
func (ds *dataSegment) setDrainAndWait() {
	for {
		v := ds.refs.Load()
		if ds.refs.CompareAndSwap(v, v|segmentDrainBit) {
			break
		}
	}
	for (ds.refs.Load() &^ segmentDrainBit) > 0 {
		ds.refsCond.Wait()
	}
}

// clearDrain clears segmentDrainBit and wakes any waiting readers.
// Caller must hold opsMu.
func (ds *dataSegment) clearDrain() {
	for {
		v := ds.refs.Load()
		if ds.refs.CompareAndSwap(v, v&^segmentDrainBit) {
			break
		}
	}
	ds.refsCond.Broadcast()
}

// addKeyValue encodes and appends a key/value entry to the data segment using
// a pooled buffer (zero heap allocations for the entry/buffer when uncompressed).
// The caller must hold a reference (acquire) on the segment.
func (ds *dataSegment) addKeyValue(key, value string, isDeleted bool, enableCompression bool) (string, int64, int64, error) {
	if ds.state.Load() != segmentStateActive {
		return "", 0, 0, ErrClosedDataSegment
	}

	checksum := computeCheckSum(key, value)
	bufPtr := writeBufferPool.Get().(*[]byte)
	encoded := encodeLogEntryInto(*bufPtr, key, value, isDeleted, checksum)
	if cap(encoded) > cap(*bufPtr) {
		*bufPtr = encoded[:cap(encoded)]
	}

	payload := encoded
	if enableCompression {
		var err error
		payload, err = compressBytes(encoded)
		if err != nil {
			writeBufferPool.Put(bufPtr)
			return "", 0, 0, err
		}
	}

	bytesLen := int64(len(payload))

	ds.mu.Lock()
	if ds.state.Load() != segmentStateActive {
		ds.mu.Unlock()
		writeBufferPool.Put(bufPtr)
		return "", 0, 0, ErrClosedDataSegment
	}

	startOffset := ds.offset.Load()
	if _, err := ds.writer.Write(payload); err != nil {
		ds.mu.Unlock()
		writeBufferPool.Put(bufPtr)
		return "", 0, 0, err
	}
	ds.dirty.Store(true)
	ds.offset.Add(bytesLen)
	ds.entriesCount.Add(1)
	ds.mu.Unlock()

	writeBufferPool.Put(bufPtr)
	return ds.id, startOffset, bytesLen, nil
}

// addLogEntry adds a log entry to the data segment. The caller must hold a
// reference (acquire) on the segment and the segment must be active.
func (ds *dataSegment) addLogEntry(logEntry *protos.LogEntry, enableCompression bool) (string, *protos.LogEntryIndex, error) {
	segID, startOffset, bytesLen, err := ds.addKeyValue(logEntry.Key, logEntry.Value, logEntry.IsDeleted, enableCompression)
	if err != nil {
		return "", nil, err
	}
	return segID, &protos.LogEntryIndex{
		Key:             logEntry.Key,
		EntrySize:       bytesLen,
		SegmentFilename: ds.fileName,
		Offset:          startOffset,
	}, nil
}

// getLogEntry retrieves the full log entry at the given index. The caller
// must hold a reference (acquire) on the segment.
func (ds *dataSegment) getLogEntry(logEntryIndex *protos.LogEntryIndex, enableCompression bool) (*protos.LogEntry, error) {
	return ds.getLogEntryAt(logEntryIndex.Key, logEntryIndex.Offset, logEntryIndex.EntrySize, enableCompression)
}

func (ds *dataSegment) getLogEntryAt(knownKey string, offset, entrySize64 int64, enableCompression bool) (*protos.LogEntry, error) {
	entrySize := int(entrySize64)
	if entrySize < 0 {
		return nil, fmt.Errorf("error getting entry: invalid entry size %d", entrySize)
	}

	// Fast path: read directly from the mmap (safe without lock while ref held).
	if mapped := ds.mapped; mapped != nil {
		if offset >= 0 && offset+entrySize64 <= int64(len(mapped)) {
			payload := mapped[offset : offset+entrySize64]
			if enableCompression {
				uncompressed, err := uncompressBytes(payload)
				if err != nil {
					return nil, err
				}
				return decodeLogEntry(uncompressed, knownKey)
			}
			return decodeLogEntry(payload, knownKey)
		}
	}

	// Fallback to ReadAt (pread64 is thread-safe; file stays open while ref held).
	bufPtr := readBufferPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < entrySize {
		buf = make([]byte, entrySize)
		*bufPtr = buf
	}
	buf = buf[:entrySize]

	_, err := ds.file.ReadAt(buf, offset)
	if enableCompression && err == nil {
		var uncompressed []byte
		uncompressed, err = uncompressBytes(buf)
		if err == nil {
			buf = uncompressed
		}
	}

	var entry *protos.LogEntry
	var decodeErr error
	if err == nil {
		entry, decodeErr = decodeLogEntry(buf, knownKey)
	}
	readBufferPool.Put(bufPtr)

	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("error getting entry: reached end of file while reading")
		}
		return nil, err
	}

	return entry, decodeErr
}

// getLogEntryValue retrieves only the value for the specific key.
// The caller must hold a reference (acquire) on the segment.
func (ds *dataSegment) getLogEntryValue(logEntryIndex *protos.LogEntryIndex, enableCompression bool) (string, bool, error) {
	return ds.getLogEntryValueAt(logEntryIndex.Offset, logEntryIndex.EntrySize, enableCompression)
}

// getLogEntryValueAt retrieves only the value at (offset, entrySize64) without
// needing a LogEntryIndex proto struct or taking any locks on the read path.
func (ds *dataSegment) getLogEntryValueAt(offset, entrySize64 int64, enableCompression bool) (string, bool, error) {
	entrySize := int(entrySize64)
	if entrySize < 0 {
		return "", false, fmt.Errorf("error getting entry: invalid entry size %d", entrySize)
	}

	// Fast path: mmap read (immutable while ref is held; no lock needed).
	if mapped := ds.mapped; mapped != nil {
		if offset >= 0 && offset+entrySize64 <= int64(len(mapped)) {
			payload := mapped[offset : offset+entrySize64]
			if enableCompression {
				uncompressed, err := uncompressBytes(payload)
				if err != nil {
					return "", false, err
				}
				return decodeLogEntryValue(uncompressed)
			}
			return decodeLogEntryValue(payload)
		}
		return "", false, fmt.Errorf("error getting entry: mmap boundary exceeded")
	}

	// Fallback to ReadAt (pread64 is thread-safe; file stays open while ref held).
	bufPtr := readBufferPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < entrySize {
		buf = make([]byte, entrySize)
		*bufPtr = buf
	}
	buf = buf[:entrySize]

	_, err := ds.file.ReadAt(buf, offset)
	if enableCompression && err == nil {
		var uncompressed []byte
		uncompressed, err = uncompressBytes(buf)
		if err == nil {
			buf = uncompressed
		}
	}

	// Decode BEFORE returning buffer to pool.
	var value string
	var isDeleted bool
	var decodeErr error
	if err == nil {
		value, isDeleted, decodeErr = decodeLogEntryValue(buf)
	}
	readBufferPool.Put(bufPtr)

	if err != nil {
		if err == io.EOF {
			return "", false, fmt.Errorf("error getting entry: reached end of file while reading")
		}
		return "", false, err
	}

	return value, isDeleted, decodeErr
}

// flush flushes the buffered writer to disk
func (ds *dataSegment) flush() error {
	// Fast path: skip if not dirty
	if !ds.dirty.Load() {
		return nil
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.writer != nil {
		if err := ds.writer.Flush(); err != nil {
			return err
		}
	}
	ds.dirty.Store(false)
	return nil
}

// sealAndMap flushes the buffered writer and memory-maps the segment for
// read optimization, transitioning it from active to sealed (read-only).
// Waits for all in-flight readers/writers to release the segment first, and
// keeps drainBit set until flush+mmap finish so subsequent readers see the
// finalized mapped slice without needing any lock.
func (ds *dataSegment) sealAndMap() error {
	start := time.Now()

	ds.opsMu.Lock()
	if ds.state.Load() != segmentStateActive {
		ds.opsMu.Unlock()
		return nil // already sealed or closed
	}
	ds.setDrainAndWait()

	var err error
	if ds.writer != nil {
		if flushErr := ds.writer.Flush(); flushErr != nil {
			err = flushErr
		} else {
			ds.writer = nil // discard writer; segment no longer accepts writes
		}
	}
	if err == nil {
		ds.dirty.Store(false)
		info, statErr := ds.file.Stat()
		if statErr != nil {
			err = statErr
		} else if size := info.Size(); size > 0 {
			mmap, mmapErr := syscall.Mmap(int(ds.file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
			if mmapErr != nil {
				ds.logger.Warnf("failed to mmap segment during seal: %v", mmapErr)
			} else {
				ds.mapped = mmap
				ds.logger.Debugf("mmap sealed segment success, size: %d", size)
			}
		}
	}

	ds.state.Store(segmentStateSealed)
	ds.clearDrain()
	ds.opsMu.Unlock()

	DataSegmentOperationDurationNanoseconds.WithLabelValues(sealDataSegmentOperation).Observe(
		float64(time.Since(start).Nanoseconds()))
	DataSegmentOperationDurationMilliseconds.WithLabelValues(sealDataSegmentOperation).Observe(
		float64(time.Since(start).Milliseconds()))

	return err
}

// close flushes any remaining buffered data, releases the memory mapping and
// closes the file. Waits for all in-flight readers/writers first. Safe to
// call on already sealed or closed segments.
func (ds *dataSegment) close() error {
	start := time.Now()

	ds.opsMu.Lock()
	if ds.state.Load() == segmentStateClosed {
		ds.opsMu.Unlock()
		return nil
	}
	ds.setDrainAndWait()

	var err error
	if ds.mapped != nil {
		if unmapErr := syscall.Munmap(ds.mapped); unmapErr != nil {
			ds.logger.Warnf("failed to munmap segment: %v", unmapErr)
		}
		ds.mapped = nil
	}

	if ds.writer != nil {
		if flushErr := ds.writer.Flush(); flushErr != nil {
			err = flushErr
		}
		ds.writer = nil
	}
	ds.dirty.Store(false)

	var fileSize int64
	if fileStat, statErr := ds.file.Stat(); statErr == nil {
		fileSize = fileStat.Size()
	} else if err == nil {
		err = statErr
	}

	if closeErr := ds.file.Close(); closeErr != nil && err == nil {
		err = closeErr
	}

	if err == nil {
		DataSegmentFileSizes.Observe(float64(fileSize))
	}

	ds.state.Store(segmentStateClosed)
	ds.clearDrain()
	ds.opsMu.Unlock()

	DataSegmentOperationDurationNanoseconds.WithLabelValues(closeDataSegmentOperation).Observe(
		float64(time.Since(start).Nanoseconds()))
	DataSegmentOperationDurationMilliseconds.WithLabelValues(closeDataSegmentOperation).Observe(
		float64(time.Since(start).Milliseconds()))

	return err
}

// getEntriesCount returns the number of entries stored in the segment.
func (ds *dataSegment) getEntriesCount() int {
	return int(ds.entriesCount.Load())
}

// newDataSegment creates a new empty data segment under the given layout
func newDataSegment(layout *dataLayout) (*dataSegment, error) {
	start := time.Now()
	id := uuid.New().String()
	fileName := layout.segmentFileName(id)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Use 256KB buffer for better write throughput
	segment := newDataSegmentInstance(fileName, id)
	segment.file = file
	segment.writer = bufio.NewWriterSize(file, 256*1024)
	segment.state.Store(segmentStateActive)
	segment.offset.Store(0)
	segment.entriesCount.Store(0)

	DataSegmentOperationDurationMilliseconds.WithLabelValues(createDataSegmentOperation).Observe(
		float64(time.Since(start).Milliseconds()))
	DataSegmentOperationDurationNanoseconds.WithLabelValues(createDataSegmentOperation).Observe(
		float64(time.Since(start).Nanoseconds()))

	return segment, nil
}

// loadDataSegment loads a sealed data segment from disk (read-only, mmap-backed)
func loadDataSegment(layout *dataLayout, id string) (*dataSegment, error) {
	start := time.Now()
	fileName := layout.segmentFileName(id)
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	segment := newDataSegmentInstance(fileName, id)
	segment.file = file
	// Loaded segments are immutable: unknown entries/offset, read-only state.
	segment.state.Store(segmentStateSealed)

	// Attempt to mmap the segment
	info, err := file.Stat()
	if err != nil {
		segment.logger.Warnf("failed to stat file for mmap: %v", err)
	} else if size := info.Size(); size > 0 {
		mmap, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			segment.logger.Warnf("failed to mmap segment: %v", err)
		} else {
			segment.mapped = mmap
			segment.logger.Debugf("mmap segment success, size: %d", size)
		}
	}

	DataSegmentOperationDurationMilliseconds.WithLabelValues(loadDataSegmentOperation).Observe(
		float64(time.Since(start).Milliseconds()))
	DataSegmentOperationDurationNanoseconds.WithLabelValues(loadDataSegmentOperation).Observe(
		float64(time.Since(start).Nanoseconds()))

	return segment, nil
}
