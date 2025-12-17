package core

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"kv/protos"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

var enableCompression = flag.Bool("enable_compression", false, "whether to enable compression or not")
var inMemoryMode = flag.Bool("in_memory", false, "skip disk writes for maximum performance (benchmark mode)")

const (
	addLogEntryOperation       = "add_log_entry"
	getLogEntryOperation       = "get_log_entry"
	closeDataSegmentOperation  = "close_data_segment"
	createDataSegmentOperation = "create_data_segment"
	loadDataSegmentOperation   = "load_data_segment"
)

// Read buffer pool to reduce allocations on hot read path
var readBufferPool = sync.Pool{
	New: func() interface{} {
		// Allocate 4KB initial buffer, will grow as needed
		return make([]byte, 4096)
	},
}

var (
	DataSegmentOperationDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_operation_duration_ns",
		Help: "how long it takes to perform a data segment operation in nanoseconds",
	}, []string{"segment_id", "operation"})

	DataSegmentOperationDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_operation_duration_ms",
		Help: "how long it takes to perform a data segment operation in milliseconds",
	}, []string{"segment_id", "operation"})

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

	DataSegmentLogEntryCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "data_segment_log_entry_count",
		Help: "number of log entries in a data segment",
	}, []string{"segment_id"})

	DataSegmentLogEntrySizes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_log_entry_sizes",
		Help: "size of log entries in a data segment",
	}, []string{"segment_id"})
)

func init() {
	prometheus.Register(DataSegmentOperationDurationNanoseconds)
	prometheus.Register(DataSegmentOperationDurationMilliseconds)
	prometheus.Register(DataSegmentFileSizes)
	prometheus.Register(DataSegmentLogEntryKeySizes)
	prometheus.Register(DataSegmentLogEntryValueSizes)
	prometheus.Register(DataSegmentLogEntryCount)
	prometheus.Register(DataSegmentLogEntrySizes)
}

// ErrClosedDataSegment occurs when an attempt to write to a closed data segment is made
var ErrClosedDataSegment = errors.New("data segment closed")

// dataSegment represents a portion of the data stored by the data store that
// is bounded by an upper limit of number of entries
type dataSegment struct {
	mu           *sync.RWMutex // mutex that synchronizes access
	entriesCount atomic.Int64  // number of entries stored in the segment
	file         *os.File      // open file descriptor of segment
	writer       *bufio.Writer // buffered writer for async writes
	fileName     string        // filename of segment on disk
	id           string        // unique identifier of the segment
	isClosed     atomic.Bool   // indicator of state of data segment (open or closed)
	offset       atomic.Int64  // current latest offset to write new log entries
	dirty        atomic.Bool   // true if buffer has unflushed data
	mapped       []byte        // memory-mapped content (read-only)
	logger       log.FieldLogger
}

// addLogEntry adds a log entry to the data segment
func (ds *dataSegment) addLogEntry(logEntry *protos.LogEntry) (string, *protos.LogEntryIndex, error) {
	// Encode outside the lock to reduce critical section
	logEntryBytes, err := encodeLogEntry(logEntry)
	if err != nil {
		return "", nil, err
	}

	if *enableCompression {
		logEntryBytes, err = compressBytes(logEntryBytes)
		if err != nil {
			return "", nil, err
		}
	}

	bytesLen := int64(len(logEntryBytes))

	// Fast check if closed (atomic read)
	if ds.isClosed.Load() {
		return "", nil, ErrClosedDataSegment
	}

	ds.mu.Lock()
	// Double-check under lock
	if ds.isClosed.Load() {
		ds.mu.Unlock()
		return "", nil, ErrClosedDataSegment
	}

	startOffset := ds.offset.Load()

	// Skip disk write in memory mode for maximum performance
	if !*inMemoryMode {
		// Use buffered writer for better performance
		if ds.writer != nil {
			_, err = ds.writer.Write(logEntryBytes)
		} else {
			_, err = ds.file.WriteAt(logEntryBytes, startOffset)
		}
		if err != nil {
			ds.mu.Unlock()
			return "", nil, err
		}
		ds.dirty.Store(true) // Mark buffer as dirty
	}

	ds.offset.Add(bytesLen)
	ds.entriesCount.Add(1)
	ds.mu.Unlock()

	return ds.id, &protos.LogEntryIndex{
		Key:             logEntry.Key,
		EntrySize:       bytesLen,
		SegmentFilename: ds.fileName,
		Offset:          startOffset,
	}, nil
}

// getLogEntry retrives the log entry from the data segment
// ReadAt is thread-safe and data once written is immutable, no lock needed
func (ds *dataSegment) getLogEntry(logEntryIndex *protos.LogEntryIndex) (*protos.LogEntry, error) {
	entrySize := int(logEntryIndex.EntrySize)

	// Get buffer from pool, ensure it's large enough
	bufPtr := readBufferPool.Get().([]byte)
	if cap(bufPtr) < entrySize {
		// Allocate larger buffer if needed
		bufPtr = make([]byte, entrySize)
	} else {
		bufPtr = bufPtr[:entrySize]
	}

	_, err := ds.file.ReadAt(bufPtr, logEntryIndex.Offset)
	if err != nil {
		readBufferPool.Put(bufPtr[:cap(bufPtr)]) // Return to pool
		if err == io.EOF {
			return nil, fmt.Errorf("error getting entry: reached end of file while reading")
		}
		return nil, err
	}

	if *enableCompression {
		bufPtr, err = uncompressBytes(bufPtr)
		if err != nil {
			return nil, err
		}
	}

	logEntry, err := decodeLogEntry(bufPtr, logEntryIndex.Key)
	readBufferPool.Put(bufPtr[:cap(bufPtr)]) // Return to pool after decode
	if err != nil {
		return nil, err
	}

	return logEntry, nil
}

// getLogEntryValue retrieves only the value for the specific key, avoiding some allocations
func (ds *dataSegment) getLogEntryValue(logEntryIndex *protos.LogEntryIndex) (string, bool, error) {
	entrySize := int(logEntryIndex.EntrySize)

	ds.mu.RLock()
	// Optimistic mmap read
	if ds.mapped != nil {
		// Boundary check
		if int(logEntryIndex.Offset)+entrySize > len(ds.mapped) {
			ds.mu.RUnlock()
			return "", false, fmt.Errorf("error getting entry: mmap boundary exceeded")
		}

		// Use mapped slice directly - no copy needed yet, usually strings are copied from byte slice anyway
		// decodeLogEntryValue takes []byte, it returns string. String creation copies.
		// So detailed copy happens in decodeLogEntryValue.
		value, isDeleted, err := decodeLogEntryValue(ds.mapped[logEntryIndex.Offset : int(logEntryIndex.Offset)+entrySize])
		ds.mu.RUnlock()
		return value, isDeleted, err
	}
	ds.mu.RUnlock()

	// Fallback to ReadAt (syscall)
	// Get buffer from pool
	bufPtr := readBufferPool.Get().([]byte)
	if cap(bufPtr) < entrySize {
		bufPtr = make([]byte, entrySize)
	} else {
		bufPtr = bufPtr[:entrySize]
	}

	_, err := ds.file.ReadAt(bufPtr, logEntryIndex.Offset)
	if err != nil {
		readBufferPool.Put(bufPtr[:cap(bufPtr)])
		if err == io.EOF {
			return "", false, fmt.Errorf("error getting entry: reached end of file while reading")
		}
		return "", false, err
	}

	if *enableCompression {
		// Compression not supported on mmap path yet (requires explicit decompress buffer)
		// But here we are in ReadAt path.
		bufPtr, err = uncompressBytes(bufPtr)
		if err != nil {
			return "", false, err
		}
	}

	// Optimized decode just for value
	value, isDeleted, err := decodeLogEntryValue(bufPtr)
	readBufferPool.Put(bufPtr[:cap(bufPtr)])
	return value, isDeleted, err
}

// close closes the data segment
func (ds *dataSegment) close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	start := time.Now()
	defer func() {
		DataSegmentOperationDurationNanoseconds.WithLabelValues(
			ds.id, closeDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))
		DataSegmentOperationDurationMilliseconds.WithLabelValues(
			ds.id, closeDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	}()

	if ds.mapped != nil {
		if err := syscall.Munmap(ds.mapped); err != nil {
			ds.logger.Warnf("failed to munmap segment: %v", err)
		}
		ds.mapped = nil
	}

	if !ds.isClosed.Load() {
		ds.logger.Debugf("closing data segment %s with id %s", ds.fileName, ds.id)

		// Flush buffered writer before closing
		if ds.writer != nil {
			if err := ds.writer.Flush(); err != nil {
				return err
			}
		}

		fileStat, err := ds.file.Stat()
		if err != nil {
			return err
		}

		if err = ds.file.Close(); err != nil {
			return err
		}

		ds.isClosed.Store(true)
		DataSegmentFileSizes.Observe(float64(fileStat.Size()))
	}

	return nil
}

// sealAndMap flushes wrier and memory maps the segment for read optimization
func (ds *dataSegment) sealAndMap() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.isClosed.Load() {
		return nil
	}

	if ds.writer != nil {
		if err := ds.writer.Flush(); err != nil {
			return err
		}
		ds.writer = nil // Discard writer
	}
	ds.dirty.Store(false)

	// Map
	info, err := ds.file.Stat()
	if err == nil {
		size := info.Size()
		if size > 0 {
			mmap, err := syscall.Mmap(int(ds.file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				ds.logger.Warnf("failed to mmap segment during seal: %v", err)
			} else {
				ds.mapped = mmap
				ds.logger.Debugf("mmap sealed segment success, size: %d", size)
			}
		}
	} else {
		return err
	}

	// We keep the file open because if Munmap fails or we don't map, we fallback to ReadAt which needs FD
	// We mark it closed for writing though?
	// The `isClosed` flag prevents writes.
	ds.isClosed.Store(true)

	return nil
}

func (ds *dataSegment) getEntriescount() int {
	return int(ds.entriesCount.Load())
}

// flush flushes the buffered writer to disk
// Optimized to skip lock if not dirty
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
		ds.dirty.Store(false) // Clear dirty flag after flush
	}
	return nil
}

// computeDataSegmentFileName computes filepath of data segment to be stored on
// disk
func computeDataSegmentFileName(id string) string {
	return path.Join(getSegmentsPath(), fmt.Sprintf("%s.segment", id))
}

// newDataSegment create a new data segment
func newDataSegment() (*dataSegment, error) {
	start := time.Now()
	id := uuid.New().String()
	fileName := computeDataSegmentFileName(id)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	// Use 256KB buffer for better write throughput
	writer := bufio.NewWriterSize(file, 256*1024)

	segment := &dataSegment{
		mu:       new(sync.RWMutex),
		file:     file,
		writer:   writer,
		fileName: fileName,
		id:       id,
		logger: log.WithFields(log.Fields{
			"fileName": fileName,
			"id":       id,
		}),
	}
	// Initialize atomic fields
	segment.entriesCount.Store(0)
	segment.isClosed.Store(false)
	segment.offset.Store(0)

	DataSegmentOperationDurationMilliseconds.WithLabelValues(
		segment.id, createDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	DataSegmentOperationDurationNanoseconds.WithLabelValues(
		segment.id, createDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))

	return segment, nil
}

// loadDataSegment loads data segment from disk to memory
func loadDataSegment(id string) (*dataSegment, error) {
	start := time.Now()
	fileName := computeDataSegmentFileName(id)
	file, err := os.Open(fileName)

	if err != nil {
		return nil, err
	}

	segment := &dataSegment{
		mu:       new(sync.RWMutex),
		file:     file,
		fileName: fileName,
		id:       id,
		logger: log.WithFields(log.Fields{
			"fileName": fileName,
			"id":       id,
		}),
	}
	// Initialize atomic fields (loaded segments are already closed, entries unknown)
	segment.entriesCount.Store(-1)
	segment.isClosed.Store(true)
	segment.offset.Store(-1)

	// Attempt to mmap the segment
	info, err := file.Stat()
	if err == nil {
		size := info.Size()
		if size > 0 {
			mmap, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
			if err != nil {
				segment.logger.Warnf("failed to mmap segment: %v", err)
			} else {
				segment.mapped = mmap
				segment.logger.Debugf("mmap segment success, size: %d", size)
			}
		}
	} else {
		segment.logger.Warnf("failed to stat file for mmap: %v", err)
	}

	DataSegmentOperationDurationMilliseconds.WithLabelValues(
		segment.id, createDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	DataSegmentOperationDurationNanoseconds.WithLabelValues(
		segment.id, createDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))

	return segment, nil
}
