package core

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"kv/protos"
	"os"
	"path"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

var enableCompression = flag.Bool("enable_compression", false, "whether to enable compression or not")

const (
	addLogEntryOperation       = "add_log_entry"
	getLogEntryOperation       = "get_log_entry"
	closeDataSegmentOperation  = "close_data_segment"
	createDataSegmentOperation = "create_data_segment"
	loadDataSegmentOperation   = "load_data_segment"
)

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
	mu           *sync.RWMutex //mutex that synchronizes access
	entriesCount int           // number of entries stored in the segment
	file         *os.File      // open file descriptor of segment
	fileName     string        // filename of segment on disk
	id           string        // unique identifier of the segment
	isClosed     bool          // indicator of state of data segment (open or closed)
	offset       int64         // current latest offset to write new log entries
	logger       log.FieldLogger
}

// addLogEntry adds a log entry to the data segment
func (ds *dataSegment) addLogEntry(logEntry *protos.LogEntry) (*protos.LogEntryIndex, error) {
	start := time.Now()
	defer func() {
		DataSegmentOperationDurationNanoseconds.WithLabelValues(
			ds.id, addLogEntryOperation).Observe(float64(time.Since(start).Nanoseconds()))
		DataSegmentOperationDurationMilliseconds.WithLabelValues(
			ds.id, addLogEntryOperation).Observe(float64(time.Since(start).Milliseconds()))
	}()

	logEntryBytes, err := encodeLogEntry(logEntry)
	if err != nil {
		return nil, err
	}

	if *enableCompression {
		logEntryBytes, err = compressBytes(logEntryBytes)
		if err != nil {
			return nil, err
		}
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.isClosed {
		return nil, ErrClosedDataSegment
	}

	// seek to the end of the file
	startOffset, err := ds.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	// ds.logger.Debugf("added log entry %v to segment %s", logEntry, ds.id)

	bytesWrittenSize, err := ds.file.WriteAt(logEntryBytes, startOffset)
	if err != nil {
		return nil, err
	}

	ds.offset += int64(bytesWrittenSize)
	ds.entriesCount++
	// ds.logger.Infof("new data segment state : entry offset = (%d - %d) new offset = %d entriesCount = %d",
	// 	startOffset, startOffset+int64(bytesWrittenSize), ds.offset, ds.entriesCount)

	DataSegmentLogEntryKeySizes.Observe(float64(len(logEntry.Key)))
	DataSegmentLogEntryValueSizes.Observe(float64(len(logEntry.Value)))
	DataSegmentLogEntrySizes.WithLabelValues(ds.id).Observe(float64(len(logEntryBytes)))
	DataSegmentLogEntryCount.WithLabelValues(ds.id).Inc()

	return &protos.LogEntryIndex{
		Key:             logEntry.Key,
		EntrySize:       int64(bytesWrittenSize),
		SegmentFilename: ds.fileName,
		Offset:          startOffset,
	}, nil
}

// getLogEntry retrives the log entry from the data segment
func (ds *dataSegment) getLogEntry(logEntryIndex *protos.LogEntryIndex) (*protos.LogEntry, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	start := time.Now()
	defer func() {
		DataSegmentOperationDurationNanoseconds.WithLabelValues(
			ds.id, getLogEntryOperation).Observe(float64(time.Since(start).Nanoseconds()))
		DataSegmentOperationDurationMilliseconds.WithLabelValues(
			ds.id, getLogEntryOperation).Observe(float64(time.Since(start).Milliseconds()))
	}()

	fileInfo, err := ds.file.Stat()
	if err != nil {
		return nil, err
	}
	if logEntryIndex.Offset+logEntryIndex.EntrySize > fileInfo.Size() {
		return nil, fmt.Errorf("error getting entry: offset + entry size (%d) exceeds file size (%d)", logEntryIndex.Offset+logEntryIndex.EntrySize, fileInfo.Size())
	}

	// ds.logger.Debugf("[%s] retrieving log entry with index %v", logEntryIndex.Key, logEntryIndex)

	logEntryBytes := make([]byte, logEntryIndex.EntrySize)
	_, err = ds.file.ReadAt(logEntryBytes, logEntryIndex.Offset)
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("error getting entry: reached end of file while reading")
		}
		fmt.Printf("error getting entry for %s with index %v: %v\n", logEntryIndex.Key, logEntryIndex, err)
		return nil, err
	}

	if *enableCompression {
		logEntryBytes, err = uncompressBytes(logEntryBytes)
		if err != nil {
			return nil, err
		}
	}

	logEntry, err := decodeLogEntry(logEntryBytes)
	if err != nil {
		return nil, err
	}

	DataSegmentLogEntryKeySizes.Observe(float64(len(logEntryIndex.Key)))
	DataSegmentLogEntryValueSizes.Observe(float64(len(logEntry.Value)))
	DataSegmentLogEntrySizes.WithLabelValues(ds.id).Observe(float64(logEntryIndex.EntrySize))

	return logEntry, nil
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

	if !ds.isClosed {
		ds.logger.Debugf("closing data segment %s with id %s", ds.fileName, ds.id)

		fileStat, err := ds.file.Stat()
		if err != nil {
			return err
		}

		if err = ds.file.Close(); err != nil {
			return err
		}

		ds.isClosed = true
		DataSegmentFileSizes.Observe(float64(fileStat.Size()))
	}

	return nil
}

func (ds *dataSegment) getEntriescount() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.entriesCount
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

	segment := &dataSegment{
		mu:           new(sync.RWMutex),
		entriesCount: 0,
		file:         file,
		fileName:     fileName,
		id:           id,
		isClosed:     false,
		offset:       0,
		logger: log.WithFields(log.Fields{
			"fileName": fileName,
			"id":       id,
		}),
	}

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
		mu:           new(sync.RWMutex),
		entriesCount: -1,
		file:         file,
		fileName:     fileName,
		id:           id,
		isClosed:     true,
		offset:       -1,
		logger: log.WithFields(log.Fields{
			"fileName": fileName,
			"id":       id,
		}),
	}

	DataSegmentOperationDurationMilliseconds.WithLabelValues(
		segment.id, createDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	DataSegmentOperationDurationNanoseconds.WithLabelValues(
		segment.id, createDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))

	return segment, nil
}
