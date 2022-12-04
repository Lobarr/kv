package core

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

const (
	AddLogEntryOperation       = "add_log_entry"
	GetLogEntryOperation       = "get_log_entry"
	CloseDataSegmentOperation  = "close_data_segment"
	CreateDataSegmentOperation = "create_data_segment"
	LoadDataSegmentOperation   = "load_data_segment"
)

var (
	DataSegmentOperationDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_operation_duration_nanoseconds",
		Help: "how long it takes to perform a data segment operation",
	}, []string{"segment_id", "operation"})

	DataSegmentOperationDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_operation_duration_milliseconds",
		Help: "how long it takes to perform a data segment operation",
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

	DataSegmentRawLogEntrySizes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_raw_log_entry_sizes",
		Help: "size of log entries in a data segment",
	}, []string{"segment_id"})

	DataSegmentCompressedLogEntrySizes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_compressed_log_entry_sizes",
		Help: "size of log entries in a data segment",
	}, []string{"segment_id"})
)

//ErrClosedDataSegment occurs when an attempt to write to a closed data segment is made
var ErrClosedDataSegment = errors.New("data segment closed")

//LogEntrySeperator represents seperator used between log entries on disk
var LogEntrySeperator = []byte("\n")

// dataSegment represents a portion of the data stored by the data store that
// is bounded by an upper limit of number of entries
type dataSegment struct {
	entriesCount int           // number of entries stored in the segment
	file         *os.File      // open file descriptor of segment
	fileName     string        // filename of segment on disk
	id           string        // unique identifier of the segment
	isClosed     bool          // indicator of state of data segment (open or closed)
	mu           *sync.RWMutex // mutex used to synchronize access and avoid race conditions
	offset       int64         // current latest offset to write new log entries
	logger       log.FieldLogger
}

// addLogEntry adds a log entry to the data segment
func (ds *dataSegment) addLogEntry(logEntry *LogEntry) (*LogEntryIndex, error) {
	var logEntryBytes []byte
	var compressedLogEntryBytes []byte
	var err error
	start := time.Now()

	ds.logger.Debugf("adding log entry %s to segment %s", logEntry.Key, ds.id)

	defer func() {
		if err != nil {
			return
		}

		DataSegmentLogEntryCount.WithLabelValues(ds.id).Inc()
		DataSegmentOperationDurationNanoseconds.WithLabelValues(ds.id, AddLogEntryOperation).Observe(float64(time.Since(start).Nanoseconds()))
		DataSegmentOperationDurationMilliseconds.WithLabelValues(ds.id, AddLogEntryOperation).Observe(float64(time.Since(start).Milliseconds()))
		DataSegmentLogEntryKeySizes.Observe(float64(len(logEntry.Key)))
		DataSegmentLogEntryValueSizes.Observe(float64(len(logEntry.Value)))
		DataSegmentRawLogEntrySizes.WithLabelValues(ds.id).Observe(float64(len(logEntryBytes)))
		DataSegmentCompressedLogEntrySizes.WithLabelValues(ds.id).Observe(float64(len(compressedLogEntryBytes)))
	}()

	if ds.isClosed {
		return nil, ErrClosedDataSegment
	}

	logEntryBytes, err = logEntry.Encode()
	if err != nil {
		return nil, err
	}

	compressedLogEntryBytes, err = compressBytes(logEntryBytes)
	if err != nil {
		return nil, err
	}

	ds.logger.Debugf("writing compressed bytes of size %d for log entry %s to segment %s", len(compressedLogEntryBytes), logEntry.Key, ds.id)

	ds.mu.Lock()
	defer ds.mu.Unlock()

	startOffset := ds.offset
	logEntryBytesWithSeperator := bytes.Join([][]byte{compressedLogEntryBytes, make([]byte, 0)}, LogEntrySeperator)
	bytesWrittenSize, err := ds.file.WriteAt(logEntryBytesWithSeperator, startOffset)

	if err != nil {
		return nil, err
	}

	ds.offset += int64(bytesWrittenSize)
	ds.entriesCount++

	ds.logger.Debugf("new data segment state : offset = %d entriesCount = %d", ds.offset, ds.entriesCount)

	return &LogEntryIndex{
		Key:                 logEntry.Key,
		EntrySize:           len(logEntryBytes),
		CompressedEntrySize: len(compressedLogEntryBytes),
		SegmentFilename:     ds.fileName,
		OffSet:              startOffset,
	}, nil
}

// getLogEntry retrives the log entry from the data segment
func (ds *dataSegment) getLogEntry(logEntryIndex *LogEntryIndex) (*LogEntry, error) {
	var logEntry *LogEntry
	var logEntryBytes []byte
	var err error
	start := time.Now()
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	defer func() {
		if err != nil {
			return
		}

		DataSegmentOperationDurationNanoseconds.WithLabelValues(ds.id, GetLogEntryOperation).Observe(float64(time.Since(start).Nanoseconds()))
		DataSegmentOperationDurationMilliseconds.WithLabelValues(ds.id, GetLogEntryOperation).Observe(float64(time.Since(start).Milliseconds()))
		DataSegmentLogEntryKeySizes.Observe(float64(len(logEntryIndex.Key)))
		DataSegmentLogEntryValueSizes.Observe(float64(len(logEntry.Value)))
		DataSegmentRawLogEntrySizes.WithLabelValues(ds.id).Observe(float64(logEntryIndex.EntrySize))
		DataSegmentCompressedLogEntrySizes.WithLabelValues(ds.id).Observe(float64(logEntryIndex.CompressedEntrySize))
	}()

	ds.logger.Debugf("retrieving log entry for key %s", logEntryIndex.Key)

	compressedLogEntryBytes := make([]byte, logEntryIndex.CompressedEntrySize)
	_, err = ds.file.ReadAt(compressedLogEntryBytes, logEntryIndex.OffSet)
	if err != nil {
		return nil, err
	}

	logEntryBytes, err = uncompressBytes(compressedLogEntryBytes)
	if err != nil {
		return nil, err
	}

	logEntry = &LogEntry{}
	err = logEntry.Decode(logEntryBytes)

	if err != nil {
		return nil, err
	}

	return logEntry, nil
}

// close closes the data segment
func (ds *dataSegment) close() error {
	var err error
	start := time.Now()
	ds.mu.Lock()
	defer ds.mu.Unlock()

	defer func() {
		if err != nil {
			return
		}

		DataSegmentOperationDurationNanoseconds.WithLabelValues(ds.id, CloseDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))
		DataSegmentOperationDurationMilliseconds.WithLabelValues(ds.id, CloseDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	}()

	if !ds.isClosed {
		ds.logger.Debug("closing data segment")

		fileStat, err := ds.file.Stat()
		if err != nil {
			return err
		}

		if err := ds.file.Close(); err != nil {
			return err
		}

		ds.isClosed = true

		DataSegmentFileSizes.Observe(float64(fileStat.Size()))
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

	segment := &dataSegment{
		entriesCount: 0,
		file:         file,
		fileName:     fileName,
		id:           id,
		isClosed:     false,
		offset:       0,
		mu:           new(sync.RWMutex),
		logger: log.WithFields(log.Fields{
			"fileName": fileName,
			"id":       id,
		}),
	}

	DataSegmentOperationDurationMilliseconds.WithLabelValues(segment.id, CreateDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	DataSegmentOperationDurationNanoseconds.WithLabelValues(segment.id, CreateDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))

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
		entriesCount: -1,
		file:         file,
		fileName:     fileName,
		id:           id,
		isClosed:     false,
		offset:       -1,
		mu:           new(sync.RWMutex),
		logger: log.WithFields(log.Fields{
			"fileName": fileName,
			"id":       id,
		}),
	}

	DataSegmentOperationDurationMilliseconds.WithLabelValues(segment.id, CreateDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	DataSegmentOperationDurationNanoseconds.WithLabelValues(segment.id, CreateDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))

	return segment, nil
}
