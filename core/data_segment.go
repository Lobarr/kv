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

	DataSegmentCompressedLogEntrySizes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_compressed_log_entry_sizes",
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
	prometheus.Register(DataSegmentCompressedLogEntrySizes)
}

// ErrClosedDataSegment occurs when an attempt to write to a closed data segment is made
var ErrClosedDataSegment = errors.New("data segment closed")

// LogEntrySeperator represents seperator used between log entries on disk
var LogEntrySeperator = []byte("\n")

// dataSegment represents a portion of the data stored by the data store that
// is bounded by an upper limit of number of entries
type dataSegment struct {
	entriesCount      int           // number of entries stored in the segment
	entriesCountMutex *sync.RWMutex // mutex that synchronizes entriesCount
	file              *os.File      // open file descriptor of segment
	fileMutex         *sync.RWMutex // mutex that synchronizes file
	fileName          string        // filename of segment on disk
	id                string        // unique identifier of the segment
	idMutex           *sync.RWMutex // mutex that synchronizes id
	isClosed          bool          // indicator of state of data segment (open or closed)
	isClosedMutex     *sync.RWMutex // mutex that synchronizes isClosed
	offset            int64         // current latest offset to write new log entries
	offsetMutex       *sync.RWMutex //mutex that synchronizes offset
	logger            log.FieldLogger
}

// addLogEntry adds a log entry to the data segment
func (ds *dataSegment) addLogEntry(logEntry *LogEntry) (*LogEntryIndex, error) {
	start := time.Now()

	ds.idMutex.RLock()
	segmentID := ds.id
	ds.idMutex.RUnlock()

	ds.logger.Debugf("adding log entry %s to segment %s", logEntry.Key, segmentID)

	defer func() {
		DataSegmentOperationDurationNanoseconds.WithLabelValues(
			segmentID, AddLogEntryOperation).Observe(float64(time.Since(start).Nanoseconds()))
		DataSegmentOperationDurationMilliseconds.WithLabelValues(
			segmentID, AddLogEntryOperation).Observe(float64(time.Since(start).Milliseconds()))
	}()

	ds.isClosedMutex.RLock()
	isClosed := ds.isClosed
	ds.isClosedMutex.RUnlock()

	if isClosed {
		return nil, ErrClosedDataSegment
	}

	logEntryBytes, err := logEntry.Encode()
	if err != nil {
		return nil, err
	}

	compressedLogEntryBytes, err := compressBytes(logEntryBytes)
	if err != nil {
		return nil, err
	}

	ds.logger.Debugf("writing compressed bytes of size %d for log entry %s to segment %s",
		len(compressedLogEntryBytes), logEntry.Key, segmentID)

	ds.offsetMutex.RLock()
	startOffset := ds.offset
	ds.offsetMutex.RUnlock()

	logEntryBytesWithSeperator := bytes.Join([][]byte{compressedLogEntryBytes, make([]byte, 0)}, LogEntrySeperator)
	bytesWrittenSize, err := ds.file.WriteAt(logEntryBytesWithSeperator, startOffset)

	if err != nil {
		return nil, err
	}

	ds.offsetMutex.Lock()
	ds.offset += int64(bytesWrittenSize)
	ds.offsetMutex.Unlock()
	ds.entriesCountMutex.Lock()
	ds.entriesCount++
	ds.entriesCountMutex.Unlock()

	ds.offsetMutex.RLock()
	ds.entriesCountMutex.RLock()
	ds.logger.Debugf("new data segment state : offset = %d entriesCount = %d", ds.offset, ds.entriesCount)
	ds.offsetMutex.RUnlock()
	ds.entriesCountMutex.RUnlock()

	DataSegmentLogEntryKeySizes.Observe(float64(len(logEntry.Key)))
	DataSegmentLogEntryValueSizes.Observe(float64(len(logEntry.Value)))
	DataSegmentLogEntrySizes.WithLabelValues(segmentID).Observe(float64(len(logEntryBytes)))
	DataSegmentLogEntryCount.WithLabelValues(segmentID).Inc()
	DataSegmentCompressedLogEntrySizes.WithLabelValues(segmentID).Observe(float64(len(compressedLogEntryBytes)))

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
	start := time.Now()
	ds.idMutex.RLock()
	segmentID := ds.id
	ds.idMutex.RUnlock()

	defer func() {
		DataSegmentOperationDurationNanoseconds.WithLabelValues(
			segmentID, GetLogEntryOperation).Observe(float64(time.Since(start).Nanoseconds()))
		DataSegmentOperationDurationMilliseconds.WithLabelValues(
			segmentID, GetLogEntryOperation).Observe(float64(time.Since(start).Milliseconds()))
	}()

	ds.logger.Debugf("retrieving log entry for key %s", logEntryIndex.Key)

	compressedLogEntryBytes := make([]byte, logEntryIndex.CompressedEntrySize)
	_, err := ds.file.ReadAt(compressedLogEntryBytes, logEntryIndex.OffSet)
	if err != nil {
		return nil, err
	}

	logEntryBytes, err := uncompressBytes(compressedLogEntryBytes)
	if err != nil {
		return nil, err
	}

	logEntry := &LogEntry{}
	err = logEntry.Decode(logEntryBytes)

	if err != nil {
		return nil, err
	}

	DataSegmentLogEntryKeySizes.Observe(float64(len(logEntryIndex.Key)))
	DataSegmentLogEntryValueSizes.Observe(float64(len(logEntry.Value)))
	DataSegmentLogEntrySizes.WithLabelValues(segmentID).Observe(float64(logEntryIndex.EntrySize))
	DataSegmentCompressedLogEntrySizes.WithLabelValues(segmentID).Observe(float64(logEntryIndex.CompressedEntrySize))

	return logEntry, nil
}

// close closes the data segment
func (ds *dataSegment) close() error {
	start := time.Now()
	ds.idMutex.RLock()
	segmentID := ds.id
	ds.idMutex.RUnlock()

	defer func() {
		DataSegmentOperationDurationNanoseconds.WithLabelValues(
			segmentID, CloseDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))
		DataSegmentOperationDurationMilliseconds.WithLabelValues(
			segmentID, CloseDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	}()

	ds.isClosedMutex.RLock()
	isClosed := ds.isClosed
	ds.isClosedMutex.RUnlock()

	if !isClosed {
		ds.logger.Debug("closing data segment")

		ds.fileMutex.RLock()
		fileStat, err := ds.file.Stat()
		ds.fileMutex.RUnlock()

		if err != nil {
			return err
		}

		ds.fileMutex.Lock()
		err = ds.file.Close()
		ds.fileMutex.Unlock()

		if err != nil {
			return err
		}

		ds.isClosedMutex.Lock()
		ds.isClosed = true
		ds.isClosedMutex.Unlock()

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
		entriesCount:      0,
		entriesCountMutex: new(sync.RWMutex),
		file:              file,
		fileMutex:         new(sync.RWMutex),
		fileName:          fileName,
		id:                id,
		idMutex:           new(sync.RWMutex),
		isClosed:          false,
		isClosedMutex:     new(sync.RWMutex),
		offset:            0,
		offsetMutex:       new(sync.RWMutex),
		logger: log.WithFields(log.Fields{
			"fileName": fileName,
			"id":       id,
		}),
	}

	DataSegmentOperationDurationMilliseconds.WithLabelValues(
		segment.id, CreateDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	DataSegmentOperationDurationNanoseconds.WithLabelValues(
		segment.id, CreateDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))

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
		entriesCount:      -1,
		entriesCountMutex: new(sync.RWMutex),
		file:              file,
		fileMutex:         new(sync.RWMutex),
		fileName:          fileName,
		id:                id,
		idMutex:           new(sync.RWMutex),
		isClosed:          false,
		isClosedMutex:     new(sync.RWMutex),
		offset:            -1,
		offsetMutex:       new(sync.RWMutex),
		logger: log.WithFields(log.Fields{
			"fileName": fileName,
			"id":       id,
		}),
	}

	DataSegmentOperationDurationMilliseconds.WithLabelValues(
		segment.id, CreateDataSegmentOperation).Observe(float64(time.Since(start).Milliseconds()))
	DataSegmentOperationDurationNanoseconds.WithLabelValues(
		segment.id, CreateDataSegmentOperation).Observe(float64(time.Since(start).Nanoseconds()))

	return segment, nil
}
