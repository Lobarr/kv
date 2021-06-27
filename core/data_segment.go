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

var (
	AddLogEntryDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_add_log_entry_duration_nanoseconds",
		Help: "how long it takes to add a log entry to a data segment",
	}, []string{"segment_id", "key", "value_size"})

	AddLogEntryDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_add_log_entry_duration_milliseconds",
		Help: "how long it takes to add a log entry to a data segment",
	}, []string{"segment_id", "key", "value_size"})

	GetLogEntryDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_get_log_entry_duration_nanoseconds",
		Help: "how long it takes to get a log entry to a data segment",
	}, []string{"segment_id", "key", "value_size"})

	GetLogEntryDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_get_log_entry_duration_milliseconds",
		Help: "how long it takes to get a log entry to a data segment",
	}, []string{"segment_id", "key", "value_size"})

	CloseDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_close_duration_nanoseconds",
		Help: "how long it takes to close a data segment",
	}, []string{"segment_id", "entries_count", "file_size"})

	CloseDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_close_duration_milliseconds",
		Help: "how long it takes to close a data segment",
	}, []string{"segment_id", "entries_count", "file_size"})

	SegmentEntriesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "data_segment_entries_count",
		Help: "number of log entries in a data segment",
	}, []string{"segment_id"})

	NewDataSegmentDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "new_data_segment_duration_milliseconds",
		Help: "how long it takes to initialize a new data segment",
	}, []string{"segment_id"})

	NewDataSegmentDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "new_data_segment_duration_nanoseconds",
		Help: "how long it takes to initialize a new data segment",
	}, []string{"segment_id"})

	LoadDataSegmentDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "load_data_segment_duration_milliseconds",
		Help: "how long it takes to load a data segment from disk",
	}, []string{"segment_id"})

	LoadDataSegmentDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "load_data_segment_duration_nanoseconds",
		Help: "how long it takes to load a data segment from disk",
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
	start := time.Now()
	ds.logger.Debugf("adding log entry %s to segment", logEntry.Key)

	if ds.isClosed {
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

	AddLogEntryDurationNanoseconds.WithLabelValues(
		ds.id, logEntry.Key, fmt.Sprint((logEntry.Value)),
	).Observe(
		float64(time.Since(start).Nanoseconds()),
	)
	AddLogEntryDurationMilliseconds.WithLabelValues(
		ds.id, logEntry.Key, fmt.Sprint((logEntry.Value)),
	).Observe(
		float64(time.Since(start).Milliseconds()),
	)
	SegmentEntriesCount.WithLabelValues(ds.id).Inc()

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
	ds.mu.RLock()
	defer ds.mu.RUnlock()

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

	GetLogEntryDurationNanoseconds.WithLabelValues(
		ds.id,
		logEntry.Key,
		fmt.Sprint(len(logEntry.Value)),
	).Observe(
		float64(time.Since(start).Nanoseconds()),
	)
	GetLogEntryDurationMilliseconds.WithLabelValues(
		ds.id,
		logEntry.Key,
		fmt.Sprint(len(logEntry.Value)),
	).Observe(
		float64(time.Since(start).Milliseconds()),
	)

	return logEntry, nil
}

// close closes the data segment
func (ds *dataSegment) close() error {
	start := time.Now()
	ds.mu.Lock()
	defer ds.mu.Unlock()

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

		CloseDurationNanoseconds.WithLabelValues(
			ds.id,
			fmt.Sprint(ds.entriesCount),
			fmt.Sprint(fileStat.Size()),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)
		CloseDurationMilliseconds.WithLabelValues(
			ds.id,
			fmt.Sprint(ds.entriesCount),
			fmt.Sprint(fileStat.Size()),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
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

	NewDataSegmentDurationNanoseconds.WithLabelValues(segment.id).Observe(
		float64(time.Since(start).Nanoseconds()),
	)
	NewDataSegmentDurationMilliseconds.WithLabelValues(segment.id).Observe(
		float64(time.Since(start).Milliseconds()),
	)

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

	LoadDataSegmentDurationNanoseconds.WithLabelValues(
		segment.id,
	).Observe(
		float64(time.Since(start).Nanoseconds()),
	)
	LoadDataSegmentDurationMilliseconds.WithLabelValues(
		segment.id,
	).Observe(
		float64(time.Since(start).Milliseconds()),
	)

	return segment, nil
}
