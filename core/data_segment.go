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
	addLogEntryDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_add_log_entry_duration_nanoseconds",
		Help: "how long it takes to add a log entry to a data segment",
	}, []string{"segment_id", "key", "value_size"})

	addLogEntryDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_add_log_entry_duration_milliseconds",
		Help: "how long it takes to add a log entry to a data segment",
	}, []string{"segment_id", "key", "value_size"})

	getLogEntryDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_get_log_entry_duration_nanoseconds",
		Help: "how long it takes to get a log entry to a data segment",
	}, []string{"segment_id", "key", "value_size"})

	getLogEntryDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_get_log_entry_duration_milliseconds",
		Help: "how long it takes to get a log entry to a data segment",
	}, []string{"segment_id", "key", "value_size"})

	closeDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_close_duration_nanoseconds",
		Help: "how long it takes to close a data segment",
	}, []string{"segment_id", "entries_count", "file_size"})

	closeDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "data_segment_close_duration_milliseconds",
		Help: "how long it takes to close a data segment",
	}, []string{"segment_id", "entries_count", "file_size"})

	segmentEntriesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "data_segment_entries_count",
		Help: "number of log entries in a data segment",
	}, []string{"segment_id"})

	newDataSegmentDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "new_data_segment_duration_milliseconds",
		Help: "how long it takes to initialize a new data segment",
	}, []string{"segment_id"})

	newDataSegmentDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "new_data_segment_duration_nanoseconds",
		Help: "how long it takes to initialize a new data segment",
	}, []string{"segment_id"})

	loadDataSegmentDurationMilliseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "load_data_segment_duration_milliseconds",
		Help: "how long it takes to load a data segment from disk",
	}, []string{"segment_id"})

	loadDataSegmentDurationNanoseconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
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

	ds.mu.Lock()
	defer ds.mu.Unlock()

	logEntryBytes, err := logEntry.Encode()
	if err != nil {
		return nil, err
	}

	compressedLogEntryBytes, err := compressBytes(logEntryBytes)
	if err != nil {
		return nil, err
	}

	startOffset := ds.offset
	logEntryBytesWithSeperator := bytes.Join([][]byte{compressedLogEntryBytes, make([]byte, 0)}, LogEntrySeperator)
	bytesWrittenSize, err := ds.file.WriteAt(logEntryBytesWithSeperator, startOffset)

	if err != nil {
		return nil, err
	}

	ds.offset += int64(bytesWrittenSize)
	ds.entriesCount++

	ds.logger.Debugf("new data segment state : offset = %d entriesCount = %d", ds.offset, ds.entriesCount)

	addLogEntryDurationNanoseconds.WithLabelValues(
		ds.id, logEntry.Key, fmt.Sprint((logEntry.Value)),
	).Observe(
		float64(time.Since(start).Nanoseconds()),
	)
	addLogEntryDurationMilliseconds.WithLabelValues(
		ds.id, logEntry.Key, fmt.Sprint((logEntry.Value)),
	).Observe(
		float64(time.Since(start).Milliseconds()),
	)
	segmentEntriesCount.WithLabelValues(ds.id).Inc()

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

	getLogEntryDurationNanoseconds.WithLabelValues(
		ds.id,
		logEntry.Key,
		fmt.Sprint(len(logEntry.Value)),
	).Observe(
		float64(time.Since(start).Nanoseconds()),
	)
	getLogEntryDurationMilliseconds.WithLabelValues(
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

		if err := ds.file.Close(); err != nil {
			return err
		}

		ds.isClosed = true
	}

	fileStat, err := ds.file.Stat()
	if err != nil {
		return err
	}

	closeDurationNanoseconds.WithLabelValues(
		ds.id, 
		fmt.Sprint(ds.entriesCount),
		fmt.Sprint(fileStat.Size()),
	).Observe(
		float64(time.Since(start).Nanoseconds()),
	)
	closeDurationMilliseconds.WithLabelValues(
		ds.id, 
		fmt.Sprint(ds.entriesCount),
		fmt.Sprint(fileStat.Size()),
	).Observe(
		float64(time.Since(start).Milliseconds()),
	)

	return nil
}

// computeDataSegmentFileName computes filepath of data segment to be stored on
// disk
func computeDataSegmentFileName(id string) string {
	return path.Join(getSegmentsPath(), fmt.Sprintf("%s.segment", id))
}

// newDataSegment create a new data segment
func newDataSegment() (*dataSegment, error) {
	id := uuid.New().String()
	fileName := computeDataSegmentFileName(id)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		return nil, err
	}

	return &dataSegment{
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
	}, nil
}

// loadDataSegment loads data segment from disk to memory
func loadDataSegment(id string) (*dataSegment, error) {
	fileName := computeDataSegmentFileName(id)
	file, err := os.Open(fileName)

	if err != nil {
		return nil, err
	}

	return &dataSegment{
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
	}, nil
}
