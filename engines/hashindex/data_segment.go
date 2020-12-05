package hashindex

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"storage-engines/core"
	"sync"

	"github.com/google/uuid"
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
}

// addLogEntry adds a log entry to the data segment
func (ds *dataSegment) addLogEntry(logEntry *core.LogEntry) (*core.LogEntryIndex, error) {
	if ds.isClosed {
		return nil, ErrClosedDataSegment
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()

	logEntryBytes, err := logEntry.Encode()

	if err != nil {
		return nil, err
	}

	startOffset := ds.offset
	logEntryBytesWithSeperator := bytes.Join([][]byte{logEntryBytes, make([]byte, 0)}, LogEntrySeperator)
	bytesWrittenSize, err := ds.file.WriteAt(logEntryBytesWithSeperator, startOffset)

	if err != nil {
		return nil, err
	}

	ds.offset += int64(bytesWrittenSize)
	ds.entriesCount++

	return &core.LogEntryIndex{
		Key:             logEntry.Key,
		EntrySize:       bytesWrittenSize,
		SegmentFilename: ds.fileName,
		OffSet:          startOffset,
	}, nil
}

// getLogEntry retrives the log entry from the data segment
func (ds *dataSegment) getLogEntry(logEntryIndex *core.LogEntryIndex) (*core.LogEntry, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	logEntry := &core.LogEntry{}
	logEntryBytes := make([]byte, logEntryIndex.EntrySize)
	_, err := ds.file.ReadAt(logEntryBytes, logEntryIndex.OffSet)

	if err != nil {
		return nil, err
	}

	err = logEntry.Decode(logEntryBytes)

	if err != nil {
		return nil, err
	}

	return logEntry, nil
}

// close closes the data segment
func (ds *dataSegment) close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := ds.file.Close(); err != nil {
		return err
	}

	ds.isClosed = true

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
		isClosed:     true,
		offset:       -1,
		mu:           new(sync.RWMutex),
	}, nil
}
