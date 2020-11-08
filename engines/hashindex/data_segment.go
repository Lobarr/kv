package hashindex

import (
	"errors"
	"fmt"
	"os"
	"path"
	"storage-engines/core"
	"sync"

	"github.com/google/uuid"
)

var ErrClosedDataSegment = errors.New("data segment closed")

type dataSegment struct {
	entriesCount int
	file         *os.File
	fileName     string
	id           string
	isClosed     bool
	mu           *sync.RWMutex
	offset       int64
}

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
	bytesWrittenSize, err := ds.file.WriteAt(logEntryBytes, startOffset)

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

func (ds *dataSegment) close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := ds.file.Close(); err != nil {
		return err
	}

	ds.isClosed = true

	return nil
}

func computeDataSegmentFileName(id string) string {
	return path.Join(getSegmentsPath(), fmt.Sprintf("%s.segment", id))
}

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
