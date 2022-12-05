package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

// ErEErrInvalidLogEntryChecksum occurs when a log entry is corrupted
var ErrInvalidLogEntryChecksum = errors.New("log entry is corrupted")

// computeCheckSum computes the cheksum of a key and it's associated value
func computeCheckSum(key string, value string) uint32 {
	keyHash := sha256.Sum256([]byte(key))
	keyHashHex := hex.EncodeToString(keyHash[:])
	valueHash := sha256.Sum256([]byte(value))
	valueHashHex := hex.EncodeToString([]byte(valueHash[:]))
	checksum := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s:%s", keyHashHex, valueHashHex)))

	return checksum
}

// LogEntry represents an entry of a record intended to be saved in
// the append-only log file
type LogEntry struct {
	Key            string        // used as a reference to identify entry
	keyMutex       *sync.RWMutex // mutex that synchornizes Key
	Value          string        // data to be saved
	valueMutex     *sync.RWMutex // mutex that synchronizes Value
	Checksum       uint32        // used to detect corrupted data
	checksumMutex  *sync.RWMutex // mutex that synchronizes Checksum
	IsDeleted      bool          // used to represent tombstone
	isDeletedMutex *sync.RWMutex // mutex that synchronizes IsDeleted

}

// Encode encodes the log entry to bytes using msgpack
func (le LogEntry) Encode() ([]byte, error) {
	le.keyMutex.RLock()
	defer le.keyMutex.RUnlock()
	le.valueMutex.RLock()
	defer le.valueMutex.RUnlock()
	le.checksumMutex.RLock()
	defer le.checksumMutex.RUnlock()
	le.isDeletedMutex.RLock()
	defer le.isDeletedMutex.RUnlock()
	return msgpack.Marshal(&le)
}

// Decode decodes log entry bytes and loads the properties
func (le *LogEntry) Decode(logEntryBytes []byte) error {
	logEntry := &LogEntry{}
	err := msgpack.Unmarshal(logEntryBytes, logEntry)

	if err != nil {
		return err
	}

	if computeCheckSum(logEntry.Key, logEntry.Value) != logEntry.Checksum {
		return ErrInvalidLogEntryChecksum
	}

	// initialize mutexes if we haven't yet
	if le.keyMutex == nil || le.valueMutex == nil || le.checksumMutex == nil || le.isDeletedMutex == nil {
		le.keyMutex = new(sync.RWMutex)
		le.valueMutex = new(sync.RWMutex)
		le.checksumMutex = new(sync.RWMutex)
		le.isDeletedMutex = new(sync.RWMutex)
	}

	le.keyMutex.Lock()
	le.Key = logEntry.Key
	le.keyMutex.Unlock()

	le.valueMutex.Lock()
	le.Value = logEntry.Value
	le.valueMutex.Unlock()

	le.checksumMutex.Lock()
	le.Checksum = logEntry.Checksum
	le.checksumMutex.Unlock()

	le.isDeletedMutex.Lock()
	le.IsDeleted = logEntry.IsDeleted
	le.isDeletedMutex.Unlock()

	return nil
}

// NewLogEntry creates a new log entry
func NewLogEntry(key string, value string) *LogEntry {
	checksum := computeCheckSum(key, value)
	return &LogEntry{
		Key:            key,
		keyMutex:       new(sync.RWMutex),
		Value:          value,
		valueMutex:     new(sync.RWMutex),
		Checksum:       checksum,
		checksumMutex:  new(sync.RWMutex),
		IsDeleted:      false,
		isDeletedMutex: new(sync.RWMutex),
	}
}
