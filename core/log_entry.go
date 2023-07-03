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
	mu        *sync.RWMutex // mutex that synchornizes Key
	Key       string        // used as a reference to identify entry
	Value     string        // data to be saved
	Checksum  uint32        // used to detect corrupted data
	IsDeleted bool          // used to represent tombstone

}

// Encode encodes the log entry to bytes using msgpack
func (le *LogEntry) Encode() ([]byte, error) {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return msgpack.Marshal(&le)
	// return json.Marshal(&le)
}

// Decode decodes log entry bytes and loads the properties
func (le *LogEntry) Decode(logEntryBytes []byte) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	logEntry := &LogEntry{}
	err := msgpack.Unmarshal(logEntryBytes, logEntry)
	// err := json.Unmarshal(logEntryBytes, logEntry)

	if err != nil {
		return err
	}

	if computeCheckSum(logEntry.Key, logEntry.Value) != logEntry.Checksum {
		return ErrInvalidLogEntryChecksum
	}

	le.Key = logEntry.Key
	le.Value = logEntry.Value
	le.Checksum = logEntry.Checksum
	le.IsDeleted = logEntry.IsDeleted
	return nil
}

// NewLogEntry creates a new log entry
func NewLogEntry(key string, value string) *LogEntry {
	checksum := computeCheckSum(key, value)
	return &LogEntry{
		mu:        new(sync.RWMutex),
		Key:       key,
		Value:     value,
		Checksum:  checksum,
		IsDeleted: false,
	}
}
