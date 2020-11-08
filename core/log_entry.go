package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/vmihailenco/msgpack/v5"
)

var ErrInvalidLogEntryChecksum = errors.New("log entry is corrupted")

func computeCheckSum(key string, value string) uint32 {
	keyHash := sha256.Sum256([]byte(key))
	keyHashHex := hex.EncodeToString(keyHash[:])
	valueHash := sha256.Sum256([]byte(value))
	valueHashHex := hex.EncodeToString([]byte(valueHash[:]))
	checksum := crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s:%s", keyHashHex, valueHashHex)))

	return checksum
}

type LogEntry struct {
	Key       string
	Value     string
	Checksum  uint32
	IsDeleted bool
}

func (le LogEntry) Encode() ([]byte, error) {
  return msgpack.Marshal(&le)
}

func (le *LogEntry) Decode(logEntryBytes []byte) error {
	logEntry := &LogEntry{}
	err := msgpack.Unmarshal(logEntryBytes, logEntry)

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

func NewLogEntry(key string, value string) *LogEntry {
	checksum := computeCheckSum(key, value)
	return &LogEntry{
		Key:       key,
		Value:     value,
		Checksum:  checksum,
		IsDeleted: false,
	}
}
