package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"kv/protos"

	"google.golang.org/protobuf/encoding/prototext"
)

// ErEErrInvalidLogEntryChecksum occurs when a log entry is corrupted
var ErrInvalidLogEntryChecksum = errors.New("log entry is corrupted")

// computeCheckSum computes the cheksum of a key and it's associated value
func computeCheckSum(key, value string) uint32 {
	keyHash := sha256.Sum256([]byte(key))
	keyHashHex := hex.EncodeToString(keyHash[:])
	valueHash := sha256.Sum256([]byte(value))
	valueHashHex := hex.EncodeToString([]byte(valueHash[:]))
	return crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s:%s", keyHashHex, valueHashHex)))
}

// EncodeLogEntry encodes the log entry to bytes
func encodeLogEntry(entry *protos.LogEntry) ([]byte, error) {
	return prototext.Marshal(entry)
}

// DecodeLogEntry decodes log entry bytes and loads the properties
func decodeLogEntry(payload []byte) (*protos.LogEntry, error) {
	entry := &protos.LogEntry{}

	if err := prototext.Unmarshal(payload, entry); err != nil {
		return nil, err
	}

	if computeCheckSum(entry.Key, entry.Value) != entry.Checksum {
		return nil, ErrInvalidLogEntryChecksum
	}

	return entry, nil
}

// NewLogEntry creates a new log entry
func newLogEntry(key string, value string) *protos.LogEntry {
	return &protos.LogEntry{
		Key:       key,
		Value:     value,
		Checksum:  computeCheckSum(key, value),
		IsDeleted: false,
	}
}
