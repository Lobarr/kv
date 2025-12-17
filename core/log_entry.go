package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"kv/protos"
	"unsafe"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// ErrInvalidLogEntryChecksum occurs when a log entry is corrupted
var ErrInvalidLogEntryChecksum = errors.New("log entry is corrupted")

// computeLegacyCheckSum computes the cheksum of a key and it's associated value using the old slow method
func computeLegacyCheckSum(key, value string) uint32 {
	keyHash := sha256.Sum256([]byte(key))
	keyHashHex := hex.EncodeToString(keyHash[:])
	valueHash := sha256.Sum256([]byte(value))
	valueHashHex := hex.EncodeToString([]byte(valueHash[:]))
	return crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s:%s", keyHashHex, valueHashHex)))
}

// unsafeStringToBytes converts a string to []byte without copying
// This is safe for read-only operations like CRC32 checksum
func unsafeStringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// computeCheckSum computes the cheksum of a key and it's associated value using CRC32 directly
// Optimized to avoid string-to-byte allocation overhead
func computeCheckSum(key, value string) uint32 {
	crc := crc32.ChecksumIEEE(unsafeStringToBytes(key))
	return crc32.Update(crc, crc32.IEEETable, unsafeStringToBytes(value))
}

// encodeLogEntry encodes the log entry to bytes using a fast custom format
// Format: [keyLen:4][key][valueLen:4][value][isDeleted:1][checksum:4]
func encodeLogEntry(entry *protos.LogEntry) ([]byte, error) {
	keyLen := len(entry.Key)
	valueLen := len(entry.Value)

	// Pre-allocate exact size needed
	totalLen := 4 + keyLen + 4 + valueLen + 1 + 4
	buf := make([]byte, totalLen)

	pos := 0

	// Write key length and key
	buf[pos] = byte(keyLen >> 24)
	buf[pos+1] = byte(keyLen >> 16)
	buf[pos+2] = byte(keyLen >> 8)
	buf[pos+3] = byte(keyLen)
	pos += 4

	copy(buf[pos:], entry.Key)
	pos += keyLen

	// Write value length and value
	buf[pos] = byte(valueLen >> 24)
	buf[pos+1] = byte(valueLen >> 16)
	buf[pos+2] = byte(valueLen >> 8)
	buf[pos+3] = byte(valueLen)
	pos += 4

	copy(buf[pos:], entry.Value)
	pos += valueLen

	// Write isDeleted flag
	if entry.IsDeleted {
		buf[pos] = 1
	} else {
		buf[pos] = 0
	}
	pos++

	// Write checksum
	checksum := entry.Checksum
	buf[pos] = byte(checksum >> 24)
	buf[pos+1] = byte(checksum >> 16)
	buf[pos+2] = byte(checksum >> 8)
	buf[pos+3] = byte(checksum)

	return buf, nil
}

// decodeLogEntry decodes log entry bytes and loads the properties, supporting both formats
// knownKey is optional; if provided, it avoids allocating the key string
func decodeLogEntry(payload []byte, knownKey string) (*protos.LogEntry, error) {
	entry := &protos.LogEntry{}

	// Try custom fast format first (new optimized format)
	// Format: [keyLen:4][key][valueLen:4][value][isDeleted:1][checksum:4]
	if len(payload) >= 13 { // Minimum size: 4+4+1+4 = 13
		pos := 0

		// Read key length
		keyLen := int(payload[pos])<<24 | int(payload[pos+1])<<16 | int(payload[pos+2])<<8 | int(payload[pos+3])
		pos += 4

		if pos+keyLen <= len(payload) {
			if knownKey != "" && len(knownKey) == keyLen {
				entry.Key = knownKey
			} else {
				entry.Key = string(payload[pos : pos+keyLen])
			}
			pos += keyLen

			if pos+4 <= len(payload) {
				// Read value length
				valueLen := int(payload[pos])<<24 | int(payload[pos+1])<<16 | int(payload[pos+2])<<8 | int(payload[pos+3])
				pos += 4

				if pos+valueLen+1+4 == len(payload) {
					// This looks like our custom format
					entry.Value = string(payload[pos : pos+valueLen])
					pos += valueLen

					// Read isDeleted
					entry.IsDeleted = payload[pos] == 1
					pos++

					// Read checksum
					entry.Checksum = uint32(payload[pos])<<24 | uint32(payload[pos+1])<<16 | uint32(payload[pos+2])<<8 | uint32(payload[pos+3])

					// Verify checksum
					if computeCheckSum(entry.Key, entry.Value) == entry.Checksum {
						return entry, nil
					}
				}
			}
		}
	}

	// Fallback to protobuf format for backward compatibility
	if err := proto.Unmarshal(payload, entry); err == nil {
		if computeCheckSum(entry.Key, entry.Value) == entry.Checksum {
			return entry, nil
		}
	}

	// Try legacy text format
	legacyEntry := &protos.LogEntry{}
	if err := prototext.Unmarshal(payload, legacyEntry); err != nil {
		return nil, err
	}

	if computeLegacyCheckSum(legacyEntry.Key, legacyEntry.Value) != legacyEntry.Checksum {
		return nil, ErrInvalidLogEntryChecksum
	}

	return legacyEntry, nil
}

// decodeLogEntryValue decodes only the value from log entry bytes, skipping struct allocation
// Returns: value, isDeleted, error
func decodeLogEntryValue(payload []byte) (string, bool, error) {
	// Try custom fast format first
	if len(payload) >= 13 {
		pos := 0

		keyLen := int(payload[pos])<<24 | int(payload[pos+1])<<16 | int(payload[pos+2])<<8 | int(payload[pos+3])
		pos += 4

		if pos+keyLen <= len(payload) {
			pos += keyLen

			if pos+4 <= len(payload) {
				valueLen := int(payload[pos])<<24 | int(payload[pos+1])<<16 | int(payload[pos+2])<<8 | int(payload[pos+3])
				pos += 4

				if pos+valueLen+1+4 == len(payload) {
					// Found value
					value := string(payload[pos : pos+valueLen])
					pos += valueLen
					isDeleted := payload[pos] == 1

					return value, isDeleted, nil
				}
			}
		}
	}

	// Fallback to full decode
	entry, err := decodeLogEntry(payload, "")
	if err != nil {
		return "", false, err
	}
	return entry.Value, entry.IsDeleted, nil
}

// newLogEntry creates a new log entry
func newLogEntry(key string, value string) *protos.LogEntry {
	return &protos.LogEntry{
		Key:       key,
		Value:     value,
		Checksum:  computeCheckSum(key, value),
		IsDeleted: false,
	}
}
