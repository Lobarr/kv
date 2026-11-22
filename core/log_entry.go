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

// computeLegacyCheckSum computes the checksum of a key and its associated value using the old slow method
func computeLegacyCheckSum(key, value string) uint32 {
	keyHash := sha256.Sum256([]byte(key))
	keyHashHex := hex.EncodeToString(keyHash[:])
	valueHash := sha256.Sum256([]byte(value))
	valueHashHex := hex.EncodeToString([]byte(valueHash[:]))
	return crc32.ChecksumIEEE([]byte(fmt.Sprintf("%s:%s", keyHashHex, valueHashHex)))
}

// unsafeStringToBytes converts a string to []byte without copying.
// This is safe for read-only operations like CRC32 checksum.
func unsafeStringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// computeCheckSum computes the checksum of a key and its associated value using CRC32 directly.
// Optimized to avoid string-to-byte allocation overhead.
func computeCheckSum(key, value string) uint32 {
	crc := crc32.ChecksumIEEE(unsafeStringToBytes(key))
	return crc32.Update(crc, crc32.IEEETable, unsafeStringToBytes(value))
}

// computeCheckSumFromPayload computes the checksum of a key/value pair stored
// contiguously (with a gap for the value length prefix) inside an encoded
// log entry payload, without copying key or value bytes.
// Layout: [keyLen:4][key][valueLen:4][value][isDeleted:1][checksum:4]
// keyStart is the offset of the first key byte, keyLen and valueLen are the
// respective lengths, valueStart the offset of the first value byte.
func computeCheckSumFromPayload(payload []byte, keyStart, keyLen, valueStart, valueLen int) uint32 {
	crc := crc32.ChecksumIEEE(payload[keyStart : keyStart+keyLen])
	return crc32.Update(crc, crc32.IEEETable, payload[valueStart:valueStart+valueLen])
}

// encodeLogEntryInto encodes a log entry into dst (growing dst if needed)
// using the fast custom binary format:
// [keyLen:4][key][valueLen:4][value][isDeleted:1][checksum:4]
func encodeLogEntryInto(dst []byte, key, value string, isDeleted bool, checksum uint32) []byte {
	keyLen := len(key)
	valueLen := len(value)
	totalLen := 4 + keyLen + 4 + valueLen + 1 + 4

	if cap(dst) < totalLen {
		dst = make([]byte, totalLen)
	} else {
		dst = dst[:totalLen]
	}

	pos := 0

	// Write key length and key
	dst[pos] = byte(keyLen >> 24)
	dst[pos+1] = byte(keyLen >> 16)
	dst[pos+2] = byte(keyLen >> 8)
	dst[pos+3] = byte(keyLen)
	pos += 4

	copy(dst[pos:], key)
	pos += keyLen

	// Write value length and value
	dst[pos] = byte(valueLen >> 24)
	dst[pos+1] = byte(valueLen >> 16)
	dst[pos+2] = byte(valueLen >> 8)
	dst[pos+3] = byte(valueLen)
	pos += 4

	copy(dst[pos:], value)
	pos += valueLen

	// Write isDeleted flag
	if isDeleted {
		dst[pos] = 1
	} else {
		dst[pos] = 0
	}
	pos++

	// Write checksum
	dst[pos] = byte(checksum >> 24)
	dst[pos+1] = byte(checksum >> 16)
	dst[pos+2] = byte(checksum >> 8)
	dst[pos+3] = byte(checksum)

	return dst
}

// encodeLogEntry encodes the log entry to bytes using a fast custom format
// Format: [keyLen:4][key][valueLen:4][value][isDeleted:1][checksum:4]
func encodeLogEntry(entry *protos.LogEntry) ([]byte, error) {
	return encodeLogEntryInto(nil, entry.Key, entry.Value, entry.IsDeleted, entry.Checksum), nil
}

// parsedEntryLayout describes the field layout of a payload that structurally
// matches the custom fast format.
type parsedEntryLayout struct {
	keyLen     int
	valueLen   int
	keyStart   int
	valueStart int
	isDeleted  bool
	checksum   uint32
}

// parseEntryLayout attempts to structurally parse payload as the custom fast
// format without allocating key/value strings. It returns ok=false when the
// payload does not match the custom format (caller should try fallback
// decoders). A structural match is a strong signal the payload was produced
// by encodeLogEntry: field lengths must tile the payload exactly.
func parseEntryLayout(payload []byte) (layout parsedEntryLayout, ok bool) {
	const headerLen = 4 // keyLen
	if len(payload) < headerLen+4+1+4 {
		return layout, false
	}

	keyLen := int(payload[0])<<24 | int(payload[1])<<16 | int(payload[2])<<8 | int(payload[3])
	keyStart := headerLen
	if keyLen < 0 || keyStart+keyLen+4 > len(payload) {
		return layout, false
	}

	vLenPos := keyStart + keyLen
	valueLen := int(payload[vLenPos])<<24 | int(payload[vLenPos+1])<<16 | int(payload[vLenPos+2])<<8 | int(payload[vLenPos+3])
	valueStart := vLenPos + 4
	if valueLen < 0 || valueStart+valueLen+1+4 != len(payload) {
		return layout, false
	}

	flagPos := valueStart + valueLen
	checksumPos := flagPos + 1

	layout = parsedEntryLayout{
		keyLen:     keyLen,
		keyStart:   keyStart,
		valueLen:   valueLen,
		valueStart: valueStart,
		isDeleted:  payload[flagPos] == 1,
		checksum:   uint32(payload[checksumPos])<<24 | uint32(payload[checksumPos+1])<<16 | uint32(payload[checksumPos+2])<<8 | uint32(payload[checksumPos+3]),
	}
	return layout, true
}

// decodeLogEntry decodes log entry bytes and loads the properties, supporting both formats.
// knownKey is optional; if provided, it avoids allocating the key string.
func decodeLogEntry(payload []byte, knownKey string) (*protos.LogEntry, error) {
	// Try custom fast format first (new optimized format)
	if layout, ok := parseEntryLayout(payload); ok {
		// Structural match: verify checksum over the raw payload bytes
		// (no copies) before allocating any strings.
		if computeCheckSumFromPayload(payload, layout.keyStart, layout.keyLen, layout.valueStart, layout.valueLen) != layout.checksum {
			return nil, ErrInvalidLogEntryChecksum
		}

		if knownKey != "" && len(knownKey) == layout.keyLen {
			return &protos.LogEntry{
				Key:       knownKey,
				Value:     string(payload[layout.valueStart : layout.valueStart+layout.valueLen]),
				IsDeleted: layout.isDeleted,
				Checksum:  layout.checksum,
			}, nil
		}
		return &protos.LogEntry{
			Key:       string(payload[layout.keyStart : layout.keyStart+layout.keyLen]),
			Value:     string(payload[layout.valueStart : layout.valueStart+layout.valueLen]),
			IsDeleted: layout.isDeleted,
			Checksum:  layout.checksum,
		}, nil
	}

	// Fallback to protobuf format for backward compatibility
	entry := &protos.LogEntry{}
	if err := proto.Unmarshal(payload, entry); err == nil {
		if computeCheckSum(entry.Key, entry.Value) == entry.Checksum {
			return entry, nil
		}
		return nil, ErrInvalidLogEntryChecksum
	}

	// Try legacy text format
	legacyEntry := &protos.LogEntry{}
	if err := prototext.Unmarshal(payload, legacyEntry); err != nil {
		return nil, ErrInvalidLogEntryChecksum
	}

	if computeLegacyCheckSum(legacyEntry.Key, legacyEntry.Value) != legacyEntry.Checksum {
		return nil, ErrInvalidLogEntryChecksum
	}

	return legacyEntry, nil
}

// decodeLogEntryValue decodes only the value from log entry bytes, skipping
// struct allocation. The CRC32 checksum is always verified on this hot path.
// Returns: value, isDeleted, error
func decodeLogEntryValue(payload []byte) (string, bool, error) {
	// Try custom fast format first
	if layout, ok := parseEntryLayout(payload); ok {
		if computeCheckSumFromPayload(payload, layout.keyStart, layout.keyLen, layout.valueStart, layout.valueLen) != layout.checksum {
			return "", false, ErrInvalidLogEntryChecksum
		}
		// Checksum verified: materialize the value string (copies out of the
		// possibly mmapped or pooled source buffer).
		return string(payload[layout.valueStart : layout.valueStart+layout.valueLen]), layout.isDeleted, nil
	}

	// Fallback to full decode for legacy formats
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
