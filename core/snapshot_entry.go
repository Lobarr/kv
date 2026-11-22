package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"kv/protos"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const fastSnapshotMagic = "KVSN"

// encodeFastSnapshot serializes EngineState into a compact, checksummed binary
// format without protobuf overhead or per-key heap allocations:
//
//	[magic:4 "KVSN"][crc32:4][count:4] + count * ([keyLen:4][key][segLen:2][seg][offset:8][size:8])
func encodeFastSnapshot(state *EngineState) []byte {
	// Estimate size: ~64 bytes per entry + 12-byte header.
	totalKeys := state.Len()
	buf := make([]byte, 12, 12+totalKeys*64)
	copy(buf[0:4], fastSnapshotMagic)

	var count uint32
	for i := range state.shards {
		shard := &state.shards[i]
		shard.mu.RLock()
		for k, e := range shard.indexes {
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], uint32(len(k)))
			buf = append(buf, hdr[:]...)
			buf = append(buf, k...)

			var shdr [2]byte
			binary.BigEndian.PutUint16(shdr[:], uint16(len(e.segmentID)))
			buf = append(buf, shdr[:]...)
			buf = append(buf, e.segmentID...)

			var meta [16]byte
			binary.BigEndian.PutUint64(meta[0:8], uint64(e.offset))
			binary.BigEndian.PutUint64(meta[8:16], uint64(e.entrySize))
			buf = append(buf, meta[:]...)
			count++
		}
		shard.mu.RUnlock()
	}

	binary.BigEndian.PutUint32(buf[8:12], count)
	checksum := crc32.ChecksumIEEE(buf[8:])
	binary.BigEndian.PutUint32(buf[4:8], checksum)
	return buf
}

// decodeFastSnapshot parses a KVSN binary snapshot, verifying its CRC32
// checksum before invoking fn for each entry. Returns handled=false if data
// does not start with the KVSN magic header.
func decodeFastSnapshot(data []byte, fn func(key string, entry indexEntry)) (bool, error) {
	if len(data) < 12 || string(data[0:4]) != fastSnapshotMagic {
		return false, nil
	}

	expectedCRC := binary.BigEndian.Uint32(data[4:8])
	if crc32.ChecksumIEEE(data[8:]) != expectedCRC {
		return true, ErrInvalidSnapshotEntryChecksum
	}

	count := binary.BigEndian.Uint32(data[8:12])
	pos := 12
	for i := uint32(0); i < count; i++ {
		if pos+4 > len(data) {
			return true, ErrInvalidSnapshotEntryChecksum
		}
		keyLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+keyLen+2 > len(data) {
			return true, ErrInvalidSnapshotEntryChecksum
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen

		segLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+segLen+16 > len(data) {
			return true, ErrInvalidSnapshotEntryChecksum
		}
		segID := string(data[pos : pos+segLen])
		pos += segLen

		offset := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		entrySize := int64(binary.BigEndian.Uint64(data[pos+8 : pos+16]))
		pos += 16

		fn(key, indexEntry{
			segmentID: segID,
			offset:    offset,
			entrySize: entrySize,
		})
	}
	if pos != len(data) {
		return true, ErrInvalidSnapshotEntryChecksum
	}
	return true, nil
}

func newSnapshotFileName(layout *dataLayout) string {
	return filepath.Join(layout.snapshotsPath(),
		fmt.Sprintf("%d-%s.snapshot", time.Now().UnixNano(), uuid.New().String()))
}

var ErrInvalidSnapshotEntryChecksum = errors.New("snapshot corrupted")

func computeSnapshotChecksum(snapshotBytes []byte) uint32 {
	return crc32.ChecksumIEEE(snapshotBytes)
}

// marshalSnapshotState marshals a snapshot state deterministically. The
// deterministic flag is essential: proto.Marshal iterates maps in random
// order, so a checksum over a non-deterministic encoding would differ
// between write time and verify time for any multi-key snapshot.
func marshalSnapshotState(state *protos.SnapshotState) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(state)
}

func encodeSnapshotEntry(entry *protos.SnapshotEntry) ([]byte, error) {
	return proto.Marshal(entry)
}

func decodeSnapshotEntry(snapshotBytes []byte) (*protos.SnapshotEntry, error) {
	snapshotEntry := &protos.SnapshotEntry{}

	// Try binary unmarshal first
	if err := proto.Unmarshal(snapshotBytes, snapshotEntry); err == nil && snapshotEntry.Snapshot != nil {
		// Verify checksum for binary format (deterministic encoding)
		data, err := marshalSnapshotState(snapshotEntry.Snapshot)
		if err != nil {
			return nil, err
		}
		if computeSnapshotChecksum(data) == snapshotEntry.Checksum {
			return snapshotEntry, nil
		}
		return nil, ErrInvalidSnapshotEntryChecksum
	}

	// Legacy text unmarshal
	if err := prototext.Unmarshal(snapshotBytes, snapshotEntry); err != nil {
		return nil, err
	}

	data, err := prototext.Marshal(snapshotEntry.Snapshot)
	if err != nil {
		return nil, err
	}

	if computeLegacySnapshotChecksum(data) != snapshotEntry.Checksum {
		return nil, ErrInvalidSnapshotEntryChecksum
	}

	return snapshotEntry, nil
}

func computeLegacySnapshotChecksum(snapshotBytes []byte) uint32 {
	return crc32.ChecksumIEEE(snapshotBytes)
}

// snapshotEntryFileName computes the final on-disk name of a snapshot file.
// Nanosecond precision keeps recovery ordering deterministic: snapshots
// written within the same second would otherwise be tie-broken by the
// random UUID suffix.
func snapshotEntryFileName(layout *dataLayout, snapshot *protos.SnapshotEntry) string {
	return filepath.Join(layout.snapshotsPath(),
		fmt.Sprintf("%d-%s.snapshot", snapshot.Timestamp.AsTime().UnixNano(), snapshot.Id))
}

func newSnapshotEntry(state *protos.SnapshotState) *protos.SnapshotEntry {
	// Compute checksum on the deterministic binary representation of the
	// state so verification after a roundtrip is stable.
	data, _ := marshalSnapshotState(state)

	return &protos.SnapshotEntry{
		Snapshot:  state,
		Checksum:  computeSnapshotChecksum(data),
		Id:        uuid.New().String(),
		Timestamp: timestamppb.Now(),
	}
}
