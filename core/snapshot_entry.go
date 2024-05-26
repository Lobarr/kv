package core

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/crc32"
	"kv/protos"
	"path"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrInvalidSnapshotEntryChecksum = errors.New("snapshot corrupted")

func computeSnapshotChecksum(snapshotBytes []byte) uint32 {
	snapshotHash := sha256.Sum256(snapshotBytes)
	return crc32.ChecksumIEEE(snapshotHash[:])
}

func encodeSnapshotEntry(entry *protos.SnapshotEntry) ([]byte, error) {
	return prototext.Marshal(entry)
}

func decodeSnapshotEntry(snapshotBytes []byte) (*protos.SnapshotEntry, error) {
	snapshotEntry := &protos.SnapshotEntry{}

	if err := prototext.Unmarshal(snapshotBytes, snapshotEntry); err != nil {
		return nil, err
	}

	data, err := prototext.Marshal(snapshotEntry.Snapshot)
	if err != nil {
		return nil, err
	}

	if computeSnapshotChecksum(data) != snapshotEntry.Checksum {
		return nil, ErrInvalidSnapshotEntryChecksum
	}

	return snapshotEntry, nil
}

func snapshotEntryFileName(snapshot *protos.SnapshotEntry) string {
	return path.Join(getSnapshotsPath(), fmt.Sprintf("%d-%s.snapshot", snapshot.Timestamp.AsTime().Unix(), snapshot.Id))
}

func newSnapshotEntry(state *protos.SnapshotState) (*protos.SnapshotEntry, error) {
	data, err := prototext.Marshal(state)
	if err != nil {
		return nil, err
	}
	snapshotChecksum := computeSnapshotChecksum(data)
	snapShotID := uuid.New().String()

	return &protos.SnapshotEntry{
		Snapshot:  state,
		Checksum:  snapshotChecksum,
		Id:        snapShotID,
		Timestamp: timestamppb.Now(),
	}, nil
}
