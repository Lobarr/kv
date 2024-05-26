package core

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/crc32"
	"kv/protos"
	"path"
	"strings"

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

func newSnapshotEntry(state *protos.SnapshotState) *protos.SnapshotEntry {
	keys := strings.Builder{}
	for k := range state.LogEntryIndexesByKey {
		keys.WriteString(k)
	}
	return &protos.SnapshotEntry{
		Snapshot:  state,
		Checksum:  computeSnapshotChecksum([]byte(keys.String())),
		Id:        uuid.New().String(),
		Timestamp: timestamppb.Now(),
	}
}
