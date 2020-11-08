package hashindex

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
	"hash/crc32"
	"path"
	"time"
)

var ErrInvalidSnapshotEntryChecksum = errors.New("snapshot corrupted")

type SnapshotEntry struct {
	Snapshot  []byte
	Checksum  uint32
	ID        string
	Timestamp int64
}

func computeSnapshotChecksum(snapshotBytes []byte) uint32 {
	snapshotHash := sha256.Sum256(snapshotBytes)
	return crc32.ChecksumIEEE(snapshotHash[:])
}

func (snapshot *SnapshotEntry) Encode() ([]byte, error) {
	return msgpack.Marshal(snapshot)
}

func (snapshot *SnapshotEntry) Decode(snapshotBytes []byte) error {
	snapshotEntry := &SnapshotEntry{}
	err := msgpack.Unmarshal(snapshotBytes, snapshotEntry)

	if err != nil {
		return err
	}

	if computeSnapshotChecksum(snapshotEntry.Snapshot) != snapshotEntry.Checksum {
		return ErrInvalidSnapshotEntryChecksum
	}

	snapshot.Checksum = snapshotEntry.Checksum
	snapshot.ID = snapshotEntry.ID
	snapshot.Snapshot = snapshotEntry.Snapshot
	snapshot.Timestamp = snapshotEntry.Timestamp

	return nil
}

func (snapshot SnapshotEntry) ComputeFilename() string {
	return path.Join(getSnapshotsPath(), fmt.Sprintf("%d-%s.snapshot", snapshot.Timestamp, snapshot.ID))
}

func newSnapshotEntry(snapshotData interface{}) (*SnapshotEntry, error) {
	snapshotBytes, err := msgpack.Marshal(snapshotData)

	if err != nil {
		return nil, err
	}

	snapshotChecksum := computeSnapshotChecksum(snapshotBytes)
	snapShotID := uuid.New().String()
	timestamp := time.Now().Unix()

	return &SnapshotEntry{
		Snapshot:  snapshotBytes,
		Checksum:  snapshotChecksum,
		ID:        snapShotID,
		Timestamp: timestamp,
	}, nil
}
