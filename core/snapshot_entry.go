package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"path"
	"time"

	"github.com/google/uuid"
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
	var snapshotEntryBytes bytes.Buffer
	encoder := gob.NewEncoder(&snapshotEntryBytes)
	err := encoder.Encode(snapshot)
	if err != nil {
		return nil, err
	}
	return snapshotEntryBytes.Bytes(), nil
}

func (snapshot *SnapshotEntry) Decode(snapshotBytes []byte) error {
	snapshotEntry := &SnapshotEntry{}
	decoder := gob.NewDecoder(bytes.NewReader(snapshotBytes))
	err := decoder.Decode(snapshotEntry)
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
	var snapshotBuffer bytes.Buffer
	encoder := gob.NewEncoder(&snapshotBuffer)
	err := encoder.Encode(snapshotData)

	if err != nil {
		return nil, err
	}

	snapshotChecksum := computeSnapshotChecksum(snapshotBuffer.Bytes())
	snapShotID := uuid.New().String()
	timestamp := time.Now().Unix()

	return &SnapshotEntry{
		Snapshot:  snapshotBuffer.Bytes(),
		Checksum:  snapshotChecksum,
		ID:        snapShotID,
		Timestamp: timestamp,
	}, nil
}
