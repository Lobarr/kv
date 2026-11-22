package core

import (
	"path/filepath"
	"strings"
)

// dataLayout defines the on-disk directory layout for a single engine
// instance. Paths are per-Engine (not package globals) so multiple Engine
// instances can coexist safely with independent data directories.
type dataLayout struct {
	dataPath string // root directory provided by the user
}

func newDataLayout(dataPath string) *dataLayout {
	return &dataLayout{dataPath: dataPath}
}

func (l *dataLayout) dataDir() string {
	return filepath.Join(l.dataPath, "hashindex")
}

func (l *dataLayout) segmentsPath() string {
	return filepath.Join(l.dataDir(), "segments")
}

func (l *dataLayout) snapshotsPath() string {
	return filepath.Join(l.dataDir(), "snapshots")
}

// segmentFileName computes the full path of a segment file on disk.
func (l *dataLayout) segmentFileName(id string) string {
	return filepath.Join(l.segmentsPath(), segmentFileBaseName(id))
}

func segmentFileBaseName(id string) string {
	return id + ".segment"
}

// segmentIDFromFileName extracts the segment ID from a segment file name
// (or full path). Returns "" if the name does not look like a segment file.
func segmentIDFromFileName(fileName string) string {
	base := fileName
	if idx := strings.LastIndexByte(base, '/'); idx >= 0 {
		base = base[idx+1:]
	}
	if idx := strings.LastIndexByte(base, '\\'); idx >= 0 {
		base = base[idx+1:]
	}
	const suffix = ".segment"
	if !strings.HasSuffix(base, suffix) {
		return ""
	}
	return strings.TrimSuffix(base, suffix)
}
