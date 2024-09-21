package core

import (
	"path"
	"sync"
)

var (
	mu       sync.RWMutex
	dataPath string
)

func getDataPath() string {
	mu.RLock()
	defer mu.RUnlock()

	if len(dataPath) == 0 {
		panic("datapath has not been set")
	}
	return path.Join(dataPath, "hashindex")
}

func setDataPath(path string) {
	mu.Lock()
	dataPath = path
	mu.Unlock()
}

func getSegmentsPath() string {
	return path.Join(getDataPath(), "segments")
}

func getSnapshotsPath() string {
	return path.Join(getDataPath(), "snapshots")
}
