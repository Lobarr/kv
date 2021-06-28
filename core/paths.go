package core

import (
	"path"
)

var dataPath string

func getDataPath() string {
	if len(dataPath) == 0 {
		panic("datapath has not been set")
	}
	return path.Join(dataPath, "hashindex")
}

func setDataPath(path string) {
	dataPath = path
}

func getSegmentsPath() string {
	return path.Join(getDataPath(), "segments")
}

func getSnapshotsPath() string {
	return path.Join(getDataPath(), "snapshots")
}
