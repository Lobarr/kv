package hashindex

import (
	"os"
	"path"
)

func getDataPath() string {
	currentWorkingDirectory, _ := os.Getwd()
	return path.Join(currentWorkingDirectory, "tmp", "hashindex")
}

func getSegmentsPath() string {
	return path.Join(getDataPath(), "segments")
}

func getSnapshotsPath() string {
	return path.Join(getDataPath(), "snapshots")
}
