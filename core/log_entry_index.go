package core

type LogEntryIndex struct {
	Key             string
	OffSet          int64
	EntrySize       int
	SegmentFilename string
}
