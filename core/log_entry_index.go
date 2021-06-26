package core

//LogEntryIndex structure used to index where a log entry is stored on disk
type LogEntryIndex struct {
	Key                 string // used as a reference to identify entry
	OffSet              int64  // offset of where log entry is located in the segment file on disk
	EntrySize           int    // size of the entry
	CompressedEntrySize int    // size of compressed entry
	SegmentFilename     string // filename of the segment file that the logentry is stored
}
