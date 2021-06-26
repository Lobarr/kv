package core

import (
	"container/heap"
	"errors"
	"time"
)

type SegmentMetadata struct {
	segmentID string // id of data segment
	createdAt int64  // timestamp of when metadata was created
	index     int    // index of item in the heap
}

var ErrSegmentIdNotFound = errors.New("unable to find segment id")

type segmentMetadataHeap []*SegmentMetadata

func (_segmentMetadataHeap segmentMetadataHeap) Len() int {
	return len(_segmentMetadataHeap)
}

func (_segmentMetadataHeap segmentMetadataHeap) Less(x, y int) bool {
	return _segmentMetadataHeap[x].createdAt > _segmentMetadataHeap[y].createdAt
}

func (_segmentMetadataHeap segmentMetadataHeap) Swap(x, y int) {
	_segmentMetadataHeap[x], _segmentMetadataHeap[y] = _segmentMetadataHeap[y], _segmentMetadataHeap[x]
	_segmentMetadataHeap[x].index = x
	_segmentMetadataHeap[y].index = y
}

func (_segmentMetadataHeap *segmentMetadataHeap) Push(ctx interface{}) {
	index := len(*_segmentMetadataHeap)
	segmentMetadata := ctx.(*SegmentMetadata)
	segmentMetadata.index = index
	*_segmentMetadataHeap = append(*_segmentMetadataHeap, segmentMetadata)
}

func (_segmentMetadataHeap *segmentMetadataHeap) Pop() interface{} {
	prevsegmentMetadataHeap := *_segmentMetadataHeap
	n := len(prevsegmentMetadataHeap)
	segmentMetadata := prevsegmentMetadataHeap[n-1]
	prevsegmentMetadataHeap[n-1] = nil // avoid memory leak
	segmentMetadata.index = -1         // for safety
	*_segmentMetadataHeap = prevsegmentMetadataHeap[0 : n-1]
	return segmentMetadata
}

type SegmentMetadataList struct {
	_segmentMetadataHeap *segmentMetadataHeap
	segmentIDByIndex     map[string]int // mapping of segment id to index position in the segment metadata heap
}

func (segmentMetadataList SegmentMetadataList) GetSegmentIDs() []string {
	segmentIDs := make([]string, len(*segmentMetadataList._segmentMetadataHeap))

	for i, segmentMetadata := range *segmentMetadataList._segmentMetadataHeap {
		segmentIDs[i] = segmentMetadata.segmentID
	}

	return segmentIDs
}

func (segmentMetadataList *SegmentMetadataList) Add(segmentID string) {
	segmentMetadata := &SegmentMetadata{segmentID: segmentID, createdAt: time.Now().Unix()}
	heap.Push(segmentMetadataList._segmentMetadataHeap, segmentMetadata)
	segmentMetadataList.segmentIDByIndex[segmentMetadata.segmentID] = segmentMetadata.index
}

func (segmentMetadataList *SegmentMetadataList) Remove(segmentID string) error {
	index, ok := segmentMetadataList.segmentIDByIndex[segmentID]

	if !ok {
		return ErrSegmentIdNotFound
	}

	heap.Remove(segmentMetadataList._segmentMetadataHeap, index)
	return nil
}

func NewSegmentMetadataList() *SegmentMetadataList {
	_segmentMetadataHeap := new(segmentMetadataHeap)

	heap.Init(_segmentMetadataHeap)

	return &SegmentMetadataList{
		_segmentMetadataHeap: _segmentMetadataHeap,
		segmentIDByIndex:     make(map[string]int),
	}
}
