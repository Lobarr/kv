package core

import (
	"container/heap"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	SegmentMetadataListAddDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "segment_metadata_list_add_duration_nanoseconds",
		Help: "how long it takes to add to the segment metadata list",
	}, []string{"segment_id", "segment_list_size"})

	SegmentMetadataListAddDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "segment_metadata_list_add_duration_milliseconds",
		Help: "how long it takes to add to the segment metadata list",
	}, []string{"segment_id", "segment_list_size"})

	SegmentMetadataListRemoveDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "segment_metadata_list_remove_duration_nanoseconds",
		Help: "how long it takes to remove to the segment metadata list",
	}, []string{"segment_id", "segment_list_size"})

	SegmentMetadataListRemoveDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "segment_metadata_list_remove_duration_milliseconds",
		Help: "how long it takes to remove to the segment metadata list",
	}, []string{"segment_id", "segment_list_size"})
)

type SegmentMetadata struct {
	segmentID string // id of data segment
	createdAt int64  // timestamp of when metadata was created
	index     int    // index of item in the heap
}

var ErrSegmentIdNotFound = errors.New("unable to find segment id")

type SegmentMetadataList struct {
	segmentMetadataHeap []*SegmentMetadata
	segmentIDByIndex    map[string]int // mapping of segment id to index position in the segment metadata heap
}

func (s SegmentMetadataList) Len() int {
	return len(s.segmentMetadataHeap)
}

func (s SegmentMetadataList) Less(x, y int) bool {
	return s.segmentMetadataHeap[x].createdAt > s.segmentMetadataHeap[y].createdAt
}

func (s SegmentMetadataList) Swap(x, y int) {
	s.segmentMetadataHeap[x], s.segmentMetadataHeap[y] = s.segmentMetadataHeap[y], s.segmentMetadataHeap[x]
	s.segmentMetadataHeap[x].index = x
	s.segmentMetadataHeap[y].index = y
	s.segmentIDByIndex[s.segmentMetadataHeap[x].segmentID] = x
	s.segmentIDByIndex[s.segmentMetadataHeap[y].segmentID] = y
}

func (s *SegmentMetadataList) Push(ctx interface{}) {
	index := len(s.segmentMetadataHeap)
	segmentMetadata := ctx.(*SegmentMetadata)
	segmentMetadata.index = index
	s.segmentMetadataHeap = append(s.segmentMetadataHeap, segmentMetadata)
	s.segmentIDByIndex[segmentMetadata.segmentID] = segmentMetadata.index
}

func (s *SegmentMetadataList) Pop() interface{} {
	prevsegmentMetadataHeap := s.segmentMetadataHeap
	n := len(prevsegmentMetadataHeap)
	segmentMetadata := prevsegmentMetadataHeap[n-1]
	prevsegmentMetadataHeap[n-1] = nil // avoid memory leak
	segmentMetadata.index = -1         // for safety
	s.segmentMetadataHeap = prevsegmentMetadataHeap[0 : n-1]
	delete(s.segmentIDByIndex, segmentMetadata.segmentID)
	return segmentMetadata
}

func (s SegmentMetadataList) GetSegmentIDs() []string {
	segmentIDs := make([]string, len(s.segmentMetadataHeap))

	for i, segmentMetadata := range s.segmentMetadataHeap {
		segmentIDs[i] = segmentMetadata.segmentID
	}

	return segmentIDs
}

func (s *SegmentMetadataList) Add(segmentID string) {
	start := time.Now()
	defer func() {
		SegmentMetadataListAddDurationNanoseconds.WithLabelValues(
			segmentID,
			fmt.Sprint(len(s.segmentMetadataHeap)),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)
		SegmentMetadataListAddDurationMilliseconds.WithLabelValues(
			segmentID,
			fmt.Sprint(len(s.segmentMetadataHeap)),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	segmentMetadata := &SegmentMetadata{segmentID: segmentID, createdAt: time.Now().Unix()}
	heap.Push(s, segmentMetadata)
}

func (s *SegmentMetadataList) Remove(segmentID string) error {
	start := time.Now()
	defer func() {
		SegmentMetadataListRemoveDurationNanoseconds.WithLabelValues(
			segmentID,
			fmt.Sprint(len(s.segmentMetadataHeap)),
		).Observe(
			float64(time.Since(start).Nanoseconds()),
		)
		SegmentMetadataListRemoveDurationMilliseconds.WithLabelValues(
			segmentID,
			fmt.Sprint(len(s.segmentMetadataHeap)),
		).Observe(
			float64(time.Since(start).Milliseconds()),
		)
	}()

	index, ok := s.segmentIDByIndex[segmentID]

	if !ok {
		return ErrSegmentIdNotFound
	}

	heap.Remove(s, index)
	delete(s.segmentIDByIndex, segmentID)

	return nil
}

func NewSegmentMetadataList() *SegmentMetadataList {
	segmentMetadataHeap := new([]*SegmentMetadata)
	segmentMetadataList := &SegmentMetadataList{
		segmentMetadataHeap: *segmentMetadataHeap,
		segmentIDByIndex:    make(map[string]int),
	}

	heap.Init(segmentMetadataList)

	return segmentMetadataList
}
