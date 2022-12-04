package core

import (
	"container/heap"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	AddOperation    = "add"
	RemoveOperation = "remove"
)

var (
	SegmentMetadataListOperationDurationMilliseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "segment_metadata_list/operation_durations_ms",
		Help: "how long the operation on the list takes",
	}, []string{"operation"})

	SegmentMetadataListOperationDurationNanoseconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "segment_metadata_list/operation_durations_ns",
		Help: "how long the operation on the list takes",
	}, []string{"operation"})

	SegmentMetadataListSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "segment_metadata_list/size",
		Help: "number of entries in the list",
	})
)

func init() {
	prometheus.Register(SegmentMetadataListOperationDurationMilliseconds)
	prometheus.Register(SegmentMetadataListOperationDurationNanoseconds)
	prometheus.Register(SegmentMetadataListSize)
}

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

func (s *SegmentMetadataList) Push(ctx any) {
	index := len(s.segmentMetadataHeap)
	segmentMetadata := ctx.(*SegmentMetadata)
	segmentMetadata.index = index
	s.segmentMetadataHeap = append(s.segmentMetadataHeap, segmentMetadata)
	s.segmentIDByIndex[segmentMetadata.segmentID] = segmentMetadata.index
}

func (s *SegmentMetadataList) Pop() any {
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
		SegmentMetadataListOperationDurationMilliseconds.WithLabelValues(AddOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		SegmentMetadataListOperationDurationNanoseconds.WithLabelValues(AddOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		SegmentMetadataListSize.Inc()
	}()

	segmentMetadata := &SegmentMetadata{segmentID: segmentID, createdAt: time.Now().Unix()}
	heap.Push(s, segmentMetadata)
}

func (s *SegmentMetadataList) Remove(segmentID string) error {
	start := time.Now()
	defer func() {
		SegmentMetadataListOperationDurationMilliseconds.WithLabelValues(RemoveOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		SegmentMetadataListOperationDurationNanoseconds.WithLabelValues(RemoveOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		SegmentMetadataListSize.Dec()
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
