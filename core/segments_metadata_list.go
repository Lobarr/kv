package core

import (
	"errors"
	"sync"
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

var ErrSegmentIdNotFound = errors.New("unable to find segment id")

type SegmentMetadataList struct {
	mu       sync.RWMutex
	segments map[string]int64 // segmentID -> createdAt timestamp
}

func (s *SegmentMetadataList) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.segments)
}

func (s *SegmentMetadataList) GetSegmentIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	segmentIDs := make([]string, 0, len(s.segments))

	for segmentID := range s.segments {
		segmentIDs = append(segmentIDs, segmentID)
	}

	return segmentIDs
}

func (s *SegmentMetadataList) Add(segmentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	start := time.Now()
	defer func() {
		SegmentMetadataListOperationDurationMilliseconds.WithLabelValues(AddOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		SegmentMetadataListOperationDurationNanoseconds.WithLabelValues(AddOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		SegmentMetadataListSize.Inc()
	}()

	s.segments[segmentID] = time.Now().Unix()
}

func (s *SegmentMetadataList) Remove(segmentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	start := time.Now()
	defer func() {
		SegmentMetadataListOperationDurationMilliseconds.WithLabelValues(RemoveOperation).Observe(
			float64(time.Since(start).Milliseconds()))
		SegmentMetadataListOperationDurationNanoseconds.WithLabelValues(RemoveOperation).Observe(
			float64(time.Since(start).Nanoseconds()))
		SegmentMetadataListSize.Dec()
	}()

	if _, ok := s.segments[segmentID]; !ok {
		return ErrSegmentIdNotFound
	}

	delete(s.segments, segmentID)

	return nil
}

func NewSegmentMetadataList() *SegmentMetadataList {
	return &SegmentMetadataList{
		segments: make(map[string]int64),
	}
}
