package core_test

import (
	"kv/core"
	"reflect"
	"testing"
)

func TestAddOperation(t *testing.T) {
	segmentId := "some-segment-id"
	expectedSegments := []string{segmentId}
	l := core.NewSegmentMetadataList()
	l.Add(segmentId)
	segments := l.GetSegmentIDs()
	for i := 0; i < len(segments); i++ {
		if segments[i] != expectedSegments[i] {
			t.Errorf("expected %#v, got %#v", expectedSegments, segments)
		}
	}
}

func TestMultipleAddOperations(t *testing.T) {
	expectedSegments := []string{"some-segment-1", "some-segment-2", "some-segment-3"}
	l := core.NewSegmentMetadataList()
	for _, segment := range expectedSegments {
		l.Add(segment)
	}
	segments := l.GetSegmentIDs()
	for i := 0; i < len(segments); i++ {
		if segments[i] != expectedSegments[i] {
			t.Errorf("expected %#v, got %#v", expectedSegments, segments)
		}
	}
}

func TestRemoveOperation(t *testing.T) {
	segmentId := "some-segment-id"
	l := core.NewSegmentMetadataList()
	l.Add(segmentId)
	// check addition
	if len(l.GetSegmentIDs()) != 1 {
		t.Errorf("expected an element added but, got %#v", l.GetSegmentIDs())
	}
	// check removal
	expectedSegments := []string{}
	l.Remove(segmentId)
	if !reflect.DeepEqual(l.GetSegmentIDs(), expectedSegments) {
		t.Errorf("expected %#v, got %#v", expectedSegments, l.GetSegmentIDs())
	}
}

func TestMultipleRemoveOperations(t *testing.T) {
	segments := []string{"some-segment-1", "some-segment-2", "some-segment-3"}
	l := core.NewSegmentMetadataList()
	for _, segment := range segments {
		l.Add(segment)
	}
	// check addition
	if len(l.GetSegmentIDs()) != len(segments) {
		t.Errorf("expected %d elements added but, got %#v", len(segments), l.GetSegmentIDs())
	}
	// check removal
	expectedSegments := []string{}
	for _, segment := range segments {
		l.Remove(segment)
	}
	if !reflect.DeepEqual(l.GetSegmentIDs(), expectedSegments) {
		t.Errorf("expected %#v, got %#v", expectedSegments, l.GetSegmentIDs())
	}
}
