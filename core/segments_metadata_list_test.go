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
	expectedSegments := map[string]bool{"some-segment-1": true, "some-segment-2": true, "some-segment-3": true}
	l := core.NewSegmentMetadataList()
	for segment := range expectedSegments {
		l.Add(segment)
	}
	segments := l.GetSegmentIDs()
	if len(segments) != len(expectedSegments) {
		t.Errorf("expected %d segments, got %d", len(expectedSegments), len(segments))
	}
	for _, segment := range segments {
		if !expectedSegments[segment] {
			t.Errorf("unexpected segment %s", segment)
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
