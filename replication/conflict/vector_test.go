package conflict

import (
	"reflect"
	"testing"
)

func TestVectorConcurrentEventsDoNotHappenBefore(t *testing.T) {
	v1 := NewVersionVectorClock()
	v2 := NewVersionVectorClock()

	v1.vector[0] = 0
	v1.vector[3] = 2
	v2.vector[0] = 1

	if v2.HappensBefore(v1) {
		t.Errorf("v2 does not happen before v1 due to v1[3]")
	}
	if v1.HappensBefore(v2) {
		t.Errorf("v1 does not happen before v2 due to v2[0] > v1[0]")
	}
}

func TestResolveConcurrentEvents(t *testing.T) {
	clock1 := VersionVectorClock{map[uint64]uint64{1: 2, 2: 1}}
	clock2 := VersionVectorClock{map[uint64]uint64{1: 3, 3: 1}}
	clock3 := VersionVectorClock{map[uint64]uint64{2: 2, 3: 1}}

	conflicts := []*KV[VersionVectorClock]{
		{Key: "foo", Value: "bar1", Clock: clock1},
		{Key: "foo", Value: "bar2", Clock: clock2},
		{Key: "foo", Value: "bar3", Clock: clock3},
	}

	// Call the function directly
	r := VersionVectorConflictResolver{}
	resolved, err := r.ResolveConcurrentEvents(conflicts...)

	if err != nil {
		t.Errorf("ResolveConcurrentEvents returned an error: %v", err)
	}

	expectedValue := "bar3"
	expectedClock := clock1

	for process, value := range clock2.vector {
		if expectedClock.vector[process] < value {
			expectedClock.vector[process] = value
		}
	}

	for process, value := range clock3.vector {
		if expectedClock.vector[process] < value {
			expectedClock.vector[process] = value
		}
	}

	if resolved.Value != expectedValue {
		t.Errorf("Expected value %s but got %s", expectedValue, resolved.Value)
	}

	if !reflect.DeepEqual(resolved.Clock, expectedClock) {
		t.Errorf("Expected clock %v but got %v", expectedClock, resolved.Clock)
	}
}

func TestVersionVectorConflictResolver_NewClock(t *testing.T) {
	vvcr := VersionVectorConflictResolver{
		vector: map[uint64]uint64{
			1: 0,
			2: 2,
			3: 1,
		},
	}
	clock := vvcr.NewClock()

	// Check that the returned clock has the same vector as vvcr
	expected := map[uint64]uint64{
		1: 0,
		2: 2,
		3: 1,
	}
	if !reflect.DeepEqual(clock.vector, expected) {
		t.Errorf("NewClock returned clock with unexpected vector: %v", clock.vector)
	}

	// Check that modifying the returned clock doesn't modify vvcr
	clock.vector[1] = 1
	if vvcr.vector[1] != 0 {
		t.Errorf("Modifying returned clock modified vvcr")
	}
}
