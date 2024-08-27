package conflict

import (
	"testing"
	"time"
)

func testCreatePhysicalClockConflictResolver() *PhysicalClockConflictResolver {
	return &PhysicalClockConflictResolver{}
}

func (c *PhysicalClockConflictResolver) testCreatePhysicalClock() PhysicalClock {
	return PhysicalClock{timestamp: uint64(time.Now().UnixNano())}
}

func (c *PhysicalClockConflictResolver) testCreatePhysicalClockGivenTimestamp(timestamp uint64) PhysicalClock {
	return PhysicalClock{timestamp: timestamp}
}

func TestPhysicalConcurrentEventsHappenBefore(t *testing.T) {
	r := testCreatePhysicalClockConflictResolver()
	c1 := r.testCreatePhysicalClockGivenTimestamp(10)
	c2 := r.testCreatePhysicalClockGivenTimestamp(20)
	c3 := r.testCreatePhysicalClockGivenTimestamp(30)

	if c1.Equals(c2) {
		t.Errorf("c1 should not equals to c2")
	}
	if !c1.HappensBefore(c2) {
		t.Errorf("c1 should happen before c2")
	}
	if !c1.HappensBefore(c3) {
		t.Errorf("c1 should happen before c3")
	}
	if !c2.HappensBefore(c3) {
		t.Errorf("c2 should happen before c3")
	}

}

func TestPhysicalClockString(t *testing.T) {
	c := PhysicalClock{timestamp: 123}
	expected := "123"
	if c.String() != expected {
		t.Errorf("Expected %v but got %v", expected, c.String())
	}
}

func TestPhysicalClockConflictResolver_ResolveConcurrentEvents(t *testing.T) {
	// Create some sample conflicts with concurrent PhysicalClocks
	conflict1 := &KV[PhysicalClock]{Key: "key1", Value: "value1", Clock: PhysicalClock{timestamp: 100}}
	conflict2 := &KV[PhysicalClock]{Key: "key1", Value: "value2", Clock: PhysicalClock{timestamp: 200}}
	conflict3 := &KV[PhysicalClock]{Key: "key1", Value: "value3", Clock: PhysicalClock{timestamp: 150}}

	// Create a PhysicalClockConflictResolver instance
	resolver := &PhysicalClockConflictResolver{}

	// Test resolving the conflicts
	winner, err := resolver.ResolveConcurrentEvents(conflict1, conflict2, conflict3)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if winner.Key != "key1" || winner.Value != "value3" {
		t.Errorf("Unexpected winner: %v", winner)
	}
}
