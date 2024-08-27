package partitioning

import (
	"encoding/binary"
	"reflect"
	"testing"

	"golang.org/x/exp/slices"
)

// checkLookup performs a lookup of key using the provided consistent hash partitioner,
// ensuring that there is no error, the returned id matches what is expected, and the
// rewritten key is a hash of the looked-up key.
func checkLookup(t *testing.T, msg string, c *ConsistentHash, key string, id uint64) {
	t.Helper()

	gotID, gotRewrittenKey, err := c.Lookup(key)
	rewrittenKey := hashToString(c.keyHash(key))
	if err != nil {
		t.Errorf("%s: Returned an error: %v", msg, err)
	} else if gotID != id {
		t.Errorf("%s: Returned the wrong shard: expected %d, got %d\nThe hashed key is %s\n Here are the virtual nodes in the assigner: %+v\n\n", msg, id, gotID, rewrittenKey, c.virtualNodes)
	} else if gotRewrittenKey != rewrittenKey {
		t.Errorf("%s: Returned the wrong rewritten key: expected %s, got %s", msg, rewrittenKey, gotRewrittenKey)
	}
}

// identityHasher returns the last 32 bytes of the input as a 32-byte array, padding with
// zeroes if necessary.
func identityHasher(b []byte) [32]byte {
	var out [32]byte

	bIndex := len(b) - 1
	for i := len(out) - 1; i >= 0; i-- {
		if bIndex < 0 {
			continue
		}
		out[i] = b[bIndex]
		bIndex--
	}
	return out
}

func newVirtualNode(c *ConsistentHash, id uint64, virtualNum int) virtualNode {
	return virtualNode{
		id:   id,
		num:  virtualNum,
		hash: c.virtualNodeHash(id, virtualNum),
	}
}

func TestConsistentHash_Lookup_SimpleIdentity(t *testing.T) {
	c := NewConsistentHash(2)
	c.hasher = identityHasher

	c.virtualNodes = []virtualNode{
		newVirtualNode(c, 1, 0),
		newVirtualNode(c, 1, 1),
		newVirtualNode(c, 50, 0),
		newVirtualNode(c, 50, 1),
	}
	slices.SortFunc(c.virtualNodes, virtualNodeLess)

	byteKey := make([]byte, 8)

	binary.BigEndian.PutUint64(byteKey, 2)
	checkLookup(t, "Lookup(10)", c, string(byteKey), 1)

	binary.BigEndian.PutUint64(byteKey, 10)
	checkLookup(t, "Lookup(10)", c, string(byteKey), 50)

	binary.BigEndian.PutUint64(byteKey, 50)
	checkLookup(t, "Lookup(50)", c, string(byteKey), 50)

	binary.BigEndian.PutUint64(byteKey, 51)
	checkLookup(t, "Lookup(51)", c, string(byteKey), 50)
}

func TestAddReplicaGroupNoOp(t *testing.T) {
	c := NewConsistentHash(3)
	reassignments := c.AddReplicaGroup(1)
	reassignments = c.AddReplicaGroup(1)

	// Ensure that calling AddReplicaGroup with an existing replica group is a no-op
	if reassignments != nil {
		t.Errorf("Expected no-op when adding existing replica group, but got reassignments: %v", reassignments)
	}
}

func TestAddReplicaGroupFirstGroup(t *testing.T) {
	c := NewConsistentHash(3)
	reassignments := c.AddReplicaGroup(1)

	// Ensure that no reassignments are generated when adding the first replica group
	if reassignments != nil {
		t.Errorf("Expected no reassignments when adding first replica group, but got: %v", reassignments)
	}

	// Ensure that the virtual nodes were added to the hash ring in ascending order
	for i := 0; i < len(c.virtualNodes)-1; i++ {
		if virtualNodeCmp(c.node(i), c.node(i+1)) > 0 {
			t.Errorf("Expected virtual nodes to be sorted in ascending order, but got: %v", c.virtualNodes)
		}
	}
}

func TestAddReplicaGroupSubsequentGroups(t *testing.T) {
	c := NewConsistentHash(3)
	id1 := uint64(1)
	c.AddReplicaGroup(id1)
	id2 := uint64(2)
	reassignments := c.AddReplicaGroup(id2)

	// Ensure that a reassignment entry is generated for each virtual node in the new replica group
	expectedReassignments := 3
	if len(reassignments) != expectedReassignments {
		t.Errorf("Expected %d reassignment entries, but got %d: %v", expectedReassignments, len(reassignments), reassignments)
	}

	// Ensure that the virtual nodes were added to the hash ring in ascending order
	for i := 0; i < len(c.virtualNodes)-1; i++ {
		if virtualNodeCmp(c.node(i), c.node(i+1)) > 0 {
			t.Errorf("Expected virtual nodes to be sorted in ascending order, but got: %v", c.virtualNodes)
		}
	}
}

func TestRemoveReplicaGroupNotExist(t *testing.T) {
	// Create a new hash ring and add some replica groups to it.
	c := NewConsistentHash(3)
	c.AddReplicaGroup(1)
	c.AddReplicaGroup(2)
	c.AddReplicaGroup(3)

	// Remove a replica group that doesn't exist. The function should be a no-op.
	reassignments := c.RemoveReplicaGroup(4)
	if reassignments != nil {
		t.Errorf("Expected RemoveReplicaGroup to return nil, but got %v", reassignments)
	}

	// Remove a replica group that exists.
	reassignments = c.RemoveReplicaGroup(2)

	// Check that the correct number of reassignments were returned.
	expectedReassignments := []Reassignment{
		{
			From: 2,
			To:   1,
			Range: KeyRange{
				Start: "cd04a4754498e06db5a13c5f371f1f04ff6d2470f24aa9bd886540e5dce77f71",
				End:   "cd04a4754498e06db5a13c5f371f1f04ff6d2470f24aa9bd886540e5dce77f70",
			},
		},
		{
			From: 2,
			To:   3,
			Range: KeyRange{
				Start: "cd2662154e6d76b2b2b92e70c0cac3ccf534f9b74eb5b89819ec509083d00a51",
				End:   "d5688a52d55a02ec4aea5ec1eadfffe1c9e0ee6a4ddbe2377f98326d42dfc975",
			},
		},
		{
			From: 2,
			To:   3,
			Range: KeyRange{
				Start: "5dee4dd60ff8d0ba9900fe91e90e0dcf65f0570d42c431f727d0300dd70dc432",
				End:   "8005f02d43fa06e7d0585fb64c961d57e318b27a145c857bcd3a6bdb413ff7fc",
			},
		},
	}
	if !reflect.DeepEqual(reassignments, expectedReassignments) {
		t.Errorf("Expected reassignments %v, but got %v", expectedReassignments, reassignments)
	}

	// Check that the removed replica group is no longer in the ring.
	for _, vnode := range c.virtualNodes {
		if vnode.id == 2 {
			t.Errorf("Expected replica group 2 to be removed, but found it in the ring")
		}
	}
}
