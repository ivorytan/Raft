/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines the RoutingTable type and provides methods for interacting
 *  with it.
 */

package tapestry

import (
	"sync"
)

// RoutingTable has a number of levels equal to the number of digits in an ID
// (default 40). Each level has a number of slots equal to the digit base
// (default 16). A node that exists on level n thereby shares a prefix of length
// n with the local node. Access to the routing table is protected by a mutex.
type RoutingTable struct {
	localId ID                 // The ID of the local tapestry node
	Rows    [DIGITS][BASE][]ID // The rows of the routing table (stores IDs of remote tapestry nodes)
	mutex   sync.Mutex         // To manage concurrent access to the routing table (could also have a per-level mutex)
}

// NewRoutingTable creates and returns a new routing table, placing the local node at the
// appropriate slot in each level of the table.
func NewRoutingTable(me ID) *RoutingTable {
	t := new(RoutingTable)
	t.localId = me

	// Create the node lists with capacity of SLOTSIZE
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			t.Rows[i][j] = make([]ID, 0, SLOTSIZE)
		}
	}

	// Make sure each row has at least our node in it
	for i := 0; i < DIGITS; i++ {
		slot := t.Rows[i][t.localId[i]]
		t.Rows[i][t.localId[i]] = append(slot, t.localId)
	}

	return t
}

// Add adds the given node to the routing table.
//
// Note you should not add the node to preceding levels. You need to add the node
// to one specific slot in the routing table (or replace an element if the slot is full
// at SLOTSIZE).
//
// Returns true if the node did not previously exist in the table and was subsequently added.
// Returns the previous node in the table, if one was overwritten.
func (t *RoutingTable) Add(remoteNodeId ID) (added bool, previous *ID) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!
	// fmt.Print(t.localId, remoteNodeId, "\n")

	// Compute the level associated with the shared prefix length
	sharedPrefixLength := SharedPrefixLength(t.localId, remoteNodeId)
	// fmt.Printf("sharedPrefixLength:%v\n", sharedPrefixLength)

	// If the shared prefix length is the same as our ID's length, then the remote node is ourselves
	if sharedPrefixLength == len(t.localId) {
		// fmt.Print("The remote node is ourselves\n")
		return false, nil
	}

	level := sharedPrefixLength

	// The first digit of the remote node after the shared prefix
	slotIdx := remoteNodeId[level]
	// slot := t.Rows[level][slotIdx]

	// Check if the remote node already exists in the slot
	for _, node := range t.Rows[level][slotIdx] {
		if node == remoteNodeId {
			// fmt.Printf("The remote node already exists in the slot\n")
			return false, nil
		}
	}

	// Check if the slot is full
	if len(t.Rows[level][slotIdx]) == SLOTSIZE {
		// Replace the least recently seen node with the remoteNodeId
		// Find the least close node in the slot
		leastCloseNode := t.Rows[level][slotIdx][0]
		replace := false
		for i := 1; i < len(t.Rows[level][slotIdx]); i++ {
			if t.localId.Closer(remoteNodeId, t.Rows[level][slotIdx][i]) {
				if t.localId.Closer(leastCloseNode, t.Rows[level][slotIdx][i]) {
					leastCloseNode = t.Rows[level][slotIdx][i]
				}
				replace = true
			}
		}

		if !replace {
			// If all existing nodes are better candidates, return false with previous as nil
			// fmt.Print("all existing nodes are better candidates\n")
			return false, nil
		}

		previous = &leastCloseNode
		// fmt.Printf("Slot is full, so we need to replace the least close node\n")
		// Replace the least close node with the remoteNodeId
		for i := range t.Rows[level][slotIdx] {
			if t.Rows[level][slotIdx][i] == leastCloseNode {
				t.Rows[level][slotIdx][i] = remoteNodeId
				return true, previous
			}
		}
	} else {
		// Add the remoteNodeId to the routing table
		// fmt.Printf("Remote node does not exist in the slot, so we need to add it\n")
		t.Rows[level][slotIdx] = append(t.Rows[level][slotIdx], remoteNodeId)
		return true, nil
	}

	return false, nil
}

// Remove removes the specified node from the routing table, if it exists.
// Returns true if the node was in the table and was successfully removed.
// Return false if a node tries to remove itself from the table.
func (t *RoutingTable) Remove(remoteNodeId ID) (wasRemoved bool) {
	// TODO(students): [Tapestry] Implement me!
	// Lock the Slot for removing
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Check if the remote node is trying to remove itself

	SharedPrefixLength := SharedPrefixLength(t.localId, remoteNodeId)

	// if the shared prefix length is the same as our ID's length, then the remote node is ourselves
	if SharedPrefixLength == len(t.localId) {
		// fmt.Print("The remote node is trying to remove itself\n")
		return false
	}

	// Compute the level associated with the shared prefix length
	// level := len(t.localId) - SharedPrefixLength - 1
	level := SharedPrefixLength
	slot := remoteNodeId[level]

	// Find the remote node in the slot and remove it
	for i := 0; i < len(t.Rows[level][slot]); i++ {
		if t.Rows[level][slot][i] == remoteNodeId {
			// Remove the remote node from the slot
			t.Rows[level][slot] = append(t.Rows[level][slot][:i], t.Rows[level][slot][i+1:]...)
			// fmt.Print("Successfully removed\n")
			return true
		}
	}

	// Remote node was not found in the slot
	// fmt.Print("Remote node was not found in the slot\n")
	return false
}

// GetLevel gets ALL nodes on the specified level of the routing table, EXCLUDING the local node.
func (t *RoutingTable) GetLevel(level int) (nodeIds []ID) {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!
	if level <= 0 || level >= DIGITS {
		return nil // level 0 for the local node and illegal cases
	}

	for _, nodeSlots := range t.Rows[level] {
		for _, node := range nodeSlots {
			if node != t.localId {
				nodeIds = append(nodeIds, node) // EXCLUDING the local node
			}
		}
	}

	return nodeIds
}

// FindNextHop searches the table for the closest next-hop node for the provided ID starting at the given level.
func (t *RoutingTable) FindNextHop(id ID, level int32) ID {

	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO(students): [Tapestry] Implement me!

	slotIdx := int(id[level])

	for {
		if len(t.Rows[level][slotIdx]) == 0 {
			slotIdx = (slotIdx + 1) % BASE
			continue
		}

		entry := t.Rows[level][slotIdx][0]

		if entry == t.localId {
			level += 1
			if level >= DIGITS {
				return t.localId
			}
			slotIdx = int(id[level])
		} else {
			return entry
		}
	}
}
