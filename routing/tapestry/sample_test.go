package tapestry

import (
	"bytes"
	"context"
	"fmt"
	pb "modist/proto"
	"testing"
	"time"
)

func TestSampleTapestrySetup(t *testing.T) {
	tap, _ := MakeTapestries(true, "1", "3", "5", "7") //Make a tapestry with these ids
	fmt.Printf("length of tap %d\n", len(tap))
	KillTapestries(tap[1], tap[2]) //Kill off two of them.
	resp, _ := tap[0].FindRoot(
		context.Background(),
		CreateIDMsg("2", 0),
	) //After killing 3 and 5, this should route to 7

	if resp.Next != tap[3].Id.String() {
		t.Errorf("Failed to kill successfully")
	}

}

func TestSampleTapestrySearch(t *testing.T) {
	tap, _ := MakeTapestries(true, "100", "456", "1234") //make a sample tap
	tap[1].Store("look at this lad", []byte("an absolute unit"))
	result, err := tap[0].Get("look at this lad") //Store a KV pair and try to fetch it
	fmt.Println(err)
	if !bytes.Equal(result, []byte("an absolute unit")) { //Ensure we correctly get our KV
		t.Errorf("Get failed")
	}
}

func TestSampleTapestryAddNodes(t *testing.T) {
	// Need to use this so that gRPC connections are set up correctly
	tap, delayNodes, _ := MakeTapestriesDelayConnecting(
		true,
		[]string{"1", "5", "9"},
		[]string{"8", "12"},
	)

	// Add some tap nodes after the initial construction
	for _, delayNode := range delayNodes {
		args := Args{
			Node:      delayNode,
			Join:      true,
			ConnectTo: tap[0].RetrieveID(tap[0].Id),
		}
		tn := Configure(args).tapestryNode
		tap = append(tap, tn)
		time.Sleep(1000 * time.Millisecond) //Wait for availability
	}

	resp, _ := tap[1].FindRoot(context.Background(), CreateIDMsg("7", 0))
	if resp.Next != tap[3].Id.String() {
		t.Errorf("Addition of node failed")
	}
}

func TestSampleTapestryLeaveAndNotifyLeave(t *testing.T) {
	tap, _ := MakeTapestries(true, "1", "3", "5", "7") //Make a tapestry with these ids

	// Leave the tapestry node with ID "5"
	tap[2].Leave()

	// Attempt to route to a key that was previously owned by node with ID "5"
	resp, err := tap[0].FindRoot(
		context.Background(),
		CreateIDMsg("6", 0),
	)

	if err != nil {
		t.Errorf("Failed to route after node leave: %s", err.Error())
	}

	if resp.Next != tap[3].Id.String() {
		t.Errorf("Failed to route to correct next node after node leave")
	}
}

func TestAddFunction(t *testing.T) {
	// Create a new routing table
	rt := new(RoutingTable)

	// Fill up all the slots in the routing table
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			rt.Rows[i][j] = append(rt.Rows[i][j], ID{Digit(i + 1), Digit(j + 1), Digit(3), Digit(4), Digit(5)})
			rt.Rows[i][j] = append(rt.Rows[i][j], ID{Digit(i + 1), Digit(j + 1), Digit(4), Digit(5), Digit(6)})
			rt.Rows[i][j] = append(rt.Rows[i][j], ID{Digit(i + 1), Digit(j + 1), Digit(5), Digit(6), Digit(7)})
		}
	}

	// Add a remote node to a slot that is already full
	remoteNodeId := ID{1, 2, 3, 4, 6}
	added, previous := rt.Add(remoteNodeId)

	// Assert that the node was not added and the previous value is nil
	if !added {
		t.Errorf("Expected node to be added, but it was not added")
	}
	if previous == nil {
		t.Errorf("Expected previous not to be nil, but got %v", *previous)
	}
}

func TestSampleTapestryAddRoute(t *testing.T) {
	rt, _ := MakeTapestries(false, "0")

	// Fill up all the slots in the routing table
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			rt[0].Table.Rows[i][j] = append(rt[0].Table.Rows[i][j], ID{Digit(i + 1), Digit(j + 1), Digit(3), Digit(4), Digit(5)})
			rt[0].Table.Rows[i][j] = append(rt[0].Table.Rows[i][j], ID{Digit(i + 1), Digit(j + 1), Digit(4), Digit(5), Digit(6)})
			rt[0].Table.Rows[i][j] = append(rt[0].Table.Rows[i][j], ID{Digit(i + 1), Digit(j + 1), Digit(5), Digit(6), Digit(7)})
		}
	}

	// Add a remote node to a slot that is already full
	remoteNodeId := ID{1, 2, 3, 4, 6}
	err := rt[0].AddRoute(remoteNodeId)

	// Assert that the node was not added and the previous value is nil
	if err != nil {
		t.Errorf("Expected node to be added, but it was not added")
	}
}

func TestTapestryRemoveBackpointer(t *testing.T) {
	// local := NewTapestryNode(CreateID("1"), true) // Create a new local node for testing
	taps, _ := MakeTapestries(false, "1")
	local := taps[0]
	backpointerID := ID{2}

	// Add a backpointer to the local node
	local.Backpointers.Add(backpointerID)

	// Call RemoveBackpointer to remove the backpointer
	_, err := local.RemoveBackpointer(context.Background(), &pb.NodeMsg{
		Id: backpointerID.String(),
	})

	// Check if the backpointer was removed successfully
	if err != nil {
		t.Errorf("Failed to remove backpointer: %v", err)
	}
}
