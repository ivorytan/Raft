/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines functions for a node leaving the Tapestry mesh, and
 *  transferring its stored locations to a new node.
 */

package tapestry

import (
	"context"
	"fmt"
	pb "modist/proto"
)

// Kill this node without gracefully leaving the tapestry.
func (local *TapestryNode) Kill() {
	local.blobstore.DeleteAll()
	local.Node.GrpcServer.Stop()
}

// Leave gracefully exits the Tapestry mesh.
//
// - Notify the nodes in our backpointers that we are leaving by calling NotifyLeave
// - If possible, give each backpointer a suitable alternative node from our routing table
func (local *TapestryNode) Leave() error {
	// TODO(students): [Tapestry] Implement me!

	// Notify the nodes in our backpointers that we are leaving
	// and give them a suitable alternative node from our routing table

	for level := 0; level < DIGITS; level++ {
		backpointerIDs := local.Backpointers.Get(level)
		for _, backpointerID := range backpointerIDs {

			altNode := local.Table.FindNextHop(backpointerID, int32(level))
			leaveNotification := &pb.LeaveNotification{
				From:        local.Id.String(),
				Replacement: altNode.String(),
			}

			backpointerConn := local.Node.PeerConns[local.RetrieveID(backpointerID)]
			backpointerNode := pb.NewTapestryRPCClient(backpointerConn)

			_, err := backpointerNode.NotifyLeave(context.Background(), leaveNotification)
			if err != nil {
				// Failed to notify backpointer node, skip to next one
				continue
			}
		}
	}

	// Delete all entries from our local blobstore
	local.blobstore.DeleteAll()

	// Gracefully stop the gRPC server of the local node
	go local.Node.GrpcServer.GracefulStop()

	return nil

	// return errors.New("Leave has not been implemented yet!")
}

// NotifyLeave occurs when another node is informing us of a graceful exit.
// - Remove references to the `from` node from our routing table and backpointers
// - If replacement is not an empty string, add replacement to our routing table
func (local *TapestryNode) NotifyLeave(
	ctx context.Context,
	leaveNotification *pb.LeaveNotification,
) (*pb.Ok, error) {
	from, err := ParseID(leaveNotification.From)
	if err != nil {
		return nil, err
	}
	// Replacement can be an empty string so we don't want to parse it here
	replacement := leaveNotification.Replacement

	local.log.Printf(
		"Received leave notification from %v with replacement node %v\n",
		from,
		replacement,
	)

	// TODO(students): [Tapestry] Implement me!
	removed := local.Table.Remove(from)
	if !removed {
		// If the leaving node was not found in the routing table, return an error
		return &pb.Ok{Ok: false}, fmt.Errorf("leaving node not found in routing table")
	}

	// Add the alternative node to the routing table
	if len(replacement) != 0 {
		altNodeId, err := ParseID(replacement)
		if err != nil {
			return &pb.Ok{Ok: false}, fmt.Errorf("failed to parse node ID")
		}
		added, _ := local.Table.Add(altNodeId)
		if !added {
			// If the alternative node was not added to the routing table, return an error
			return &pb.Ok{Ok: false}, fmt.Errorf("failed to add alternative node to routing table")
		}
	}

	return &pb.Ok{Ok: true}, nil

	// return nil, errors.New("NotifyLeave has not been implemented yet!")
}
