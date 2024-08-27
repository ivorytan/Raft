/*
 *  Brown University, CS138, Spring 2023
 *
 *  Purpose: Defines functions to publish and lookup objects in a Tapestry mesh
 */

package tapestry

import (
	"context"
	"errors"
	"fmt"
	pb "modist/proto"
	"time"
)

// Store a blob on the local node and publish the key to the tapestry.
func (local *TapestryNode) Store(key string, value []byte) (err error) {
	done, err := local.Publish(key)
	if err != nil {
		return err
	}
	local.blobstore.Put(key, value, done)
	return nil
}

// Get looks up a key in the tapestry then fetch the corresponding blob from the
// remote blob store.
func (local *TapestryNode) Get(key string) ([]byte, error) {
	// Lookup the key
	routerIds, err := local.Lookup(key)
	if err != nil {
		return nil, err
	}
	if len(routerIds) == 0 {
		return nil, fmt.Errorf("No routers returned for key %v", key)
	}

	// Contact router
	keyMsg := &pb.TapestryKey{
		Key: key,
	}

	var errs []error
	for _, routerId := range routerIds {
		conn := local.Node.PeerConns[local.RetrieveID(routerId)]
		router := pb.NewTapestryRPCClient(conn)
		resp, err := router.BlobStoreFetch(context.Background(), keyMsg)
		if err != nil {
			errs = append(errs, err)
		} else if resp.Data != nil {
			return resp.Data, nil
		}
	}

	return nil, fmt.Errorf("Error contacting routers, %v: %v", routerIds, errs)
}

// Remove the blob from the local blob store and stop advertising
func (local *TapestryNode) Remove(key string) bool {
	return local.blobstore.Delete(key)
}

// Publishes the key in tapestry.
//
// - Start periodically publishing the key. At each publishing:
//   - Find the root node for the key
//   - Register the local node on the root
//   - if anything failed, retry; until RETRIES has been reached.
//
// - Return a channel for cancelling the publish
//   - if receiving from the channel, stop republishing
//
// Some note about publishing behavior:
//   - The first publishing attempt should attempt to retry at most RETRIES times if there is a failure.
//     i.e. if RETRIES = 3 and FindRoot errored or returned false after all 3 times, consider this publishing
//     attempt as failed. The error returned for Publish should be the error message associated with the final
//     retry.
//   - If any of these attempts succeed, you do not need to retry.
//   - In addition to the initial publishing attempt, you should repeat this entire publishing workflow at the
//     appropriate interval. i.e. every 5 seconds we attempt to publish, and THIS publishing attempt can either
//     succeed, or fail after at most RETRIES times.
//   - Keep trying to republish regardless of how the last attempt went
func (local *TapestryNode) Publish(key string) (chan bool, error) {
	// TODO(students): [Tapestry] Implement me!
	// Create a channel for cancelling the publish
	cancel := make(chan bool)

	PublishOperation := func(key string) {
		for i := 0; i < RETRIES; i++ {
			// Find the root node for the key
			rootnodeMsg, err := local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key).String(), Level: 0})
			if err != nil {
				// Retry if FindRoot failed
				local.log.Printf("Failed to find root on node %v, reason:%v\n", local.Id, err)
				continue
			}

			// Register the local node on the root
			registration := &pb.Registration{
				FromNode: local.Id.String(),
				Key:      key,
			}

			rootNodeID, err := ParseID(rootnodeMsg.Next)
			if err != nil {
				local.log.Printf("Failed to parse the ID\n")
				continue
			}

			rootNodeconn := local.Node.PeerConns[local.RetrieveID(rootNodeID)]
			rootNode := pb.NewTapestryRPCClient(rootNodeconn)

			ok, err := rootNode.Register(context.Background(), registration)

			if err != nil || !ok.Ok {
				local.log.Printf("Failed to register on rootNode:%v\n", rootNodeID)
				continue
			}
			return // succeed publish
		}
		return
	}

	PublishOperation(key)

	// Start a goroutine for periodic publishing
	go func() {
		for {
			select {
			case <-time.After(REPUBLISH):
				PublishOperation(key) // republishing
			case <-cancel:
				// Stop republishing if cancellation signal received
				return
			}
		}
	}()

	return cancel, nil
	// return nil, errors.New("Publish has not been implemented yet!")
}

// Lookup look up the Tapestry nodes that are storing the blob for the specified key.
//
// - Find the root node for the key
// - Fetch the routers (nodes storing the blob) from the root's location map
// - Attempt up to RETRIES times
func (local *TapestryNode) Lookup(key string) ([]ID, error) {
	// TODO(students): [Tapestry] Implement me!

	for attempt := 0; attempt < RETRIES; attempt++ {
		// Find the root node for the key
		rootMsg, err := local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key).String(), Level: 0})
		if err != nil {
			fmt.Printf("Error find root on local node: %v\n", local.Id)
			continue
		}

		// Fetch the routers (nodes storing the blob) from the root's location map
		rootNodeId, err := ParseID(rootMsg.GetNext())
		if err != nil {
			fmt.Printf("Error Parse root node ID: %v\n", rootNodeId)
			continue
		}

		rootNodeConn := local.Node.PeerConns[local.RetrieveID(rootNodeId)]
		rootNode := pb.NewTapestryRPCClient(rootNodeConn)

		fetchedLocations, err := rootNode.Fetch(context.Background(), &pb.TapestryKey{Key: key})
		if err != nil {
			fmt.Printf("Error fetch node on: %v\n", rootNodeId)
			continue
		}

		// Extract the node IDs from the fetched locations
		return stringSliceToIds(fetchedLocations.GetValues())
	}

	// Nodes not found after retries
	return nil, errors.New("Tapestry nodes not found for the specified key")

	// return nil, errors.New("Lookup has not been implemented yet!")
}

// FindRoot returns the root for the id in idMsg by recursive RPC calls on the next hop found in our routing table
//   - find the next hop from our routing table
//   - call FindRoot on nextHop
//   - if failed, add nextHop to toRemove, remove them from local routing table, retry
func (local *TapestryNode) FindRoot(ctx context.Context, idMsg *pb.IdMsg) (*pb.RootMsg, error) {
	id, err := ParseID(idMsg.Id)
	if err != nil {
		return nil, err
	}
	level := idMsg.Level

	// TODO(students): [Tapestry] Implement me!
	toRemove := make([]string, 0)

	// recursion here
	for {
		// Find the next hop from our routing table
		nextHop := local.Table.FindNextHop(id, level)

		if nextHop == local.Id {
			return &pb.RootMsg{
				Next:     nextHop.String(),
				ToRemove: toRemove,
			}, nil
		}

		// Call FindRoot on nextHop
		nextHopconn := local.Node.PeerConns[local.RetrieveID(nextHop)]
		nextHopNode := pb.NewTapestryRPCClient(nextHopconn)
		rootMsg, err := nextHopNode.FindRoot(ctx, &pb.IdMsg{
			Id:    idMsg.Id,
			Level: level + 1,
		})

		// If failed, add nextHop to toRemove, retry
		if err != nil {
			local.log.Print("Found root failed, retry\n")
			local.Table.Remove(nextHop)
			toRemove = append(toRemove, nextHop.String())
			continue // Retry
		}

		// remove them from local routing table
		toRemove = append(toRemove, rootMsg.ToRemove...)
		local.RemoveBadNodes(ctx, &pb.Neighbors{Neighbors: toRemove})
		rootMsg.ToRemove = toRemove

		local.log.Print("Root found: ", rootMsg.Next, "\n")
		return rootMsg, nil
	}

	// return nil, errors.New("FindRoot has not been implemented yet!")
}

// The node that stores some data with key is registering themselves to us as an advertiser of the key.
// - Check that we are the root node for the key, return true in pb.Ok if we are
// - Add the node to the location map (local.locationsByKey.Register)
//   - local.locationsByKey.Register kicks off a timer to remove the node if it's not advertised again
//     after TIMEOUT
func (local *TapestryNode) Register(
	ctx context.Context,
	registration *pb.Registration,
) (*pb.Ok, error) {
	from, err := ParseID(registration.FromNode)
	if err != nil {
		return nil, err
	}
	key := registration.Key

	// TODO(students): [Tapestry] Implement me!
	// Check if we are the root node for the key
	rootMsg, err := local.FindRoot(context.Background(), &pb.IdMsg{Id: Hash(key).String(), Level: 0})
	if err != nil {
		return nil, fmt.Errorf("Error finding root on remote node %v, reason: %v", Hash(key).String(), err)
	}

	// Check if current node is the root node for the requested key
	if rootMsg.GetNext() == local.Id.String() {
		// Add the node to the location map
		local.LocationsByKey.Register(key, from, TIMEOUT)

		// Kick off a timer to remove the node from the location map after TIMEOUT
		time.AfterFunc(TIMEOUT, func() {
			local.LocationsByKey.Unregister(key, from)
		})

		return &pb.Ok{Ok: true}, nil
	}

	return &pb.Ok{Ok: false}, nil

	// return nil, errors.New("Register has not been implemented yet!")
}

// Fetch checks that we are the root node for the requested key and
// return all nodes that are registered in the local location map for this key
func (local *TapestryNode) Fetch(
	ctx context.Context,
	key *pb.TapestryKey,
) (*pb.FetchedLocations, error) {
	// TODO(students): [Tapestry] Implement me!
	local.log.Println("Fetching locations for key:", key.Key)

	nodeId := Hash(key.GetKey())
	rootMsg, err := local.FindRoot(context.Background(), &pb.IdMsg{Id: nodeId.String(), Level: 0})
	if err != nil {
		return nil, fmt.Errorf("Error finding root on remote node %v, reason: %v", nodeId, err)
	}

	// Check if current node is the root node for the requested key
	if rootMsg.GetNext() == local.Id.String() {
		// If current node is the root node, return all registered locations
		locations := local.LocationsByKey.Get(key.GetKey())
		fmt.Printf("Fetching: %v", locations)

		// Create and return FetchedLocations message
		fetchedLocations := &pb.FetchedLocations{
			IsRoot: true,
			Values: idsToStringSlice(locations),
		}
		return fetchedLocations, nil
	}
	// If current node is not the root node, return error
	return &pb.FetchedLocations{IsRoot: false}, nil

	// return nil, errors.New("Fetch has not been implemented yet!")
}

// Retrieves the blob corresponding to a key
func (local *TapestryNode) BlobStoreFetch(
	ctx context.Context,
	key *pb.TapestryKey,
) (*pb.DataBlob, error) {
	data, isOk := local.blobstore.Get(key.Key)

	var err error
	if !isOk {
		err = errors.New("Key not found")
	}

	return &pb.DataBlob{
		Key:  key.Key,
		Data: data,
	}, err
}

// Transfer registers all of the provided objects in the local location map. (local.locationsByKey.RegisterAll)
// If appropriate, add the from node to our local routing table
func (local *TapestryNode) Transfer(
	ctx context.Context,
	transferData *pb.TransferData,
) (*pb.Ok, error) {
	from, err := ParseID(transferData.From)
	if err != nil {
		return nil, err
	}

	nodeMap := make(map[string][]ID)
	for key, set := range transferData.Data {
		nodeMap[key], err = stringSliceToIds(set.Neighbors)
		if err != nil {
			return nil, err
		}
	}

	// TODO(students): [Tapestry] Implement me!
	// Register all the provided objects in the local location map
	local.LocationsByKey.RegisterAll(nodeMap, TIMEOUT)

	// Add the from node to our local routing table
	err = local.AddRoute(from)

	return &pb.Ok{Ok: err == nil}, nil

	// return nil, errors.New("Transfer has not been implemented yet!")
}

// calls FindRoot on a remote node to find the root of the given id
func (local *TapestryNode) FindRootOnRemoteNode(remoteNodeId ID, id ID) (*ID, error) {
	// TODO(students): [Tapestry] Implement me!
	// local.log.Println("Finding root on remote node", remoteNodeId, "for ID", id)

	// Prepare the remote node ID message
	nodeIdMsg := &pb.IdMsg{
		Id:    id.String(),
		Level: 0, // Set the level argument to 0
	}

	// Make an RPC call to the remote node to find the root of the given ID
	conn := local.Node.PeerConns[local.RetrieveID(remoteNodeId)]
	remoteNode := pb.NewTapestryRPCClient(conn)

	// Create a gRPC context with timeout
	// ctx, cancel := context.WithTimeout(context.Background(), TIMEOUT)
	// defer cancel()

	// Call the FindRoot RPC on the remote node
	resp, err := remoteNode.FindRoot(context.Background(), nodeIdMsg)
	if err != nil {
		return nil, fmt.Errorf("Error finding root on remote node %v, reason: %v", remoteNodeId, err)
	}

	// Parse the response and return the root ID
	rootId, err := ParseID(resp.Next)
	if err != nil {
		return nil, fmt.Errorf("Error parsing root ID from response, reason: %v", err)
	}

	return &rootId, nil

	// return nil, errors.New("FindRootOnRemoteNode has not been implemented yet!")
}
