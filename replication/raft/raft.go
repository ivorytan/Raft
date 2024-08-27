package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"modist/orchestrator/node"
	pb "modist/proto"
	"sync"
	"time"
)

// RETRIES is the number of retries upon failure. By default, we have 3
const RETRIES = 3

// RETRY_TIME is the amount of time to wait between retries
const RETRY_TIME = 100 * time.Millisecond

type State struct {
	// In-memory key value store
	store map[string]string
	mu    sync.RWMutex

	// Channels given back by the underlying Raft implementation
	proposeC chan<- []byte
	commitC  <-chan *commit

	// Observability
	log *log.Logger

	// The public-facing API that this replicator must implement
	pb.ReplicatorServer
}

type Args struct {
	Node   *node.Node
	Config *Config
}

// Configure is called by the orchestrator to start this node
//
// The "args" are any to support any replicator that might need arbitrary
// set of configuration values.
//
// You are free to add any fields to the State struct that you feel may help
// with implementing the ReplicateKey or GetReplicatedKey functions. Likewise,
// you may call any additional functions following the second TODO. Apart
// from the two TODOs, DO NOT EDIT any other part of this function.
func Configure(args any) *State {
	a := args.(Args)

	node := a.Node
	config := a.Config

	proposeC := make(chan []byte)
	commitC := NewRaftNode(node, config, proposeC)

	s := &State{
		store:    make(map[string]string),
		proposeC: proposeC,
		commitC:  commitC,

		log: node.Log,

		// TODO(students): [Raft] Initialize any additional fields and add to State struct
		// add some struct fields to State struct
		mu: sync.RWMutex{},
	}

	// We registered RaftRPCServer when calling NewRaftNode above so we only need to
	// register ReplicatorServer here
	s.log.Printf("starting gRPC server at %s", node.Addr.Host)
	grpcServer := node.GrpcServer
	pb.RegisterReplicatorServer(grpcServer, s)
	go grpcServer.Serve(node.Listener)

	// TODO(students): [Raft] Call helper functions if needed
	// Attention: commit's definition: type commit []byte
	// the goroutine that reads from commitC and updates the store
	// Configure function is written to start a goroutine that reads from commitC and updates the store
	go func() {
		// should use for range instead of for { select {} } (S1000)
		for commit := range commitC { // read from commitC
			// unmarshal the commit
			var kv pb.ResolvableKV
			err := json.Unmarshal(*commit, &kv) // unmarshal the commit
			if err != nil {
				s.log.Printf("error unmarshalling commit: %v", err)
				continue
			}
			// update the store
			s.mu.Lock()
			s.store[kv.Key] = kv.Value
			s.mu.Unlock()
		}
	}()

	return s
}

// ReplicateKey replicates the (key, value) given in the PutRequest by relaying it to the
// underlying Raft cluster/implementation.
//
// You should first marshal the KV struct using the encoding/json library. Then, put the
// marshalled struct in the proposeC channel and wait until the Raft cluster has confirmed that
// it has been committed. Think about how you can use the commitC channel to find out when
// something has been committed.
//
// If the proposal has not been committed after RETRY_TIME, you should retry it. You should retry
// the proposal a maximum of RETRIES times, at which point you can return a nil reply and an error.
func (s *State) ReplicateKey(ctx context.Context, r *pb.PutRequest) (*pb.PutReply, error) {

	// TODO(students): [Raft] Implement me!
	// 1. First marshal the KV struct using the encoding/json library
	kv := pb.ResolvableKV{
		Key:   r.Key,
		Value: r.Value,
	}
	data, err := json.Marshal(kv)
	if err != nil {
		s.log.Printf("marshal data error: %v", err)
		return nil, err
	}

	// 2. Put the marshalled struct in the proposeC channel and wait until the Raft cluster has confirmed that it has been committed. Think about how you can use the commitC channel to find out when something has been committed.
	commitChan := make(chan []byte) // dataChan is used to store the committed data
	go func() {
		for range s.commitC { // for range s.commitC
			commitChan <- data // put the committed data in the dataChan channel
		}
	}()

	s.proposeC <- data // put the marshalled struct in the proposeC channel

	// 3. If the proposal has not been committed after RETRY_TIME, you should retry it. You should retry the proposal a maximum of RETRIES times, at which point you can return a nil reply and an error.
	// should use a simple channel send/receive instead of select with a single case (S1000)
	for i := 0; i < RETRIES; i++ { // retry RETRIES times
		select {
		case <-time.After(RETRY_TIME): // wait for RETRY_TIME
			s.proposeC <- data // retry
		case data := <-commitChan:
			if string(data) == string(kv.Key) {
				return &pb.PutReply{}, nil // return a nil reply and no error if the proposal has been committed
			}
		}
	}

	return nil, nil // return a nil reply and an error if the proposal has not been committed after RETRY_TIME
}

// GetReplicatedKey reads the given key from s.store. The implementation of
// this method should be pretty short (roughly 8-12 lines).
func (s *State) GetReplicatedKey(ctx context.Context, r *pb.GetRequest) (*pb.GetReply, error) {

	// TODO(students): [Raft] Implement me!
	// 1. Read the given key from s.store
	// pretty short (roughly 8-12 lines)
	// ctx is the context of the request, r is the request that contains the key
	s.mu.RLock()                // lock
	value, ok := s.store[r.Key] // read the given key from s.store
	s.mu.RUnlock()              // unlock

	if ok {
		return &pb.GetReply{Value: value}, nil // return the value and no error if the key is found
	}

	// 2. If the key is not found, return a nil reply and an error
	return nil, fmt.Errorf("key not found") // return a nil reply and an error

	// return nil, nil
}
