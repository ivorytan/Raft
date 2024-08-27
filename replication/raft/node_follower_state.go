package raft

import (
	"context"
	"errors"
	"modist/proto"
	pb "modist/proto"
)

// doFollower implements the logic for a Raft node in the follower state.
func (rn *RaftNode) doFollower() stateFunction {
	rn.state = FollowerState
	rn.log.Printf("transitioning to %s state at term %d", rn.state, rn.GetCurrentTerm())

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// follower state should do when it receives an incoming message on every
	// possible channel.

	// Print out the initial values of the Raft node for debugging
	rn.log.Printf("follower %d starting at term %d with leader %d", rn.node.ID, rn.GetCurrentTerm(), rn.leader)

	// create a cancellable context that cleans up when the state function returns (for making an RPC call)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rn.becomeFollower(rn.GetCurrentTerm(), None)

	// printout for debugging
	rn.log.Printf("0: last log index: %d, last log term: %d", rn.LastLogIndex(), rn.GetLog(rn.LastLogIndex()).Term)

	// Loop until we receive a message on the channel to transition to a different state
	for {
		select {
		case msg := <-rn.appendEntriesC:
			// handle AppendEntries RPC from peers
			rn.log.Printf("\t[Debug::] follower %d received AppendEntries RPC from leader %d", rn.node.ID, msg.request.From) // print out to debug
			fallback := rn.handleAppendEntries(msg)
			// if received a valid AppendEntries RPC from current leader: reset election timeout

			if fallback {
				rn.log.Printf("\t[Debug::] follower %d stay in follower state", rn.node.ID)
				rn.becomeFollower(rn.GetCurrentTerm(), None)
			}

		case msg := <-rn.requestVoteC:
			// printout for debugging
			rn.log.Printf("\t[Debug::] follower %d received RequestVote RPC from candidate %d", rn.node.ID, msg.request.From)
			// handle RequestVote RPC from peers
			fallback := rn.handleRequestVote(msg)
			if fallback {
				rn.log.Printf("\t[Debug::] follower %d stay in follower state", rn.node.ID)
				rn.becomeFollower(rn.GetCurrentTerm(), None)
			}

		case msg, ok := <-rn.proposeC:
			// handle client requests
			if !ok {
				// check if channel closed: channel closed, raft node is shutting down, should also close RaftNode.stopC and use rn.Stop() to clean up remaining data structures
				rn.log.Printf("\t[Debug::] follower %d received shutdown signal", rn.node.ID)
				close(rn.stopC)
				rn.Stop()
				return nil
			}

			// should handle client requests and get error using handlePropose func
			rn.handlePropose(ctx, &pb.ProposalRequest{Data: msg})

		case <-rn.electionTicker.C:
			// if election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
			rn.log.Printf("\t[Debug::] follower %d election timeout elapses, restart election", rn.node.ID)
			return rn.doCandidate

		case <-rn.stopC:
			rn.log.Printf("\t[Debug::] follower %d received shutdown signal", rn.node.ID)
			close(rn.stopC)
			rn.Stop()
			return nil

		case <-ctx.Done():
			rn.log.Printf("\t[Debug::] follower %d context done", rn.node.ID)
			return nil
		}
	}

	// return nil
}

// handlePropose handles a client request to the Raft node.
func (rn *RaftNode) handlePropose(ctx context.Context, request *proto.ProposalRequest) error {
	//The node is in the leader state: we accept the proposal and proceed normally
	if rn.state == LeaderState {
		// Handle client request by appending a new log entry to our own log and replicate it to all followers
		rn.log.Printf("\t[Debug::] leader %d received client request %s", rn.node.ID, request.GetData())
		// append entry to local log
		entry := &pb.LogEntry{
			Term:  rn.GetCurrentTerm(),
			Index: rn.LastLogIndex() + 1,
			Data:  request.GetData(),
		}

		rn.StoreLog(entry) // store entry to local log

		// broadcast AppendEntries RPC to all followers
		rn.log.Printf("\t[Debug::] leader %d replicating entry %s to all followers", rn.node.ID, entry)

		rn.broadcastAppendEntries(ctx)

		return nil
	} else if rn.state == FollowerState && rn.leader != None {
		// The node is in the follower state and is aware of a leader: we forward the client’s proposal to the leader
		rn.log.Printf("\t[Debug::] follower %d received client request %s, forwarding to leader %d", rn.node.ID, request.GetData(), rn.leader)
		// forward client request to leader
		leaderConn := rn.node.PeerConns[rn.leader]
		leaderClient := pb.NewRaftRPCClient(leaderConn)

		_, err := leaderClient.Propose(ctx, &proto.ProposalRequest{Data: request.GetData()})

		return err
	} else {
		// The node is in the candidate state or is not aware of a leader: we drop the client’s proposal and return an error. The client will be responsible for retrying until the proposal succeeds (implemented within the replicateKey function as described below).
		rn.log.Printf("\t[Debug::] candidate received client request or follower received client request but is not aware of a leader")
		return errors.New("candidate received client request or follower received client request but is not aware of a leader")
	}
}

// handleRequestVote handles RequestVote RPC from peers
func (rn *RaftNode) handleRequestVote(msg RequestVoteMsg) (fallback bool) {

	request := msg.request
	replyChan := msg.reply

	fallback = false

	// 1. Reply false if term < currentTerm (§5.1)
	if request.Term < rn.GetCurrentTerm() {
		replyChan <- proto.RequestVoteReply{
			From:        rn.node.ID,
			To:          request.From,
			Term:        rn.GetCurrentTerm(),
			VoteGranted: false,
		}
		return
	}

	fallback = true
	rn.state = FollowerState

	// 0. If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	if request.Term > rn.GetCurrentTerm() {
		rn.log.Printf("follower %d received RequestVote RPC from candidate %d with higher term %d", rn.node.ID, request.From, request.Term)

		rn.SetCurrentTerm(request.Term)
	}

	// printout log
	rn.log.Printf("Breakpoint 0: VotedFor: %d, LastLogIndex: %d, LastLogTerm: %d", rn.GetVotedFor(), rn.LastLogIndex(), rn.GetLog(rn.LastLogIndex()).Term)

	// 2. If we have already voted for another candidate in this term, reply false
	if rn.GetVotedFor() != None && rn.GetVotedFor() != request.From {
		rn.log.Printf("Breakpoint 1: VotedFor: %d, CandidateID: %d", rn.GetVotedFor(), request.From)
		replyChan <- proto.RequestVoteReply{
			From:        rn.node.ID,
			To:          request.From,
			Term:        rn.GetCurrentTerm(),
			VoteGranted: false,
		}
		return
	}

	// 3. If the candidate's log is not at least as up-to-date as ours, reply false
	// if lastlogterm < our lastlogterm, reply false
	if request.LastLogTerm < rn.GetLog(rn.LastLogIndex()).Term {
		rn.log.Printf("Breakpoint 2: VotedFor: %d, LastLogIndex: %d, LastLogTerm: %d, RequestIndex: %d, RequestTerm: %d", rn.GetVotedFor(), rn.LastLogIndex(), rn.GetLog(rn.LastLogIndex()).Term, request.LastLogIndex, request.LastLogTerm)
		replyChan <- proto.RequestVoteReply{
			From:        rn.node.ID,
			To:          request.From,
			Term:        rn.GetCurrentTerm(),
			VoteGranted: false,
		}
		return
	} else if request.LastLogTerm == rn.GetLog(rn.LastLogIndex()).Term && request.LastLogIndex < rn.LastLogIndex() { // if lastlogterm = our lastlogterm, lastlogindex < our lastlogindex, reply false
		rn.log.Printf("Breakpoint 3: VotedFor: %d, LastLogIndex: %d, LastLogTerm: %d, RequestIndex: %d, RequestTerm: %d", rn.GetVotedFor(), rn.LastLogIndex(), rn.GetLog(rn.LastLogIndex()).Term, request.LastLogIndex, request.LastLogTerm)
		replyChan <- proto.RequestVoteReply{
			From:        rn.node.ID,
			To:          request.From,
			Term:        rn.GetCurrentTerm(),
			VoteGranted: false,
		}
		return
	}

	// 4. Otherwise, vote for the candidate and reply true
	rn.setVotedFor(request.From)
	rn.SetCurrentTerm(request.Term)
	replyChan <- proto.RequestVoteReply{
		From:        rn.node.ID,
		To:          request.From,
		Term:        rn.GetCurrentTerm(),
		VoteGranted: true,
	}

	return
}

// becomeLeader makes the node become a leader
func (rn *RaftNode) becomeLeader() {
	rn.leaderMu.Lock()
	defer rn.leaderMu.Unlock()

	rn.state = LeaderState
	rn.matchIndex = make(map[uint64]uint64, len(rn.node.PeerNodes))
	rn.nextIndex = make(map[uint64]uint64, len(rn.node.PeerNodes))
	rn.leader = rn.node.ID
	rn.votesReceived = 0
	rn.rejectReceived = 0

	rn.setVotedFor(rn.node.ID)

	for _, peer := range rn.node.PeerNodes {
		rn.matchIndex[peer.ID] = 0
		rn.nextIndex[peer.ID] = rn.LastLogIndex() + 1
	}

	// reset heartbeat timer
	rn.heartbeatTicker.Reset(rn.heartbeatTimeout)
	// defer rn.heartbeatTicker.Stop()
}

// becomeFollower makes the node become a follower
func (rn *RaftNode) becomeFollower(term uint64, candidateID uint64) {
	rn.leaderMu.Lock()
	defer rn.leaderMu.Unlock()

	rn.state = FollowerState
	rn.SetCurrentTerm(term)
	rn.setVotedFor(candidateID)
	rn.votesReceived = 0
	rn.rejectReceived = 0

	// reset election timer
	rn.electionTicker.Reset(rn.getElectionTimeout())
	// defer rn.electionTicker.Stop()

	// reset heartbeat timer
	rn.heartbeatTicker.Reset(rn.heartbeatTimeout)
}

// becomeCandidate makes the node become a candidate
func (rn *RaftNode) becomeCandidate() uint64 {
	rn.leaderMu.Lock()
	defer rn.leaderMu.Unlock()

	rn.state = CandidateState
	rn.setVotedFor(rn.node.ID)
	rn.SetCurrentTerm(rn.GetCurrentTerm() + 1)
	rn.votesReceived = 1 // vote for self
	rn.rejectReceived = 0

	// reset election timer
	rn.electionTicker.Reset(rn.getElectionTimeout())
	// defer rn.electionTicker.Stop()

	return rn.GetCurrentTerm()
}
