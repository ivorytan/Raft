package raft

import (
	"context"
	"modist/orchestrator/node"
	pb "modist/proto"
)

// doCandidate implements the logic for a Raft node in the candidate state.
func (rn *RaftNode) doCandidate() stateFunction {
	rn.state = CandidateState
	rn.log.Printf("transitioning to %s state at term %d", rn.state, rn.GetCurrentTerm())

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.

	// Print out the initial values of the Raft node for debugging
	rn.log.Printf("\t[Debug::] candidate %d starting at term %d with leader %d", rn.node.ID, rn.GetCurrentTerm(), rn.leader)

	// create a cancellable context that cleans up when the state function returns (for making an RPC call)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rn.becomeCandidate()

	rn.log.Printf("\t[Debug::] candidate %d starting election", rn.node.ID)

	// send RequestVote RPCs to all other servers
	rn.sendRequestVoteRPCs(ctx)

	rn.electionTicker.Reset(rn.getElectionTimeout())

	// Loop until we receive a message on the channel to transition to a different state
	for {
		select {
		case msg := <-rn.appendEntriesC:
			// handle AppendEntries RPC from peers
			fallback := rn.handleAppendEntries(msg)
			rn.log.Printf("\t[Debug::] candidate %d reset election timeout", rn.node.ID)
			rn.electionTicker.Reset(rn.getElectionTimeout())
			if fallback {
				rn.log.Printf("\t[Debug::] candidate %d fallback to follower state", rn.node.ID)
				return rn.doFollower
			}

		case msg := <-rn.requestVoteC:
			// handle RequestVote RPC from peers
			fallback := rn.handleRequestVote(msg)
			if fallback {
				rn.log.Printf("\t[Debug::] candidate %d fallback to follower state", rn.node.ID)
				return rn.doFollower
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
			rn.handlePropose(ctx, &pb.ProposalRequest{Data: msg}) // TODO: candidate didn't need to handle propose

		case <-rn.electionTicker.C:
			// election timeout elapses: start new election
			rn.log.Printf("\t[Debug::] candidate %d election timeout elapses, start new election", rn.node.ID)
			return rn.doCandidate

		case <-rn.stopC:
			rn.log.Printf("\t[Debug::] follower %d received shutdown signal", rn.node.ID)
			close(rn.stopC)
			rn.Stop()
			return nil

		case <-ctx.Done():
			// context cancelled: cancel all pending RPCs
			rn.log.Printf("\t[Debug::] candidate %d context cancelled", rn.node.ID)
			return nil
		}
	}

	// return nil
}

// sendRequestVoteRPCs sends RequestVote RPCs to all other servers, gather votes and become leader if received majority of votes
func (rn *RaftNode) sendRequestVoteRPCs(ctx context.Context) {
	// use RequestVote function defined in node.go to send RequestVote RPCs to all other servers

	// If the cluster has only one node, automatically become leader
	if len(rn.node.PeerNodes) == 1 {
		rn.log.Printf("\t[Debug::] candidate %d automatically become leader", rn.node.ID)
		rn.leaderMu.Lock()
		rn.state = LeaderState
		rn.setVotedFor(rn.node.ID)
		rn.leaderMu.Unlock()

		// reset election timeout
		rn.electionTicker.Reset(rn.getElectionTimeout())

		rn.commitEntries() // check immediately if there are any uncommitted entries and commit them

		return
	}

	for _, peer := range rn.node.PeerNodes {
		if peer.ID != rn.node.ID { // exclude self
			go func(peer *node.Node) {
				rn.leaderMu.Lock()

				if rn.state != CandidateState {
					rn.leaderMu.Unlock()
					return
				}

				rn.log.Printf("\t[Debug::] candidate %d sending RequestVote RPC to peer %d", rn.node.ID, peer.ID)

				req := &pb.RequestVoteRequest{
					Term:         rn.GetCurrentTerm(),
					From:         rn.node.ID,
					To:           peer.ID,
					LastLogIndex: rn.LastLogIndex(),
					LastLogTerm:  rn.GetLog(rn.LastLogIndex()).Term,
				}
				rn.leaderMu.Unlock()

				peerConn := rn.node.PeerConns[peer.ID]
				peerClient := pb.NewRaftRPCClient(peerConn)

				reply, err := peerClient.RequestVote(ctx, req)

				if err != nil {
					rn.log.Printf("\t[Debug::] candidate %d failed to send RequestVote RPC to peer %d: %v", rn.node.ID, peer.ID, err)
					return
				}

				// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
				if reply.Term > rn.GetCurrentTerm() {
					rn.log.Printf("\t[Debug::] candidate %d received higher term %d from peer %d, convert to follower", rn.node.ID, reply.Term, peer.ID)
					// rn.convertToFollower(reply.Term)
					rn.becomeFollower(reply.Term, None)
					return
				}

				if rn.state != CandidateState {
					return
				}

				// If votes received from majority of servers: become leader
				if reply.VoteGranted {
					rn.log.Printf("\t[Debug::] candidate %d received vote from peer %d", rn.node.ID, reply.From)
					rn.votesReceived++
				} else {
					rn.log.Printf("\t[Debug::] candidate %d didn't receive vote from peer %d", rn.node.ID, reply.From)
					rn.rejectReceived++
				}

				majority := len(rn.node.PeerNodes)/2 + 1

				if rn.votesReceived >= majority {
					// rn.convertToLeader()
					rn.log.Printf("\t[Debug::] candidate %d received majority of votes, become leader", rn.node.ID)
					rn.becomeLeader()
					// broadcast to other peers to let them become followwers
					rn.broadcastAppendEntries(ctx)
				} else if rn.rejectReceived >= majority {
					// rn.convertToFollower(rn.GetCurrentTerm())
					rn.log.Printf("\t[Debug::] candidate %d received majority of rejects, become follower", rn.node.ID)
					rn.becomeFollower(rn.GetCurrentTerm(), None)
				}

			}(peer)
		}
	}

	// return
}

// // convertToLeader converts candidate to leader
// func (rn *RaftNode) convertToLeader() {
// 	rn.leaderMu.Lock()
// 	defer rn.leaderMu.Unlock()

// 	// Update state to leader
// 	rn.state = LeaderState
// 	rn.leader = rn.node.ID

// 	// Initialize nextIndex and matchIndex for each peer
// 	for _, peer := range rn.node.PeerNodes {
// 		rn.nextIndex[peer.ID] = rn.LastLogIndex() + 1
// 		rn.matchIndex[peer.ID] = 0
// 	}

// 	// TODO: send empty AppendEntries RPCs to all peers

// 	// reset election timer
// 	rn.electionTicker.Stop()
// 	rn.electionTicker = time.NewTicker(rn.getElectionTimeout())
// }
