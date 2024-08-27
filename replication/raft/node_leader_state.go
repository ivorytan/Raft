package raft

import (
	"context"
	"math"
	"math/rand"
	"modist/orchestrator/node"
	pb "modist/proto"
	"time"
)

// doLeader implements the logic for a Raft node in the leader state.
func (rn *RaftNode) doLeader() stateFunction {
	rn.log.Printf("transitioning to leader state at term %d", rn.GetCurrentTerm())
	rn.state = LeaderState

	// TODO(students): [Raft] Implement me!
	// Hint: perform any initial work, and then consider what a node in the
	// leader state should do when it receives an incoming message on every
	// possible channel.

	// create a cancellable context that cleans up when the state function returns (for making an RPC call)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize leader state
	rn.becomeLeader()

	// Print out the initial values of the Raft node for debugging
	rn.log.Printf("\t[Debug::] leader %d starting at term %d with leader %d and votedfor %d", rn.node.ID, rn.GetCurrentTerm(), rn.leader, rn.GetVotedFor())

	// Each leader stores a blank NOOP entry into the log at the start of its term
	// This entry will be used to send heartbeats to followers
	emptyEntry := &pb.LogEntry{
		Term:  rn.GetCurrentTerm(),
		Index: rn.LastLogIndex() + 1,
		Type:  pb.EntryType_NORMAL,
		Data:  nil,
	}

	// store NOOP entry in log
	rn.StoreLog(emptyEntry)

	rn.broadcastAppendEntries(ctx)

	// start ticker to send heartbeats to followers
	rn.heartbeatTicker.Reset(rn.heartbeatTimeout)

	// Loop until we receive a message on the channel to transition to a different state
	for {
		select {
		case msg, ok := <-rn.proposeC: // handle client requests
			if !ok { // check if channel closed: channel closed, raft node is shutting down, should also close RaftNode.stopC and use rn.Stop() to clean up remaining data structures
				rn.log.Printf("leader %d received shutdown signal", rn.node.ID)
				close(rn.stopC)
				rn.Stop()
				return nil
			}

			rn.handlePropose(ctx, &pb.ProposalRequest{Data: msg}) // handle client request

		case msg := <-rn.appendEntriesC: // handle AppendEntries RPC from peers
			fallback := rn.handleAppendEntries(msg)

			if fallback {
				rn.log.Printf("\t[Debug::] leader %d received AppendEntries RPC from %d and is falling back to follower state", rn.node.ID, msg.request.From)
				return rn.doFollower
			}

		case msg := <-rn.requestVoteC: // handle RequestVote RPC from peers
			// TODO: might need to make sure leader state is not changed
			rn.leaderMu.Lock()
			rn.setVotedFor(rn.node.ID)
			rn.leaderMu.Unlock()

			fallback := rn.handleRequestVote(msg)
			if fallback {
				rn.log.Printf("\t[Debug::] leader %d received RequestVote RPC from %d and is falling back to follower state", rn.node.ID, msg.request.From)
				return rn.doFollower
			}

		case <-rn.heartbeatTicker.C: // handle timeout
			rn.log.Printf("\t[Debug::] leader %d heartbeat timeout, broadcasting AppendEntries RPC", rn.node.ID)
			rn.broadcastAppendEntries(ctx)

		case <-rn.stopC:
			rn.log.Printf("\t[Debug::] leader %d received shutdown signal", rn.node.ID)
			close(rn.stopC)
			rn.Stop()
			return nil
		case <-ctx.Done():
			// exit if context is cancelled
			rn.log.Printf("\t[Debug::] leader %d context cancelled", rn.node.ID)
			return nil
		}
	}
}

// broadcastAppendEntries sends AppendEntries RPCs to all peers
func (rn *RaftNode) broadcastAppendEntries(ctx context.Context) {
	rn.leaderMu.Lock()

	// check if we are still the leader
	if rn.state != LeaderState {
		rn.leaderMu.Unlock()
		return
	}

	rn.log.Printf("\t[Debug::] leader %d broadcasting AppendEntries RPC Start", rn.node.ID)

	// reset heartbeat timer
	rn.heartbeatTicker.Reset(rn.heartbeatTimeout)

	rn.leaderMu.Unlock()

	rn.commitEntries() // double check at the beginning of each broadcast

	for _, peer := range rn.node.PeerNodes {
		if peer.ID != rn.node.ID { // skip ourselves
			go func(peer *node.Node) {
				rn.leaderMu.Lock()

				if rn.state != LeaderState {
					rn.leaderMu.Unlock()
					return
				}

				entries := []*pb.LogEntry{}
				// get all log entries to store
				for i := rn.nextIndex[peer.ID]; i <= rn.LastLogIndex(); i++ {
					entries = append(entries, rn.GetLog(i))
				}

				// create an AppendEntriesRequest
				req := &pb.AppendEntriesRequest{
					From:         rn.node.ID,
					To:           peer.ID,
					Term:         rn.GetCurrentTerm(),
					LeaderCommit: rn.commitIndex,
					PrevLogIndex: rn.nextIndex[peer.ID] - 1,
					PrevLogTerm:  rn.GetLog(rn.nextIndex[peer.ID] - 1).Term,
					Entries:      entries,
				}

				rn.leaderMu.Unlock()

				// print out the leader's request for debugging
				rn.log.Printf("\t[Debug::] leader %d sending AppendEntries RPC to %d with entries %+v, prevLogIndex %d, prevLogTerm %d, commitIndex %d", rn.node.ID, peer.ID, req.Entries, req.PrevLogIndex, req.PrevLogTerm, req.LeaderCommit)

				// send AppendEntries RPC
				followerConn := rn.node.PeerConns[peer.ID]
				follower := pb.NewRaftRPCClient(followerConn)
				resp, err := follower.AppendEntries(ctx, req)

				// check if AppendEntries RPC failed
				if err != nil {
					rn.log.Printf("\t[Debug::] leader %d AppendEntries RPC failed: %v", rn.node.ID, err)
					rn.nextIndex[peer.ID]-- // decrement nextIndex
					return
				}

				// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
				if resp.Term > rn.GetCurrentTerm() {
					rn.log.Printf("\t[Debug::] leader %d received reply with higher term %d from follower %d", rn.node.ID, resp.Term, peer.ID)
					// rn.convertToFollower(resp.Term)
					rn.becomeFollower(resp.Term, None)
					return
				}

				if rn.state != LeaderState {
					return
				}

				// check if AppendEntries RPC failed due to log inconsistency, decrement nextIndex and wait until the next round of broadcastAppendEntries
				if !resp.Success {
					rn.log.Printf("\t[Debug::] leader %d AppendEntries RPC failed due to log inconsistency", rn.node.ID)
					rn.leaderMu.Lock()
					rn.nextIndex[peer.ID]-- // decrement nextIndex
					rn.leaderMu.Unlock()
					return
				} else {
					rn.log.Printf("\t[Debug::] leader %d AppendEntries RPC succeeded to follower %d", rn.node.ID, peer.ID)

					// update nextIndex and matchIndex for follower
					rn.leaderMu.Lock()
					rn.nextIndex[peer.ID] = rn.LastLogIndex() + 1
					rn.matchIndex[peer.ID] = rn.LastLogIndex()
					rn.leaderMu.Unlock()

					rn.commitEntries() // commit entries if possible
				}
			}(peer)
		}
	}

	// rn.commitEntries() // commit entries if possible
}

// // convertToFollower converts the node to follower state
// func (rn *RaftNode) convertToFollower(term uint64) {
// 	rn.log.Printf("node %d converting to follower", rn.node.ID)
// 	rn.state = FollowerState
// 	rn.SetCurrentTerm(term)
// 	rn.setVotedFor(None)

// 	// reset election timer
// 	rn.electionTicker.Stop()
// 	rn.electionTicker.Reset(rn.getElectionTimeout())
// }

// electionTimeout returns a random timeout between rn.electionTimeout and 2 * rn.electionTimeout
func (rn *RaftNode) getElectionTimeout() time.Duration {
	// rn.electionTimeout is a time.Duration not int
	return time.Duration(rand.Intn(int(rn.electionTimeout)) + int(rn.electionTimeout))
}

// commitEntries updates commit index if there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm
func (rn *RaftNode) commitEntries() {
	rn.leaderMu.Lock()
	defer rn.leaderMu.Unlock()

	// update commitIndex if possible
	for i := rn.lastApplied + 1; i <= rn.LastLogIndex(); i++ {
		if rn.GetLog(i).Term == rn.GetCurrentTerm() {
			count := 1
			for _, peer := range rn.node.PeerNodes {
				if rn.matchIndex[peer.ID] >= i && peer.ID != rn.node.ID {
					count++
				}
			}
			if count > len(rn.node.PeerNodes)/2 {
				rn.log.Printf("\t[Debug::] leader %d updating commitIndex to %d", rn.node.ID, i)
				rn.commitIndex = i
			}
		}
	}

	// update lastApplied if possible
	for rn.lastApplied < rn.commitIndex {
		rn.lastApplied++

		// apply the log entry to the state machine by committing it to the channel commitC
		data := rn.GetLog(rn.lastApplied).GetData()
		c := make(commit, len(data))
		copy(c, data)
		rn.commitC <- &c
	}
}

// handleAppendEntries handles an AppendEntries RPC from a peer (rn could be any state)
func (rn *RaftNode) handleAppendEntries(msg AppendEntriesMsg) (fallback bool) {
	// get request and reply channel from message
	req := msg.request
	replyChan := msg.reply

	// Print out for debugging
	rn.log.Printf("\t[Debug::] Node %d received AppendEntries RPC from node %d, term %d, prevLogIndex %d, prevLogTerm %d, entries %+v, leaderCommit %d", rn.node.ID, req.From, req.Term, req.PrevLogIndex, req.PrevLogTerm, req.Entries, req.LeaderCommit)

	// check if sender's term is at least as large as receiver's term
	if req.Term < rn.GetCurrentTerm() {
		// reply false if the request is from an old term, reject the request
		rn.log.Printf("\t[Debug::] Node %d received AppendEntries RPC from node %d with old term %d", rn.node.ID, req.From, req.Term)
		replyChan <- pb.AppendEntriesReply{
			From:    rn.node.ID,
			To:      req.From,
			Term:    rn.GetCurrentTerm(),
			Success: false,
		}
		return false
	}

	// fallback since request term is equal or greater than current term
	fallback = true
	// update state to follower
	rn.state = FollowerState
	// // set leader to be the sender
	rn.leader = req.From

	// for all servers, if RPC request or response contains term T > currentTerm:
	if req.Term > rn.GetCurrentTerm() {
		rn.log.Printf("\t[Debug::] Node %d received AppendEntries RPC from node %d with higher term %d", rn.node.ID, req.From, req.Term)
		// rn.leader = req.From
		rn.SetCurrentTerm(req.Term) // set currentTerm = T, convert to follower
	}

	if req.PrevLogIndex > rn.LastLogIndex() {
		rn.log.Printf("\t[Debug::] Node %d received AppendEntries RPC from node %d with PrevLogIndex %d greater than last log index %d", rn.node.ID, req.From, req.PrevLogIndex, rn.LastLogIndex())

		replyChan <- pb.AppendEntriesReply{
			From:    rn.node.ID,
			To:      req.From,
			Term:    rn.GetCurrentTerm(),
			Success: false,
		}
	} else if entry := rn.GetLog(req.PrevLogIndex); entry == nil || entry.Term != req.PrevLogTerm {
		// // reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
		// rn.log.Printf("Node %d received AppendEntries RPC from node %d with PrevLogTerm %d different from last log term %d", rn.node.ID, req.From, req.PrevLogTerm, entry.Term)

		replyChan <- pb.AppendEntriesReply{
			From:    rn.node.ID,
			To:      req.From,
			Term:    rn.GetCurrentTerm(),
			Success: false,
		}
	} else {

		// Accept the request; truncate the log if necessary and append any new entries
		rn.log.Printf("\t[Debug::] Successfully received AppendEntries RPC from node %d", req.From)

		// // truncate the log if necessary
		// if req.PrevLogIndex+1 < rn.LastLogIndex() {
		// 	rn.TruncateLog(req.PrevLogIndex + 1)
		// }

		// // append any new entries
		// for _, entry := range req.Entries {
		// 	rn.log.Printf("\t[Debug::] Node %d appending entry %+v", rn.node.ID, entry)
		// 	rn.StoreLog(entry)
		// }

		lastNewIndex := req.PrevLogIndex
		for _, IncomingEntry := range req.Entries {
			lastNewIndex = IncomingEntry.GetIndex()
			logEntry := rn.GetLog(lastNewIndex)
			if logEntry == nil {
				rn.StoreLog(IncomingEntry)
			} else if logEntry.Term != IncomingEntry.GetTerm() {
				rn.TruncateLog(lastNewIndex)
				rn.StoreLog(IncomingEntry)
			}
		}

		// Update commit index, process any committed entries, update last applied
		if req.LeaderCommit > rn.commitIndex {
			// update commitIndex to be the minimum of leaderCommit and index of last new entry
			rn.commitIndex = uint64(math.Min(float64(req.LeaderCommit), float64(lastNewIndex)))

			rn.log.Printf("\t[Debug::] Node %d updated commitIndex to %d", rn.node.ID, rn.commitIndex)

			for rn.lastApplied < rn.commitIndex {
				rn.lastApplied++

				// apply log[rn.lastApplied] to state machine
				commit := &commit{}
				*commit = rn.GetLog(rn.lastApplied).GetData()
				rn.log.Printf("\t[Debug::] Node %d applying commit %+v", rn.node.ID, commit)
				rn.commitC <- commit
			}

			rn.log.Printf("\t[Debug::] Node %d updated last applied to %d", rn.node.ID, rn.lastApplied)
		}

		rn.log.Printf("\t[Debug::] Node %d successfully processed AppendEntries RPC from node %d", rn.node.ID, req.From)

		// reply true if the request is valid
		replyChan <- pb.AppendEntriesReply{
			From:    rn.node.ID,
			To:      req.From,
			Term:    rn.GetCurrentTerm(),
			Success: true,
		}
	}

	return
}
