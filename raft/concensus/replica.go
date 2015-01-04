package raft

import (
	"dss/util"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

var delay = 30

const (
	FOLLOWER  = iota
	CANDIDATE = iota
	LEADER    = iota
)

type Raft struct {
	numServers int
}

func (raft *Raft) NumServers() int {
	return raft.numServers
}

type PersistentState struct {
	CurrentTerm uint64
	VotedFor    *string
	Log         []Entry
}

const (
	_ = iota
	STABLE
	NON_COMMITED_GROUP_CHANGE
	JOINT_CONSENSUS
)

type Server struct {
	// Persistent State
	persistent *PersistentState
	// Volatile State
	commitIndex uint64
	lastApplied uint64
	// Volatile Leader State
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	// Mine
	state                 int
	votesForUs            int
	receivedAppendRPC     chan *RaftMessage
	receivedVoteRPC       chan *RaftMessage
	receivedClientRequest chan *RaftMessage
	clientResponse        chan *RaftMessage
	receivedMajority      chan bool
	raft                  Raft
	newConfig             Raft
	comm                  *util.Communicator
	ourId                 string
	timestamp             int // for heartbeats
	leaderId              string
	mutex                 sync.Mutex
	replicas              map[string]*util.ReplicaInfo
	groupChangeState      int
	lastSizeEntry         uint64
}

func NewServer(pid string, comm *util.Communicator, replicas map[string]*util.ReplicaInfo) *Server {
	server := &Server{
		commitIndex:           0,
		lastApplied:           0,
		nextIndex:             nil,
		matchIndex:            nil,
		state:                 FOLLOWER,
		receivedAppendRPC:     make(chan *RaftMessage),
		receivedVoteRPC:       make(chan *RaftMessage),
		receivedClientRequest: make(chan *RaftMessage),
		clientResponse:        make(chan *RaftMessage),
		receivedMajority:      make(chan bool),
		comm:                  comm,
		ourId:                 pid,
		timestamp:             0,
		leaderId:              pid,
		replicas:              replicas,
		groupChangeState:      STABLE,
	}
	server.raft.numServers = 3
	server.persistent = server.loadPersistentState()
	if server.persistent == nil {
		log := make([]Entry, 1)
		log[0].Term = 0
		server.persistent = &PersistentState{
			CurrentTerm: 0,
			VotedFor:    nil,
			Log:         log,
		}
	}
	go server.comm.RaftMessageListener(server.handleRaftMessage)
	go server.comm.RaftClientMessageListener(server.handleClientMessage)
	return server
}

func (server *Server) Lock() {
	//server.mutex.Lock()
}

func (server *Server) Unlock() {
	//server.mutex.Unlock()
}

func (server *Server) getPersistentStateFilePath() string {
	return "/tmp/slunkRaftReplica" + server.ourId
}

func (server *Server) savePersistentState() {
	server.Lock()
	defer server.Unlock()
	outfile, _ := os.Create(server.getPersistentStateFilePath())
	defer outfile.Close()
	marshalled, _ := json.Marshal(server.persistent)
	outfile.Write(marshalled)
}

func (server *Server) loadPersistentState() *PersistentState {
	if data, err := ioutil.ReadFile(server.getPersistentStateFilePath()); err == nil {
		persistent := new(PersistentState)
		if err := json.Unmarshal(data, persistent); len(data) > 0 && err == nil {
			return persistent
		}
	}
	return nil
}

func (server *Server) getCurrentTerm() uint64 {
	return server.persistent.CurrentTerm
}

func (server *Server) getVotedFor() *string {
	return server.persistent.VotedFor
}

func (server *Server) getLog() []Entry {
	return server.persistent.Log
}

func (server *Server) getEntry(idx uint64) Entry {
	return server.persistent.Log[idx]
}

func (server *Server) logLength() uint64 {
	return uint64(len(server.persistent.Log))
}

func (server *Server) incrementCurrentTerm() {
	server.persistent.CurrentTerm++
}

func (server *Server) setCurrentTerm(term uint64) {
	server.persistent.CurrentTerm = term
}

func (server *Server) setVotedFor(pid string) {
	server.persistent.VotedFor = &pid
}

func (server *Server) clearVotedFor() {
	server.persistent.VotedFor = nil
}

func (server *Server) setEntry(idx uint64, entry Entry) {
	server.persistent.Log[idx] = entry
}

func (server *Server) appendEntry(entry Entry) {
	server.persistent.Log = append(server.persistent.Log, entry)
}

func (server *Server) handleClientMessage(message []byte) []byte {
	if server.state != LEADER {
		leaderId, _ := strconv.Atoi(server.leaderId)
		resp, _ := json.Marshal(RaftMessage{
			Success:  false,
			Ivalue:   leaderId,
			LeaderID: server.leaderId,
		})
		return resp
	}
	rmsg := new(RaftMessage)
	if err := json.Unmarshal(message, rmsg); err != nil {
		return nil
	}
	switch rmsg.Vtype {
	case RAFT_CLIENT_VALUE_REQ:
		fmt.Println("RECV CLIENT_VALUE")
		server.receivedClientRequest <- rmsg
		resp := <-server.clientResponse
		tmp, _ := json.Marshal(resp)
		return tmp
	case RAFT_CLIENT_SIZE_REQ:
		fmt.Println("RECV CLIENT_SIZE", server.newConfig.numServers)
		server.newConfig.numServers = rmsg.Entries[0].Size
		server.groupChangeState = JOINT_CONSENSUS
		server.receivedClientRequest <- rmsg
		resp := <-server.clientResponse
		server.raft.numServers = server.newConfig.numServers
		server.groupChangeState = STABLE
		tmp, _ := json.Marshal(resp)
		return tmp
	}
	return nil
}

func (server *Server) AppendEntriesRPC(args *RaftMessage) {
	server.Lock()
	defer server.Unlock()
	term, success, status := server.AppendEntries(args)
	msg, _ := json.Marshal(RaftMessage{
		Vtype:        RAFT_APPEND_REP,
		Ivalue:       status,
		Term:         term,
		Success:      success,
		Entries:      args.Entries,
		PrevLogIndex: args.PrevLogIndex,
	})
	server.comm.RaftBroadcast(msg)
	fmt.Println("SEND APPEND_REP success", success)
}

func (server *Server) AppendEntries(args *RaftMessage) (uint64, bool, int) {
	if args.Term < server.getCurrentTerm() {
		return server.getCurrentTerm(), false, INVALID_TERM_FAILURE
	}
	if server.logLength() <= args.PrevLogIndex || server.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		return server.getCurrentTerm(), false, INCONSISTENT_LOGS_FAILURE
	}
	logIdx := args.PrevLogIndex + 1
	for _, entry := range args.Entries {
		if entry.Vtype != 1 {
			if server.logLength() > logIdx {
				if server.getEntry(logIdx).Term != entry.Term {
					server.setEntry(logIdx, entry)
					// Only use a new configuration if we're seeing it for the first time
					if entry.Vtype == 2 {
						server.newConfig.numServers = entry.Size
						server.groupChangeState = JOINT_CONSENSUS
						server.lastSizeEntry = logIdx
					}
				}
			} else {
				server.appendEntry(entry)
				// Only use a new configuration if we're seeing it for the first time
				if entry.Vtype == 2 {
					server.newConfig.numServers = entry.Size
					server.groupChangeState = JOINT_CONSENSUS
					server.lastSizeEntry = logIdx
				}
			}
		}
		logIdx++
	}
	if args.LeaderCommit > server.commitIndex {
		if args.LeaderCommit < logIdx-1 {
			server.commitIndex = args.LeaderCommit
		} else {
			server.commitIndex = logIdx - 1
		}
		if server.groupChangeState == JOINT_CONSENSUS && server.commitIndex > server.lastSizeEntry {
			server.raft.numServers = server.raft.numServers
			server.groupChangeState = STABLE
		}
	}
	return server.getCurrentTerm(), true, SUCCESS
}

func (server *Server) otherIsUpToDate(args *RaftMessage) bool {
	if server.commitIndex == 0 {
		return true
	}
	lastIdx := server.logLength() - 1
	lastTerm := server.getEntry(lastIdx).Term
	if lastTerm != args.LastLogTerm {
		return lastTerm < args.LastLogTerm
	}
	return uint64(lastIdx) <= args.LastLogIndex
}

func (server *Server) RequestVoteRPC(args *RaftMessage) {
	server.Lock()
	defer server.Unlock()
	term, success := server.RequestVote(args)
	msg, _ := json.Marshal(RaftMessage{
		Vtype:       RAFT_VOTE_REP,
		Term:        term,
		Success:     success,
		CandidateId: args.CandidateId,
	})
	server.comm.RaftBroadcast(msg)
	fmt.Println("SEND VOTE_REP candidate", args.CandidateId, "success", success)
}

func (server *Server) RequestVote(args *RaftMessage) (uint64, bool) {
	if args.Term < server.getCurrentTerm() {
		return server.getCurrentTerm(), false
	}
	if server.getVotedFor() == nil || *server.getVotedFor() == args.CandidateId {
		if server.otherIsUpToDate(args) {
			server.setVotedFor(args.CandidateId)
			return server.getCurrentTerm(), true
		}
	}
	return server.getCurrentTerm(), false
}

const ELECT_TIMEOUT_LO = 150
const ELECT_TIMEOUT_HI = 300

func WaitForElectionTimeout() <-chan time.Time {
	millis := rand.Int63n(ELECT_TIMEOUT_HI-ELECT_TIMEOUT_LO) + ELECT_TIMEOUT_LO
	//fmt.Println("Waiting for ", millis, " milliseconds")
	return time.After(time.Duration(millis) * time.Millisecond * time.Duration(delay))
}

const BROADCAST_TIMEOUT = 10 * time.Millisecond

func (server *Server) MainLoop() {
	for {
		switch server.state {
		case FOLLOWER:
			server.clearVotedFor()
			select {
			case rmsg := <-server.receivedAppendRPC:
				server.savePersistentState()
				server.AppendEntriesRPC(rmsg)
				break
			case rmsg := <-server.receivedVoteRPC:
				server.savePersistentState()
				server.RequestVoteRPC(rmsg)
				break
			case <-WaitForElectionTimeout():
				server.state = CANDIDATE
				break
			}
		case CANDIDATE:
			server.Lock()
			server.incrementCurrentTerm()
			server.setVotedFor(server.ourId)
			server.votesForUs = 1
			lastIdx := server.logLength() - 1
			lastLogTerm := server.getEntry(lastIdx).Term
			msg, _ := json.Marshal(RaftMessage{
				Vtype:        RAFT_VOTE_REQ,
				Term:         server.getCurrentTerm(),
				CandidateId:  server.ourId,
				LastLogIndex: uint64(lastIdx),
				LastLogTerm:  lastLogTerm,
			})
			fmt.Println("SEND VOTE_REQ term", server.getCurrentTerm())
			server.comm.RaftBroadcast(msg)
			server.Unlock()
			select {
			case rmsg := <-server.receivedAppendRPC:
				server.savePersistentState()
				server.state = FOLLOWER
				server.AppendEntriesRPC(rmsg)
				break
			case rmsg := <-server.receivedVoteRPC:
				server.savePersistentState()
				server.RequestVoteRPC(rmsg)
				break
			case <-server.receivedMajority:
				server.state = LEADER
				server.nextIndex = make(map[string]uint64)
				server.matchIndex = make(map[string]uint64)
				server.sendHeartbeat()
				break
			case <-WaitForElectionTimeout():
				break
			}
		case LEADER:
			select {
			case <-server.receivedAppendRPC:
				server.savePersistentState()
				// uhhh. what here?
				break
			case <-server.receivedVoteRPC:
				server.savePersistentState()
				// uhh. what here?
				break
			case rmsg := <-server.receivedClientRequest:
				server.Lock()
				rmsg.Entries[0].Term = server.getCurrentTerm()
				server.appendEntry(rmsg.Entries[0])
				i := server.logLength() - 1
				//server.getEntry(i).Term = server.getCurrentTerm()
				server.matchIndex[server.ourId] = uint64(i)
				server.sendAppend(server.getLog()[i:], uint64(i-1))
				server.Unlock()
				server.savePersistentState()
				break
			case <-time.After(BROADCAST_TIMEOUT * time.Duration(delay)):
				server.sendHeartbeat()
				// Send heartbeat
				break
			}
		}
	}
}

func (server *Server) sendAppend(entries []Entry, prevIdx uint64) {
	prevTerm := uint64(0)
	if prevIdx >= 0 {
		prevTerm = server.getEntry(prevIdx).Term
	} else {
		prevIdx = 0
	}
	msg, _ := json.Marshal(RaftMessage{
		Vtype:        RAFT_APPEND_REQ,
		Entries:      entries,
		Term:         server.getCurrentTerm(),
		LeaderID:     server.ourId,
		LeaderCommit: server.commitIndex,
		PrevLogIndex: uint64(prevIdx),
		PrevLogTerm:  uint64(prevTerm),
	})
	server.comm.RaftBroadcast(msg)
	fmt.Println("SEND APPEND_REQ")
}

func (server *Server) sendHeartbeat() {
	entries := make([]Entry, 1)
	entries[0] = Entry{
		Vtype: 1,
		Value: server.ourId + "-" + strconv.Itoa(server.timestamp),
	}
	server.timestamp++
	prevIdx := server.logLength() - 2
	if server.logLength() == 1 {
		prevIdx = 0
	}
	server.sendAppend(entries, uint64(prevIdx))
}

func (server *Server) handleRaftMessage(pid string, msg []byte) {
	raftMsg := new(RaftMessage)
	if err := json.Unmarshal(msg, raftMsg); err == nil {
		server.Lock()
		if server.shouldListenTo(pid) {
			if raftMsg.Term > server.getCurrentTerm() {
				server.setCurrentTerm(raftMsg.Term)
				server.state = FOLLOWER
			}
		}
		server.Unlock()
		switch raftMsg.Vtype {
		case RAFT_APPEND_REQ:
			server.handleAppendReq(pid, raftMsg)
		case RAFT_APPEND_REP:
			if server.shouldListenTo(pid) {
				server.handleAppendRep(pid, raftMsg)
			}
		case RAFT_VOTE_REQ:
			server.handleVoteReq(pid, raftMsg)
		case RAFT_VOTE_REP:
			if server.shouldListenTo(pid) {
				server.handleVoteRep(pid, raftMsg)
			}
		}
	}
}

func (server *Server) shouldListenTo(pid string) bool {
	return server.oldConfigContains(pid) || server.newConfigContains(pid)
}

func (server *Server) oldConfigContains(pid string) bool {
	return server.replicas[pid].IndexInList < server.raft.NumServers()
}

func (server *Server) newConfigContains(pid string) bool {
	return server.groupChangeState != STABLE && server.replicas[pid].IndexInList < server.newConfig.NumServers()
}

func (server *Server) handleAppendReq(pid string, rmsg *RaftMessage) {
	fmt.Println("RECV APPEND_REQ from", pid, "term", rmsg.Term, "index", rmsg.PrevLogIndex)
	server.leaderId = pid
	server.receivedAppendRPC <- rmsg
}

func (server *Server) hasReachedConsensusOnEntry() (bool, int) {
	counts := make(map[uint64]int)
	for pid, val := range server.matchIndex {
		if server.oldConfigContains(pid) {
			counts[val]++
		}
	}
	countsNew := make(map[uint64]int)
	for pid, val := range server.matchIndex {
		if server.newConfigContains(pid) {
			countsNew[val]++
		}
	}
	rval := false
	entryType := 1
	switch server.groupChangeState {
	case STABLE:
		for N, numReplicated := range counts {
			if N > server.commitIndex && numReplicated+1 > server.raft.NumServers()/2 && server.getEntry(N).Term == server.getCurrentTerm() {
				server.commitIndex = N
				entryType = server.getEntry(N).Vtype
				rval = true
			}
		}
	case JOINT_CONSENSUS:
		for N, numReplicated := range counts {
			if N > server.commitIndex && numReplicated+1 > server.raft.NumServers()/2 && server.getEntry(N).Term == server.getCurrentTerm() {
				if val, ok := countsNew[N]; ok && val+1 > server.newConfig.NumServers()/2 {
					server.commitIndex = N
					entryType = server.getEntry(N).Vtype
					rval = true
				}
			}
		}
	}
	return rval, entryType
}

func (server *Server) handleAppendRep(pid string, rmsg *RaftMessage) {
	fmt.Println("RECV APPEND_REP from", pid, "term", rmsg.Term, "index", rmsg.PrevLogIndex, "success", rmsg.Success)
	if server.state == LEADER {
		if rmsg.Success {
			matchIndex := rmsg.PrevLogIndex + uint64(len(rmsg.Entries))
			if rmsg.Entries[0].Vtype != 1 {
				if matchIndex > server.matchIndex[pid] {
					server.nextIndex[pid] = matchIndex + 1
					server.matchIndex[pid] = matchIndex
				}
			}
			if ok, entryType := server.hasReachedConsensusOnEntry(); ok {
				switch entryType {
				case 2:
					server.clientResponse <- &RaftMessage{
						Vtype:   RAFT_CLIENT_SIZE_REPLY,
						Success: true,
					}
				case 3:
					server.clientResponse <- &RaftMessage{
						Vtype:   RAFT_CLIENT_VALUE_REPLY,
						Success: true,
					}
				}
			}
		} else {
			switch rmsg.Ivalue {
			case INVALID_TERM_FAILURE:
				server.state = FOLLOWER
			case INCONSISTENT_LOGS_FAILURE:
				if _, ok := server.nextIndex[pid]; !ok {
					server.nextIndex[pid] = server.logLength()
				}
				server.nextIndex[pid]--
				fmt.Println(server.nextIndex[pid])
				server.sendAppend(server.getLog()[server.nextIndex[pid]:], server.nextIndex[pid]-1)
			}
		}
	}
}

func (server *Server) handleVoteReq(pid string, rmsg *RaftMessage) {
	fmt.Println("RECV VOTE_REQ from", pid, "term", rmsg.Term)
	server.receivedVoteRPC <- rmsg
}

func (server *Server) handleVoteRep(pid string, rmsg *RaftMessage) {
	fmt.Println("RECV VOTE_REP from", pid, rmsg.Success)
	if server.state == CANDIDATE && rmsg.CandidateId == server.ourId && rmsg.Success {
		fmt.Println("IN IF")
		server.votesForUs++
		if server.votesForUs > server.raft.NumServers()/2 {
			// still not quite right
			server.state = LEADER
			server.receivedMajority <- true
		}
	}
}
