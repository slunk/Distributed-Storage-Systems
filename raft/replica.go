package raft

import (
	"math/rand"
	"time"
)

type Entry struct {
	Vtype int    // 1 - null, 2 - group size change, 3 - client value
	Size  int    // only used for changing group size
	Value string // string instead of []byte for debugging and me
	Term  uint64
}

const (
	FOLLOWER  = iota
	CANDIDATE = iota
	LEADER    = iota
)

type Server struct {
	// Persistent State
	currentTerm uint64
	votedFor    string
	log         []Entry
	// Volatile State
	commitIndex uint64
	lastApplied uint64
	// Volatile Leader State
	nextIndex   []uint64
	matechIndex []uint64
	// Mine
	state int
}

func NewServer() *Server {
	return &Server{
		currentTerm: 0,                // TODO: persistence
		votedFor:    nil,              // TODO: persistence
		log:         make([]Entry, 0), // TODO: persistence
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   nil,
		matchIndex:  nil,
		state:       FOLLOWER,
	}
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

func (server *Server) AppendEntries(args AppendEntriesArgs) (uint64, bool) {
	if args.term < server.CurrentTerm {
		return server.currentTerm, false
	}
	if server.lastApplied < args.PrevLogIndex || server.log[args.PrevLogIndex].Term != term.PrevLogTerm {
		return server.currentTerm, false
	}
	for i, entry := range args.entries {
		logIdx = server.lastApplied + i + 1
		if len(server.log) > logIdx {
			if server.log[logIdx].Term != entry.Term {
				server.log[logIdx] = entry
			}
		} else {
			server.log = append(server.log, entry)
		}
		server.lastApplied++
	}
	if args.LeaderCommit > server.commitIndex {
		lastNewEntryIndex := args.PrevLogIndex + len(args.entries)
		if args.LeaderCommit < lastNewEntryIndex {
			server.leaderCommit = args.LeaderCommit
		} else {
			server.leaderCommit = lastNewEntryIndex
		}
	}
	return server.currentTerm, true
}

type RequestVoteArgs struct {
	term         uint64
	candidateId  string
	lastLogIndex uint64
	lastLogTerm  uint64
}

func RequestVote(args RequestVoteArgs) (uint64, bool) {
	if args.term < server.currentTerm {
		return server.currentTerm, false
	}
}

const ELECT_TIMEOUT_LO = 150 * time.Millisecond
const ELECT_TIMEOUT_HI = 300 * time.Millisecond

func WaitForElectionTimeout() <-chan time.Time {
	millis := rand.Int63n(ELECT_TIMEOUT_HI-ELECT_TIMEOUT_LO) + ELECT_TIMEOUT_LO
	return time.After(millis)
}

const BROADCAST_TIMEOUT = 10 * time.Millisecond

func (server *Server) DoConsensus() {
	for {
		switch server.state {
		case FOLLOWER:
			receivedRPC := make(chan bool)
			select {
			case <-receivedRPC:
				break
			case <-WaitForElectionTimeout():
				server.state = CANDIDATE
				break
			}
		case CANDIDATE:
			server.currentTerm++
			receivedMajority := make(chan bool)
			select {
			case <-receivedMajority:
				server.state = LEADER
				// immediately send heartbeat
				break
			case <-WaitForElectionTimeout():
				break
			}
		case LEADER:
			receivedClientRequest := make(chan bool)
			select {
			case <-receivedClientRequest:
				// apply to log and perform RPCs
				break
			case <-time.After(BROADCAST_TIMEOUT):
				// Send empty append RPCs
				break
			}
		}
	}
}
