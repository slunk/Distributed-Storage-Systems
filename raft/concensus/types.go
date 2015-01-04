package raft

import ()

type Entry struct {
	Vtype int    // 1 - null, 2 - group size change, 3 - client value
	Size  int    // only used for changing group size
	Value string // string instead of []byte for debugging and me
	Term  uint64
}

type RaftMessage struct {
	Vtype   int
	Success bool
	Ivalue  int // multipurpose integer value
	Entries []Entry
	// My stuff
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	CandidateId  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// log entry types
const (
	_            = iota
	ENTRY_SIZE   // ’Size’ has new size request
	ENTRY_CLIENT // ’Value’ has data
)

const (
	_ = iota
	SUCCESS
	INVALID_TERM_FAILURE
	INCONSISTENT_LOGS_FAILURE
)

const (
	_                      = iota
	RAFT_APPEND_REQ        // ’Value’ has data
	RAFT_APPEND_REP        // ’Value has data
	RAFT_VOTE_REQ          // ’Value has data
	RAFT_VOTE_REP          // ’Value has data
	RAFT_CLIENT_SIZE_REQ   // To replica, asking to change size of group
	RAFT_CLIENT_SIZE_REPLY // To client, w/ ’Success’ and ’Ivalue’ (used to hold
	// pid of actual leader replica.
	RAFT_CLIENT_VALUE_REQ   // To replica, asking to append a value to log.
	RAFT_CLIENT_VALUE_REPLY // To client, w/ ’Success’ and ’Ivalue’ (used to hold
	// pid of actual leader replica.
)
