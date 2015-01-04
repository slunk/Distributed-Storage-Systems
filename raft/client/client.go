package main

import (
	//"dss/raft/concensus"
	"dss/raft/concensus"
	"dss/util"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	command := flag.String("command", "auto", "replica name")
	//to := flag.String("to", "auto", "replica name")
	flag.Parse()
	replicas := util.ReadReplicaInfo("config.txt")
	fmt.Println(replicas)
	comm, _ := util.NewCommunicator("auto", replicas)
	switch *command {
	case "append":
		entries := make([]raft.Entry, 1)
		entries[0].Vtype = 3
		entries[0].Value = "lol"

		rmsg := raft.RaftMessage{
			Vtype:   raft.RAFT_CLIENT_VALUE_REQ,
			Entries: entries,
		}
		req, _ := json.Marshal(rmsg)
		msg, _ := comm.RaftClientSend(req)
		fmt.Println(string(msg))
	case "size":
		entries := make([]raft.Entry, 1)
		entries[0].Vtype = 2
		entries[0].Size = 5
		entries[0].Value = "lol"

		rmsg := raft.RaftMessage{
			Vtype:   raft.RAFT_CLIENT_SIZE_REQ,
			Entries: entries,
		}
		req, _ := json.Marshal(rmsg)
		msg, _ := comm.RaftClientSend(req)
		fmt.Println(string(msg))
	default:
		fmt.Println("Command not supported")
	}
}
