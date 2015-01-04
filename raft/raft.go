package main

import (
	"dss/raft/concensus"
	"dss/util"
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
	name := flag.String("name", "auto", "replica name")
	flag.Parse()
	fmt.Println(*name)
	replicas := util.ReadReplicaInfo("config.txt")
	fmt.Println(replicas)
	pid := util.GetOurPid("config.txt", *name)
	comm, _ := util.NewCommunicator(*name, replicas)
	defer comm.Close()
	server := raft.NewServer(pid, comm, replicas)
	server.MainLoop()
}
