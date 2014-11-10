package test

import (
	"dss/myfs"
	"testing"
)

func ReplicaFieldsAreCorrect(rep *myfs.ReplicaInfo, pid string, mnt string, dbPath string, ipAddr string, port string) bool {
	return rep.Pid == pid && rep.MntPoint == mnt && rep.DbPath == dbPath && rep.IpAddr == ipAddr && rep.Port == port
}

func TestReadReplicaInfo(t *testing.T) {
	replicas := myfs.ReadReplicaInfo("input/replica_info")
	assertWithMsg(t, len(replicas) == 4, "There should be 4 replicas")
	assertWithMsg(t, replicas["1"] != nil, "hub should be present")
	assertWithMsg(t, replicas["7"] != nil, "triffid should be present")
	assertWithMsg(t, replicas["11"] != nil, "hyper should be present")
	assertWithMsg(t, replicas["12"] != nil, "hyper2 should be present")
	assertWithMsg(t, ReplicaFieldsAreCorrect(replicas["1"], "1", "/tmp/kel1", "/tmp/dbkel1", "216.164.48.37", "3000"), "Replica fields should match what's in the input file")
	assertWithMsg(t, ReplicaFieldsAreCorrect(replicas["7"], "7", "/tmp/kel1", "/tmp/dbkel1", "128.8.126.119", "3000"), "Replica fields should match what's in the input file")
	assertWithMsg(t, ReplicaFieldsAreCorrect(replicas["11"], "11", "/tmp/kel1", "/tmp/dbkel1", "128.8.126.55", "3000"), "Replica fields should match what's in the input file")
	assertWithMsg(t, ReplicaFieldsAreCorrect(replicas["12"], "12", "/tmp/kel2", "/tmp/dbkel2", "128.8.126.55", "3010"), "Replica fields should match what's in the input file")
}

func TestMetaBroadcast(t *testing.T) {
	comm1, _ := myfs.NewCommunicator("one", myfs.ReadReplicaInfo("input/comm_repl_input"))
	comm2, _ := myfs.NewCommunicator("two", myfs.ReadReplicaInfo("input/comm_repl_input"))
	defer comm1.Close()
	defer comm2.Close()
	go comm2.MetaBroadcastListener(func(payload []byte) {
		//assertWithMsg(t, false, string(payload))
		//assertWithMsg(t, string(payload) == "metainfo", "Payload should match what we broadcast")
	})
	blah := []byte("metainfo")
	t.Log(string(blah))
	comm1.BroadcastMeta(blah)
}

func TestDemandFetch(t *testing.T) {
	comm1, _ := myfs.NewCommunicator("one", myfs.ReadReplicaInfo("input/comm_repl_input"))
	comm2, _ := myfs.NewCommunicator("two", myfs.ReadReplicaInfo("input/comm_repl_input"))
	defer comm1.Close()
	defer comm2.Close()
	go comm2.ChunkRequestListener(func(hash []byte) []byte {
		return []byte("omg")
	})
	resp, _ := comm1.RequestChunk("7", []byte("qwer"))
	assertWithMsg(t, string(resp) == "omg", "Expected chunk should be sent in response")
}
