package util

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"net"
	"os"
	"strconv"
	"strings"
)

type ReplicaInfo struct {
	Name           string
	Pid            string
	MntPoint       string
	DbPath         string
	IpAddr         string
	Port           string
	DemandPort     string
	RaftPubPort    string
	RaftP2PPort    string
	RaftClientPort string
	IndexInList    int
}

type Communicator struct {
	tag                    string
	context                *zmq.Context
	pubSocket              *zmq.Socket
	subSocket              *zmq.Socket
	chunkSocket            *zmq.Socket
	raftPubSocket          *zmq.Socket
	raftSubSocket          *zmq.Socket
	raftPointToPointSocket *zmq.Socket
	raftClientSocket       *zmq.Socket
	replicas               map[string]*ReplicaInfo
}

var errNonexistentReplica = errors.New("Unknown replica")

func ForEachReplica(path string, fn func(rep *ReplicaInfo)) {
	if f, err := os.Open(path); err == nil {
		defer f.Close()
		reader := bufio.NewReader(f)
		idx := 0
		for line, _, err := reader.ReadLine(); err == nil; line, _, err = reader.ReadLine() {
			if !strings.HasPrefix(string(line), "#") {
				tmp := strings.Split(string(line), ",")
				if len(tmp) == 6 {
					portInt, _ := strconv.Atoi(tmp[5])
					replica := &ReplicaInfo{
						Name:           tmp[0],
						Pid:            tmp[1],
						MntPoint:       tmp[2],
						DbPath:         tmp[3],
						IpAddr:         tmp[4],
						Port:           tmp[5],
						DemandPort:     string(strconv.Itoa(portInt + 1)),
						RaftPubPort:    string(strconv.Itoa(portInt + 2)),
						RaftP2PPort:    string(strconv.Itoa(portInt + 3)),
						RaftClientPort: string(strconv.Itoa(portInt + 4)),
						IndexInList:    idx,
					}
					fn(replica)
				}
			}
			idx++
		}
	}
}

func IsOurIpAddr(someAddr string) bool {
	addrs, _ := net.InterfaceAddrs()
	isOurs := false
	for _, addr := range addrs {
		isOurs = isOurs || strings.HasPrefix(addr.String(), someAddr)
	}
	return isOurs
}

func GetOurPid(path string, name string) string {
	pid := ""
	ForEachReplica(path, func(rep *ReplicaInfo) {
		if pid == "" {
			if (name == "auto" && (rep.IpAddr == "localhost" || IsOurIpAddr(rep.IpAddr))) || name == rep.Name {
				pid = rep.Pid
			}
		}
	})
	return pid
}

// Read and parse replica info from a file
// Return a map populated with info about all the replicas
func ReadReplicaInfo(path string) map[string]*ReplicaInfo {
	replicas := make(map[string]*ReplicaInfo)
	ForEachReplica(path, func(rep *ReplicaInfo) {
		replicas[rep.Pid] = rep
	})
	return replicas
}

// Initiate connections to all replicas
func NewCommunicator(name string, replicas map[string]*ReplicaInfo) (*Communicator, error) {
	communicator := new(Communicator)
	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}
	communicator.context = context
	communicator.pubSocket, err = context.NewSocket(zmq.PUB)
	if err != nil {
		return nil, err
	}
	communicator.subSocket, err = context.NewSocket(zmq.SUB)
	communicator.chunkSocket, err = context.NewSocket(zmq.REP)
	communicator.raftPubSocket, err = context.NewSocket(zmq.PUB)
	communicator.raftSubSocket, err = context.NewSocket(zmq.SUB)
	communicator.raftPointToPointSocket, err = context.NewSocket(zmq.REP)
	communicator.raftClientSocket, err = context.NewSocket(zmq.REP)
	found := false
	for _, val := range replicas {
		if !found && ((name == "auto" && (val.IpAddr == "localhost" || IsOurIpAddr(val.IpAddr))) || val.Name == name) {
			communicator.pubSocket.Bind("tcp://*:" + val.Port)
			communicator.chunkSocket.Bind("tcp://*:" + val.DemandPort)
			// raft
			communicator.raftPubSocket.Bind("tcp://*:" + val.RaftPubPort)
			communicator.raftPointToPointSocket.Bind("tcp://*:" + val.RaftP2PPort)
			communicator.raftClientSocket.Bind("tcp://*:" + val.RaftClientPort)
			communicator.tag = val.Pid
			found = true
		} else {
			communicator.subSocket.Connect("tcp://" + val.IpAddr + ":" + val.Port)
			communicator.subSocket.SetSubscribe(val.Pid)
			communicator.raftSubSocket.Connect("tcp://" + val.IpAddr + ":" + val.RaftPubPort)
			communicator.raftSubSocket.SetSubscribe(val.Pid)
		}
	}
	communicator.replicas = replicas
	return communicator, nil
}

func (comm *Communicator) getPid() string {
	return comm.tag
}

// Close all sockets and terminate context
func (comm *Communicator) Close() {
	comm.chunkSocket.Close()
	comm.pubSocket.Close()
	comm.subSocket.Close()
	comm.raftPubSocket.Close()
	comm.raftSubSocket.Close()
	comm.raftPointToPointSocket.Close()
	comm.raftClientSocket.Close()
	comm.context.Term()
}

// Broadcast changes to the fs to all connected replicas
func (comm *Communicator) BroadcastMeta(payload []byte) {
	msg := comm.tag + " " + string(payload)
	comm.pubSocket.SendBytes([]byte(msg), 0)
}

// GoRoutine for listening for meta broadcasts
func (comm *Communicator) MetaBroadcastListener(updateFs func(payload []byte)) {
	for {
		msg, _ := comm.subSocket.RecvBytes(0)
		updateFs(msg)
	}
}

// GoRoutine for listening for chunk requests
func (comm *Communicator) ChunkRequestListener(getChunk func(hash []byte) []byte) {
	pointToPointListener(comm.chunkSocket, getChunk)
}

// Requests blocks from replica with name "them"
// Returns block received from the other replica
func (comm *Communicator) RequestChunks(them string, chunkHashes [][]byte) ([][]byte, error) {
	socket, _ := comm.context.NewSocket(zmq.REQ)
	defer socket.Close()
	replica := comm.replicas[them]
	if replica == nil {
		return nil, errNonexistentReplica
	}
	err := socket.Connect("tcp://" + replica.IpAddr + ":" + replica.DemandPort)
	if err != nil {
		return nil, err
	}
	chunks := make([][]byte, len(chunkHashes))
	for i, hash := range chunkHashes {
		socket.SendBytes(hash, 0)
		chunk, _ := socket.RecvBytes(0)
		chunks[i] = chunk
	}
	return chunks, nil
}

// Requests a single block from replica with name "them"
// Returns block received from the other replica
func (comm *Communicator) RequestChunk(them string, chunkHash []byte) ([]byte, error) {
	socket, _ := comm.context.NewSocket(zmq.REQ)
	defer socket.Close()
	replica := comm.replicas[them]
	if replica == nil {
		return nil, errNonexistentReplica
	}
	err := socket.Connect("tcp://" + replica.IpAddr + ":" + replica.DemandPort)
	if err != nil {
		return nil, err
	}
	socket.SendBytes(chunkHash, 0)
	ans, _ := socket.RecvBytes(0)
	//ans, _ := socket.RecvBytes(0)
	return ans, nil
}

func (comm *Communicator) RaftBroadcast(payload []byte) {
	msg := comm.tag + " " + string(payload)
	comm.raftPubSocket.SendBytes([]byte(msg), 0)
}

func (comm *Communicator) RaftMessageListener(handleRaftMsg func(pid string, payload []byte)) {
	for {
		msg, _ := comm.raftSubSocket.RecvBytes(0)
		i := 0
		for i = range msg {
			if msg[i] == byte(' ') {
				break
			}
		}
		if i >= len(msg) {
			break
		}
		handleRaftMsg(string(msg[0:i]), msg[i+1:])
	}
}

func (comm *Communicator) RaftP2P(getResponse func(hash []byte) []byte) {
	pointToPointListener(comm.raftPointToPointSocket, getResponse)
}

func (comm *Communicator) RaftClientMessageListener(getResponse func(hash []byte) []byte) {
	pointToPointListener(comm.raftClientSocket, getResponse)
}

type RaftMessage struct {
	Success  bool
	Ivalue   int
	LeaderID string
}

func (comm *Communicator) RaftClientSend(message []byte) ([]byte, error) {
	socket, _ := comm.context.NewSocket(zmq.REQ)
	defer socket.Close()
	replica := comm.replicas[comm.tag]
	err := socket.Connect("tcp://" + replica.IpAddr + ":" + replica.RaftClientPort)
	if err != nil {
		return nil, err
	}
	fmt.Println("init send")
	socket.SendBytes(message, 0)
	fmt.Println("init recv")
	ans, _ := socket.RecvBytes(0)
	fmt.Println(string(ans))
	fmt.Println(string(ans) == "null")
	rmsg := new(RaftMessage)
	fmt.Println(rmsg)
	json.Unmarshal(ans, rmsg)
	if string(ans) == "null" || rmsg.Success {
		return ans, nil
	}
	leader := strconv.Itoa(rmsg.Ivalue)
	socket2, _ := comm.context.NewSocket(zmq.REQ)
	fmt.Println(leader)
	replica = comm.replicas[leader]
	err = socket2.Connect("tcp://" + replica.IpAddr + ":" + replica.RaftClientPort)
	if err != nil {
		return nil, err
	}
	fmt.Println("second send")
	socket2.SendBytes(message, 0)
	fmt.Println("second recv")
	ans, _ = socket2.RecvBytes(0)
	return ans, nil
}

func pointToPointListener(socket *zmq.Socket, getResponse func(hash []byte) []byte) {
	for {
		req, _ := socket.RecvBytes(0)
		socket.SendBytes(getResponse(req), 0)
	}
}
