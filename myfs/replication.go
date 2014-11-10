package myfs

import (
	"bufio"
	"errors"
	zmq "github.com/pebbe/zmq4"
	"os"
	"strconv"
	"strings"
)

type ReplicaInfo struct {
	Name       string
	Pid        string
	MntPoint   string
	DbPath     string
	IpAddr     string
	Port       string
	DemandPort string
}

type Communicator struct {
	tag         string
	context     *zmq.Context
	pubSocket   *zmq.Socket
	subSocket   *zmq.Socket
	chunkSocket *zmq.Socket
	replicas    map[string]*ReplicaInfo
}

var errNonexistentReplica = errors.New("Unknown replica")

// Read and parse replica info from a file
// Return a map populated with info about all the replicas
func ReadReplicaInfo(path string) map[string]*ReplicaInfo {
	replicas := make(map[string]*ReplicaInfo)
	if f, err := os.Open(path); err == nil {
		reader := bufio.NewReader(f)
		for line, _, err := reader.ReadLine(); err == nil; line, _, err = reader.ReadLine() {
			if !strings.HasPrefix(string(line), "#") {
				tmp := strings.Split(string(line), ",")
				if len(tmp) == 6 {
					portInt, _ := strconv.Atoi(tmp[5])
					replica := &ReplicaInfo{
						Name:       tmp[0],
						Pid:        tmp[1],
						MntPoint:   tmp[2],
						DbPath:     tmp[3],
						IpAddr:     tmp[4],
						Port:       tmp[5],
						DemandPort: string(strconv.Itoa(portInt + 1)),
					}
					replicas[replica.Pid] = replica
				}
			}
		}
	}
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
	for key, val := range replicas {
		if replicas[key].Name == name {
			communicator.pubSocket.Bind("tcp://*:" + val.Port)
			communicator.chunkSocket.Bind("tcp://*:" + val.DemandPort)
			communicator.tag = val.Pid
		} else {
			communicator.subSocket.Connect("tcp://" + val.IpAddr + ":" + val.Port)
			communicator.subSocket.SetSubscribe(val.Pid)
		}
	}
	communicator.replicas = replicas
	return communicator, nil
}

// Close all sockets and terminate context
func (comm *Communicator) Close() {
	comm.chunkSocket.Close()
	comm.pubSocket.Close()
	comm.subSocket.Close()
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
	for {
		req, _ := comm.chunkSocket.RecvBytes(0)
		comm.chunkSocket.SendBytes(getChunk(req), 0)
	}
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
