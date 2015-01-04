package myfs

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"

	"dss/util"
)

const FLUSHER_PERIOD = 5 * time.Second

var filesystem FS
var UseMtime bool = false

func init() {
	go signalHandler()
}

func signalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	<-c
	filesystem.Lock(nil)
	if filesystem.database != nil {
		FlushNode(filesystem.root)
		filesystem.database.Close()
		if val, err := json.Marshal(filesystem); err == nil {
			filesystem.database.PutBlock([]byte("metadata"), val)
		}
	}
	filesystem.Unlock(nil)
	os.Exit(1)
}

type FS struct {
	database  FsDatabase
	root      *Directory
	replInfo  ReplicaInfo
	NextInd   uint64
	NextVid   uint64
	mutex     sync.Mutex
	ChunkInDb map[string]bool
	comm      *Communicator
}

var NULL_VERSION []byte = make([]byte, 0)

// Initializes and returns a filesystem with the database provided
// Note: This MUST be called before some node methods are called
// Because unfortunately for the time being, it has to set up some
// global state.
func NewFs(db FsDatabase, us *ReplicaInfo, replicas map[string]*ReplicaInfo) FS {
	newFs := FS{
		database:  db,
		NextInd:   0,
		NextVid:   1,
		ChunkInDb: make(map[string]bool),
	}
	if data, err := db.GetBlock([]byte("metadata")); err == nil {
		if jsonErr := json.Unmarshal(data, &filesystem); jsonErr != nil {
			filesystem = newFs
		} else {
			filesystem.database = db
		}
	} else {
		filesystem = newFs
	}
	if root, err := filesystem.database.GetRoot(); err != nil || root == nil {
		util.P_out("Couldn't find root in db, making new one")
		filesystem.root = new(Directory)
		filesystem.root.InitDirectory("", os.ModeDir|0755, nil)
	} else {
		filesystem.root = root
	}
	filesystem.comm, _ = NewCommunicator(us.Name, replicas)
	if filesystem.comm != nil {
		go filesystem.comm.MetaBroadcastListener(func(payload []byte) {
			changes := make(map[string][]byte)
			i := 0
			for i = range payload {
				if payload[i] == byte(' ') {
					break
				}
			}
			if i >= len(payload) {
				return
			}
			json.Unmarshal(payload[i+1:], &changes)
			filesystem.Lock(nil)
			defer filesystem.Unlock(nil)
			filesystem.updateFromBroadcast(changes)
		})
		go filesystem.comm.ChunkRequestListener(func(sha []byte) []byte {
			data, _ := filesystem.database.GetBlock(sha)
			return data
		})
	}
	filesystem.replInfo = *us
	//filesystem.replInfo = *replicas[pid]
	return filesystem
}

func (fsys FS) Root() (fs.Node, fuse.Error) {
	util.P_out("root returns as %d\n", int(fsys.root.Attr().Inode))
	return fsys.root, nil
}

// TODO: do away with the global mutex, this could be more efficient
func (fsys *FS) Lock(node fs.Node) {
	fsys.mutex.Lock()
}

// TODO: do away with the global mutex, this could be more efficient
func (fsys *FS) Unlock(node fs.Node) {
	fsys.mutex.Unlock()
}

func (fsys *FS) getNextInd() uint64 {
	currInd := fsys.NextInd
	fsys.NextInd++
	return currInd
}

func (fsys *FS) getNextVid() uint64 {
	util.P_out(strconv.FormatUint(fsys.NextVid, 10))
	currVid := fsys.NextVid
	fsys.NextVid++
	return currVid
}

func (fsys *FS) PeriodicFlush() {
	for {
		util.P_out("Flushing")
		fsys.Lock(nil)
		FlushNode(fsys.root)
		if val, err := json.Marshal(filesystem); err == nil {
			fsys.database.PutBlock([]byte("metadata"), val)
		}
		fsys.Unlock(nil)
		time.Sleep(FLUSHER_PERIOD)
	}
}

func (fs *FS) getFile(vid []byte) (*File, error) {
	if string(vid) == string(NULL_VERSION) {
		return nil, errors.New("No such version")
	}
	return fs.database.GetFile(vid)
}

func (fs *FS) getDirectory(vid []byte) (*Directory, error) {
	if string(vid) == string(NULL_VERSION) {
		return nil, errors.New("No such version")
	}
	return fs.database.GetDirectory(vid)
}

func (fsys *FS) DbContains(sha1 []byte) bool {
	/*if _, err := fsys.database.GetBlock(sha1); err != nil {
		return false
	}
	return true*/
	key := hex.EncodeToString(sha1)
	return fsys.ChunkInDb[key]
}

func (fsys *FS) GetChunk(sha1 []byte, remoteSource string) ([]byte, error) {
	if !fsys.DbContains(sha1) && fsys.comm != nil {
		data, err := fsys.comm.RequestChunk(remoteSource, sha1)
		fsys.PutChunk(sha1, data)
		return data, err
	}
	return fsys.database.GetBlock(sha1)
}

func (fsys *FS) PutChunk(sha1 []byte, data []byte) error {
	key := hex.EncodeToString(sha1)
	fsys.ChunkInDb[key] = true
	return fsys.database.PutBlock(sha1, data)
}

func (fsys *FS) GetFile(sha1 []byte, remoteSource string) (*File, error) {
	if chunk, _ := fsys.database.GetBlock(sha1); chunk == nil {
		chunk, _ = fsys.comm.RequestChunk(remoteSource, sha1)
		if chunk != nil {
			fsys.database.PutBlock(sha1, chunk)
		}
	}
	return fsys.database.GetFile(sha1)
}

func (fsys *FS) GetDirectory(sha1 []byte, remoteSource string) (*Directory, error) {
	if chunk, _ := fsys.database.GetBlock(sha1); chunk == nil {
		chunk, _ = fsys.comm.RequestChunk(remoteSource, sha1)
		if chunk != nil {
			fsys.database.PutBlock(sha1, chunk)
		}
	}
	return fsys.database.GetDirectory(sha1)

}

func FlushNode(node NamedNode) {
	changes := make(map[string][]byte)
	flushNode(time.Now(), node, changes)
	if filesystem.comm != nil && len(changes) > 0 {
		marshalled, _ := json.Marshal(changes)
		filesystem.comm.BroadcastMeta(marshalled)
	}
}

func flushNode(flushTime time.Time, node NamedNode, changes map[string][]byte) {
	if dir, ok := node.(*Directory); ok {
		util.P_out("Visiting node: "+dir.Name+" at version %d", dir.Vid)
		for _, node := range dir.children {
			if node != nil {
				if !node.isArchive() {
					flushNode(flushTime, node, changes)
				}
			}
		}
		// TODO: make this a dir method or another function here
		if dir.dirty {
			util.P_out("Putting " + dir.Name)
			dir.LastVid = dir.Vid
			//dir.Vid = filesystem.getNextVid()
			dir.Vid = dir.ComputeVid()
			dir.Vtime = flushTime
			dir.Source = filesystem.replInfo.Pid
			if dir.parent != nil {
				dir.parent.setChild(dir)
				if err := filesystem.database.PutDirectory(dir); err != nil {
					util.P_err("Error putting directory in db: ", err)
				}
				dir.parent.dirty = true
			} else {
				if err := filesystem.database.SetRoot(dir); err != nil {
					util.P_err("Error setting root in db: ", err)
				}
				changes[HEAD] = dir.Vid
			}
			changes[hex.EncodeToString(dir.Vid)], _ = json.Marshal(dir)
		}
	}
	if file, ok := node.(*File); ok {
		util.P_out("Visiting " + file.Name)
	}
	// TODO: make this a file method or another function here
	if file, ok := node.(*File); ok && file.dirty {
		util.P_out("Putting " + file.Name)
		file.LastVid = file.Vid
		//file.Vid = filesystem.getNextVid()
		file.Vid = file.ComputeVid()
		file.Vtime = flushTime
		file.Source = filesystem.replInfo.Pid
		file.commitChunks()
		if err := filesystem.database.PutFile(file); err != nil {
			util.P_err("Error putting file in db: ", err)
		}
		if file.parent != nil {
			file.parent.setChild(file)
			file.parent.dirty = true
		}
		changes[hex.EncodeToString(file.Vid)], _ = json.Marshal(file)
	}
}

func (fs *FS) updateFromBroadcast(changes map[string][]byte) {
	for key, val := range changes {
		if key != HEAD {
			vid, _ := hex.DecodeString(key)
			fs.PutChunk(vid, val)
		} else {
			fs.PutChunk([]byte(HEAD), val)
		}
	}
	rootHash := changes[HEAD]
	updateNode(fs.root, hex.EncodeToString(rootHash), changes)
}

func updateNode(node NamedNode, newHash string, changes map[string][]byte) {
	if dir, ok := node.(*Directory); ok {
		newDir := new(Directory)
		err := json.Unmarshal(changes[newHash], newDir)
		if err != nil {
		}
		updateDir(dir, newDir, changes)
	} else if file, ok := node.(*File); ok {
		newFile := new(File)
		err := json.Unmarshal(changes[newHash], newFile)
		if err != nil {
		}
		updateFile(file, newFile)
	}
}

func updateDir(oldDir *Directory, newDir *Directory, changes map[string][]byte) {
	for name, _ := range oldDir.ChildVids {
		if newDir.ChildVids[name] == nil {
			delete(oldDir.children, name)
			delete(oldDir.ChildVids, name)
			delete(oldDir.IsDir, name)
		}
	}
	for name, vid := range newDir.ChildVids {
		if oldDir.ChildVids[name] == nil {
			oldDir.ChildVids[name] = vid
			oldDir.IsDir[name] = newDir.IsDir[name]
			if newDir.IsDir[name] {
				child, _ := filesystem.GetDirectory(vid, newDir.Source)
				child.parent = oldDir
				oldDir.children[name] = child
			} else {
				child, _ := filesystem.GetFile(vid, newDir.Source)
				child.parent = oldDir
				oldDir.children[name] = child
			}
		} else if changes[hex.EncodeToString(vid)] != nil {
			updateNode(oldDir.children[name], hex.EncodeToString(vid), changes)
		}
	}
	oldDir.Attrs = newDir.Attrs
	oldDir.Vid = newDir.Vid
	oldDir.LastVid = newDir.LastVid
	oldDir.Vtime = newDir.Vtime
	oldDir.Source = newDir.Source
}

func updateFile(oldFile *File, newFile *File) {
	oldFile.Attrs = newFile.Attrs
	oldFile.Vid = newFile.Vid
	oldFile.LastVid = newFile.LastVid
	oldFile.Vtime = newFile.Vtime
	oldFile.DataBlocks = newFile.DataBlocks
	oldFile.loaded = false
}
