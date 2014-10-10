package myfs

import (
	"encoding/json"
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
const WRITE_QUEUE_SIZE = 100

var filesystem FS

func init() {
	go signalHandler()
}

func signalHandler() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	signal.Notify(c, syscall.SIGTERM)
	<-c
	if filesystem.database != nil {
		FlushNode(filesystem.root)
		filesystem.database.Close()
	}
	os.Exit(1)
}

type Block struct {
	sha1 []byte
	data []byte
}

type FS struct {
	database  FsDatabase
	root      *Directory
	NextInd   uint64
	NextVid   uint64
	mutex     sync.Mutex
	ChunkInDb map[string]bool
}

// Initializes and returns a filesystem with the database provided
// Note: This MUST be called before some node methods are called
// Because unfortunately for the time being, it has to set up some
// global state.
func NewFs(db FsDatabase) FS {
	newFs := FS{
		database:  db,
		NextInd:   0,
		NextVid:   0,
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

func (fsys *FS) DbContains(sha1 []byte) bool {
	return fsys.ChunkInDb[string(sha1[:])]
}

func (fsys *FS) PutChunk(sha1 []byte, data []byte) error {
	fsys.ChunkInDb[string(sha1[:])] = true
	return fsys.database.PutBlock(sha1, data)
}

func FlushNode(node NamedNode) {
	if dir, ok := node.(*Directory); ok {
		util.P_out("Visiting node: "+dir.Name+" at version %d", dir.Vid)
		for _, node := range dir.children {
			FlushNode(node)
		}
		if dir.dirty {
			util.P_out("Putting " + dir.Name)
			dir.Vid = filesystem.getNextVid()
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
			}
		}
	}
	if file, ok := node.(*File); ok {
		util.P_out("Visiting " + file.Name)
	}
	if file, ok := node.(*File); ok && file.dirty {
		util.P_out("Putting " + file.Name)
		file.Vid = filesystem.getNextVid()
		file.commitChunks()
		if err := filesystem.database.PutFile(file); err != nil {
			util.P_err("Error putting file in db: ", err)
		}
		if file.parent != nil {
			file.parent.setChild(file)
			file.parent.dirty = true
		}
	}
}
