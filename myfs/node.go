package myfs

import (
	"crypto/sha1"
	"encoding/json"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"dss/util"
)

// TODO: should eventually get rid of these...
var uid = os.Geteuid()
var gid = os.Getegid()

type NamedNode interface {
	fs.Node
	getName() string
	setName(name string)
	getVid() []byte
	getLastVid() []byte
	setVid([]byte)
	isDir() bool
	isArchive() bool
	//setDirty(dirty bool)
}

// Generic information for files and directories
type Node struct {
	Vid     []byte
	LastVid []byte
	Vtime   time.Time
	Source  string
	Name    string
	Attrs   fuse.Attr
	dirty   bool
	parent  *Directory
	archive bool
}

func isDir(node fs.Node) bool {
	return (node.Attr().Mode & os.ModeDir) != 0
}

func fuseType(node fs.Node) fuse.DirentType {
	if isDir(node) {
		return fuse.DT_Dir
	}
	return fuse.DT_File
}

func (node *Node) InitNode(name string, mode os.FileMode, parent *Directory) {
	node.Source = filesystem.replInfo.Pid
	node.Vid = NULL_VERSION
	node.LastVid = NULL_VERSION
	util.P_out("VID: ", node.Vid)
	node.Attrs.Inode = filesystem.getNextInd()
	node.Attrs.Nlink = 1
	node.Name = name

	tm := time.Now()
	node.Attrs.Atime = tm
	node.Attrs.Mtime = tm
	node.Attrs.Ctime = tm
	node.Attrs.Crtime = tm
	node.Attrs.Mode = mode

	node.Attrs.Gid = uint32(gid)
	node.Attrs.Uid = uint32(uid)

	node.Attrs.Size = 0

	node.dirty = true
	node.parent = parent

	node.archive = false

	util.P_out("inited node inode %d, %q\n", filesystem.NextInd, name)
}

func (node Node) isArchive() bool {
	return node.archive
}

func (node *Node) Attr() fuse.Attr {
	var attrs fuse.Attr
	attrs = node.Attrs
	return attrs
}

func (node Node) getName() string {
	return node.Name
}

func (node *Node) setName(name string) {
	node.Name = name
}

func (node Node) getVid() []byte {
	return node.Vid
}

func (node Node) getLastVid() []byte {
	return node.LastVid
}

func (node *Node) setVid(vid []byte) {
	node.Vid = vid
}

func (node Node) isDir() bool {
	return isDir(&node)
}

// Get file attributes for this node
func (node *Node) Getattr(req *fuse.GetattrRequest, resp *fuse.GetattrResponse, intr fs.Intr) fuse.Error {
	resp.Attr = node.Attr()
	if node.isArchive() {
		resp.Attr.Mode = (os.ModeDir & node.Attr().Mode) | 0444
	}
	return nil
}

// Set file attributes for this node
func (node *Node) Setattr(req *fuse.SetattrRequest, resp *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	// General
	if req.Valid.Mode() {
		node.Attrs.Mode = req.Mode
	}
	if req.Valid.Uid() {
		node.Attrs.Uid = req.Uid
	}
	if req.Valid.Gid() {
		node.Attrs.Gid = req.Gid
	}
	if req.Valid.Size() {
		node.Attrs.Size = req.Size
	}
	if req.Valid.Atime() {
		node.Attrs.Atime = req.Atime
	}
	if req.Valid.Mtime() {
		node.Attrs.Mtime = req.Mtime
	}
	// OSX specific
	if req.Valid.Crtime() {
		node.Attrs.Crtime = req.Crtime
	}
	if req.Valid.Flags() {
		node.Attrs.Flags = req.Flags
	}
	resp.Attr = node.Attrs
	node.dirty = true
	return nil
}

func (node *Node) SetDirty(dirty bool) {
	node.dirty = dirty
}

func (node Node) ComputeVid() []byte {
	if marshalled, err := json.Marshal(node); err == nil {
		hasher := sha1.New()
		hasher.Write(marshalled)
		return hasher.Sum(nil)
	}
	return make([]byte, 0)
}
