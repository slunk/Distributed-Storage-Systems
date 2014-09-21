package myfs

import (
	"os"
	"time"
	
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"dss/util"
)

var nextInd uint64 = 0
var uid = os.Geteuid()
var gid = os.Getegid()

func isDir(node fs.Node) bool {
	return (node.Attr().Mode & os.ModeDir) != 0
}

func fuseType(node fs.Node) fuse.DirentType {
	if isDir(node) {
		return fuse.DT_Dir
	}
	return fuse.DT_File
}


type NamedNode interface {
	fs.Node
	setName(name string)
}

// Generic information for files and directories
type Node struct {
	nid    fuse.NodeID
	name   string
	attr   fuse.Attr
	dirty  bool
	parent *Directory
}

func (node *Node) InitNode(name string, mode os.FileMode, parent *Directory) {
	nextInd++
	node.attr.Inode = nextInd
	node.attr.Nlink = 1
	node.name = name

	tm := time.Now()
	node.attr.Atime = tm
	node.attr.Mtime = tm
	node.attr.Ctime = tm
	node.attr.Crtime = tm
	node.attr.Mode = mode

	node.attr.Gid = uint32(gid)
	node.attr.Uid = uint32(uid)

	node.attr.Size = 0

	node.parent = parent

	util.P_out("inited node inode %d, %q\n", nextInd, name)
}

func (node *Node) Attr() fuse.Attr {
	return node.attr
}

func (node *Node) setName(name string) {
	node.name = name
}

// Get file attributes for this node
func (node *Node) Getattr(req *fuse.GetattrRequest, resp *fuse.GetattrResponse, intr fs.Intr) fuse.Error {
	resp.Attr = node.Attr()
	return nil
}

// Set file attributes for this node
func (node *Node) Setattr(req *fuse.SetattrRequest, resp *fuse.SetattrResponse, intr fs.Intr) fuse.Error {
	// General
	if req.Valid.Mode() {
		node.attr.Mode = req.Mode
	}
	if req.Valid.Uid() {
		node.attr.Uid = req.Uid
	}
	if req.Valid.Gid() {
		node.attr.Gid = req.Gid
	}
	if req.Valid.Size() {
		node.attr.Size = req.Size
	}
	if req.Valid.Atime() {
		node.attr.Atime = req.Atime
	}
	if req.Valid.Mtime() {
		node.attr.Mtime = req.Mtime
	}
	// OSX specific
	if req.Valid.Crtime() {
		node.attr.Crtime = req.Crtime
	}
	if req.Valid.Flags() {
		node.attr.Flags = req.Flags
	}
	resp.Attr = node.attr
	return nil
}