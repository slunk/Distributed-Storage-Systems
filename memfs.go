// Peter Enns' implementation of project 1 in CMSC818E
// memfs implements a simple in-memory file system.
package main

/*
 Two main files are ../fuse.go and ../fs/serve.go
*/

import (
	"flag"
	"fmt"
	"log"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

//=============================================================================

func p_out(s string, args ...interface{}) {
	if !debug {
		return
	}
	log.Printf(s, args...)
}

func p_err(s string, args ...interface{}) {
	log.Printf(s, args...)
}

//=============================================================================

const MIN_FILE_CAPACITY = 1

var root *Directory
var nextInd uint64
var debug = true
var uid = os.Geteuid()
var gid = os.Getegid()

type FS struct{}

func (fs FS) Root() (fs.Node, fuse.Error) {
	p_out("root returns as %d\n", int(root.attr.Inode))
	return root, nil
}

//=============================================================================

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

// Everything from Node and a map of the directory's children
type Directory struct {
	Node
	children map[string]NamedNode
}

// Everything from Node and a place to shove data for reads/writes
type File struct {
	Node
	data []byte
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

//=============================================================================

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

	p_out("inited node inode %d, %q\n", nextInd, name)
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

//=============================================================================

func (dir *Directory) InitDirectory(name string, mode os.FileMode, parent *Directory) {
	dir.InitNode(name, mode, parent)
	dir.children = make(map[string]NamedNode)
}

// Checks for a file called name in dir and returns it if it exists
func (dir *Directory) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	p_out("Lookup on %q from %q\n", name, dir.name)
	if file, ok := dir.children[name]; ok {
		return file, nil
	}
	return nil, fuse.ENOENT
}

// Returns a list of Nodes (Files or Directories) in dir
func (dir *Directory) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	p_out("readdir %q\n", dir.name)
	files := make([]fuse.Dirent, 0, 10)
	files = append(files, fuse.Dirent{Inode: dir.attr.Inode, Name: ".", Type: fuse.DT_Dir})
	parent := dir.parent
	if parent == nil {
		parent = dir
	}
	files = append(files, fuse.Dirent{Inode: parent.attr.Inode, Name: "..", Type: fuse.DT_Dir})
	for name, file := range dir.children {
		files = append(files, fuse.Dirent{Inode: file.Attr().Inode, Name: name, Type: fuseType(file)})
	}
	return files, nil
}

// Makes a directory called req.Name in dir
func (dir *Directory) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	p_out(req.String())
	subdir := new(Directory)
	subdir.InitDirectory(req.Name, os.ModeDir|req.Mode, dir)
	dir.children[req.Name] = subdir
	return subdir, nil
}

// Creates a regular file in dir with the attributes supplied in req
func (dir *Directory) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	p_out(req.String())
	rval := new(File)
	rval.InitFile(req.Name, req.Mode, dir)
	p_out(req.Mode.String())
	dir.children[req.Name] = rval
	rval.attr.Gid = req.Gid
	rval.attr.Uid = req.Uid
	resp.Attr = rval.Attr()
	resp.Node = rval.nid
	return rval, rval, nil
}

// Removes a file named req.Name from dir if it exists
func (dir *Directory) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
	p_out(req.String())
	if _, ok := dir.children[req.Name]; ok {
		delete(dir.children, req.Name)
		return nil
	}
	return fuse.ENOENT
}

// Moves a file from dir to newDir (potentially the same as dir) and changes its name from req.OldName
// to req.NewName
func (dir *Directory) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {
	p_out(req.String())
	if d, newDirOk := newDir.(*Directory); newDirOk {
		if v, oldNameInDir := dir.children[req.OldName]; oldNameInDir {
			delete(dir.children, req.OldName)
			d.children[req.NewName] = v
			v.setName(req.NewName)
			return nil
		}
		return fuse.ENOENT
	}
	return fuse.Errno(syscall.ENOTDIR)
}

//=============================================================================

func (file *File) InitFile(name string, mode os.FileMode, parent *Directory) {
	file.InitNode(name, mode, parent)
	file.data = make([]byte, 0, MIN_FILE_CAPACITY)
}

func (file *File) Fsync(req *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	p_out(req.String())
	// NOOP
	return nil
}

func (file *File) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	p_out(req.String())
	// NOOP
	return nil
}

// Returns the File's data array
func (file *File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	p_out("read all: %s", file.name)
	file.attr.Atime = time.Now()
	return file.data, nil
}

func max64(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// Copies data from req.Data to the File's data array beginning at index req.Offset.
// Grows file.data if necessary.
func (file *File) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	p_out(req.String())
	writeSize := len(req.Data)
	size := max64(uint64(req.Offset)+uint64(writeSize), file.attr.Size)
	if size > uint64(cap(file.data)) {
		tmp := file.data
		file.data = make([]byte, size, size*2)
		copy(file.data, tmp)
	} else {
		file.data = file.data[:size]
	}
	copy(file.data[req.Offset:], req.Data)
	resp.Size = writeSize
	file.attr.Size = uint64(size)
	file.attr.Mtime = time.Now()
	return nil
}

//=============================================================================

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage

	debugPtr := flag.Bool("debug", false, "print lots of stuff")
	flag.Parse()
	debug = *debugPtr

	p_out("main\n")

	root = new(Directory)
	root.InitDirectory("", os.ModeDir|0755, nil)

	//nodeMap[uint64(root.attr.Inode)] = root
	p_out("root inode %d", int(root.attr.Inode))

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}

	mountpoint := flag.Arg(0)

	fuse.Unmount(mountpoint) //!!
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, FS{})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// 818E - YOU DON'T NEED THESE
//func (n *Node) Getattr(req *fuse.GetattrRequest, resp *fuse.GetattrResponse, intr fs.Intr) fuse.Error {}
// func (n *Node) Setattr(req *fuse.SetattrRequest, resp *fuse.SetattrResponse, intr fs.Intr) fuse.Error {}
// func (n fs.Node) Open(req *fuse.OpenRequest, resp *fuse.OpenResponse, intr fs.Intr) (fs.Handle, fuse.Error){}
// func (n fs.Node) Release(req *fuse.ReleaseRequest, intr Intr) fuse.Error {}
// func (n fs.Node) Removexattr(req *fuse.RemovexattrRequest, intr Intr) fuse.Error {}
// func (n fs.Node) Setxattr(req *fuse.SetxattrRequest, intr Intr) fuse.Error {}
// func (n fs.Node) Listxattr(req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse, intr Intr) fuse.Error {}
// func (n fs.Node) Getxattr(req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse, intr Intr) fuse.Error {}
