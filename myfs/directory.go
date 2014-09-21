package myfs

import (
	"os"
	"syscall"
	
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"dss/util"
)

type Directory struct {
	Node
	children map[string]NamedNode
}

func (dir *Directory) InitDirectory(name string, mode os.FileMode, parent *Directory) {
	dir.InitNode(name, mode, parent)
	dir.children = make(map[string]NamedNode)
}

// Checks for a file called name in dir and returns it if it exists
func (dir *Directory) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	util.P_out("Lookup on %q from %q\n", name, dir.name)
	if file, ok := dir.children[name]; ok {
		return file, nil
	}
	return nil, fuse.ENOENT
}

// Returns a list of Nodes (Files or Directories) in dir
func (dir *Directory) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	util.P_out("readdir %q\n", dir.name)
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
	util.P_out(req.String())
	subdir := new(Directory)
	subdir.InitDirectory(req.Name, os.ModeDir|req.Mode, dir)
	dir.children[req.Name] = subdir
	return subdir, nil
}

// Creates a regular file in dir with the attributes supplied in req
func (dir *Directory) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	util.P_out(req.String())
	rval := new(File)
	rval.InitFile(req.Name, req.Mode, dir)
	dir.children[req.Name] = rval
	rval.attr.Gid = req.Gid
	rval.attr.Uid = req.Uid
	resp.Attr = rval.Attr()
	resp.Node = rval.nid
	return rval, rval, nil
}

// Removes a file named req.Name from dir if it exists
func (dir *Directory) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
	util.P_out(req.String())
	if _, ok := dir.children[req.Name]; ok {
		delete(dir.children, req.Name)
		return nil
	}
	return fuse.ENOENT
}

// Moves a file from dir to newDir (potentially the same as dir) and changes its name from req.OldName
// to req.NewName
func (dir *Directory) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {
	util.P_out(req.String())
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