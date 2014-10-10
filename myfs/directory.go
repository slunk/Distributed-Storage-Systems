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
	children         map[string]NamedNode
	ChildVids        map[string]uint64
	IsDir            map[string]bool
	childrenInMemory bool
}

func (dir *Directory) InitDirectory(name string, mode os.FileMode, parent *Directory) {
	dir.InitNode(name, mode, parent)
	dir.children = make(map[string]NamedNode)
	dir.ChildVids = make(map[string]uint64)
	dir.IsDir = make(map[string]bool)
	dir.childrenInMemory = true
}

func (dir *Directory) loadChildren() {
	util.P_out("Loading children")
	for key := range dir.ChildVids {
		util.P_out(key)
		if dir.IsDir[key] {
			child, err := filesystem.database.GetDirectory(dir.ChildVids[key])
			if err != nil {
				util.P_err("Error loading directory from db: ", err)
			} else {
				child.parent = dir
				dir.setChild(child)
			}
		} else {
			child, err := filesystem.database.GetFile(dir.ChildVids[key])
			if err != nil {
				util.P_err("Error loading file from db: ", err)
			} else {
				child.parent = dir
				dir.setChild(child)
			}
		}
	}
	dir.childrenInMemory = true
}

// Checks for a file called name in dir and returns it if it exists
// Loads children lazily if they aren't in memory
func (dir *Directory) Lookup(name string, intr fs.Intr) (fs.Node, fuse.Error) {
	filesystem.Lock(dir)
	defer filesystem.Unlock(dir)
	util.P_out("Lookup on %q from %q\n", name, dir.Name)
	if !dir.childrenInMemory {
		dir.loadChildren()
	}
	if file, ok := dir.children[name]; ok {
		return file, nil
	}
	return nil, fuse.ENOENT
}

// Returns a list of Nodes (Files or Directories) in dir
// Loads children lazily if they aren't in memory
func (dir *Directory) ReadDir(intr fs.Intr) ([]fuse.Dirent, fuse.Error) {
	filesystem.Lock(dir)
	defer filesystem.Unlock(dir)
	util.P_out("readdir %q\n", dir.Name)
	if !dir.childrenInMemory {
		dir.loadChildren()
	}
	files := make([]fuse.Dirent, 0, 10)
	files = append(files, fuse.Dirent{Inode: dir.Attr().Inode, Name: ".", Type: fuse.DT_Dir})
	parent := dir.parent
	if parent == nil {
		parent = dir
	}
	files = append(files, fuse.Dirent{Inode: parent.Attr().Inode, Name: "..", Type: fuse.DT_Dir})
	for name, file := range dir.children {
		files = append(files, fuse.Dirent{Inode: file.Attr().Inode, Name: name, Type: fuseType(file)})
	}
	return files, nil
}

// Makes a directory called req.Name in dir
func (dir *Directory) Mkdir(req *fuse.MkdirRequest, intr fs.Intr) (fs.Node, fuse.Error) {
	filesystem.Lock(dir)
	defer filesystem.Unlock(dir)
	util.P_out(req.String())
	subdir := new(Directory)
	subdir.InitDirectory(req.Name, os.ModeDir|req.Mode, dir)
	dir.setChild(subdir)
	subdir.dirty = true
	dir.dirty = true
	return subdir, nil
}

// Creates a regular file in dir with the attributes supplied in req
func (dir *Directory) Create(req *fuse.CreateRequest, resp *fuse.CreateResponse, intr fs.Intr) (fs.Node, fs.Handle, fuse.Error) {
	filesystem.Lock(dir)
	defer filesystem.Unlock(dir)
	util.P_out(req.String())
	rval := new(File)
	rval.InitFile(req.Name, req.Mode, dir)
	rval.Attrs.Gid = req.Gid
	rval.Attrs.Uid = req.Uid
	dir.setChild(rval)
	resp.Attr = rval.Attr()
	resp.Node = fuse.NodeID(rval.Attr().Inode)
	rval.dirty = true
	dir.dirty = true
	return rval, rval, nil
}

// Removes a file named req.Name from dir if it exists
func (dir *Directory) Remove(req *fuse.RemoveRequest, intr fs.Intr) fuse.Error {
	filesystem.Lock(dir)
	defer filesystem.Unlock(dir)
	util.P_out(req.String())
	if _, ok := dir.children[req.Name]; ok {
		dir.removeChild(req.Name)
		dir.dirty = true
		return nil
	}
	return fuse.ENOENT
}

// Moves a file from dir to newDir (potentially the same as dir) and changes its name from req.OldName
// to req.NewName
func (dir *Directory) Rename(req *fuse.RenameRequest, newDir fs.Node, intr fs.Intr) fuse.Error {
	filesystem.Lock(dir)
	defer filesystem.Unlock(dir)
	util.P_out(req.String())
	if d, newDirOk := newDir.(*Directory); newDirOk {
		if v, oldNameInDir := dir.children[req.OldName]; oldNameInDir {
			v.setName(req.NewName)
			d.setChild(v)
			if file, ok := v.(*File); ok {
				file.dirty = true
				file.parent = d
			}
			dir.removeChild(req.OldName)
			d.dirty = true
			dir.dirty = true
			return nil
		}
		return fuse.ENOENT
	}
	return fuse.Errno(syscall.ENOTDIR)
}

func (dir *Directory) setChild(node NamedNode) {
	name := node.getName()
	dir.children[name] = node
	dir.ChildVids[name] = node.getVid()
	dir.IsDir[name] = node.isDir()
}

func (dir *Directory) removeChild(name string) {
	delete(dir.children, name)
	delete(dir.ChildVids, name)
	delete(dir.IsDir, name)
}
