package myfs

import (
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"dss/util"
)

const MIN_FILE_CAPACITY = 1

type File struct {
	Node
	data []byte
}

func (file *File) InitFile(name string, mode os.FileMode, parent *Directory) {
	file.InitNode(name, mode, parent)
	file.data = make([]byte, 0, MIN_FILE_CAPACITY)
}

func (file *File) Fsync(req *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	util.P_out(req.String())
	// NOOP
	return nil
}

func (file *File) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	util.P_out(req.String())
	// NOOP
	return nil
}

// Returns the File's data array
func (file *File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	util.P_out("read all: %s", file.name)
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
	util.P_out(req.String())
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
