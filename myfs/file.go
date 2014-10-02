package myfs

import (
	"crypto/sha1"
	"os"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	//"github.com/syndtr/goleveldb/leveldb"
	"dss/util"
)

const MIN_FILE_CAPACITY = 1

type File struct {
	Node
	data       []byte
	DataBlocks [][]byte
	loaded bool
}

func (file *File) InitFile(name string, mode os.FileMode, parent *Directory) {
	file.InitNode(name, mode, parent)
	file.data = make([]byte, 0, MIN_FILE_CAPACITY)
	file.loaded = true
}

func (file *File) Fsync(req *fuse.FsyncRequest, intr fs.Intr) fuse.Error {
	filesystem.Lock(file)
	defer filesystem.Unlock(file)
	util.P_out(req.String())
	FlushNode(file)
	return nil
}

// TODO: should this be a *Node method?
func (file *File) Flush(req *fuse.FlushRequest, intr fs.Intr) fuse.Error {
	filesystem.Lock(file)
	defer filesystem.Unlock(file)
	util.P_out(req.String())
	FlushNode(file)
	return nil
}

// Returns the File's data array
func (file *File) ReadAll(intr fs.Intr) ([]byte, fuse.Error) {
	filesystem.Lock(file)
	defer filesystem.Unlock(file)
	util.P_out("read all: %s", file.Name)
	if !file.loaded {
		file.loadChunks()
		file.loaded = true
	}
	file.Attrs.Atime = time.Now()
	return file.data, nil
}

// Copies data from req.Data to the File's data array beginning at index req.Offset.
// Grows file.data if necessary.
func (file *File) Write(req *fuse.WriteRequest, resp *fuse.WriteResponse, intr fs.Intr) fuse.Error {
	filesystem.Lock(file)
	defer filesystem.Unlock(file)
	util.P_out(req.String())
	writeSize := len(req.Data)
	size := uint64(req.Offset) + uint64(writeSize)
	if size < file.Attr().Size {
		size = file.Attr().Size
	}
	if size > uint64(cap(file.data)) {
		tmp := file.data
		file.data = make([]byte, size, size*2)
		copy(file.data, tmp)
	} else {
		file.data = file.data[:size]
	}
	copy(file.data[req.Offset:], req.Data)
	file.commitChunks()
	resp.Size = writeSize
	file.Attrs.Size = uint64(size)
	file.Attrs.Mtime = time.Now()
	file.dirty = true
	return nil
}

func (file *File) loadChunks() {
	tmp := make([][]byte, len(file.DataBlocks))
	size := 0
	for i, chunkSha := range file.DataBlocks {
		if data, err := filesystem.database.GetBlock(chunkSha); err != nil {
			util.P_err("Unable to load block", err)
			tmp[i] = []byte{} // TODO: probably shouldn't fail this silently
		} else {
			tmp[i] = data
		}
		size += len(tmp[i])
	}
	file.data = make([]byte, size)
	offset := 0
	for _, chunk := range tmp {
		copy(file.data[offset:], chunk)
		offset += len(chunk)
	}
}

func (file *File) commitChunks() {
	chunker := util.DefaultChunker()
	chunks := chunker.Chunks(file.data)
	file.DataBlocks = make([][]byte, len(chunks))
	for i, chunk := range chunks {
		hasher := sha1.New()
		hasher.Write(chunk)
		dataHash := hasher.Sum(nil)
		// Academic assumption: collisions aren't a thing, so we'll assume there will
		// never be any corruption, might as well save on writes in the process.
		util.P_out(string(dataHash))
		file.DataBlocks[i] = dataHash
		if !filesystem.DbContains(dataHash) {
			if dbErr := filesystem.PutChunk(dataHash, chunk); dbErr != nil {
				util.P_err("Failed to write chunk to db: ", dbErr)
			}
		}
	}
}
