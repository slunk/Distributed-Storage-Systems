package myfs

import (
	"encoding/json"
	"errors"
	"strconv"

	"github.com/syndtr/goleveldb/leveldb"

	"dss/util"
)

const (
	HEAD = "head"
)

type FsDatabase interface {
	Close() error
	GetRoot() (*Directory, error)
	SetRoot(dir *Directory) error
	GetFile(vid uint64) (*File, error)
	GetDirectory(vid uint64) (*Directory, error)
	GetBlock(sha1 []byte) ([]byte, error)
	PutFile(file *File) error
	PutDirectory(dir *Directory) error
	PutBlock(sha1 []byte, data []byte) error
}

var errDummy = errors.New("Using DummyFsDb")

type DummyFsDb struct{}

func (fsdb *DummyFsDb) Close() error {
	return nil
}

func (fsdb *DummyFsDb) GetRoot() (*Directory, error) {
	return nil, errDummy
}

func (fsdb *DummyFsDb) SetRoot(dir *Directory) error {
	return errDummy
}

func (fsdb *DummyFsDb) GetFile(vid uint64) (*File, error) {
	return nil, errDummy
}

func (fsdb *DummyFsDb) GetDirectory(vid uint64) (*Directory, error) {
	return nil, errDummy
}

func (fsdb *DummyFsDb) GetBlock(sha1 []byte) ([]byte, error) {
	return nil, errDummy
}

func (fsdb *DummyFsDb) PutFile(file *File) error {
	return errDummy
}

func (fsdb *DummyFsDb) PutDirectory(dir *Directory) error {
	return errDummy
}

func (fsdb *DummyFsDb) PutBlock(sha1 []byte, data []byte) error {
	return errDummy
}

type LeveldbFsDatabase struct {
	database *leveldb.DB
}

func NewLeveldbFsDatabase(dbPath string) (*LeveldbFsDatabase, error) {
	if db, err := leveldb.OpenFile(dbPath, nil); err != nil {
		return nil, err
	} else {
		fsdb := &LeveldbFsDatabase{
			database: db,
		}
		return fsdb, nil
	}
}

func (fsdb *LeveldbFsDatabase) GetRoot() (*Directory, error) {
	head := []byte(HEAD)
	vid, err := fsdb.database.Get(head, nil)
	if err != nil {
		return nil, err
	}
	if root, err := fsdb.getDirectory(vid); err != nil {
		return nil, err
	} else {
		return root, nil
	}
}

func (fsdb *LeveldbFsDatabase) SetRoot(dir *Directory) error {
	head := []byte(HEAD)
	vidStr := []byte(strconv.FormatUint(dir.Vid, 10))
	if err := fsdb.database.Put(head, vidStr, nil); err != nil {
		return err
	}
	fsdb.PutDirectory(dir)
	return nil
}

func (fsdb *LeveldbFsDatabase) getDirectory(key []byte) (*Directory, error) {
	dir := new(Directory)
	dir.children = make(map[string]NamedNode)
	if val, dbErr := fsdb.database.Get(key, nil); dbErr == nil {
		if jsonErr := json.Unmarshal(val, dir); jsonErr != nil {
			return nil, jsonErr
		}
	} else {
		return nil, dbErr
	}
	dir.dirty = false
	dir.childrenInMemory = false
	return dir, nil
}

func (fsdb *LeveldbFsDatabase) GetDirectory(vid uint64) (*Directory, error) {
	key := []byte(strconv.FormatUint(vid, 10))
	return fsdb.getDirectory(key)
}

func (fsdb *LeveldbFsDatabase) GetFile(vid uint64) (*File, error) {
	key := []byte(strconv.FormatUint(vid, 10))
	if val, dbErr := fsdb.database.Get(key, nil); dbErr == nil {
		file := new(File)
		if jsonErr := json.Unmarshal(val, file); jsonErr == nil {
			//file.data = make([]byte, 0, MIN_FILE_CAPACITY)
			file.loaded = false
			return file, nil
		} else {
			return nil, jsonErr
		}
	} else {
		return nil, dbErr
	}
}

func (fsdb *LeveldbFsDatabase) GetBlock(sha1 []byte) ([]byte, error) {
	return fsdb.database.Get(sha1, nil)
}

func (fsdb *LeveldbFsDatabase) PutDirectory(dir *Directory) error {
	key := []byte(strconv.FormatUint(dir.Vid, 10))
	//dir.updateChildVids()
	if val, jsonErr := json.Marshal(dir); jsonErr == nil {
		if dbErr := fsdb.database.Put(key, val, nil); dbErr != nil {
			util.P_err("Error writing to the db: ", dbErr)
			return dbErr
		} else {
			dir.dirty = false
			return nil
		}
	} else {
		util.P_err("Error jsonifying direcotory: ", jsonErr)
		return jsonErr
	}
}

func (fsdb *LeveldbFsDatabase) PutFile(file *File) error {
	key := []byte(strconv.FormatUint(file.Vid, 10))
	if val, jsonErr := json.Marshal(file); jsonErr == nil {
		if dbErr := fsdb.database.Put(key, val, nil); dbErr != nil {
			return dbErr
		} else {
			file.dirty = false
			return nil
		}
	} else {
		return jsonErr
	}
}

func (fsdb *LeveldbFsDatabase) PutBlock(sha1 []byte, data []byte) error {
	return fsdb.database.Put(sha1, data, nil)
}

func (fsdb *LeveldbFsDatabase) Close() error {
	return fsdb.database.Close()
}
