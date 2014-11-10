package test

import (
	"os"
	"testing"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"dss/myfs"
)

const dbPath = "/tmp/testdb"

var fsdb *myfs.LeveldbFsDatabase

func setup(t *testing.T) {
	t.Log("Starting up")
	fsdb, _ = myfs.NewLeveldbFsDatabase(dbPath)
	myfs.NewFs(fsdb)
}

func teardown(t *testing.T) {
	t.Log("Tearing down")
	fsdb.Close()
	os.RemoveAll(dbPath)
}

func genericDbTest(t *testing.T, test func()) {
	setup(t)
	defer teardown(t)
	test()
}

func TestSaveSingleDirectory(t *testing.T) {
	genericDbTest(t, func() {
		dir := new(myfs.Directory)
		dir.InitDirectory("dirname", os.ModeDir|0755, nil)
		dir.SetDirty(true)
		fsdb.PutDirectory(dir)
		anotherDir, _ := fsdb.GetDirectory(dir.Vid)
		assertWithMsg(t, dir.Vid == anotherDir.Vid,
			"Expected Vid to be the same after load.")
		assertWithMsg(t, dir.Name == anotherDir.Name,
			"Expected Name to be the same after load.")
	})
}

func TestSaveSingleFile(t *testing.T) {
	genericDbTest(t, func() {
		file := new(myfs.File)
		file.InitFile("someFile", 755, nil)
		writeReq := &fuse.WriteRequest{
			Offset: 0,
			Data:   []byte("qwejkrhbwqkjhebrjhkqw"),
		}
		file.Write(writeReq, new(fuse.WriteResponse), *new(fs.Intr))
		file.CommitChunks()
		fsdb.PutFile(file)
		fileFromDb, _ := fsdb.GetFile(file.Vid)
		assertWithMsg(t, file.Vid == fileFromDb.Vid,
			"Expected Vid to be same after load.")
		assertWithMsg(t, file.Name == fileFromDb.Name,
			"Expected Name to be the same after load.")
		assertWithMsg(t, len(file.DataBlocks) == len(fileFromDb.DataBlocks),
			"Expected same number of data blocks after load.")
		for i, expectedBlockHash := range file.DataBlocks {
			assertWithMsg(t, string(expectedBlockHash) == string(fileFromDb.DataBlocks[i]),
				"Expected all data block hashes to be indentical.")
		}
		assertWithMsg(t, !fileFromDb.DataIsLoaded(), "Data should not be loaded before read")
		inData, _ := file.ReadAll(*new(fs.Intr))
		outData, _ := fileFromDb.ReadAll(*new(fs.Intr))
		assertWithMsg(t, fileFromDb.DataIsLoaded(), "Data should be loaded after read")
		t.Log(inData)
		t.Log(outData)
		assertWithMsg(t, string(inData) == string(outData),
			"Expected file data to be identical after load.")
	})
}

func TestSaveRoot(t *testing.T) {
	genericDbTest(t, func() {
		dir := new(myfs.Directory)
		dir.InitDirectory("", 0755, nil)
		subdir := new(myfs.Directory)
		subdir.InitDirectory("name", 0755, dir)
		dir.Mkdir(&fuse.MkdirRequest{
			Name: "name",
			Mode: 0755,
		}, nil)
		fsdb.SetRoot(dir)
		fromDb, _ := fsdb.GetRoot()
		assertWithMsg(t, len(dir.ChildVids) == len(fromDb.ChildVids),
			"Expected same number of ChildVids after load.")
	})
}
