# Distributed Storage Systems Projects

Peter Enns' implementations of the class projects for CMSC818e.

## Project 3

### Building / Running

Same as project 2 except there is no need to wait before killing the program.

If the behavior is slightly different from what was expected, try the
-mtimeArchives command line option. By default, archives are created with a
timestamp when the versions were committed to the database instead of
"modified time" (this allows for directories to have multiple versions when a
file inside them is written to, which is not true of Mtime).

### Relevant portions

1. Directory archive code is mostly in myfs/directory.go, and some small
modifications in myfs/node.go and myfs/fs.go.

## Project 2

### Building / Running

1. Save this folder in $GOPATH/src. It must be called dss.
2. "go get" bazil fuse and goleveldb
3. Run "go run memfs.go /mount/point /path/do/db" (db directory doesn't need to exist)
4. Make sure to wait at least 5 seconds after any action before you kill it so it has a chance to write to the db.

If you run into trouble running it, email me at slunk@umd.edu

### Relevant Portions

* Persistence is mostly taken care of in myfs/db.go, myfs/fs.go, myfs/dir.go, and myfs/file.go
* Rabin-Karp chunking is taken care of in util/rk.go
* Concurrent flusher code is mostly in fs.go, goroutine run in memfs.go

### Testing

You can run "go test dss/test" to run a small suite of unit tests. Should pass
as long as it has write access to /tmp.
