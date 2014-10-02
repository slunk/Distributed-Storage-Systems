# Distributed Storage Systems Projects

Peter Enns' implementations of the class projects for CMSC818e.

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
