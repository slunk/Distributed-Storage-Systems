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

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"dss/myfs"
	"dss/util"
)


var root *myfs.Directory

type FS struct{}

func (fs FS) Root() (fs.Node, fuse.Error) {
	util.P_out("root returns as %d\n", int(root.Attr().Inode))
	return root, nil
}

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage

	debugPtr := flag.Bool("debug", false, "print lots of stuff")
	flag.Parse()
	util.SetDebug(*debugPtr)
	//debug = *debugPtr

	util.P_out("main\n")

	root = new(myfs.Directory)
	root.InitDirectory("", os.ModeDir|0755, nil)

	//nodeMap[uint64(root.attr.Inode)] = root
	util.P_out("root inode %d", int(root.Attr().Inode))

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
