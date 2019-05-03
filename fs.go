package main

import (
	"database/sql"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

// Root ...
type Root struct {
	nodefs.Node
	db *sql.DB
}

// TblDir ...
type TblDir struct {
	nodefs.Node
	tbl string
}

func newRoot(db *sql.DB) *Root {
	return &Root{
		Node: nodefs.NewDefaultNode(),
		db:   db,
	}
}

func (root *Root) mount(mtpt string, isDbg bool) error {
	opts := nodefs.Options{
		AttrTimeout:  time.Second,
		EntryTimeout: time.Second,
		Debug:        isDbg,
	}
	s, _, err := nodefs.MountRoot(mtpt, root, &opts)
	if err != nil {
		return err
	}
	s.Serve()
	return nil
}

/* Root */

// GetAttr ...
func (root *Root) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	out.Mode = fuse.S_IFDIR | 0755
	return fuse.OK
}

// OpenDir ...
func (root *Root) OpenDir(ctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	tbls, err := Tables(root.db)
	if err != nil {
		panic(err)
	}
	dirs := make([]fuse.DirEntry, len(tbls))
	for i, tbl := range tbls {
		// Create DirEntry
		dirs[i] = fuse.DirEntry{
			Mode: fuse.S_IFDIR | 0755,
			Name: tbl,
		}
		// Create TblDir
		tblDir := &TblDir{
			Node: nodefs.NewDefaultNode(),
			tbl:  tbl,
		}
		root.Inode().NewChild(tbl, true, tblDir)
	}
	return dirs, fuse.OK
}

// Lookup ...
func (root *Root) Lookup(out *fuse.Attr, name string, ctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	c := root.Inode().GetChild(name)
	if c == nil {
		return nil, fuse.ENOENT
	}
	return c, fuse.OK
}
