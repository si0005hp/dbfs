package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
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
	db     *sql.DB
	tbl    string
	pkCols []string
}

// RowFile ...
type RowFile struct {
	nodefs.Node
	id   string // id represents the file name same time
	data []byte
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
			db:   root.db,
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

/* TblDir */

// GetAttr ...
func (dir *TblDir) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	out.Mode = fuse.S_IFDIR | 0755
	return fuse.OK
}

// OpenDir ...
func (dir *TblDir) OpenDir(ctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	// Fetch tables rows
	rows, err := Rows(dir.db, dir.tbl)
	if err != nil {
		panic(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// Create childs
	rowIdx := 1
	var dirs []fuse.DirEntry
	for rows.Next() {
		// Scan row
		vals := make([]interface{}, len(cols))
		for i := range vals {
			var s string
			vals[i] = &s
		}
		err = rows.Scan(vals...)
		if err != nil {
			panic(err)
		}
		// Create file content
		lines := make([]string, len(cols))
		for i, p := range vals {
			v := *(p.(*string))
			lines[i] = fmt.Sprintf("%s=%s", cols[i], v)
		}
		// Add child
		fileNm := strconv.Itoa(rowIdx)
		file := &RowFile{
			Node: nodefs.NewDefaultNode(),
			id:   fileNm,
			data: []byte(strings.Join(lines, "\n")),
		}
		dir.Inode().NewChild(fileNm, false, file)
		// Add DirEntry
		dirs = append(dirs, fuse.DirEntry{
			Mode: fuse.S_IFREG | 0755,
			Name: fileNm,
		})
		rowIdx++
	}
	return dirs, fuse.OK
}
