package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/magiconair/properties"
)

// Root ...
type Root struct {
	nodefs.Node
	db *sql.DB
}

// TblDir ...
type TblDir struct {
	nodefs.Node
	db   *sql.DB
	meta TblMeta
}

// RowFile ...
type RowFile struct {
	nodefs.Node
	id   string // id represents the file name same time
	data []byte
	meta *TblMeta
}

// TblMeta ...
type TblMeta struct {
	tblNm  string
	pkCols []string
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

func lookup(n nodefs.Node, name string) (*nodefs.Inode, fuse.Status) {
	c := n.Inode().GetChild(name)
	if c == nil {
		return nil, fuse.ENOENT
	}
	return c, fuse.OK
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
			meta: TblMeta{tblNm: tbl},
		}
		if root.Inode().GetChild(tbl) == nil {
			root.Inode().NewChild(tbl, true, tblDir)
		}
	}
	return dirs, fuse.OK
}

// Lookup ...
func (root *Root) Lookup(out *fuse.Attr, name string, ctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	return lookup(root, name)
}

/* TblDir */

// GetAttr ...
func (dir *TblDir) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	out.Mode = fuse.S_IFDIR | 0755
	return fuse.OK
}

// Lookup ...
func (dir *TblDir) Lookup(out *fuse.Attr, name string, ctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	return lookup(dir, name)
}

// OpenDir ...
func (dir *TblDir) OpenDir(ctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	// Fetch tables rows
	rows, err := Rows(dir.db, dir.meta.tblNm)
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
			meta: &dir.meta,
		}
		if dir.Inode().GetChild(fileNm) == nil {
			dir.Inode().NewChild(fileNm, false, file)
		}
		// Add DirEntry
		dirs = append(dirs, fuse.DirEntry{
			Mode: fuse.S_IFREG | 0755,
			Name: fileNm,
		})
		rowIdx++
	}
	return dirs, fuse.OK
}

/* RowFile */

// GetAttr ...
func (f *RowFile) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	out.Mode = fuse.S_IFREG | 0755
	out.Size = uint64(int64(len(f.data)))
	return fuse.OK
}

// Open ...
func (f *RowFile) Open(flags uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	return nodefs.NewDataFile(f.data), fuse.OK
}

// Write ...
func (f *RowFile) Write(file nodefs.File, data []byte, off int64, context *fuse.Context) (written uint32, code fuse.Status) {
	if !reflect.DeepEqual(f.data, data) {
		f.update(data)
	}
	return uint32(int32(len(data))), fuse.OK
}

func (f *RowFile) update(data []byte) {
	newP := properties.MustLoadString(string(data))
	oldP := properties.MustLoadString(string(f.data))

	updCols := make(map[string]string)
	for k, newV := range newP.Map() {
		oldV, ok := oldP.Get(k)
		if ok && oldV != newV {
			updCols[k] = newV
		}
	}

	f.data = data
}
