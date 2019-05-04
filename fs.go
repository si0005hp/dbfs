package main

import (
	"database/sql"
	"fmt"
	"os"
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
	FileInfo
	con DBCon
}

// TblDir ...
type TblDir struct {
	nodefs.Node
	FileInfo
	con  DBCon
	meta *TblMeta
}

// RowFile ...
type RowFile struct {
	nodefs.Node
	FileInfo
	id     string // id represents the file name same time
	data   []byte
	parent *TblDir // parent TblDir
}

// FileInfo ... refs os.FileInfo
type FileInfo struct {
	name         string
	size         int64
	mode         os.FileMode
	modTime      time.Time
	creationTime time.Time
}

// IsDir ... refs os.FileMode#IsDir
func (i *FileInfo) isDir() bool {
	return i.mode&os.ModeDir != 0
}

func (i *FileInfo) mapAttr(out *fuse.Attr) {
	if i.isDir() {
		out.Mode = fuse.S_IFDIR | uint32(i.mode)&07777
	} else {
		out.Mode = fuse.S_IFREG | uint32(i.mode)&07777
	}
	out.Mtime = uint64(i.modTime.Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime
	out.Size = uint64(i.size)
}

func (i *FileInfo) mapDirEntry(out *fuse.DirEntry) {
	if i.isDir() {
		out.Mode = fuse.S_IFDIR | uint32(i.mode)&07777
	} else {
		out.Mode = fuse.S_IFREG | uint32(i.mode)&07777
	}
	out.Name = i.name
}

func newRoot(con DBCon) *Root {
	now := time.Now()
	return &Root{
		Node: nodefs.NewDefaultNode(),
		FileInfo: FileInfo{
			mode:         os.ModeDir | 0755,
			modTime:      now,
			creationTime: now,
		},
		con: con,
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

func lookup(n nodefs.Node, out *fuse.Attr, name string, ctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	_, st := n.OpenDir(ctx)
	if st != fuse.OK {
		return nil, st
	}
	c := n.Inode().GetChild(name)
	if c == nil {
		return nil, fuse.ENOENT
	}
	st = c.Node().GetAttr(out, nil, ctx)
	if st != fuse.OK {
		return nil, st
	}
	return c, fuse.OK
}

/* Root */

// GetAttr ...
func (root *Root) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	root.FileInfo.mapAttr(out)
	return fuse.OK
}

func (root *Root) getChildren() ([]*TblDir, error) {
	now := time.Now()
	ms, err := root.con.GetTblsMetadata()
	if err != nil {
		return nil, err
	}
	i := 0
	dirs := make([]*TblDir, len(ms))
	for tbl, m := range ms {
		dirs[i] = &TblDir{
			Node: nodefs.NewDefaultNode(),
			FileInfo: FileInfo{
				name:         tbl,
				mode:         os.ModeDir | 0755,
				modTime:      now,
				creationTime: now,
			},
			con:  root.con,
			meta: m,
		}
		i++
	}
	return dirs, nil
}

// OpenDir ...
func (root *Root) OpenDir(ctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	children, err := root.getChildren()
	if err != nil {
		panic(err)
	}
	dirs := make([]fuse.DirEntry, len(children))
	for i, c := range children {
		c.FileInfo.mapDirEntry(&dirs[i])
		if root.Inode().GetChild(c.FileInfo.name) == nil {
			root.Inode().NewChild(c.FileInfo.name, true, c)
		}
	}
	return dirs, fuse.OK
}

// Lookup ...
func (root *Root) Lookup(out *fuse.Attr, name string, ctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	return lookup(root, out, name, ctx)
}

/* TblDir */

// GetAttr ...
func (dir *TblDir) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	dir.FileInfo.mapAttr(out)
	return fuse.OK
}

// Lookup ...
func (dir *TblDir) Lookup(out *fuse.Attr, name string, ctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	return lookup(dir, out, name, ctx)
}

func (dir *TblDir) getChildren() ([]*RowFile, error) {
	rows, err := dir.con.FetchRows(dir.meta.tblNm)
	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rowIdx := 1
	files := make([]*RowFile, 0)
	for rows.Next() {
		// Scan row
		vals := make([]interface{}, len(cols))
		for i := range vals {
			var s string
			vals[i] = &s
		}
		err = rows.Scan(vals...)
		if err != nil {
			return nil, err
		}
		// Create file content
		lines := make([]string, len(cols))
		for i, p := range vals {
			v := *(p.(*string))
			lines[i] = fmt.Sprintf("%s=%s", cols[i], v)
		}
		// Create RowFile
		fileNm := strconv.Itoa(rowIdx)
		data := []byte(strings.Join(lines, "\n"))
		files = append(files, &RowFile{
			Node: nodefs.NewDefaultNode(),
			FileInfo: FileInfo{
				name:         fileNm,
				size:         int64(len(data)),
				mode:         0666,
				modTime:      dir.FileInfo.modTime,
				creationTime: dir.FileInfo.creationTime,
			},
			id:     fileNm,
			data:   data,
			parent: dir,
		})
		rowIdx++
	}
	return files, nil
}

// OpenDir ...
func (dir *TblDir) OpenDir(ctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	children, err := dir.getChildren()
	if err != nil {
		panic(err)
	}
	dirs := make([]fuse.DirEntry, len(children))
	for i, c := range children {
		c.FileInfo.mapDirEntry(&dirs[i])
		if dir.Inode().GetChild(c.FileInfo.name) == nil {
			dir.Inode().NewChild(c.FileInfo.name, false, c)
		}
	}
	return dirs, fuse.OK
}

/* RowFile */

// GetAttr ...
func (f *RowFile) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	f.FileInfo.mapAttr(out)
	return fuse.OK
}

// Open ...
func (f *RowFile) Open(flags uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	return nodefs.NewDataFile(f.data), fuse.OK
}

// Write ...
func (f *RowFile) Write(file nodefs.File, data []byte, off int64, context *fuse.Context) (written uint32, code fuse.Status) {
	if !reflect.DeepEqual(f.data, data) {
		_, err := f.update(data)
		if err != nil {
			panic(err)
		} else {
			f.data = data
		}
	}
	return uint32(int32(len(data))), fuse.OK
}

func (f *RowFile) update(data []byte) (sql.Result, error) {
	newP := properties.MustLoadString(string(data))
	oldP := properties.MustLoadString(string(f.data))

	updCols := make(map[string]string)
	for k, newV := range newP.Map() {
		oldV, ok := oldP.Get(k)
		if ok && oldV != newV {
			updCols[k] = newV
		}
	}

	pkCols := make(map[string]string)
	for _, pk := range f.parent.meta.pkCols {
		pkV, ok := oldP.Get(pk)
		if !ok {
			return nil, fmt.Errorf("%s: pk %s is not found", f.parent.meta.tblNm, pk)
		}
		pkCols[pk] = pkV
	}

	return f.parent.con.UpdateRow(f.parent.meta.tblNm, updCols, pkCols)
}
