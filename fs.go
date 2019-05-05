package main

import (
	"database/sql"
	"errors"
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

// Node ...
type Node interface {
	nodefs.Node
	GetFileInfo() *FileInfo
	GetChildren() ([]Node, error)
}

// Root ...
type Root struct {
	nodefs.Node
	fileInfo FileInfo
	con      DBCon
}

// TblDir ...
type TblDir struct {
	nodefs.Node
	fileInfo FileInfo
	con      DBCon
	meta     *TblMeta
}

// RowFile ...
type RowFile struct {
	nodefs.Node
	fileInfo FileInfo
	data     []byte
	parent   *TblDir // parent TblDir
}

// FileInfo ... @see os.FileInfo
type FileInfo struct {
	name         string
	size         int64
	mode         os.FileMode
	modTime      time.Time
	creationTime time.Time
}

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
		fileInfo: FileInfo{
			mode:         os.ModeDir | 0755,
			modTime:      now,
			creationTime: now,
		},
		con: con,
	}
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

func openDir(n Node) ([]fuse.DirEntry, fuse.Status) {
	children, err := n.GetChildren()
	if err != nil {
		panic(err)
	}
	dirs := make([]fuse.DirEntry, len(children))
	for i, c := range children {
		fi := c.GetFileInfo()
		fi.mapDirEntry(&dirs[i])
		if n.Inode().GetChild(fi.name) == nil {
			n.Inode().NewChild(fi.name, fi.isDir(), c)
		}
	}
	return dirs, fuse.OK
}

/* Root */

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

// GetAttr ...
func (root *Root) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	root.GetFileInfo().mapAttr(out)
	return fuse.OK
}

// OpenDir ...
func (root *Root) OpenDir(ctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	return openDir(root)
}

// Lookup ...
func (root *Root) Lookup(out *fuse.Attr, name string, ctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	return lookup(root, out, name, ctx)
}

// GetChildren ...
func (root *Root) GetChildren() ([]Node, error) {
	now := time.Now()
	ms, err := root.con.GetTblsMetadata()
	if err != nil {
		return nil, err
	}
	i := 0
	dirs := make([]Node, len(ms))
	for tbl, m := range ms {
		dirs[i] = &TblDir{
			Node: nodefs.NewDefaultNode(),
			fileInfo: FileInfo{
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

// GetFileInfo ...
func (root *Root) GetFileInfo() *FileInfo {
	return &root.fileInfo
}

/* TblDir */

// GetAttr ...
func (dir *TblDir) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	dir.GetFileInfo().mapAttr(out)
	return fuse.OK
}

// Lookup ...
func (dir *TblDir) Lookup(out *fuse.Attr, name string, ctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	return lookup(dir, out, name, ctx)
}

// OpenDir ...
func (dir *TblDir) OpenDir(ctx *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	return openDir(dir)
}

// GetChildren ...
func (dir *TblDir) GetChildren() ([]Node, error) {
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
	files := make([]Node, 0)
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
			fileInfo: FileInfo{
				name:         fileNm,
				size:         int64(len(data)),
				mode:         0666,
				modTime:      dir.GetFileInfo().modTime,
				creationTime: dir.GetFileInfo().creationTime,
			},
			data:   data,
			parent: dir,
		})
		rowIdx++
	}
	return files, nil
}

// GetFileInfo ...
func (dir *TblDir) GetFileInfo() *FileInfo {
	return &dir.fileInfo
}

/* RowFile */

// GetAttr ...
func (f *RowFile) GetAttr(out *fuse.Attr, file nodefs.File, ctx *fuse.Context) fuse.Status {
	f.GetFileInfo().mapAttr(out)
	return fuse.OK
}

// Open ...
func (f *RowFile) Open(flags uint32, ctx *fuse.Context) (nodefs.File, fuse.Status) {
	return nodefs.NewDataFile(f.data), fuse.OK
}

// GetChildren ...
func (f *RowFile) GetChildren() ([]Node, error) {
	return nil, errors.New("Unsupported operation")
}

// GetFileInfo ...
func (f *RowFile) GetFileInfo() *FileInfo {
	return &f.fileInfo
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
