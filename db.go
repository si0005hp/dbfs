package main

import (
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/mattn/go-sqlite3"
)

// DBCon ...
type DBCon interface {
	GetTblsMetadata() (map[string]*TblMeta, error)
	FetchRows(tbl string) (*sql.Rows, error)
	UpdateRow(q string, params []interface{}) (sql.Result, error)
	Close() error
}

// TblMeta ...
type TblMeta struct {
	tblNm  string
	pkCols []string
}

// SQLiteCon ...
type SQLiteCon struct {
	db *sql.DB
}

// OpenSQLiteCon ...
func OpenSQLiteCon(dsn string) (*SQLiteCon, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Add("_txlock", "immediate")
	u.RawQuery = q.Encode()

	db, err := sql.Open("sqlite3", u.String())
	if err != nil {
		return nil, err
	}
	return &SQLiteCon{db: db}, nil
}

// GetTblsMetadata ...
func (con *SQLiteCon) GetTblsMetadata() (map[string]*TblMeta, error) {
	q := "SELECT name FROM sqlite_master WHERE type='table'"
	rows, err := con.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ms := make(map[string]*TblMeta)
	for rows.Next() {
		var tblNm string
		err = rows.Scan(&tblNm)
		if err != nil {
			return nil, err
		}

		m, err := con.createTblMeta(con.db, tblNm)
		if err != nil {
			return nil, err
		}
		ms[tblNm] = m
	}
	return ms, nil
}

// FetchRows ...
func (con *SQLiteCon) FetchRows(tbl string) (*sql.Rows, error) {
	q := fmt.Sprintf("SELECT * FROM %s", tbl)
	rows, err := con.db.Query(q)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// UpdateRow ...
func (con *SQLiteCon) UpdateRow(q string, params []interface{}) (sql.Result, error) {
	stmt, err := con.db.Prepare(q)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.Exec(params...)
}

// Close ...
func (con *SQLiteCon) Close() error {
	return con.db.Close()
}

func (con *SQLiteCon) createTblMeta(db *sql.DB, tbl string) (*TblMeta, error) {
	q := fmt.Sprintf("SELECT name, pk FROM pragma_table_info('%s')", tbl)
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var pkCols []string
	for rows.Next() {
		var name string
		var pk int
		err = rows.Scan(&name, &pk)
		if err != nil {
			return nil, err
		}
		if pk == 1 {
			pkCols = append(pkCols, name)
		}
	}
	return &TblMeta{tblNm: tbl, pkCols: pkCols}, nil
}
