package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// OpenDB ...
func OpenDB(dsn string) (*sql.DB, error) {
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
	return db, nil
}

// TblsMetadata ...
func TblsMetadata(db *sql.DB) (map[string]*TblMeta, error) {
	q := `SELECT name FROM sqlite_master WHERE type='table'`
	rows, err := db.Query(q)
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

		m, err := createTblMeta(db, tblNm)
		if err != nil {
			return nil, err
		}
		ms[tblNm] = m
	}
	return ms, nil
}

func createTblMeta(db *sql.DB, tbl string) (*TblMeta, error) {
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

// Rows ...
func Rows(db *sql.DB, tbl string) (*sql.Rows, error) {
	q := fmt.Sprintf("SELECT * FROM %s", tbl)
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// Update ...
func Update(db *sql.DB, tbl string, updCols map[string]string, pkCols map[string]string) (sql.Result, error) {
	updClause, updParams := clauseAndParams(updCols, " , ")
	whereClause, whereParams := clauseAndParams(pkCols, " AND ")

	q := fmt.Sprintf("UPDATE %s SET %s WHERE %s", tbl, updClause, whereClause)
	stmt, err := db.Prepare(q)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.Exec(append(updParams, whereParams...)...)
}

func clauseAndParams(cols map[string]string, sep string) (string, []interface{}) {
	i := 0
	clauses := make([]string, len(cols))
	params := make([]interface{}, len(cols))
	for k, v := range cols {
		clauses[i] = fmt.Sprintf("%s = ?", k)
		params[i] = v
		i++
	}
	return strings.Join(clauses, sep), params
}
