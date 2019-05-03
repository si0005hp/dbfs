package main

import (
	"database/sql"
	"fmt"
	"net/url"

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

// Tables ...
func Tables(db *sql.DB) ([]string, error) {
	q := `SELECT name FROM sqlite_master WHERE type='table'`
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tbls []string
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		tbls = append(tbls, name)
	}
	return tbls, nil
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
