package main

import (
	"flag"
	"log"
)

func main() {
	dsn := flag.String("dsn", "file:sqlite-fs.db", "sqlite3 data source name")
	mtpt := flag.String("mtpt", "./mnt/sqlitefs", "mount point")
	isDbg := flag.Bool("dbg", true, "enable fuse debug")
	flag.Parse()

	con, err := OpenSQLiteCon(*dsn)
	if err != nil {
		log.Fatalf("FATAL %s", err)
	}
	defer con.Close()

	fs := newRoot(con)
	fs.mount(*mtpt, *isDbg)
}
