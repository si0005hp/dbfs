package main

import (
	"flag"
	"log"
)

func main() {
	dbType := flag.String("db", "sqlite", "DB product name")
	dsn := flag.String("dsn", "file:sample.db", "data source name")
	mtpt := flag.String("mtpt", "./mnt/dbfs", "mount point")
	isDbg := flag.Bool("dbg", true, "enable fuse debug")
	flag.Parse()

	con, err := OpenDB(*dbType, *dsn)
	if err != nil {
		log.Fatalf("FATAL %s", err)
	}
	defer con.Close()

	fs := newRoot(con)
	fs.mount(*mtpt, *isDbg)
}
