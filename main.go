package main

import (
	"flag"
	"fmt"
	"log"
)

func main() {
	dsn := flag.String("dsn", "file:sqlite-fs.db", "sqlite3 data source name")
	flag.Parse()

	db, err := OpenDB(*dsn)
	if err != nil {
		log.Fatalf("FATAL %s", err)
	}
	defer db.Close()

	tbls, err := Tables(db)
	if err != nil {
		log.Fatalf("FATAL %s", err)
	}
	fmt.Println(tbls)
}
