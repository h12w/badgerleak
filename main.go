package main

import (
	"log"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/dgraph-io/badger"
)

/*
detect with:

watch "ps x -o command,rss | grep -v grep | grep badgerleak"

*/

func main() {
	leakFast()
	// leakSlow()
	// leakSlowest()
}

// loop: open, prefix scan and close
func leakFast() {
	rootDir := "testbadgerdb"
	for {
		db, err := openDB(rootDir)
		if err != nil {
			log.Fatal(err)
		}
		if err := db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()
			prefix := []byte{4}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				_, err := item.Value()
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			log.Fatal(err)
		}
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
		runtime.GC()
		pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	}
}

// loop: open, scan all and close
func leakSlow() {
	rootDir := "testbadgerdb"
	for {
		db, err := openDB(rootDir)
		if err != nil {
			log.Fatal(err)
		}
		if err := db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				_, err := item.Value()
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			log.Fatal(err)
		}
		if err := db.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

// open once, loop prefix scan
func leakSlowest() {
	rootDir := "testbadgerdb"
	db, err := openDB(rootDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	for {
		if err := db.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := txn.NewIterator(opts)
			defer it.Close()
			prefix := []byte{4}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				_, err := item.Value()
				if err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			log.Fatal(err)
		}
	}
}

func openDB(dir string) (*badger.DB, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = opts.Dir
	return badger.Open(opts)
}
