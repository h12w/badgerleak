package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger"
)

func gendb2(dstDir string) {
	os.RemoveAll(dstDir)
	os.MkdirAll(dstDir, 0755)

	srcDir := "01"
	srcDB, err := openDB(srcDir)
	if err != nil {
		log.Fatal(err)
	}
	defer srcDB.Close()
	dstDB, err := openDB(dstDir)
	if err != nil {
		log.Fatal(err)
	}
	defer dstDB.Close()
	dstDB.Update(func(dstTX *badger.Txn) error {
		srcDB.View(func(srcTX *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := srcTX.NewIterator(opts)
			for it.Rewind(); it.Valid(); it.Next() {
				item := it.Item()
				key := item.Key()
				value, err := item.Value()
				if err != nil {
					return err
				}
				if key[0] == 4 ||
					(key[0] == 5 && len(value) < 300) {
					dstTX.Set(key, value)
				}
			}
			return nil
		})
		return nil
	})
}

func gendb(rootDir string, prefix []byte) {
	os.RemoveAll(rootDir)
	os.MkdirAll(rootDir, 0755)
	db, err := openDB(rootDir)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	kk := strings.Repeat("k", 40)
	if err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 47279; i++ {
			if err := txn.Set(append([]byte{0}, []byte(kk+strconv.Itoa(i))...), bytes.Repeat([]byte("v"), 1000)); err != nil {
				return err
			}
		}
		for i := 0; i < 3; i++ {
			if err := txn.Set(append(prefix, []byte(kk+strconv.Itoa(i))...), bytes.Repeat([]byte("v"), 1000)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("db generated")
}
