/**
*
* @author: thoainh
 */
package main

import (
	"fmt"
	"log"

	"github.com/tecbot/gorocksdb"
)

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var (
	db             *gorocksdb.DB
	rdOpts         *gorocksdb.ReadOptions
	wrOpts         *gorocksdb.WriteOptions
	handles        []*gorocksdb.ColumnFamilyHandle
	kDefaultPathDB string
	cfNames        []string
	cfOpts         []*gorocksdb.Options
	err            error
	result         *gorocksdb.Slice
)

func init() {
	kDefaultPathDB = "volumes"
	rdOpts = gorocksdb.NewDefaultReadOptions()
	wrOpts = gorocksdb.NewDefaultWriteOptions()

	cfNames = []string{"default", "new_cf"}
	cfOpts = []*gorocksdb.Options{
		gorocksdb.NewDefaultOptions(),
		gorocksdb.NewDefaultOptions(),
	}
}

func main() {
	// open DB
	var options = gorocksdb.NewDefaultOptions()
	options.SetCreateIfMissing(true)

	db, err = gorocksdb.OpenDb(options, kDefaultPathDB)
	checkError(err)

	// create column family
	var cf *gorocksdb.ColumnFamilyHandle
	cf, err = db.CreateColumnFamily(options, "new_cf")
	checkError(err)

	// close DB
	cf.Destroy()
	db.Close()

	// open DB with two column families
	db, handles, err = gorocksdb.OpenDbColumnFamilies(options, kDefaultPathDB, cfNames, cfOpts)
	checkError(err)

	// put and get from non-default column family
	err = db.PutCF(wrOpts, handles[1], []byte("key"), []byte("value"))
	checkError(err)

	result, err = db.GetCF(rdOpts, handles[1], []byte("key"))
	checkError(err)

	fmt.Println(string(result.Data()))

	// atomic write
	var batch *gorocksdb.WriteBatch
	batch = gorocksdb.NewWriteBatch()
	batch.PutCF(handles[0], []byte("key2"), []byte("value2"))
	batch.PutCF(handles[1], []byte("key3"), []byte("value3"))
	batch.DeleteCF(handles[0], []byte("key"))
	err = db.Write(wrOpts, batch)
	checkError(err)

	// drop column family
	err = db.DropColumnFamily(handles[1])
	checkError(err)

	// close db
	for i, _ := range handles {
		handles[i].Destroy()
	}

	db.Close()

}
