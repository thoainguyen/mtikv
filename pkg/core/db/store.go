package db

import (
	"log"

	"github.com/tecbot/gorocksdb"
)

type Storage struct {
	db             *gorocksdb.DB
	rdOpts         *gorocksdb.ReadOptions
	wrOpts         *gorocksdb.WriteOptions
	handles        []*gorocksdb.ColumnFamilyHandle
	kDefaultPathDB string
	cfNames        []string
	cfOpts         []*gorocksdb.Options
}

func CreateStorage(path string) *Storage {
	var (
		db             *gorocksdb.DB
		rdOpts         = gorocksdb.NewDefaultReadOptions()
		wrOpts         = gorocksdb.NewDefaultWriteOptions()
		kDefaultPathDB = path
		cfNames        = []string{"default", "lock", "write", "info"}

		// + Default: ${key}_${start_ts} => ${value}
		// + Lock: ${key} => ${start_ts,primary_key,..etc}
		// + Write: ${key}_${commit_ts} => ${start_ts}
		// + Info: ${key} => ${commit_ts}

		cfOpts = []*gorocksdb.Options{
			gorocksdb.NewDefaultOptions(),
			gorocksdb.NewDefaultOptions(),
			gorocksdb.NewDefaultOptions(),
			gorocksdb.NewDefaultOptions(),
		}
		options = gorocksdb.NewDefaultOptions()
		handles []*gorocksdb.ColumnFamilyHandle
		err     error
	)

	options.SetCreateIfMissing(true)

	db, err = gorocksdb.OpenDb(options, kDefaultPathDB)
	checkError(err)

	for i := 1; i < len(cfNames); i++ {
		cf, err := db.CreateColumnFamily(options, cfNames[i])
		checkError(err)
		cf.Destroy()
	}
	db.Close()

	db, handles, err = gorocksdb.OpenDbColumnFamilies(options, kDefaultPathDB, cfNames, cfOpts)
	checkError(err)

	return &Storage{db, rdOpts, wrOpts, handles, kDefaultPathDB, cfNames, cfOpts}
}

func (store *Storage) Interator(cf int) *gorocksdb.Iterator {
	return store.db.NewIteratorCF(store.rdOpts, store.handles[cf])
}

func (store *Storage) Get(cf int, key []byte) []byte {
	data, err := store.db.GetCF(store.rdOpts, store.handles[cf], key)
	checkError(err)
	return data.Data()
}

func (store *Storage) Put(cf int, key []byte, value []byte) {
	err := store.db.PutCF(store.wrOpts, store.handles[cf], key, value)
	checkError(err)
}

func (store *Storage) Delete(cf int, key []byte) {
	err := store.db.DeleteCF(store.wrOpts, store.handles[cf], key)
	checkError(err)
}

func (store *Storage) Destroy() {
	var err error
	// drop column family
	for i := 1; i < len(store.handles); i++ {
		err = store.db.DropColumnFamily(store.handles[i])
		checkError(err)
	}
	// close db
	for i := 0; i < len(store.handles); i++ {
		store.handles[i].Destroy()
	}
	store.db.Close()
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
