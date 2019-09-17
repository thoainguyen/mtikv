package store

import (
	"github.com/tecbot/gorocksdb"
	"github.com/thoainguyen/mtikv/pkg/core/utils"
)

type Store struct {
	db             *gorocksdb.DB
	rdOpts         *gorocksdb.ReadOptions
	wrOpts         *gorocksdb.WriteOptions
	handles        []*gorocksdb.ColumnFamilyHandle
	kDefaultPathDB string
	cfNames        []string
	cfOpts         []*gorocksdb.Options
}

func CreateStore(path string) *Store {
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
	utils.CheckError(err)

	for i := 1; i < len(cfNames); i++ {
		cf, err := db.CreateColumnFamily(options, cfNames[i])
		utils.CheckError(err)
		cf.Destroy()
	}
	db.Close()

	db, handles, err = gorocksdb.OpenDbColumnFamilies(options, kDefaultPathDB, cfNames, cfOpts)
	utils.CheckError(err)

	return &Store{db, rdOpts, wrOpts, handles, kDefaultPathDB, cfNames, cfOpts}
}

func (store *Store) GetDir() string {
	return store.kDefaultPathDB
}

func (store *Store) Get(cf int, key []byte) []byte {
	data, err := store.db.GetCF(store.rdOpts, store.handles[cf], key)
	utils.CheckError(err)
	return data.Data()
}

func (store *Store) Put(cf int, key []byte, value []byte) {
	err := store.db.PutCF(store.wrOpts, store.handles[cf], key, value)
	utils.CheckError(err)
}

func (store *Store) Delete(cf int, key []byte) {
	err := store.db.DeleteCF(store.wrOpts, store.handles[cf], key)
	utils.CheckError(err)
}

func (store *Store) Destroy() {
	var err error
	// drop column family
	for i := 1; i < len(store.handles); i++ {
		err = store.db.DropColumnFamily(store.handles[i])
		utils.CheckError(err)
	}
	// close db
	for i := 0; i < len(store.handles); i++ {
		store.handles[i].Destroy()
	}
	store.db.Close()
}
