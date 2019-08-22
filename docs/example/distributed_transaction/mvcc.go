package main


import (
	"fmt"
	"log"
	"errors"
	"encoding/binary"
	"github.com/tecbot/gorocksdb"
)

type MvccStorage struct {
	db             *gorocksdb.DB
	rdOpts         *gorocksdb.ReadOptions
	wrOpts         *gorocksdb.WriteOptions
	handles        []*gorocksdb.ColumnFamilyHandle
	kDefaultPathDB string
	cfNames        []string
	cfOpts         []*gorocksdb.Options
}

var (
	ErrorWriteConflict = errors.New("ErrorWriteConflict")
	ErrorTxnConflict = errors.New("ErrorTxnConflict")
	ErrorKeyIsLocked = errors.New("ErrorKeyIsLocked")
)

func CreateMvccStorage(pathDB string) *MvccStorage {
	var (
		db *gorocksdb.DB
		rdOpts = gorocksdb.NewDefaultReadOptions()
		wrOpts = gorocksdb.NewDefaultWriteOptions()
		kDefaultPathDB = pathDB
		cfNames = []string{"default", "cf_lock", "cf_write", "cf_raft"}
		cfOpts = []*gorocksdb.Options{
			gorocksdb.NewDefaultOptions(),
			gorocksdb.NewDefaultOptions(),
			gorocksdb.NewDefaultOptions(),
			gorocksdb.NewDefaultOptions(),
		}
		options = gorocksdb.NewDefaultOptions()
		handles []*gorocksdb.ColumnFamilyHandle
		err error
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

	return &MvccStorage{db, rdOpts, wrOpts, handles, kDefaultPathDB, cfNames, cfOpts}
}

func (store *MvccStorage) Destroy(){
	var err error
	// drop column family
	for i := 1; i < len(store.handles);i++ {
		err = store.db.DropColumnFamily(store.handles[i])
		checkError(err)	
	}
	// close db
	for i := 0; i < len(store.handles); i++ {
		store.handles[i].Destroy()
	}
	store.db.Close()
}

type Mutation struct {
	Key, Value string
	Op   byte
}

func (store *MvccStorage) Prewrite(mutations []Mutation, start_ts uint64) (keyIsLockedErrors []error, err error) {
	
	var result  *gorocksdb.Slice

	for idx, mutation := range mutations {
		result, _ = store.db.GetCF(store.rdOpts, store.handles[3], []byte(mutation.Key))
		last_commit_ts := binary.BigEndian.Uint64(result.Data())
		if last_commit_ts >= start_ts {
			err = ErrorWriteConflict
			return
		}
		
		result, _ = store.db.GetCF(store.rdOpts, store.handles[1], []byte(mutation.Key))
		lock_ts := binary.BigEndian.Uint64(result.Data())
		if lock_ts != start_ts {
			keyIsLockedErrors = append(keyIsLockedErrors, ErrorKeyIsLocked) 
		}
		// TODO: write in buffer, and commit later
		batch := gorocksdb.NewWriteBatch()
		batch.PutCF(store.handles[0], []byte(mutation.Key + "_" + string(start_ts)), []byte(mutation.Value))
		batch.PutCF(store.handles[1], []byte(mutation.Key), []byte(string(start_ts)))
		_ = store.db.Write(store.wrOpts, batch)
	}

	if len(keyIsLockedErrors) != 0 {
		err = ErrorKeyIsLocked
		return
	}

	
}


func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}


func main() {

	var (
		err            error
		result         *gorocksdb.Slice
	)

	// open DB
	store := CreateMvccStorage("volumes")

	// put and get from non-default column family
	err = store.db.PutCF(store.wrOpts, store.handles[0], []byte("key"), []byte("value"))
	checkError(err)

	result, err = store.db.GetCF(store.rdOpts, store.handles[0], []byte("key"))
	checkError(err)

	fmt.Println(string(result.Data()))

	// atomic write
	var batch *gorocksdb.WriteBatch
	batch = gorocksdb.NewWriteBatch()
	batch.PutCF(store.handles[0], []byte("key2"), []byte("value2"))
	batch.PutCF(store.handles[1], []byte("key3"), []byte("value3"))
	batch.DeleteCF(store.handles[0], []byte("key"))

	err = store.db.Write(store.wrOpts, batch)
	checkError(err)

	store.Destroy()
	
}
