package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

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
	ErrorTxnConflict   = errors.New("ErrorTxnConflict")
	ErrorKeyIsLocked   = errors.New("ErrorKeyIsLocked")
)

func CreateMvccStorage(pathDB string) *MvccStorage {
	var (
		db             *gorocksdb.DB
		rdOpts         = gorocksdb.NewDefaultReadOptions()
		wrOpts         = gorocksdb.NewDefaultWriteOptions()
		kDefaultPathDB = pathDB
		cfNames        = []string{"default", "cf_lock", "cf_write", "cf_raft"}
		cfOpts         = []*gorocksdb.Options{
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

	return &MvccStorage{db, rdOpts, wrOpts, handles, kDefaultPathDB, cfNames, cfOpts}
}

func (store *MvccStorage) Destroy() {
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

type Mutation struct {
	Key, Value string
	Op         byte
}

func (store *MvccStorage) Prewrite(mutations []Mutation, start_ts uint64, primary_key string) (keyIsLockedErrors []error, err error) {

	var (
		result  *gorocksdb.Slice
		cf_data = make(map[string]string)
		cf_lock = make(map[string]string)
	)

	for _, mutation := range mutations {
		result, _ = store.db.GetCF(store.rdOpts, store.handles[3], []byte(mutation.Key))
		if len(result.Data()) != 0 {
			last_commit_ts := binary.BigEndian.Uint64(result.Data())
			if last_commit_ts >= start_ts {
				err = ErrorWriteConflict
				return
			}
		}

		result, _ = store.db.GetCF(store.rdOpts, store.handles[1], []byte(mutation.Key))
		if len(result.Data()) != 0 {
			lock_ts := binary.BigEndian.Uint64(result.Data())
			if lock_ts != start_ts {
				keyIsLockedErrors = append(keyIsLockedErrors, ErrorKeyIsLocked)
			} else {
				cf_data[mutation.Key+"_"+string(start_ts)] = mutation.Value
				cf_lock[mutation.Key] = string(mutation.Op) + "_" + string(primary_key) + "_" + string(start_ts)
			}
		}
	}

	if len(keyIsLockedErrors) != 0 {
		err = ErrorKeyIsLocked
		return
	} else {
		for key, value := range cf_data {
			store.db.PutCF(store.wrOpts, store.handles[0], []byte(key), []byte(value))
		}
		for key, value := range cf_lock {
			store.db.PutCF(store.wrOpts, store.handles[1], []byte(key), []byte(value))
		}
	}
	return
}

func (store *MvccStorage) Commit(start_ts, commit_ts uint64, mutations []Mutation) error {
	var (
		result   *gorocksdb.Slice
		cf_lock  = make(map[string]string)
		cf_write = make(map[string]string)
		cf_raft  = make(map[string]string)
	)
	for _, mutation := range mutations {
		result, _ = store.db.GetCF(store.rdOpts, store.handles[1], []byte(mutation.Key))
		if len(result.Data()) != 0 {
			value := strings.Split(string(result.Data()), "_")
			lock_ts, _ := strconv.ParseUint(value[2], 10, 64)
			if lock_ts == start_ts {
				cf_write[mutation.Key+"_"+string(commit_ts)] = string(mutation.Op) + "_" + string(start_ts)
				cf_lock[mutation.Key] = string(commit_ts)
			}
		} else { // lock not exist or txn dismatch
			result, _ = store.db.GetCF(store.rdOpts, store.handles[2], []byte(mutation.Key+"_"+string(commit_ts)))
			if len(result.Data()) != 0 {
				write_type := strings.Split(string(result.Data()), "_")[0]
				switch write_type {
				case "P", "D", "L":
					break
				default:
					return ErrorTxnConflict

				}
			}
		}
	}
	for key, value := range cf_write {
		store.db.PutCF(store.wrOpts, store.handles[2], []byte(key), []byte(value))
	}
	for key, value := range cf_lock {
		store.db.DeleteCF(store.wrOpts, store.handles[1], []byte(key))
		store.db.PutCF(store.wrOpts, store.handles[3], []byte(key), []byte(value))
	}
	return nil
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	var (
		err    error
		result *gorocksdb.Slice
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
