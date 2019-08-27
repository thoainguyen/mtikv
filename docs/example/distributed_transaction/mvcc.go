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
		cfNames        = []string{"default", "cf_lock", "cf_write", "cf_info"}

		// + Default: ${key}_${start_ts} => ${value}
		// + Lock: ${key} => ${start_ts,primary_key,..etc}
		// + Write: ${key}_${commit_ts} => ${start_ts}
		// + Info: ${key} => $(commit_ts), latest commit timestamp

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

type KeyValuePair struct {
	Key, Value string
}
type Mutation struct {
	KeyValuePair
	Op string
}

// prewrite(start_ts, data_list)
func (store *MvccStorage) Prewrite(mutations []Mutation, start_ts uint64, primary_key string) (keyIsLockedErrors []error, err error) {
	var (
		result  *gorocksdb.Slice
		cf_data []KeyValuePair
		cf_lock []KeyValuePair
	)
	// for keys in data_list, prewrite each key with start_ts in memory
	for _, mutation := range mutations {
		// get key's latest commit info write column with max_i64
		result, _ = store.db.GetCF(store.rdOpts, store.handles[3], []byte(mutation.Key))
		if len(result.Data()) != 0 {
			last_commit_ts := binary.BigEndian.Uint64(result.Data())
			// if commit_ts >= start_ts => return Error WriteConflict
			if last_commit_ts >= start_ts {
				err = ErrorWriteConflict
				return
			}
		}
		// get key's lock info
		result, _ = store.db.GetCF(store.rdOpts, store.handles[1], []byte(mutation.Key))
		// if lock exist
		if len(result.Data()) != 0 {
			lock_ts := binary.BigEndian.Uint64(result.Data())
			// if lock_ts != start_ts => add one KeyIsLocked Error
			if lock_ts != start_ts {
				keyIsLockedErrors = append(keyIsLockedErrors, ErrorKeyIsLocked)
			}
		} else {
			// write in memory:lock(key, start_ts, primary) & default(value)
			cf_data = append(cf_data, KeyValuePair{mutation.Key + "_" + strconv.FormatUint(start_ts, 10), mutation.Value})
			cf_lock = append(cf_lock, KeyValuePair{mutation.Key, mutation.Op + "_" + string(primary_key) + "_" + strconv.FormatUint(start_ts, 10)})
		}
	}
	// if KeyIsLocked exist => return slice KeyIsLocked Error
	if len(keyIsLockedErrors) != 0 {
		err = ErrorKeyIsLocked
		return
	} else { // commit change, write into rocksdb column family
		for _, pair := range cf_data { // loop: write in cf_data
			store.db.PutCF(store.wrOpts, store.handles[0], []byte(pair.Key), []byte(pair.Value))
		}
		for _, pair := range cf_lock { // loop: write in cf_lock
			store.db.PutCF(store.wrOpts, store.handles[1], []byte(pair.Key), []byte(pair.Value))
		}
	}
	return
}

// commit(keys, start_ts, commit_ts)
func (store *MvccStorage) Commit(start_ts, commit_ts uint64, mutations []Mutation) error {
	var (
		result   *gorocksdb.Slice
		cf_lock  []KeyValuePair
		cf_write []KeyValuePair
	)
	// for each key in keys, do commit
	for _, mutation := range mutations {
		// get key's lock
		result, _ = store.db.GetCF(store.rdOpts, store.handles[1], []byte(mutation.Key))
		if len(result.Data()) != 0 { // if lock exist
			value := strings.Split(string(result.Data()), "_")
			lock_ts, _ := strconv.ParseUint(value[2], 10, 64)
			// if lock_ts == start_ts
			if lock_ts == start_ts {
				// write memory:set write(commit_ts, lock_type, start_ts)
				cf_write = append(cf_write, KeyValuePair{mutation.Key + "_" + strconv.FormatUint(commit_ts, 10), string(mutation.Op) + "_" + strconv.FormatUint(start_ts, 10)})
				// write memory: current lock(key) will be removed and latest commit_ts
				cf_lock = append(cf_lock, KeyValuePair{mutation.Key, strconv.FormatUint(commit_ts, 10)})
			}
		} else { // lock not exist or txn dismatch
			// get(key, start_ts) from write
			result, _ = store.db.GetCF(store.rdOpts, store.handles[2], []byte(mutation.Key+"_"+strconv.FormatUint(commit_ts, 10)))
			if len(result.Data()) != 0 { // if write exist
				write_type := strings.Split(string(result.Data()), "_")[0]
				switch write_type {
				// write_type in {PUT/DELETE/Lock}, the tx is already commited
				case "P", "D", "L":
					break
				// write_type is Rollback or None
				default:
					// return ERROR Txn Conflict, lock not found
					return ErrorTxnConflict
				}
			}
		}
	}
	// commit change
	for _, pair := range cf_write {
		store.db.PutCF(store.wrOpts, store.handles[2], []byte(pair.Key), []byte(pair.Value))
	}
	for _, pair := range cf_lock {
		store.db.DeleteCF(store.wrOpts, store.handles[1], []byte(pair.Key))
		// write key's latest commit
		store.db.PutCF(store.wrOpts, store.handles[3], []byte(pair.Key), []byte(pair.Value))
	}
	return nil
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	// open MvccStorage
	store := CreateMvccStorage("volumes")
	mutations := []Mutation{
		Mutation{KeyValuePair{"thoainh", "Nguyen Huynh Thoai"}, "P"},
		Mutation{KeyValuePair{"nhthoai", "Thoai Nguyen Huynh"}, "P"},
		Mutation{KeyValuePair{"thoainh", "Thoai Huynh"}, "P"},
	}
	_, errPrewrite := store.Prewrite(mutations, 0, "thoainh")
	if errPrewrite != nil {
		fmt.Println(errPrewrite)
	}

	errCommit := store.Commit(0, 1, mutations)
	if errCommit != nil {
		fmt.Println(errCommit)
	}

	result, err := store.db.GetCF(store.rdOpts, store.handles[0], []byte("thoainh_0"))
	checkError(err)
	fmt.Println(string(result.Data()))

	// destroy MvccStorage
	store.Destroy()

}
