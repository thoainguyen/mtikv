package txn

import (
	"encoding/binary"
	"errors"
	"log"
	"strconv"
	"strings"

	"github.com/thoainguyen/mtikv/pkg/core/db"
)

const (
	CF_DATA = iota
	CF_LOCK
	CF_WRITE
	CF_INFO
)

type Mvcc struct {
	store          *db.Storage
	kDefaultPathDB string
}

var (
	ErrorWriteConflict = errors.New("ErrorWriteConflict")
	ErrorTxnConflict   = errors.New("ErrorTxnConflict")
	ErrorKeyIsLocked   = errors.New("ErrorKeyIsLocked")
	ErrorLockNotFound  = errors.New("ErrorLockNotFound")
)

func (m *Mvcc) GetStore() *db.Storage {
	return m.store
}

func CreateMvcc(path string) *Mvcc {
	return &Mvcc{
		store:          db.CreateStorage(path),
		kDefaultPathDB: path,
	}
}

func (m *Mvcc) Destroy() {
	m.store.Destroy()
}

type KeyValuePair struct {
	Key, Value string
}
type Mutation struct {
	KeyValuePair
	Op string
}

// prewrite(start_ts, data_list)
func (m *Mvcc) Prewrite(mutations []Mutation, start_ts uint64, primary_key string) (keyIsLockedErrors []error, err error) {
	var (
		result  []byte
		cf_data []KeyValuePair
		cf_lock []KeyValuePair
	)
	// for keys in data_list, prewrite each key with start_ts in memory
	for _, mutation := range mutations {
		// get key's latest commit info write column with max_i64
		result = m.store.Get(CF_INFO, []byte(mutation.Key))

		if len(result) != 0 {
			last_commit_ts := binary.BigEndian.Uint64(result)
			// if commit_ts >= start_ts => return Error WriteConflict
			if last_commit_ts >= start_ts {
				err = ErrorWriteConflict
				return
			}
		}
		// get key's lock info
		result = m.store.Get(CF_LOCK, []byte(mutation.Key))
		// if lock exist
		if len(result) != 0 {
			lock_ts := binary.BigEndian.Uint64(result)
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

			m.store.Put(CF_DATA, []byte(pair.Key), []byte(pair.Value))
		}
		for _, pair := range cf_lock { // loop: write in cf_lock
			m.store.Put(CF_LOCK, []byte(pair.Key), []byte(pair.Value))
		}
	}
	return
}

// commit(keys, start_ts, commit_ts)
func (m *Mvcc) Commit(start_ts, commit_ts uint64, mutations []Mutation) error {
	var (
		result   []byte
		cf_lock  []KeyValuePair
		cf_write []KeyValuePair
	)
	// for each key in keys, do commit
	for _, mutation := range mutations {
		// get key's lock
		result = m.store.Get(CF_LOCK, []byte(mutation.Key))
		if len(result) != 0 { // if lock exist
			value := strings.Split(string(result), "_")
			lock_ts, _ := strconv.ParseUint(value[2], 10, 64)
			// if lock_ts == start_ts
			if lock_ts == start_ts {
				// write memory:set write(commit_ts, lock_type, start_ts)
				cf_write = append(cf_write, KeyValuePair{mutation.Key + "_" + strconv.FormatUint(commit_ts, 10), string(mutation.Op) + "_" + strconv.FormatUint(start_ts, 10)})
				// write memory: current lock(key) will be removed and latest commit_ts will be recorded in cf_info
				cf_lock = append(cf_lock, KeyValuePair{mutation.Key, strconv.FormatUint(commit_ts, 10)})
			}
		} else { // lock not exist or txn dismatch
			// get(key, start_ts) from write
			result = m.store.Get(CF_WRITE, []byte(mutation.Key+"_"+strconv.FormatUint(commit_ts, 10)))
			if len(result) != 0 { // if write exist
				write_type := strings.Split(string(result), "_")[0]
				switch write_type {
				// write_type in {PUT/DELETE/Lock}, the tx is already commited
				case "P", "D", "L":
					break
				// write_type is Rollback or None
				default:
					// return ERROR Txn Conflict, lock not found
					return ErrorTxnConflict
				}
			} else {
				return ErrorLockNotFound
			}
		}
	}
	// commit change
	for _, pair := range cf_write {
		m.store.Put(CF_WRITE, []byte(pair.Key), []byte(pair.Value))
	}
	for _, pair := range cf_lock {
		m.store.Delete(CF_LOCK, []byte(pair.Key))
		// write key's latest commit
		m.store.Put(CF_INFO, []byte(pair.Key), []byte(pair.Value))
	}
	return nil
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
