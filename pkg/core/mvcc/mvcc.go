package mvcc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"

	"github.com/thoainguyen/mtikv/pkg/core/db"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
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

// prewrite(start_ts, data_list)
func (m *Mvcc) Prewrite(mutations []pb.Mutation, start_ts uint64, primary_key []byte) (keyIsLockedErrors []error, err error) {
	var (
		result  []byte
		cf_data []pb.KeyValue
		cf_lock []pb.KeyValue
	)

	startTs := make([]byte, 8)
	binary.BigEndian.PutUint64(startTs, start_ts)

	// for keys in data_list, prewrite each key with start_ts in memory
	for _, mutation := range mutations {
		// get key's latest commit info write column with max_i64
		result = m.store.Get(CF_INFO, mutation.Key)

		if len(result) != 0 {
			last_commit_ts := binary.BigEndian.Uint64(result)
			// if commit_ts >= start_ts => return Error WriteConflict
			if last_commit_ts >= start_ts {
				err = ErrorWriteConflict
				return
			}
		}
		// get key's lock info
		result = m.store.Get(CF_LOCK, mutation.Key)
		// if lock exist
		if len(result) != 0 {
			lock_ts := binary.BigEndian.Uint64(result)
			// if lock_ts != start_ts => add one KeyIsLocked Error
			if lock_ts != start_ts {
				keyIsLockedErrors = append(keyIsLockedErrors, ErrorKeyIsLocked)
			}
		} else {

			cf_data = append(cf_data, pb.KeyValue{
				Key:   bytes.Join([][]byte{mutation.Key, startTs}, []byte("|")),
				Value: mutation.Value,
			})

			op := make([]byte, 4)
			binary.BigEndian.PutUint32(op, uint32(mutation.Op))

			cf_lock = append(cf_lock, pb.KeyValue{
				Key:   mutation.Key,
				Value: bytes.Join([][]byte{op, primary_key, startTs}, []byte("|")),
			})
		}
	}
	// if KeyIsLocked exist => return slice KeyIsLocked Error
	if len(keyIsLockedErrors) != 0 {
		err = ErrorKeyIsLocked
		return
	} else { // commit change, write into rocksdb column family
		for _, pair := range cf_data { // loop: write in cf_data

			m.store.Put(CF_DATA, pair.Key, pair.Value)
		}
		for _, pair := range cf_lock { // loop: write in cf_lock
			m.store.Put(CF_LOCK, pair.Key, pair.Value)
		}
	}
	return
}

// commit(keys, start_ts, commit_ts)
func (m *Mvcc) Commit(start_ts, commit_ts uint64, mutations []pb.Mutation) error {
	var (
		result   []byte
		cf_lock  []pb.KeyValue
		cf_write []pb.KeyValue
	)

	startTs := make([]byte, 8)
	binary.BigEndian.PutUint64(startTs, start_ts)

	commitTs := make([]byte, 8)
	binary.BigEndian.PutUint64(commitTs, commit_ts)

	// for each key in keys, do commit
	for _, mutation := range mutations {

		// get key's lock
		result = m.store.Get(CF_LOCK, mutation.Key)
		if len(result) != 0 { // if lock exist
			value := bytes.Split(result, []byte("|"))
			lock_ts := binary.BigEndian.Uint64(value[2])

			// if lock_ts == start_ts
			if lock_ts == start_ts {

				// get key's operator
				op := make([]byte, 4)
				binary.BigEndian.PutUint32(op, uint32(mutation.Op))

				// write memory:set write(commit_ts, lock_type, start_ts)
				cf_write = append(cf_write, pb.KeyValue{
					Key:   bytes.Join([][]byte{mutation.Key, commitTs}, []byte("|")),
					Value: bytes.Join([][]byte{op, startTs}, []byte("|")),
				})

				// write memory: current lock(key) will be removed and latest commit_ts will be recorded in cf_info
				cf_lock = append(cf_lock, pb.KeyValue{
					Key:   mutation.Key,
					Value: commitTs,
				})
			}
		} else { // lock not exist or txn dismatch
			// get(key, start_ts) from write
			result = m.store.Get(CF_WRITE, bytes.Join([][]byte{mutation.Key, commitTs}, []byte("|")))
			if len(result) != 0 { // if write exist
				write_type := pb.Op(binary.BigEndian.Uint32(bytes.Split(result, []byte("|"))[0]))
				if write_type != pb.Op_RBACK { // case 'P', 'D', 'L'
					// the txn is already committed
					return nil
				}
			}
			// write_type is Rollback or None
			return ErrorLockNotFound
		}
	}
	// commit change
	for _, pair := range cf_write {
		m.store.Put(CF_WRITE, pair.Key, pair.Value)
	}
	for _, pair := range cf_lock {
		m.store.Delete(CF_LOCK, pair.Key)
		// write key's latest commit
		m.store.Put(CF_INFO, pair.Key, pair.Value)
	}
	return nil
}

func (m *Mvcc) MvccGet(ts uint64, key []byte) (value []byte, err error) {

	return nil, nil
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
