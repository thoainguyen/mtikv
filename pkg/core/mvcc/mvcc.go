package mvcc

import (
	"errors"
	"fmt"

	"github.com/thoainguyen/mtikv/pkg/core/raftstore"
	"github.com/thoainguyen/mtikv/pkg/core/store"
	"github.com/thoainguyen/mtikv/pkg/core/utils"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
	"go.etcd.io/etcd/raft/raftpb"
)

const (
	CF_DATA = iota
	CF_LOCK
	CF_WRITE
	CF_INFO
)

type Storage interface {
	Get(cf int, key []byte) []byte
	Put(cf int, key, value []byte)
	Delete(cf int, key []byte)
	PrewriteBatch(data *pb.MvccObject)
	CommitBatch(data *pb.MvccObject)
	Destroy()
}

type Mvcc struct {
	store          Storage
	kDefaultPathDB string
}

var (
	ErrorWriteConflict = errors.New("ErrorWriteConflict")
	ErrorKeyIsLocked   = errors.New("ErrorKeyIsLocked")
	ErrorLockNotFound  = errors.New("ErrorLockNotFound")
)

func (m *Mvcc) GetStore() Storage {
	return m.store
}

func CreateMvcc(path string, proposeC chan []byte, confChangeC chan raftpb.ConfChange, id int, cluster string, join bool) *Mvcc {

	path = fmt.Sprintf("%s-%02d", path, id)

	st := store.CreateStore(path)
	sr := raftstore.CreateRaftStore(st, proposeC, confChangeC, id, cluster, join)
	return &Mvcc{
		store:          sr,
		kDefaultPathDB: path,
	}
}

func CreateMvccV1(path string) *Mvcc {
	return &Mvcc{
		store:          store.CreateStore(path),
		kDefaultPathDB: path,
	}
}

func (m *Mvcc) Destroy() {
	m.store.Destroy()
}

// prewrite(start_ts, data_list)
func (m *Mvcc) Prewrite(mutations []*pb.MvccObject, start_ts uint64, primary_key []byte) (keyIsLockedErrors []error, err error) {
	var (
		result       []byte
		prewriteList = mutations[:0]
	)

	// for keys in data_list, prewrite each key with start_ts in memory
	for _, mutation := range mutations {
		// get key's latest commit info write column with max_i64
		result = m.store.Get(CF_INFO, utils.Marshal(&pb.MvccObject{Key: mutation.GetKey()}))

		if len(result) != 0 {
			info := &pb.MvccObject{}
			utils.Unmarshal(result, info)

			// if commit_ts >= start_ts => return Error WriteConflict
			if info.GetLatestCommit() >= start_ts {
				err = ErrorWriteConflict
				return
			}
		}
		// get key's lock info
		result = m.store.Get(CF_LOCK, utils.Marshal(&pb.MvccObject{Key: mutation.GetKey()}))
		// if lock exist
		if len(result) != 0 {
			lock := &pb.MvccObject{}
			utils.Unmarshal(result, lock)

			// if lock_ts != start_ts => add one KeyIsLocked Error
			if lock.GetStartTs() != start_ts {
				keyIsLockedErrors = append(keyIsLockedErrors, ErrorKeyIsLocked)
			}
		} else {
			// TODO:...write in buffer
			prewriteList = append(prewriteList, mutation)
		}
	}
	// if KeyIsLocked exist => return slice KeyIsLocked Error
	if len(keyIsLockedErrors) != 0 {
		err = ErrorKeyIsLocked
		return
	} else { // commit change, write into rocksdb column family
		for _, mutation := range prewriteList {
			m.store.PrewriteBatch(mutation)
		}
	}
	return
}

// commit(keys, start_ts, commit_ts)
func (m *Mvcc) Commit(start_ts, commit_ts uint64, mutations []*pb.MvccObject) error {
	var (
		result     []byte
		commitList = mutations[:0]
	)

	// for each key in keys, do commit
	for _, mutation := range mutations {

		// get key's lock
		result = m.store.Get(CF_LOCK, utils.Marshal(&pb.MvccObject{Key: mutation.GetKey()}))
		if len(result) != 0 { // if lock exist
			lock := &pb.MvccObject{}
			utils.Unmarshal(result, lock)

			// if lock_ts == start_ts
			if lock.GetStartTs() == start_ts {
				// TODO: ...write buffer
				commitList = append(commitList, mutation)
			}
		} else { // lock not exist or txn dismatch
			// get(key, start_ts) from write
			result = m.store.Get(CF_WRITE, utils.Marshal(&pb.MvccObject{Key: mutation.Key, CommitTs: commit_ts}))
			if len(result) != 0 { // if write exist

				write := &pb.MvccObject{}
				utils.Unmarshal(result, write)

				if write.GetOp() != pb.Op_RBACK { // case 'P', 'D', 'L'
					// the txn is already committed
					return nil
				}
			}
			// write_type is Rollback or None
			return ErrorLockNotFound
		}
	}

	for _, mutation := range commitList {
		m.store.CommitBatch(mutation)
	}

	return nil
}

func (m *Mvcc) Get(start_ts uint64, key []byte) ([]byte, error) {

	var (
		keyGet   []byte
		valueGet []byte
		write    = &pb.MvccObject{}
		counter  = start_ts
	)

	for counter >= 0 {
		// TODO : check lock error
		keyGet = utils.Marshal(&pb.MvccObject{Key: key, CommitTs: counter})
		valueGet = m.store.Get(CF_WRITE, keyGet)
		if len(valueGet) == 0 {
			counter -= 1
			continue
		}
		utils.Unmarshal(valueGet, write)
		if write.GetOp() == pb.Op_DEL {
			return nil, nil
		}
		if write.GetOp() == pb.Op_RBACK {
			counter -= 1
			continue
		}
		return m.store.Get(CF_DATA, utils.Marshal(&pb.MvccObject{Key: key, StartTs: write.GetStartTs()})), nil
	}
	return nil, nil
}
