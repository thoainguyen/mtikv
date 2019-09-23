package raftstore

import (
	"log"
	"sync"

	"github.com/thoainguyen/mtikv/pkg/core/store"
	"github.com/thoainguyen/mtikv/pkg/core/utils"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
	"go.etcd.io/etcd/raft/raftpb"
)

type RaftStore struct {
	store       *store.Store
	mu          sync.RWMutex
	raftID      int
	confChangeC chan<- raftpb.ConfChange
	proposeC    chan<- []byte
}

func CreateRaftStore(store *store.Store, proposeC chan []byte, confChangeC chan raftpb.ConfChange,
	id int, cluster []string, join bool, waldir string) *RaftStore {

	commitC, errorC := NewRaftNode(id, cluster, join, proposeC, confChangeC, waldir)

	rs := &RaftStore{
		store:       store,
		raftID:      id,
		confChangeC: confChangeC,
		proposeC:    proposeC,
	}

	go func(rs *RaftStore, commitC <-chan *[]byte, errorC <-chan error) {
		for data := range commitC {
			if data == nil {
				continue
			}
			mut := &pb.MvccObject{}
			utils.Unmarshal(*data, mut)

			rs.mu.Lock()

			if mut.MvccOp == pb.MvccOp_PRWITE {
				rs.store.PrewriteBatch(mut)
			} else if mut.MvccOp == pb.MvccOp_COMMIT {
				rs.store.CommitBatch(mut)
			} else if mut.MvccOp == pb.MvccOp_RAWPUT {
				rs.store.RawPutBatch(mut)
			}

			rs.mu.Unlock()
		}
		if err, ok := <-errorC; ok {
			log.Fatal(err)
		}
	}(rs, commitC, errorC)

	return rs
}

func (rs *RaftStore) Get(cf int, key []byte) []byte {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.store.Get(cf, key)
}

func (rs *RaftStore) Put(cf int, key, value []byte) {
	rs.proposeC <- utils.Marshal(&pb.MvccObject{Cf: int32(cf), Op: pb.Op_PUT, Key: key, Value: value})
}

// write batch here ProposeC <- Put + Delete
func (rs *RaftStore) PrewriteBatch(data *pb.MvccObject) {
	data.MvccOp = pb.MvccOp_PRWITE
	rs.proposeC <- utils.Marshal(data)
}

func (rs *RaftStore) CommitBatch(data *pb.MvccObject) {
	data.MvccOp = pb.MvccOp_COMMIT
	rs.proposeC <- utils.Marshal(data)
}

func (rs *RaftStore) RawPutBatch(data *pb.MvccObject) {
	data.MvccOp = pb.MvccOp_RAWPUT
	rs.proposeC <- utils.Marshal(data)
}

func (rs *RaftStore) Delete(cf int, key []byte) {
	rs.proposeC <- utils.Marshal(&pb.MvccObject{Cf: int32(cf), Op: pb.Op_DEL, Key: key})
}

func (rs *RaftStore) AddNode(nodeId uint64, url string) {
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeId,
		Context: []byte(url),
	}
	rs.confChangeC <- cc
}

func (rs *RaftStore) RemoveNode(nodeId uint64) {
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeId,
	}
	rs.confChangeC <- cc
}

func (rs *RaftStore) Destroy() {
	rs.store.Destroy()
}
