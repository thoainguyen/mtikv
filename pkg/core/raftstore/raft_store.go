package raftstore

import (
	"log"
	"strings"
	"sync"

	"github.com/thoainguyen/mtikv/pkg/core/store"
	"github.com/thoainguyen/mtikv/pkg/core/utils"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
	"go.etcd.io/etcd/raft/raftpb"
)

type RaftStore struct {
	store       *store.Store
	mu          sync.RWMutex
	confChangeC chan<- raftpb.ConfChange
	proposeC    chan<- []byte
}

func CreateRaftStore(store *store.Store, proposeC chan []byte, confChangeC chan raftpb.ConfChange,
	id int, cluster string, join bool) *RaftStore {

	commitC, errorC := NewRaftNode(id, strings.Split(cluster, ","), join, proposeC, confChangeC, store.GetDir())

	rs := &RaftStore{
		store:       store,
		confChangeC: confChangeC,
		proposeC:    proposeC,
	}

	go func(rs *RaftStore, commitC <-chan *[]byte, errorC <-chan error) {
		for data := range commitC {
			if data == nil {
				continue
			}
			mut := &pb.Mutation{}
			utils.Unmarshal(*data, mut)
			rs.mu.Lock()

			if mut.Op == pb.Op_PUT {
				rs.store.Put(int(mut.GetCf()), mut.GetKey(), mut.GetValue())
			} else { // if mut.Op == pb.Op_DEL {
				rs.store.Delete(int(mut.GetCf()), mut.GetKey())
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

// write batch here ProposeC <- Put + Delete
func (rs *RaftStore) Put(cf int, key, value []byte) {
	rs.proposeC <- utils.Marshal(&pb.Mutation{Cf: int32(cf), Op: pb.Op_PUT, Key: key, Value: value})
}

func (rs *RaftStore) Delete(cf int, key []byte) {
	rs.proposeC <- utils.Marshal(&pb.Mutation{Cf: int32(cf), Op: pb.Op_DEL, Key: key})
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
