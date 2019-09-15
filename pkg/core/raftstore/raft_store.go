package raftstore

import (

	"github.com/thoainguyen/mtikv/pkg/core/store"
	"github.com/thoainguyen/mtikv/pkg/core/utils"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
	"go.etcd.io/etcd/raft/raftpb"
	"log"
	"strings"
	"sync"
)

type RaftStore struct {
	store *store.Store
	rnode *RaftNode
	mu     sync.RWMutex
	confChangeC chan<- raftpb.ConfChange
	proposeC    chan<- []byte
}


func CreateRaftStore(store *store.Store, id *int, cluster *string, join *bool) *RaftStore {

	proposeC := make(chan []byte)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	rnode, commitC, errorC := NewRaftNode(*id, strings.Split(*cluster, ","), *join, proposeC, confChangeC)

	rs := &RaftStore{
		store: store,
		rnode: rnode,
		confChangeC: confChangeC,
		proposeC: proposeC,
	}

	go func(rs *RaftStore, commitC <-chan *[]byte, errorC <-chan error){
		for data := range commitC {
			if data == nil {
				continue
			}
			mut := &pb.Mutation{}
			utils.Unmarshal(*data, mut)

			rs.mu.Lock()
			if mut.Op == pb.Op_PUT {
				rs.store.Put(int(mut.Cf), mut.Key, mut.Value)
			}
			if mut.Op == pb.Op_DEL {
				rs.store.Delete(int(mut.Cf), mut.Key)
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

func (rs *RaftStore) Destroy(){
	rs.store.Destroy()
	rs.rnode.stop()
}

