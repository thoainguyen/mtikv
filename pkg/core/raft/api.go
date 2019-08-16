package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"

	db "github.com/thoainguyen/mtikv/pkg/core/db"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
)

type KeyValuePair struct {
	Key   string
	Value string
}

// Handler for a api based key-value store backed by raft
type RaftLayer struct {
	kvStore     *db.DB
	mu          sync.RWMutex
	confChangeC chan<- raftpb.ConfChange
	proposeC    chan<- string
	snapshotter *snap.Snapshotter
}

func NewRaftApiMTikv(snapshotter *snap.Snapshotter, kv *db.DB, proposeC chan<- string, commitC <-chan *string,
	confChangeC chan<- raftpb.ConfChange, errorC <-chan error) *RaftLayer {
	s := &RaftLayer{
		kvStore:     kv,
		confChangeC: confChangeC,
		proposeC:    proposeC,
		snapshotter: snapshotter,
	}
	s.rReadCommits(commitC, errorC)
	go s.rReadCommits(commitC, errorC)
	return s
}

func (raftLayer *RaftLayer) rReadCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := raftLayer.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := raftLayer.RecoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var dataKv KeyValuePair
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raft -> could not decode message (%v)", err)
		}
		raftLayer.mu.Lock()
		err := raftLayer.kvStore.PutData(dataKv.Key, dataKv.Value)
		if err != nil {
			log.Fatal(err)
		}
		raftLayer.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (raftLayer *RaftLayer) PutData(key string, value string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(KeyValuePair{key, value}); err != nil {
		log.Fatal(err)
	}
	raftLayer.proposeC <- buf.String()
	return nil
}

func (raftLayer *RaftLayer) GetData(key string) (string, error) {
	raftLayer.mu.RLock()
	defer raftLayer.mu.RUnlock()
	return raftLayer.kvStore.GetData(key)
}

func (raftLayer *RaftLayer) DeleteData(key string) error {
	return nil
}

func (raftLayer *RaftLayer) AddNode(nodeId uint64, url string) {
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeId,
		Context: []byte(url),
	}
	raftLayer.confChangeC <- cc
}

func (raftLayer *RaftLayer) RemoveNode(nodeId uint64) {
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeId,
	}
	raftLayer.confChangeC <- cc
}

func (raftLayer *RaftLayer) GetSnapshot() ([]byte, error) {
	raftLayer.mu.RLock()
	defer raftLayer.mu.RUnlock()
	return []byte(raftLayer.kvStore.SaveSnapShot()), nil
}

func (raftLayer *RaftLayer) RecoverFromSnapshot(snapshot []byte) error {
	raftLayer.mu.Lock()
	defer raftLayer.mu.Unlock()
	raftLayer.kvStore.LoadSnapShot(string(snapshot))
	return nil
}
