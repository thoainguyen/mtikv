package raftstore

import (
	"io/ioutil"
	"log"
	"bytes"
	"github.com/thoainguyen/mtikv/pkg/core/storage"
	"net/http"
	"strconv"
	"encoding/json"
	"sync"
	"encoding/gob"
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
}

func NewRaftApiMTikv(kv *db.DB, proposeC chan<- string, commitC <- chan *string,
	confChangeC chan<- raftpb.ConfChange, errorC <-chan error) (*RaftLayer) {
	s := &RaftLayer{
		kvStore: kv,
		confChangeC: confChangeC,
		proposeC: proposeC,
	}
	s.rReadCommits(commitC, errorC)
	go s.rReadCommits(commitC, errorC)
	return s
}

func (raftLayer *RaftLayer) rReadCommits(commitC <- chan *string, errorC <- chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.rRecoverFromSnapshot(snapshot.Data); err != nil {
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
		err := raftLayer.kvStore.PutData(dataKv.Key, dataKv.Val)
		if err != nil {
			log.Fatal(err)
		}
		raftLayer.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (raftLayer *RaftLayer) rPutData(key string, value string) error {
	var buf bytes.Buffer()
	if err := gob.NewEncoder(&buf).Encode(KeyValuePair{key, value}); err == nil {
		s.proposeC <- buf.String()
	}
	return err
}

func (raftLayer *RaftLayer) rGetData(key string) (string, error) {
	raftLayer.mu.RLock()
	defer raftLayer.mu.RUnlock()
	return raftLayer.kvStore.GetData(key)
}

func (raftLayer *RaftLayer) rDeleteData(key string) error {
	return nil
}

func (raftLayer *RaftLayer) rAddNode(nodeId, url string) {
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  nodeId,
		Context: url,
	}
	raftLayer.confChangeC <- cc
}

func (raftLayer *RaftLayer) rRemoveNode(nodeId string) {
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: nodeId,
	}
	raftLayer.confChangeC <- cc
}

func (raftLayer *RaftLayer) rGetSnapshot() ([]byte, error) {
	raftLayer.mu.RLock()
	defer raftLayer.mu.RUnlock()
	return []byte(raftLayer.kvStore.SaveSnapShot()), nil
}

func (raftLayer *RaftLayer) rRecoverFromSnapshot(snapshot []byte) error {
	raftLayer.mu.Lock()
	defer raftLayer.mu.Unlock()
	raftLayer.kvStore.LoadSnapshot(string(snapshot))
	return nil
}