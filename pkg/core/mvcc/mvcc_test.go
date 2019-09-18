package mvcc

import (
	"bytes"
	"log"
	"testing"
	"time"

	"github.com/thoainguyen/mtikv/pkg/core/store"
	"github.com/thoainguyen/mtikv/pkg/core/utils"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
	"go.etcd.io/etcd/raft/raftpb"
)

func TestPrewrite(t *testing.T) {
	proposeC := make(chan []byte)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	st := store.CreateStore("data")
	defer st.Destroy()

	m := CreateMvcc(st, proposeC, confChangeC, 1, []string{"http://127.0.0.1:12379"}, false)

	mutations := []*pb.MvccObject{
		{
			Key:        []byte("thoainh"),
			PrimaryKey: []byte("thoainh"),
			StartTs:    1,
			Value:      []byte("Nguyen Huynh Thoai"),
			Op:         pb.Op_PUT,
		},
	}

	_, errPrewrite := m.Prewrite(mutations, 1, []byte("thoainh"))
	if errPrewrite != nil {
		log.Fatal(errPrewrite)
	}

	// wait for a moment for processing message, otherwise get would be failed.
	<-time.After(2 * time.Second)

	data := m.GetStore().Get(0, utils.Marshal(
		&pb.MvccObject{Key: []byte("thoainh"), StartTs: 1},
	))

	if bytes.Compare(data, utils.Marshal(&pb.MvccObject{Value: []byte("Nguyen Huynh Thoai")})) != 0 {
		t.Errorf("CF_DATA is incorrect")
	}

	lock := m.GetStore().Get(1, utils.Marshal(&pb.MvccObject{Key: []byte("thoainh")}))
	if bytes.Compare(lock, utils.Marshal(&pb.MvccObject{
		Op: pb.Op_PUT, PrimaryKey: []byte("thoainh"), StartTs: 1})) != 0 {
		t.Errorf("CF_LOCK is incorrect")
	}
}

func TestCommit(t *testing.T) {
	proposeC := make(chan []byte)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	st := store.CreateStore("data")
	defer st.Destroy()

	m := CreateMvcc(st, proposeC, confChangeC, 1, []string{"http://127.0.0.1:12379"}, false)

	mutations := []*pb.MvccObject{
		{
			Key:        []byte("thoainh"),
			PrimaryKey: []byte("thoainh"),
			StartTs:    1,
			CommitTs:   2,
			Value:      []byte("Nguyen Huynh Thoai"),
			Op:         pb.Op_PUT,
		},
	}

	_, errPrewrite := m.Prewrite(mutations, 1, []byte("thoainh"))
	if errPrewrite != nil {
		log.Fatal(errPrewrite)
	}

	// wait for a moment for processing message, otherwise get would be failed.
	<-time.After(2 * time.Second)

	errCommit := m.Commit(1, 2, mutations)
	if errCommit != nil {
		log.Fatal(errCommit)
	}

	// wait for a moment for processing message, otherwise get would be failed.
	<-time.After(1 * time.Second)

	write := m.GetStore().Get(CF_WRITE, utils.Marshal(&pb.MvccObject{Key: []byte("thoainh"), CommitTs: 2}))

	if bytes.Compare(write, utils.Marshal(&pb.MvccObject{Op: pb.Op_PUT, StartTs: 1})) != 0 {
		t.Errorf("CF_WRITE isn't writen")
	}

	lock := m.GetStore().Get(CF_LOCK, utils.Marshal(&pb.MvccObject{Key: []byte("thoainh")}))
	if bytes.Compare(lock, []byte(nil)) != 0 {
		t.Errorf("CF_LOCK is still reserved")
	}

	info := m.GetStore().Get(CF_INFO, utils.Marshal(&pb.MvccObject{Key: []byte("thoainh")}))

	if bytes.Compare(info, utils.Marshal(&pb.MvccObject{LatestCommit: 2})) != 0 {
		t.Errorf("Latest commit in CF_INFO isn't correct")
	}

}

func TestGet(t *testing.T) {
	proposeC := make(chan []byte)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	st := store.CreateStore("data")
	defer st.Destroy()

	m := CreateMvcc(st, proposeC, confChangeC, 1, []string{"http://127.0.0.1:12379"}, false)

	mutations := []*pb.MvccObject{
		{
			Key:        []byte("thoainh"),
			PrimaryKey: []byte("thoainh"),
			StartTs:    1,
			CommitTs:   2,
			Value:      []byte("Nguyen Huynh Thoai"),
			Op:         pb.Op_PUT,
		},
		{
			Key:        []byte("thuyenpt"),
			PrimaryKey: []byte("thoainh"),
			StartTs:    1,
			CommitTs:   2,
			Value:      []byte("Phan Trong Thuyen"),
			Op:         pb.Op_PUT,
		},
	}

	_, errPrewrite := m.Prewrite(mutations, 1, []byte("thoainh"))
	if errPrewrite != nil {
		log.Fatal(errPrewrite)
	}

	// wait for a moment for processing message, otherwise get would be failed.
	<-time.After(time.Second)

	errCommit := m.Commit(1, 2, mutations)
	if errCommit != nil {
		log.Fatal(errCommit)
	}

	// wait for a moment for processing message, otherwise get would be failed.
	<-time.After(time.Second)

	value := m.Get(4, []byte("thoainh"))

	if bytes.Compare(value, utils.Marshal(&pb.MvccObject{Value: []byte("Nguyen Huynh Thoai")})) != 0 {
		t.Errorf("Can't get expected value")
	}
}
