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

	st := store.CreateStore("data0")
	defer st.Destroy()

	m := CreateMvcc(st, proposeC, confChangeC, 1, []string{"http://127.0.0.1:12379"}, false, "wal01")

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

	st := store.CreateStore("data0")
	defer st.Destroy()

	m := CreateMvcc(st, proposeC, confChangeC, 1, []string{"http://127.0.0.1:12379"}, false, "wal01")

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
	proposeC1 := make(chan []byte)
	defer close(proposeC1)
	confChangeC1 := make(chan raftpb.ConfChange)
	defer close(confChangeC1)

	proposeC2 := make(chan []byte)
	defer close(proposeC2)
	confChangeC2 := make(chan raftpb.ConfChange)
	defer close(confChangeC2)

	proposeC3 := make(chan []byte)
	defer close(proposeC3)
	confChangeC3 := make(chan raftpb.ConfChange)
	defer close(confChangeC3)

	st1 := store.CreateStore("data1")
	defer st1.Destroy()
	st2 := store.CreateStore("data2")
	defer st2.Destroy()
	st3 := store.CreateStore("data3")
	defer st3.Destroy()

	m1 := CreateMvcc(st1, proposeC1, confChangeC1, 1, []string{"http://127.0.0.1:12379", "http://127.0.0.1:12389", "http://127.0.0.1:12399"}, false, "wal01")
	m2 := CreateMvcc(st2, proposeC2, confChangeC2, 2, []string{"http://127.0.0.1:12379", "http://127.0.0.1:12389", "http://127.0.0.1:12399"}, false, "wal02")
	m3 := CreateMvcc(st3, proposeC3, confChangeC3, 3, []string{"http://127.0.0.1:12379", "http://127.0.0.1:12389", "http://127.0.0.1:12399"}, false, "wal03")

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

	_, errPrewrite := m1.Prewrite(mutations, 1, []byte("thoainh"))
	if errPrewrite != nil {
		log.Fatal(errPrewrite)
	}

	// wait for a moment for processing message, otherwise get would be failed.
	<-time.After(2 * time.Second)

	errCommit := m2.Commit(1, 2, mutations)
	if errCommit != nil {
		log.Fatal(errCommit)
	}

	// wait for a moment for processing message, otherwise get would be failed.
	<-time.After(2 * time.Second)

	value := m3.Get(4, []byte("thoainh"))

	if bytes.Compare(value, utils.Marshal(&pb.MvccObject{Value: []byte("Nguyen Huynh Thoai")})) != 0 {
		t.Errorf("Can't get expected value")
	}
}
