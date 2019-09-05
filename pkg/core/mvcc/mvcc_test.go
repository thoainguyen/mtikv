package mvcc

import (
	"bytes"
	"log"
	"testing"

	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
)

func TestPrewrite(t *testing.T) {
	m := CreateMvcc("volumes")
	defer m.Destroy()

	mutations := []pb.Mutation{
		pb.Mutation{
			Key:   []byte("thoainh"),
			Value: []byte("Nguyen Huynh Thoai"),
			Op:    pb.Op_PUT,
		},
	}

	_, errPrewrite := m.Prewrite(mutations, 1, []byte("thoainh"))
	if errPrewrite != nil {
		log.Fatal(errPrewrite)
	}

	data := m.GetStore().Get(0, m.Marshal(
		&pb.MvccObject{Key: []byte("thoainh"), StartTs: 1},
	))

	if bytes.Compare(data, []byte("Nguyen Huynh Thoai")) != 0 {
		t.Errorf("CF_DATA isn't writen")
	}

	lock := m.GetStore().Get(1, []byte("thoainh"))
	if bytes.Compare(lock, m.Marshal(&pb.MvccObject{
		Op: pb.Op_PUT, PrimaryKey: []byte("thoainh"), StartTs: 1})) != 0 {
		t.Errorf("CF_LOCK isn't writen")
	}
}

func TestCommit(t *testing.T) {
	m := CreateMvcc("volumes")
	defer m.Destroy()

	mutations := []pb.Mutation{
		pb.Mutation{
			Key:   []byte("thoainh"),
			Value: []byte("Nguyen Huynh Thoai"),
			Op:    pb.Op_PUT,
		},
	}

	_, errPrewrite := m.Prewrite(mutations, 1, []byte("thoainh"))
	if errPrewrite != nil {
		log.Fatal(errPrewrite)
	}

	errCommit := m.Commit(1, 2, mutations)
	if errCommit != nil {
		log.Fatal(errCommit)
	}

	write := m.GetStore().Get(2, m.Marshal(&pb.MvccObject{Key: []byte("thoainh"), CommitTs: 2}))

	if bytes.Compare(write, m.Marshal(&pb.MvccObject{Op: pb.Op_PUT, StartTs: 1})) != 0 {
		t.Errorf("CF_WRITE isn't writen")
	}

	lock := m.GetStore().Get(1, []byte("thoainh"))
	if bytes.Compare(lock, []byte(nil)) != 0 {
		t.Errorf("CF_LOCK is still reserved")
	}

	info := m.GetStore().Get(3, []byte("thoainh"))

	if bytes.Compare(info, m.Marshal(&pb.MvccObject{LatestCommit: 2})) != 0 {
		t.Errorf("Latest commit in CF_INFO isn't correct")
	}
}

func TestGet(t *testing.T) {
	m := CreateMvcc("volumes")
	defer m.Destroy()

	mutations := []pb.Mutation{
		pb.Mutation{
			Key:   []byte("thoainh"),
			Value: []byte("Nguyen Huynh Thoai"),
			Op:    pb.Op_PUT,
		},
		pb.Mutation{
			Key:   []byte("thuyenpt"),
			Value: []byte("Phan Trong Thuyen"),
			Op:    pb.Op_PUT,
		},
	}

	_, errPrewrite := m.Prewrite(mutations, 1, []byte("thoainh"))
	if errPrewrite != nil {
		log.Fatal(errPrewrite)
	}

	errCommit := m.Commit(1, 2, mutations)
	if errCommit != nil {
		log.Fatal(errCommit)
	}

	value, errGet := m.Get(4, []byte("thoainh"))
	if errGet != nil {
		log.Fatal(errGet)
	}

	if bytes.Compare(value, []byte("Nguyen Huynh Thoai")) != 0 {
		t.Errorf("Can't get expected value")
	}
}
