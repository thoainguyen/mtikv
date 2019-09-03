package mvcc

import (
	"bytes"
	"encoding/binary"
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

	startTs := make([]byte, 8)
	binary.BigEndian.PutUint64(startTs, 1)

	op := make([]byte, 4)
	binary.BigEndian.PutUint32(op, uint32(pb.Op_PUT))

	_, errPrewrite := m.Prewrite(mutations, 1, []byte("thoainh"))
	if errPrewrite != nil {
		log.Fatal(errPrewrite)
	}

	data := m.GetStore().Get(0, bytes.Join([][]byte{[]byte("thoainh"), startTs}, []byte("|")))
	if bytes.Compare(data, []byte("Nguyen Huynh Thoai")) != 0 {
		t.Errorf("CF_DATA isn't writen")
	}

	lock := m.GetStore().Get(1, []byte("thoainh"))
	if bytes.Compare(lock, bytes.Join([][]byte{op, []byte("thoainh"), startTs}, []byte("|"))) != 0 {
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

	startTs := make([]byte, 8)
	binary.BigEndian.PutUint64(startTs, 1)

	commitTs := make([]byte, 8)
	binary.BigEndian.PutUint64(commitTs, 2)

	op := make([]byte, 4)
	binary.BigEndian.PutUint32(op, uint32(pb.Op_PUT))

	_, errPrewrite := m.Prewrite(mutations, 1, []byte("thoainh"))
	if errPrewrite != nil {
		log.Fatal(errPrewrite)
	}

	errCommit := m.Commit(1, 2, mutations)
	if errCommit != nil {
		log.Fatal(errCommit)
	}

	write := m.GetStore().Get(2, bytes.Join([][]byte{[]byte("thoainh"), commitTs}, []byte("|")))
	if bytes.Compare(write, bytes.Join([][]byte{op, startTs}, []byte("|"))) != 0 {
		t.Errorf("CF_WRITE isn't writen")
	}

	lock := m.GetStore().Get(1, []byte("thoainh"))
	if bytes.Compare(lock, []byte(nil)) != 0 {
		t.Errorf("CF_LOCK is still reserved")
	}

	info := m.GetStore().Get(3, []byte("thoainh"))
	if bytes.Compare(info, commitTs) != 0 {
		t.Errorf("Latest commit in CF_INFO isn't correct")
	}
}
