package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/thoainguyen/mtikv/config"
	clipb "github.com/thoainguyen/mtikv/proto/mtikv_clipb"
	pb "github.com/thoainguyen/mtikv/proto/mtikvpb"
	pdpb "github.com/thoainguyen/mtikv/proto/pdpb"

	"log"

	"google.golang.org/grpc"
)

type mtikvCli struct {
	transID    uint64
	mu         sync.Mutex
	buffer     cmap.ConcurrentMap
	raftGroups cmap.ConcurrentMap
	pdCli      pdpb.PDClient
	tsoRecvC   chan uint64
	tsoSendC   chan struct{}
}

type raftGroup struct {
	raftID string
	from   []byte
	to     []byte
	addr   []string
}

type kvBuffer struct {
	startTs uint64
	data    map[string][]byte
}

func (kv *kvBuffer) Set(key, value []byte) {
	kv.data[string(key)] = value
}

func (kv *kvBuffer) Get(key []byte) []byte {
	return kv.data[string(key)]
}

func (kv *kvBuffer) Delete(key []byte) {
	delete(kv.data, string(key))
}

func (kv *kvBuffer) GetStartTs() uint64 {
	return kv.startTs
}

func (cli *mtikvCli) getTransID() string {
	return strconv.FormatUint(cli.transID, 10)
}

func (cli *mtikvCli) getRaftIDByKey(key []byte) (string, bool) {
	rGroups := cli.raftGroups.Items()
	for u, v := range rGroups {
		if betweenRightIncl(key, v.(raftGroup).from, v.(raftGroup).to) {
			return u, true
		}
	}
	return "", false
}

func (cli *mtikvCli) BeginTxn(ctx context.Context, in *clipb.BeginTxnRequest) (*clipb.BeginTxnResponse, error) {
	cli.mu.Lock()
	defer cli.mu.Unlock()

	cli.transID++

	tid := cli.getTransID()

	cli.buffer.Set(tid, kvBuffer{
		startTs: cli.getTimeStamp(),
		data:    make(map[string][]byte),
	})

	return &clipb.BeginTxnResponse{TransID: cli.transID}, nil
}

func (cli *mtikvCli) CommitTxn(ctx context.Context, in *clipb.CommitTxnRequest) (*clipb.CommitTxnResponse, error) {

	tid := strconv.FormatUint(in.GetTransID(), 10)
	buf, ok := cli.buffer.Get(tid)
	if !ok {
		return &clipb.CommitTxnResponse{Error: clipb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	c := buf.(kvBuffer)

	twoPhaseData, primaryKey := cli.prepareTwoPhaseData(tid)
	if primaryKey == nil {
		return &clipb.CommitTxnResponse{Error: clipb.Error_SUCCESS, TransID: in.GetTransID()}, nil
	}

	// TODO: parallel prewrite
	for rGroupID, cmData := range twoPhaseData {
		ok := cli.runPrewrite(c.GetStartTs(), primaryKey, rGroupID, cmData)
		if !ok {
			return &clipb.CommitTxnResponse{Error: clipb.Error_FAILED}, nil
		}
	}

	commitTs := cli.getTimeStamp()

	// TODO: parallel commit
	for rGroupID, cmData := range twoPhaseData {

		for idx := range cmData {
			cmData[idx] = &pb.MvccObject{
				Key: cmData[idx].GetKey(),
				Op:  cmData[idx].GetOp(),
			}
		}

		ok := cli.runCommit(c.GetStartTs(), commitTs, primaryKey, rGroupID, cmData)
		if !ok {
			return &clipb.CommitTxnResponse{Error: clipb.Error_FAILED}, nil
		}
	}

	return &clipb.CommitTxnResponse{TransID: in.GetTransID(), Error: clipb.Error_SUCCESS}, nil
}

func (cli *mtikvCli) tryToConnect(rid string) (*grpc.ClientConn, error) {

	var (
		conn *grpc.ClientConn
		err  error
	)
	rg, ok := cli.raftGroups.Get(rid)

	if ok {
		rGroup := rg.(raftGroup)
		for i := range rGroup.addr {
			conn, err = grpc.Dial(rGroup.addr[i], grpc.WithInsecure())
			if err == nil {
				return conn, err
			}
		}
	}

	return conn, err
}

func (cli *mtikvCli) runCommit(startTs, commitTs uint64, primaryKey []byte, rGroupID string, cmData []*pb.MvccObject) bool {

	conn, err := cli.tryToConnect(rGroupID)
	if err != nil {
		return false
	}
	defer conn.Close()

	mtikvCli := pb.NewMTikvClient(conn)

	ctx := context.TODO()

	commitResult, _ := mtikvCli.Commit(ctx, &pb.CommitRequest{
		Context: &pb.Context{
			ClusterId: rGroupID,
		},
		Keys:          cmData,
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})

	if commitResult.GetError() != pb.Error_ErrOk {
		return false
	}

	return true
}

func (cli *mtikvCli) runPrewrite(startTs uint64, primaryKey []byte, rGroupID string, cmData []*pb.MvccObject) bool {

	conn, err := cli.tryToConnect(rGroupID)
	if err != nil {
		return false
	}
	defer conn.Close()

	mtikvCli := pb.NewMTikvClient(conn)

	ctx := context.TODO()

	prewriteResult, _ := mtikvCli.Prewrite(ctx, &pb.PrewriteRequest{
		Context: &pb.Context{
			ClusterId: rGroupID,
		},
		Mutation:     cmData,
		PrimaryLock:  primaryKey,
		StartVersion: startTs,
	})

	if prewriteResult.GetError() != pb.Error_ErrOk {
		return false
	}
	return true
}

func (cli *mtikvCli) getCluster(clusID string) (raftGroup, bool) {
	clus, ok := cli.raftGroups.Get(clusID)
	if !ok {
		return raftGroup{}, false
	}
	return clus.(raftGroup), true
}

func (cli *mtikvCli) Set(ctx context.Context, in *clipb.SetRequest) (*clipb.SetResponse, error) {
	tid := strconv.FormatUint(in.GetTransID(), 10)
	if tid == "0" { // RawPut(Op = Op_PUT)
		err := cli.putToMtikv(in.GetKey(), in.GetValue(), cli.getTimeStamp())
		switch err {
		case pb.Error_KeyNotInRegion, pb.Error_RegionNotFound:
			return &clipb.SetResponse{TransID: 0, Error: clipb.Error_INVALID}, nil
		case pb.Error_ErrOk:
			return &clipb.SetResponse{TransID: 0, Error: clipb.Error_SUCCESS}, nil
		default:
			return &clipb.SetResponse{TransID: 0, Error: clipb.Error_FAILED}, nil
		}
	}

	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &clipb.SetResponse{TransID: in.GetTransID(), Error: clipb.Error_INVALID}, nil
	}
	c := reg.(kvBuffer)
	c.Set(in.GetKey(), in.GetValue())
	return &clipb.SetResponse{TransID: in.GetTransID(), Error: clipb.Error_SUCCESS}, nil
}

func (cli *mtikvCli) Get(ctx context.Context, in *clipb.GetRequest) (*clipb.GetResponse, error) {
	tid := strconv.FormatUint(in.GetTransID(), 10)

	if tid == "0" { // Get(key, current_version)
		return &clipb.GetResponse{Value: cli.getFromMtikv(in.GetKey(), cli.getTimeStamp()),
			Error: clipb.Error_SUCCESS, TransID: 0}, nil
	}

	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &clipb.GetResponse{Error: clipb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	c := reg.(kvBuffer)
	val := c.Get(in.GetKey())

	if bytes.Compare(val, []byte(nil)) == 0 {
		val = cli.getFromMtikv(in.GetKey(), c.GetStartTs())
	}
	return &clipb.GetResponse{Value: val, Error: clipb.Error_SUCCESS, TransID: in.GetTransID()}, nil
}

func (cli *mtikvCli) putToMtikv(key, value []byte, version uint64) pb.Error {

	rid, ok := cli.getRaftIDByKey(key)
	if !ok {
		return pb.Error_RegionNotFound
	}

	conn, err := cli.tryToConnect(rid)
	if err != nil {
		return pb.Error_RegionNotFound
	}
	defer conn.Close()

	mtikvCli := pb.NewMTikvClient(conn)

	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()

	result, err := mtikvCli.RawPut(ctx, &pb.RawPutRequest{
		Context: &pb.Context{
			ClusterId: rid,
		},
		Key: key, Value: value, Version: version,
	})

	if err != nil {
		log.Println(err)
	}

	return result.GetError()
}

func (cli *mtikvCli) deleteToMtikv(key []byte, version uint64) pb.Error {

	rid, ok := cli.getRaftIDByKey(key)
	if !ok {
		return pb.Error_RegionNotFound
	}

	conn, err := cli.tryToConnect(rid)
	if err != nil {
		return pb.Error_RegionNotFound
	}
	defer conn.Close()

	mtikvCli := pb.NewMTikvClient(conn)

	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()

	result, err := mtikvCli.RawDelete(ctx, &pb.RawDeleteRequest{
		Context: &pb.Context{
			ClusterId: rid,
		},
		Key: key, Version: version,
	})

	if err != nil {
		log.Println(err)
	}

	return result.GetError()
}

func (cli *mtikvCli) getFromMtikv(key []byte, version uint64) []byte {

	rid, ok := cli.getRaftIDByKey(key)
	if !ok {
		return nil
	}

	conn, err := cli.tryToConnect(rid)
	if err != nil {
		return nil
	}
	defer conn.Close()

	mtikvCli := pb.NewMTikvClient(conn)

	ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
	defer cancel()

	result, err := mtikvCli.Get(ctx, &pb.GetRequest{
		Context: &pb.Context{
			ClusterId: rid,
		},
		Version: version,
		Key:     key,
	})

	if err != nil {
		log.Println(err)
	}

	if result.GetError() == pb.Error_ErrOk {
		return result.GetValue()
	}

	return nil

}

func (cli *mtikvCli) RollBackTxn(ctx context.Context, in *clipb.RollBackTxnRequest) (*clipb.RollBackTxnResponse, error) {
	tid := strconv.FormatUint(in.GetTransID(), 10)
	cli.buffer.Remove(tid)
	return &clipb.RollBackTxnResponse{TransID: in.GetTransID()}, nil
}

func (cli *mtikvCli) Delete(ctx context.Context, in *clipb.DeleteRequest) (*clipb.DeleteResponse, error) {

	tid := strconv.FormatUint(in.GetTransID(), 10)

	if tid == "0" { // RawPut(Op = Op_DEL)
		err := cli.deleteToMtikv(in.GetKey(), cli.getTimeStamp())
		switch err {
		case pb.Error_KeyNotInRegion, pb.Error_RegionNotFound:
			return &clipb.DeleteResponse{TransID: 0, Error: clipb.Error_INVALID}, nil
		case pb.Error_ErrOk:
			return &clipb.DeleteResponse{TransID: 0, Error: clipb.Error_SUCCESS}, nil
		default:
			return &clipb.DeleteResponse{TransID: 0, Error: clipb.Error_FAILED}, nil
		}
	}

	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &clipb.DeleteResponse{Error: clipb.Error_INVALID, TransID: in.GetTransID()}, nil
	}

	c := reg.(kvBuffer)
	c.Delete(in.GetKey())

	return &clipb.DeleteResponse{Error: clipb.Error_SUCCESS, TransID: in.GetTransID()}, nil
}

func newMtikvCli(raftGroups cmap.ConcurrentMap, client pdpb.PDClient, tsoRecvC chan uint64, tsoSendC chan struct{}) *mtikvCli {
	return &mtikvCli{
		transID:    0,
		buffer:     cmap.New(),
		raftGroups: raftGroups,
		pdCli:      client,
		tsoRecvC:   tsoRecvC,
		tsoSendC:   tsoSendC,
	}
}

func (cli *mtikvCli) getTimeStamp() uint64 {
	go func() {
		cli.tsoSendC <- struct{}{}
	}()
	return <-cli.tsoRecvC

}

func getTso(client pdpb.PDClient, tsoRecvC chan<- uint64, tsoSendC <-chan struct{}) {

	ctx := context.TODO()

	stream, err := client.Tso(ctx)
	if err != nil {
		log.Fatal(client, err)
	}
	waitc := make(chan struct{})
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// read done.
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("Failed to receive a note : %v", err)
			}
			tsoRecvC <- in.GetTimestamp()
		}
	}()
	for u := range tsoSendC {
		if err := stream.Send(&pdpb.TsoRequest{}); err != nil {
			log.Fatalf("Failed to send a note: %v %v", err, u)
		}
	}
	stream.CloseSend()
	<-waitc
}

func main() {

	ctx := context.TODO()

	cfg := config.LoadMTikvClientConfig()

	listen, err := net.Listen("tcp", cfg.Host)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	conn, err := grpc.Dial(cfg.PD, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pdpb.NewPDClient(conn)

	tsoRecvC := make(chan uint64)
	defer close(tsoRecvC)
	tsoSendC := make(chan struct{})
	defer close(tsoSendC)

	go getTso(client, tsoRecvC, tsoSendC)

	cluster := cmap.New() // save store info

	for _, v := range cfg.RaftGroup {
		cluster.Set(v.RaftID, raftGroup{
			raftID: v.RaftID,
			from:   []byte(v.From),
			to:     []byte(v.To),
			addr:   v.Address,
		})
	}

	server := grpc.NewServer()
	clipb.RegisterMTikvCliServer(server, newMtikvCli(cluster, client, tsoRecvC, tsoSendC))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			log.Println("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	log.Println("Start mtikv_cli on " + cfg.Host + " ...")

	err = server.Serve(listen)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

func (cli *mtikvCli) prepareTwoPhaseData(tid string) (map[string][]*pb.MvccObject, []byte) {

	buf, ok := cli.buffer.Get(tid)
	if !ok {
		return nil, nil
	}

	rGroups := cli.raftGroups.Items()

	commitData := make(map[string][]*pb.MvccObject, len(rGroups))
	kvBuffer := buf.(kvBuffer)

	var primaryKey []byte

	for raftID := range rGroups {
		commitData[raftID] = make([]*pb.MvccObject, 0, 5)
	}

	for key, value := range kvBuffer.data {
		for rid, rGroup := range rGroups {
			if betweenRightIncl([]byte(key), rGroup.(raftGroup).from, rGroup.(raftGroup).to) {
				if primaryKey == nil {
					primaryKey = []byte(key)
				}
				op := pb.Op_PUT

				if len(value) == 0 {
					op = pb.Op_DEL
				}
				commitData[rid] = append(commitData[rid], &pb.MvccObject{
					Key:   []byte(key),
					Value: value,
					Op:    op,
				})
			}
		}
	}
	return commitData, primaryKey
}

func betweenRightIncl(key, a, b []byte) bool {
	return between(key, a, b) || bytes.Equal(key, b)
}

func between(key, a, b []byte) bool {
	switch bytes.Compare(a, b) {
	case 1:
		return bytes.Compare(a, key) == -1 || bytes.Compare(b, key) >= 0
	case -1:
		return bytes.Compare(a, key) == -1 && bytes.Compare(b, key) >= 0
	case 0:
		return bytes.Compare(a, key) != 0
	}
	return false
}
