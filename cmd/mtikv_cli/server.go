package main

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/thoainguyen/mtikv/config"
	"github.com/thoainguyen/mtikv/pkg/pb/pdpb"

	"log"

	cmap "github.com/orcaman/concurrent-map"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikv_clipb"
	pb1 "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
	"google.golang.org/grpc"
)

// var (
// 	pd   = flag.String("pd", "127.0.0.1:2379", "placement driver for mtikv")
// 	port = flag.String("port", "59303", "port mtikv_cli grpc")
// )

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
	start_ts uint64
	data     map[string][]byte
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

func (kv *kvBuffer) SetStartTs(start_ts uint64) {
	kv.start_ts = start_ts
}

func (kv *kvBuffer) GetStartTs() uint64 {
	return kv.start_ts
}

func (cli *mtikvCli) getRaftGroupByKey(key []byte) (raftGroup, bool) {
	rGroups := cli.raftGroups.Items()
	for u := range rGroups {
		if betweenRightIncl(key, rGroups[u].(raftGroup).from, rGroups[u].(raftGroup).to) {
			reg, _ := cli.raftGroups.Get(u)
			return reg.(raftGroup), true
		}
	}
	return raftGroup{}, false
}

func (cli *mtikvCli) BeginTxn(ctx context.Context, in *pb.BeginTxnRequest) (*pb.BeginTxnResponse, error) {
	cli.mu.Lock()
	defer cli.mu.Unlock()

	cli.transID++

	tid := strconv.FormatUint(cli.transID, 10)

	cli.buffer.Set(tid, kvBuffer{
		start_ts: cli.getTimeStamp(),
		data:     make(map[string][]byte),
	})

	// TODO: Set transaction value
	return &pb.BeginTxnResponse{TransID: cli.transID}, nil
}

func (cli *mtikvCli) CommitTxn(ctx context.Context, in *pb.CommitTxnRequest) (*pb.CommitTxnResponse, error) {

	tid := strconv.FormatUint(in.GetTransID(), 10)
	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &pb.CommitTxnResponse{Error: pb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	c := reg.(kvBuffer)

	twoPhaseData, primaryKey := cli.prepareTwoPhaseData(tid)
	if primaryKey == nil {
		return &pb.CommitTxnResponse{Error: pb.Error_SUCCESS, TransID: in.GetTransID()}, nil
	}

	// TODO: parallel prewrite
	for rGroupID, cmData := range twoPhaseData {
		ok := cli.runPrewrite(c.GetStartTs(), primaryKey, rGroupID, cmData)
		if !ok {
			return &pb.CommitTxnResponse{Error: pb.Error_FAILED}, nil
		}
	}

	commitTs := cli.getTimeStamp()

	// TODO: parallel commit
	for rGroupID, cmData := range twoPhaseData {

		for idx := range cmData {
			cmData[idx] = &pb1.MvccObject{
				Key: cmData[idx].GetKey(),
				Op:  cmData[idx].GetOp(),
			}
		}

		ok := cli.runCommit(c.GetStartTs(), commitTs, primaryKey, rGroupID, cmData)
		if !ok {
			return &pb.CommitTxnResponse{Error: pb.Error_FAILED}, nil
		}
	}

	return &pb.CommitTxnResponse{TransID: in.GetTransID(), Error: pb.Error_SUCCESS}, nil
}

func (cli *mtikvCli) runCommit(startTs, commitTs uint64, primaryKey []byte, rGroupID string, cmData []*pb1.MvccObject) bool {

	rg, ok := cli.raftGroups.Get(rGroupID)
	if !ok {
		return false
	}
	rGroup := rg.(raftGroup)

	conn, err := grpc.Dial(rGroup.addr[rand.Intn(len(rGroup.addr))], grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	mtikvCli := pb1.NewMTikvClient(conn)

	ctx := context.TODO()

	commitResult, _ := mtikvCli.Commit(ctx, &pb1.CommitRequest{
		Context: &pb1.Context{
			ClusterId: rGroupID,
		},
		Keys:          cmData,
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})

	if commitResult.GetError() != pb1.Error_ErrOk {
		return false
	}

	return true
}

func (cli *mtikvCli) runPrewrite(startTs uint64, primaryKey []byte, rGroupID string, cmData []*pb1.MvccObject) bool {
	rg, ok := cli.raftGroups.Get(rGroupID)
	if !ok {
		return false
	}
	rGroup := rg.(raftGroup)

	conn, err := grpc.Dial(rGroup.addr[rand.Intn(len(rGroup.addr))], grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	mtikvCli := pb1.NewMTikvClient(conn)

	ctx := context.TODO()

	prewriteResult, _ := mtikvCli.Prewrite(ctx, &pb1.PrewriteRequest{
		Context: &pb1.Context{
			ClusterId: rGroupID,
		},
		Mutation:     cmData,
		PrimaryLock:  primaryKey,
		StartVersion: startTs,
	})

	if prewriteResult.GetError() != pb1.Error_ErrOk {
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

func (cli *mtikvCli) Set(ctx context.Context, in *pb.SetRequest) (*pb.SetResponse, error) {
	tid := strconv.FormatUint(in.GetTransID(), 10)
	if tid == "0" { // Raw Set
		err := cli.putToMtikv(in.GetKey(), in.GetValue(), cli.getTimeStamp())
		switch err {
		case pb1.Error_KeyNotInRegion, pb1.Error_RegionNotFound:
			return &pb.SetResponse{TransID: 0, Error: pb.Error_INVALID}, nil
		case pb1.Error_ErrOk:
			return &pb.SetResponse{TransID: 0, Error: pb.Error_SUCCESS}, nil
		default:
			return &pb.SetResponse{TransID: 0, Error: pb.Error_FAILED}, nil
		}
	}

	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &pb.SetResponse{TransID: in.GetTransID(), Error: pb.Error_INVALID}, nil
	}
	c := reg.(kvBuffer)
	c.Set(in.GetKey(), in.GetValue())
	return &pb.SetResponse{TransID: in.GetTransID(), Error: pb.Error_SUCCESS}, nil
}

func (cli *mtikvCli) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	tid := strconv.FormatUint(in.GetTransID(), 10)

	if tid == "0" {
		return &pb.GetResponse{Value: cli.getFromMtikv(in.GetKey(), cli.getTimeStamp()),
			Error: pb.Error_SUCCESS, TransID: 0}, nil
	}

	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &pb.GetResponse{Error: pb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	c := reg.(kvBuffer)
	val := c.Get(in.GetKey())

	if bytes.Compare(val, []byte(nil)) == 0 {
		val = cli.getFromMtikv(in.GetKey(), c.GetStartTs())
	}
	return &pb.GetResponse{Value: val, Error: pb.Error_SUCCESS, TransID: in.GetTransID()}, nil
}

func (cli *mtikvCli) putToMtikv(key, value []byte, version uint64) pb1.Error {
	rg, ok := cli.getRaftGroupByKey(key)
	if ok {
		conn, err := grpc.Dial(rg.addr[rand.Intn(len(rg.addr))], grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()
		mtikvCli := pb1.NewMTikvClient(conn)

		ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		defer cancel()

		result, err := mtikvCli.RawPut(ctx, &pb1.RawPutRequest{
			Context: &pb1.Context{
				ClusterId: rg.raftID,
			},
			Key: key, Value: value, Version: version,
		})

		if err != nil {
			log.Println(err)
		}
		return result.GetError()
	}
	return pb1.Error_RegionNotFound
}

func (cli *mtikvCli) deleteToMtikv(key []byte, version uint64) pb1.Error {
	rg, ok := cli.getRaftGroupByKey(key)
	if ok {
		conn, err := grpc.Dial(rg.addr[rand.Intn(len(rg.addr))], grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()
		mtikvCli := pb1.NewMTikvClient(conn)

		ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		defer cancel()

		result, err := mtikvCli.RawDelete(ctx, &pb1.RawDeleteRequest{
			Context: &pb1.Context{
				ClusterId: rg.raftID,
			},
			Key: key, Version: version,
		})

		if err != nil {
			log.Println(err)
		}
		return result.GetError()
	}
	return pb1.Error_RegionNotFound
}

func (cli *mtikvCli) getFromMtikv(key []byte, version uint64) []byte {
	rg, ok := cli.getRaftGroupByKey(key)

	if ok {
		conn, err := grpc.Dial(rg.addr[rand.Intn(len(rg.addr))], grpc.WithInsecure())
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
		}
		defer conn.Close()
		mtikvCli := pb1.NewMTikvClient(conn)

		ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
		defer cancel()

		result, err := mtikvCli.Get(ctx, &pb1.GetRequest{
			Context: &pb1.Context{
				ClusterId: rg.raftID,
			},
			Version: version,
			Key:     key,
		})

		if err != nil {
			log.Println(err)
		}

		if result.GetError() == pb1.Error_ErrOk {
			return result.GetValue()
		}
	}
	return nil
}

func (cli *mtikvCli) RollBackTxn(ctx context.Context, in *pb.RollBackTxnRequest) (*pb.RollBackTxnResponse, error) {
	tid := strconv.FormatUint(in.GetTransID(), 10)
	_, ok := cli.buffer.Get(tid)
	if !ok {
		return &pb.RollBackTxnResponse{Error: pb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	// _ := reg.(kvBuffer)
	// TODO: do rollback Transaction, remove data in buffer
	return &pb.RollBackTxnResponse{TransID: in.GetTransID()}, nil
}

func (cli *mtikvCli) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	tid := strconv.FormatUint(in.GetTransID(), 10)
	if tid == "0" { // Raw Del
		err := cli.deleteToMtikv(in.GetKey(), cli.getTimeStamp())
		switch err {
		case pb1.Error_KeyNotInRegion, pb1.Error_RegionNotFound:
			return &pb.DeleteResponse{TransID: 0, Error: pb.Error_INVALID}, nil
		case pb1.Error_ErrOk:
			return &pb.DeleteResponse{TransID: 0, Error: pb.Error_SUCCESS}, nil
		default:
			return &pb.DeleteResponse{TransID: 0, Error: pb.Error_FAILED}, nil
		}
	}
	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &pb.DeleteResponse{Error: pb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	c := reg.(kvBuffer)
	c.Delete(in.GetKey())
	return &pb.DeleteResponse{Error: pb.Error_SUCCESS, TransID: in.GetTransID()}, nil
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
	// flag.Parse()

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
	pb.RegisterMTikvCliServer(server, newMtikvCli(cluster, client, tsoRecvC, tsoSendC))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			log.Println("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	log.Println("Start mtikv service host " + cfg.Host + " ...")

	err = server.Serve(listen)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

func (cli *mtikvCli) prepareTwoPhaseData(tid string) (map[string][]*pb1.MvccObject, []byte) {

	buf, ok := cli.buffer.Get(tid)
	if !ok {
		return nil, nil
	}

	clus := cli.raftGroups.Items()

	commitData := make(map[string][]*pb1.MvccObject, len(clus))
	kvBuffer := buf.(kvBuffer)

	var primaryKey []byte

	for clusID := range clus {
		commitData[clusID] = make([]*pb1.MvccObject, 0, 5)
	}

	for key, value := range kvBuffer.data {
		for clusK, clusV := range clus {
			if betweenRightIncl([]byte(key), clusV.(raftGroup).from, clusV.(raftGroup).to) {
				if primaryKey == nil {
					primaryKey = []byte(key)
				}
				op := pb1.Op_PUT

				if len(value) == 0 {
					op = pb1.Op_DEL
				}
				commitData[clusK] = append(commitData[clusK], &pb1.MvccObject{
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
