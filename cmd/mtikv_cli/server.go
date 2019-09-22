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
	"sync/atomic"

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
	data map[string][]byte
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

func newKvBuffer() *kvBuffer {
	return &kvBuffer{data: make(map[string][]byte)}
}

func (cli *mtikvCli) BeginTxn(ctx context.Context, in *pb.BeginTxnRequest) (*pb.BeginTxnResponse, error) {
	atomic.AddUint64(&cli.transID, 1)

	// TODO: Set transaction value
	return &pb.BeginTxnResponse{TransID: cli.transID}, nil
}

func (cli *mtikvCli) CommitTxn(ctx context.Context, in *pb.CommitTxnRequest) (*pb.CommitTxnResponse, error) {
	tid := strconv.Itoa(int(in.GetTransID()))

	twoPhaseData, firstClus := cli.prepareTwoPhaseData(tid)
	if twoPhaseData == nil {
		return &pb.CommitTxnResponse{Error: pb.Error_INVALID}, nil
	}

	startTs := cli.getTimeStamp()
	primaryKey := twoPhaseData[firstClus][0].GetKey()

	for rGroupID, cmData := range twoPhaseData {
		ok := cli.runPrewrite(startTs, primaryKey, rGroupID, cmData)
		if !ok {
			return &pb.CommitTxnResponse{Error: pb.Error_INVALID}, nil
		}
	}

	commitTs := cli.getTimeStamp()

	for rGroupID, cmData := range twoPhaseData {
		ok := cli.runCommit(startTs, commitTs, primaryKey, rGroupID, cmData)
		if !ok {
			return &pb.CommitTxnResponse{Error: pb.Error_INVALID}, nil
		}
	}

	// TODO: return CommitTxnResponse{}

	return nil, nil
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

	ctx := context.Background()
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()

	commitResult, _ := mtikvCli.Commit(ctx, &pb1.CommitRequest{})

	if commitResult.GetRegionError() != pb1.Error_Ok {
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

	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	ctx := context.Background()

	prewriteResult, _ := mtikvCli.Prewrite(ctx, &pb1.PrewriteRequest{
		Context: &pb1.Context{
			ClusterId: rGroupID,
		},
		Mutation:     cmData,
		PrimaryLock:  primaryKey,
		StartVersion: startTs,
	})

	if prewriteResult.GetRegionError() != pb1.Error_Ok {
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
	tid := strconv.Itoa(int(in.GetTransID()))
	reg, ok := cli.buffer.Get(tid)
	if !ok {
		cli.buffer.Set(tid, newKvBuffer())
	} else {
		c := reg.(*kvBuffer)
		c.Set(in.GetKey(), in.GetValue())
	}
	return &pb.SetResponse{TransID: in.GetTransID(), Error: pb.Error_SUCCESS}, nil
}

func (cli *mtikvCli) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	tid := strconv.Itoa(int(in.GetTransID()))
	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &pb.GetResponse{Error: pb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	c := reg.(*kvBuffer)
	return &pb.GetResponse{Value: c.Get(in.GetKey()), Error: pb.Error_SUCCESS, TransID: in.GetTransID()}, nil
}

func (cli *mtikvCli) RollBackTxn(ctx context.Context, in *pb.RollBackTxnRequest) (*pb.RollBackTxnResponse, error) {
	tid := strconv.Itoa(int(in.GetTransID()))
	_, ok := cli.buffer.Get(tid)
	if !ok {
		return &pb.RollBackTxnResponse{Error: pb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	// _ := reg.(*kvBuffer)
	// TODO: do rollback Transaction, remove data in buffer
	return &pb.RollBackTxnResponse{TransID: in.GetTransID()}, nil
}

func (cli *mtikvCli) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	tid := strconv.Itoa(int(in.GetTransID()))
	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &pb.DeleteResponse{Error: pb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	c := reg.(*kvBuffer)
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
	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	ctx := context.Background()

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

	ctx := context.Background()
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

func (cli *mtikvCli) prepareTwoPhaseData(tid string) (map[string][]*pb1.MvccObject, string) {

	buf, ok := cli.buffer.Get(tid)
	if !ok {
		return nil, ""
	}

	clus := cli.raftGroups.Items()
	firstClusID := func() string {
		for clusID := range clus {
			return clusID
		}
		return ""
	}()

	commitData := make(map[string][]*pb1.MvccObject, len(clus))
	kvBuffer := buf.(*kvBuffer)

	for clusID := range clus {
		commitData[clusID] = make([]*pb1.MvccObject, 0, 5)
	}

	for key, value := range kvBuffer.data {
		for clusK, clusV := range clus {
			if betweenRightIncl([]byte(key), clusV.(raftGroup).from, clusV.(raftGroup).to) {
				commitData[clusK] = append(commitData[clusK], &pb1.MvccObject{
					Key:   []byte(key),
					Value: value,
				})
			}
		}
	}
	return commitData, firstClusID
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
