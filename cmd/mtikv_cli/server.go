package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/thoainguyen/mtikv/pkg/pb/pdpb"

	"log"

	cmap "github.com/orcaman/concurrent-map"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikv_clipb"
	pb1 "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
	"google.golang.org/grpc"
)

var (
	pd   = flag.String("pd", "127.0.0.1:2379", "placement driver for mtikv")
	port = flag.String("port", "59303", "port mtikv_cli grpc")
)

type mtikvCli struct {
	transID  uint64
	buffer   cmap.ConcurrentMap
	cluster  cmap.ConcurrentMap
	pdCli    pdpb.PDClient
	tsoRecvC chan uint64
	tsoSendC chan struct{}
}

type cluster struct {
	clusID string
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
	return &pb.BeginTxnResponse{TransID: cli.transID}, nil
}

func (cli *mtikvCli) CommitTxn(ctx context.Context, in *pb.CommitTxnRequest) (*pb.CommitTxnResponse, error) {
	tid := strconv.Itoa(int(in.GetTransID()))

	twoPhaseData := cli.prepareTwoPhaseData(tid)
	if twoPhaseData == nil {
		return &pb.CommitTxnResponse{Error: pb.Error_INVALID}, nil
	}

	// for clusID, cmData := range twoPhaseData {
	// TODO: Prewire + Commit
	// }

	return nil, nil
}

func (cli *mtikvCli) getCluster(clusID string) (cluster, bool) {
	clus, ok := cli.cluster.Get(clusID)
	if !ok {
		return cluster{}, false
	}
	return clus.(cluster), true
}

func (cli *mtikvCli) Set(ctx context.Context, in *pb.SetRequest) (*pb.SetResponse, error) {
	tid := strconv.Itoa(int(in.GetTransID()))
	reg, ok := cli.buffer.Get(tid)
	if !ok {
		cli.buffer.Set(tid, newKvBuffer())
	} else {
		c := reg.(kvBuffer)
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
	c := reg.(kvBuffer)
	return &pb.GetResponse{Value: c.Get(in.GetKey()), Error: pb.Error_SUCCESS, TransID: in.GetTransID()}, nil
}

func (cli *mtikvCli) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	tid := strconv.Itoa(int(in.GetTransID()))
	reg, ok := cli.buffer.Get(tid)
	if !ok {
		return &pb.DeleteResponse{Error: pb.Error_INVALID, TransID: in.GetTransID()}, nil
	}
	c := reg.(kvBuffer)
	c.Delete(in.GetKey())
	return &pb.DeleteResponse{Error: pb.Error_SUCCESS, TransID: in.GetTransID()}, nil
}

func newMtikvCli(client pdpb.PDClient, tsoRecvC chan uint64, tsoSendC chan struct{}) *mtikvCli {
	return &mtikvCli{
		transID:  0,
		buffer:   cmap.New(),
		cluster:  cmap.New(),
		pdCli:    client,
		tsoRecvC: tsoRecvC,
		tsoSendC: tsoSendC,
	}
}

func (cli *mtikvCli) getTimeStamp() uint64 {
	go func() {
		cli.tsoSendC <- struct{}{}
	}()
	return <-cli.tsoRecvC

}

func getTso(client pdpb.PDClient, tsoRecvC chan<- uint64, tsoSendC <-chan struct{}) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
	flag.Parse()

	listen, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	conn, err := grpc.Dial(*pd, grpc.WithInsecure())
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

	server := grpc.NewServer()
	pb.RegisterMTikvCliServer(server, newMtikvCli(client, tsoRecvC, tsoSendC))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			log.Println("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	log.Println("Start mtikv service port " + *port + " ...")

	err = server.Serve(listen)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}

type CommitData []*pb1.MvccObject

func (cli *mtikvCli) prepareTwoPhaseData(tid string) map[string]CommitData {

	buf, ok := cli.buffer.Get(tid)
	if !ok {
		return nil
	}

	clus := cli.cluster.Items()

	commitData := make(map[string]CommitData, len(clus))
	kvBuffer := buf.(kvBuffer)

	for clusID := range clus {
		commitData[clusID] = make(CommitData, 0, 5)
	}

	for key, value := range kvBuffer.data {
		for clusK, clusV := range clus {
			if betweenRightIncl([]byte(key), clusV.(cluster).from, clusV.(cluster).to) {
				commitData[clusK] = append(commitData[clusK], &pb1.MvccObject{
					Key:   []byte(key),
					Value: value,
				})
			}
		}
	}
	return commitData
}

// check if key is between a and b, right inclusive
func betweenRightIncl(key, a, b []byte) bool {
	return between(key, a, b) || bytes.Equal(key, b)
}

// Checks if a key is STRICTLY between two ID's exclusively
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
