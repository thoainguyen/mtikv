package main

import (
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
	"google.golang.org/grpc"
)

var (
	pd   = flag.String("pd", "127.0.0.1:2379", "placement driver for mtikv")
	port = flag.String("port", "59303", "port mtikv_cli grpc")
)

type mtikvCli struct {
	transID  uint64
	buffer   cmap.ConcurrentMap
	client   pdpb.PDClient
	tsoRecvC chan uint64
	tsoSendC chan struct{}
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
	
	return nil, nil
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
		client:   client,
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

	conn, err := grpc.Dial(*pd)
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
