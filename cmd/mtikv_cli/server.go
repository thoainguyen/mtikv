package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"

	"github.com/labstack/gommon/log"
	cmap "github.com/orcaman/concurrent-map"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikv_clipb"
	"google.golang.org/grpc"
)

var (
	pd   = flag.String("pd", "127.0.0.1:2379", "placement driver for mtikv")
	port = flag.String("port", "59303", "port mtikv_cli grpc")
)

type mtikvCli struct {
	transID uint64
	buffer  cmap.ConcurrentMap
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

func newMtikvCli() *mtikvCli {
	return &mtikvCli{
		transID: 0,
		buffer:  cmap.New(),
	}
}

func main() {

	ctx := context.Background()
	flag.Parse()

	listen, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := grpc.NewServer()
	pb.RegisterMTikvCliServer(server, newMtikvCli())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			log.Info("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	log.Info("Start mtikv service port " + *port + " ...")

	err = server.Serve(listen)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
