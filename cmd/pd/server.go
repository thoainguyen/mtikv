package main

import (
	"context"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync/atomic"

	"github.com/thoainguyen/mtikv/config"
	pb "github.com/thoainguyen/mtikv/proto/pdpb"
	"google.golang.org/grpc"
)

var (
	host = "localhost:2379"
)

// server is used to implement pd.PDService.
type pD struct {
	timestamp uint64
}

// GetTimestamp implements pd.PDService.
func (s *pD) Tso(stream pb.PD_TsoServer) error {

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		atomic.AddUint64(&s.timestamp, 1)

		note := &pb.TsoResponse{Timestamp: s.timestamp}

		if err := stream.Send(note); err != nil {
			return err
		}
	}
}

func main() {

	cfg := config.LoadPDConfig()
	if cfg.Host != "" {
		host = cfg.Host
	}

	listen, err := net.Listen("tcp", host)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Println("Begin placement driver PD on " + host)

	server := grpc.NewServer()

	pb.RegisterPDServer(server, &pD{0})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	ctx := context.TODO()

	go func() {
		for range c {
			log.Println("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	if err := server.Serve(listen); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
