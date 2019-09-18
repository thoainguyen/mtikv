package main

import (
	"io"
	"log"
	"net"
	"sync/atomic"

	pb "github.com/thoainguyen/mtikv/pkg/pb/pdpb"
	"google.golang.org/grpc"
)

const (
	port = ":50051"
)

// server is used to implement pd.PDService.
type server struct {
	timestamp uint64
}

// GetTimestamp implements pd.PDService.
func (s *server) Tso(stream pb.PD_TsoServer) error {

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
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPDServer(s, &server{0})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
