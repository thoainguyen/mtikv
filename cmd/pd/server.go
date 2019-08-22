package main

import (
	"context"
	"log"
	"net"
	"sync/atomic"

	pb "github.com/thoainguyen/mtikv/pkg/api/pd"
	"google.golang.org/grpc"
)

const (
	port = ":50051"
)

// server is used to implement pd.PDService.
type server struct {
	ops uint64
}

// GetTimestamp implements pd.PDService.
func (s *server) GetTimestamp(ctx context.Context, in *pb.TsoRequest) (*pb.TsoReponse, error) {
	atomic.AddUint64(&s.ops, 1)
	return &pb.TsoReponse{Timestamp: atomic.LoadUint64(&s.ops)}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPDServiceServer(s, &server{0})
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
