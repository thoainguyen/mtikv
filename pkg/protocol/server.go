package protocol

import (
	"context"
	"github.com/thoainguyen/mtikv/pkg/api/raftkv"
	"net"
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

//RunServer run gRPC service
func RunServer(ctx context.Context, raftServer raftkv.RaftServiceServer, port string) error {
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	raftkv.RegisterRaftServiceServer(server, raftServer)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			log.Info("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	log.Info("Start raft service port " + port + " ...")
	return server.Serve(listen)
}
