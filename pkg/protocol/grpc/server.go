package grpc

import (
	"context"
	"net"
	"os"
	"os/signal"
	pb "mtikv/pkg/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

//RunServer run gRPC service
func RunServer(ctx context.Context, mTikvServer pb.MTikvServiceServer, port string) error {
	listen, err := net.Listen("tcp", ":" + port)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	pb.RegisterMTikvServiceServer(server, mTikvServer)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			log.Info("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	log.Info("Start Ping service port " + port + " ...")
	return server.Serve(listen)
}