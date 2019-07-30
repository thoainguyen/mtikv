package kvpb

import (
	"context"
	"github.com/thoainguyen/mtikv/pkg/api/kvpb"
	"net"
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

//RunServer run gRPC service
func RunServer(ctx context.Context, kvServer kvpb.KvServiceServer, port string) error {
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	kvpb.RegisterKvServiceServer(server, kvServer)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			log.Info("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	log.Info("Start kvstore service port " + port + " ...")
	return server.Serve(listen)
}
