package main

import (
	"context" // Use "golang.org/x/net/context" for Golang version <= 1.6
	"flag"
	"net/http"

	"github.com/golang/glog"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"

	gw "github.com/thoainguyen/mtikv/pkg/pb/raftkvpb" // Update

	log "github.com/sirupsen/logrus"
)

var (
	raftServerEndpoint = flag.String("raft", "localhost:10002", "gRPC server endpoint")
)

func run() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Register gRPC server endpoint
	// Note: Make sure the gRPC server is running properly and accessible
	mux := runtime.NewServeMux()
	opts := []grpc.DialOption{grpc.WithInsecure()}
	err := gw.RegisterRaftServiceHandlerFromEndpoint(ctx, mux, *raftServerEndpoint, opts)
	if err != nil {
		return err
	}
	// Start HTTP server (and proxy calls to gRPC server endpoint)
	return http.ListenAndServe(":8081", mux)
}

func main() {
	flag.Parse()
	defer glog.Flush()
	log.Info("Start HTTP server (and proxy calls to gRPC server endpoint) port 8081")
	if err := run(); err != nil {
		glog.Fatal(err)
	}
}
