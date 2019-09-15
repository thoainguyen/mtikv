package cmd

import (
	"context"
	"fmt"
	"github.com/thoainguyen/mtikv/pkg/core/raftstore"
	"github.com/thoainguyen/tidb-internals/mtikv/pkg/core/raft"
	"net"
	"os"
	"os/signal"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/thoainguyen/mtikv/config"
	"github.com/thoainguyen/mtikv/pkg/core/db"
	raftkv "github.com/thoainguyen/mtikv/pkg/pb/raftkvpb"
	raftservice "github.com/thoainguyen/mtikv/pkg/service"
	"google.golang.org/grpc"

	"go.etcd.io/etcd/raft/raftpb"
)

//RunServer run gRPC server
func RunServer(cluster *string, id *int, port *string, join *bool) error {
	ctx := context.Background()

	dba, err := db.CreateDB(fmt.Sprintf("%s-%d", config.DBPath, *id))
	if err != nil {
		return err
	}

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	commitC, errorC := raftstore.NewRaftNode(*id, strings.Split(*cluster, ","), *join, proposeC, confChangeC)

	raftStore := raft.NewRaftApiMTikv(dba, proposeC, commitC, confChangeC, errorC)
	raftService := raftservice.NewRaftService(raftStore)
	return RunRaftService(ctx, raftService, *port)
}

//RunRaftService run gRPC service
func RunRaftService(ctx context.Context, raftServer raftkv.RaftServiceServer, port string) error {
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

	log.Info("Start raftstore service port " + port + " ...")
	return server.Serve(listen)
}
