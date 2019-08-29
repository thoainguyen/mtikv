package cmd

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/thoainguyen/mtikv/config"
	db "github.com/thoainguyen/mtikv/pkg/core/db"
	raftstore "github.com/thoainguyen/mtikv/pkg/core/raft"
	"google.golang.org/grpc"
	raftservice "github.com/thoainguyen/mtikv/pkg/service"
	raftkv "github.com/thoainguyen/mtikv/pkg/pb/raftkvpb"

	"go.etcd.io/etcd/raft/raftpb"
)

//RunServer run gRPC server
func RunServer(cluster *string, id *int, kvport *int, join *bool) error {
	ctx := context.Background()

	dba, err := db.CreateDB(
		fmt.Sprintf("%s-%d", config.DBPath, *id),
		fmt.Sprintf("%s-%d", config.SnapPath, *id))
	if err != nil {
		return err
	}

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var rkv *raftstore.RaftLayer
	getSnapshot := func() ([]byte, error) { return rkv.GetSnapshot() }
	commitC, errorC, snapshotterReady := raftstore.NewRaftNode(*id, strings.Split(*cluster, ","),
		*join, getSnapshot, proposeC, confChangeC)

	raftStore := raftstore.NewRaftApiMTikv(<-snapshotterReady, dba, proposeC, commitC, confChangeC, errorC)
	raftService := raftservice.NewRaftService(raftStore)
	return RunRaftService(ctx, raftService, config.GRPCPort)
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

	log.Info("Start raft service port " + port + " ...")
	return server.Serve(listen)
}
