package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/thoainguyen/mtikv/config"
	db "github.com/thoainguyen/mtikv/pkg/core/db"
	raftstore "github.com/thoainguyen/mtikv/pkg/core/raft"
	grpc "github.com/thoainguyen/mtikv/pkg/protocol"
	raftservice "github.com/thoainguyen/mtikv/pkg/service"

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
	return grpc.RunServer(ctx, raftService, config.GRPCPort)
}
