package raftcmd

import (
	"context"
	"strconv"
	"strings"

	"github.com/thoainguyen/mtikv/configs"
	"github.com/thoainguyen/mtikv/pkg/core/raftstore"
	db "github.com/thoainguyen/mtikv/pkg/core/storage"
	grpc "github.com/thoainguyen/mtikv/pkg/protocol/grpc/raftcmd"
	raftservice "github.com/thoainguyen/mtikv/pkg/service/raftcmd"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.etcd.io/etcd/raft/raftpb"
)

//RunServer run gRPC server
func RunServer(cluster *string, id *int, kvport *int, join *bool) error {
	ctx := context.Background()

	//load config
	config := &configs.RaftServiceConfig{}
	if err := configs.LoadConfig(); err != nil {
		log.Fatalf("LoadConfig: %v\n", err)
	}
	if err := viper.Unmarshal(config); err != nil {
		log.Fatalf("Unmarshal: %v\n", err)
	}

	dba, err := db.CreateDB(config.DBPath+string(*id), config.DBSnapPath+string(*id))
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
	return grpc.RunServer(ctx, raftService, strconv.Itoa(config.GRPCPort))
}
