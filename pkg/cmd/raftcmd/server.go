package raftcmd

import (
	"context"
	"mtikv/configs"
	db "mtikv/pkg/core/storage"
	grpc "mtikv/pkg/protocol/grpc/raftcmd"
	raftservice "mtikv/pkg/service/raftcmd"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

//  kvStore     *db.DB
//	mu          sync.RWMutex
//	confChangeC chan<- raftpb.ConfChange
//	proposeC    chan<- string

//RunServer run gRPC server
func RunServer() error {
	ctx := context.Background()

	//load config
	config := &configs.RaftServiceConfig{}

	if err := configs.LoadConfig(); err != nil {
		log.Fatalf("LoadConfig: %v\n", err)
	}
	if err := viper.Unmarshal(config); err != nil {
		log.Fatalf("Unmarshal: %v\n", err)
	}
	dba, err := db.CreateDB(config.DBPath)
	if err != nil {
		return err
	}

	raftService := raftservice.NewRaftService(dba)

	return grpc.RunServer(ctx, raftService, strconv.Itoa(config.GRPCPort))
}
