package cmd

import (
	"context"
	"mtikv/config"
	"mtikv/pkg/db"
	grpc "mtikv/pkg/protocol/grpc"
	mtikvservice "mtikv/pkg/service"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

//RunServer run gRPC server
func RunServer() error {
	ctx := context.Background()

	//load config
	config := &configs.MTikvServiceConfig{}
	configs.LoadConfig()
	if err := viper.Unmarshal(config); err != nil {
		log.Fatal("load config: ", err)
	}

	dba, err := db.CreateDB(config.DBPath)
	if err != nil {
		return err
	}

	mTikvService := mtikvservice.NewMTiKVService(dba)

	return grpc.RunServer(ctx, mTikvService, strconv.Itoa(config.GRPCPort))
}