package cmd

import (
	"context"
	"mtikv/configs"
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
	
	if err := configs.LoadConfig(); err != nil {
		log.Fatalf("LoadConfig: %v\n",err)
	}
	if err := viper.Unmarshal(config); err != nil {
		log.Fatalf("Unmarshal: %v\n", err)
	}
	dba, err := db.CreateDB(config.DBPath)
	if err != nil {
		return err
	}

	mTikvService := mtikvservice.NewMTikvService(dba)

	return grpc.RunServer(ctx, mTikvService, strconv.Itoa(config.GRPCPort))
}