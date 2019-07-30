package kvpb

import (
	"context"
	"github.com/thoainguyen/mtikv/configs"
	db "github.com/thoainguyen/mtikv/pkg/core/storage"
	grpc "mtikv/pkg/protocol/grpc/kvpb"
	kvservice "mtikv/pkg/service/kvpb"
	"strconv"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

//RunServer run gRPC server
func RunServer() error {
	ctx := context.Background()

	//load config
	config := &configs.KvServiceConfig{}

	if err := configs.LoadConfig(); err != nil {
		log.Fatalf("LoadConfig: %v\n", err)
	}
	if err := viper.Unmarshal(config); err != nil {
		log.Fatalf("Unmarshal: %v\n", err)
	}
	dba, err := db.CreateDB(config.DBPath, config.DBSnapPath)
	if err != nil {
		return err
	}

	kvService := kvservice.NewKvService(dba)

	return grpc.RunServer(ctx, kvService, strconv.Itoa(config.GRPCPort))
}
