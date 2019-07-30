package service

import (
	"context"
	"mtikv/pkg/api/kvpb"
	db "mtikv/pkg/core/storage"

	log "github.com/sirupsen/logrus"
)

type KvService struct {
	db *db.DB
}

func NewKvService(db *db.DB) kvpb.KvServiceServer {
	return &KvService{
		db: db,
	}
}

func (service KvService) RawPut(ctx context.Context, msg *kvpb.RawPutRequest) (*kvpb.RawPutResponse, error) {
	log.Info("Received Put: Key: %#v Value: %#v\n", msg.Key, msg.Value)

	err := service.db.PutData(msg.Key, msg.Value)
	if err != nil {
		log.Fatalf("Can't Put Key-Value: %v", err)
		return &kvpb.RawPutResponse{Error: "failure"}, err
	}
	return &kvpb.RawPutResponse{Error: "success"}, nil
}

func (service KvService) RawGet(ctx context.Context, msg *kvpb.RawGetRequest) (*kvpb.RawGetResponse, error) {
	log.Info("Received Get: Key: %#v\n", msg.Key)
	data, err := service.db.GetData(msg.Key)
	if err != nil {
		log.Fatalf("Can't Get Value: %v", err)
		return &kvpb.RawGetResponse{Error: "failure"}, err
	}
	return &kvpb.RawGetResponse{Value: data, Error: "success"}, nil
}

func (service KvService) RawDelete(ctx context.Context, msg *kvpb.RawDeleteRequest) (*kvpb.RawDeleteResponse, error) {
	log.Info("Received Delete: Key: %#v\n", msg.Key)
	err := service.db.DeleteData(msg.Key)
	if err != nil {
		log.Fatalf("Can't Delete Key: %v", err)
		return &kvpb.RawDeleteResponse{Error: "failure"}, err
	}
	return &kvpb.RawDeleteResponse{Error: "success"}, nil
}
