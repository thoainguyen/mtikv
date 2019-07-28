package service

import (
	"context"
	"mtikv/pkg/api/kvpb"
	"mtikv/pkg/db"
	log "github.com/sirupsen/logrus"
)

type MTikvService struct {
	db *db.DB
}

func NewMTikvService(db *db.DB) kvpb.MTikvServiceServer {
	return &MTikvService{
		db: db,
	}
}

func (service MTikvService) Put(ctx context.Context, msg *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	log.Info("Received Put: Key: %#v Value: %#v\n", msg.Key, msg.Value)
	
	err := service.db.PutData(msg.Key, msg.Value)
	if err != nil {
		log.Fatalf("Can't Put Key-Value: %v", err)
		return &kvpb.PutResponse{Error: "failure"}, err
	}
	return &kvpb.PutResponse{Error: "success"}, nil
}

func (service MTikvService) Get(ctx context.Context, msg *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	log.Info("Received Get: Key: %#v\n", msg.Key)
	data, err := service.db.GetData(msg.Key)
	if err != nil {
		log.Fatalf("Can't Get Value: %v", err)
		return &kvpb.GetResponse{Error: "failure"}, err
	}
	return &kvpb.GetResponse{Value: data, Error: "success"}, nil
}

func (service MTikvService) Delete(ctx context.Context, msg *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	log.Info("Received Delete: Key: %#v\n", msg.Key)
	err := service.db.DeleteData(msg.Key)
	if err != nil {
		log.Fatalf("Can't Delete Key: %v", err)
		return &kvpb.DeleteResponse{Error: "failure"}, err
	}
	return &kvpb.DeleteResponse{Error: "success"}, nil
}