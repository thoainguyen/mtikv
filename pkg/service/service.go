package service

import (
	"context"
	"mtikv/pkg/api/kvpb"
	"mtikv/pkg/db"
	log "github.com/sirupsen/logrus"
)

type MTiKVService struct {
	db *db.DB
}

func NewMTiKVService(db *db.DB) pb.MTikvServiceServer {
	return &MTiKVService{
		db: db,
	}
}

func (service MTikvService) Put(ctx context.Context, msg *pb.PutRequest) (*pb.PutResponse, error) {
	log.Fatalf("Received: Key: %#v Value: %#v\n", in.Key, in.Value)
	
	err := service.db.PutData(in.Key, in.Value)
	if err != nil {
		log.Fatalf("Can't Put Key-Value: %v", err)
		return &kvpb.PutResponse{Error: "failure"}, err
	}
	return &kvpb.PutResponse{Error: "success"}, nil
}

func (service MTikvService) Get(ctx context.Context, msg *pb.GetRequest) (*pb.GetResponse, error) {
	log.Fatalf("Received: Key: %#v\n", in.Key)
	data, err := service.db.GetData(in.Key)
	if err != nil {
		log.Fatalf("Can't Get Value: %v", err)
		return &kvpb.GetResponse{Error: "failure"}, err
	}
	return &kvpb.GetResponse{Value: data, Error: "success"}, nil
}

func (service MTikvService) Delete(ctx context.Context, msg *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Fatalf("Received: Key: %#v\n", in.Key)
	err := service.db.DeleteData(in.Key)
	if err != nil {
		log.Fatalf("Can't Delete Key: %v", err)
		return &kvpb.PutResponse{Error: "failure"}, err
	}
	return &kvpb.PutResponse{Error: "success"}, nil
}