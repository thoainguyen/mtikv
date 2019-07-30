package service

import (
	"context"
	"mtikv/pkg/api/raftcmd"
	db "mtikv/pkg/core/storage"

	log "github.com/sirupsen/logrus"
)

type RaftService struct {
}

func NewRaftService(db *db.DB) *raftcmd.RaftServiceServer {
	return &RaftService{}
}

func (service RaftService) Put(ctx context.Context, msg *raftcmd.PutRequest) (*raftcmd.PutResponse, error) {
	log.Info("Received Put: Key: %#v Value: %#v\n", msg.Key, msg.Value)

	// err := service.db.PutData(msg.Key, msg.Value)
	// if err != nil {
	// 	log.Fatalf("Can't Put Key-Value: %v", err)
	// 	return &kvpb.RawPutResponse{Error: "failure"}, err
	// }
	// return &kvpb.RawPutResponse{Error: "success"}, nil
}

func (service RaftService) Get(ctx context.Context, msg *raftcmd.GetRequest) (*raftcmd.GetResponse, error) {
	log.Info("Received Get: Key: %#v\n", msg.Key)
	// data, err := service.db.GetData(msg.Key)
	// if err != nil {
	// 	log.Fatalf("Can't Get Value: %v", err)
	// 	return &kvpb.RawGetResponse{Error: "failure"}, err
	// }
	// return &kvpb.RawGetResponse{Value: data, Error: "success"}, nil
}

func (service RaftService) Delete(ctx context.Context, msg *raftcmd.DeleteRequest) (*raftcmd.DeleteResponse, error) {
	log.Info("Received Delete Key: %#v\n", msg.Key)
	// err := service.db.DeleteData(msg.Key)
	// if err != nil {
	// 	log.Fatalf("Can't Delete Key: %v", err)
	// 	return &kvpb.RawDeleteResponse{Error: "failure"}, err
	// }
	// return &kvpb.RawDeleteResponse{Error: "success"}, nil
}

func (service RaftService) AddNode(ctx context.Context, msg *raftcmd.AddNodeRequest) (*raftcmd.AddNodeResponse, error) {
	log.Info("Received AddNode %v - %v\n", msg.NodeId, msg.Url)

}

func (service RaftService) RemoveNode(ctx context.Context, msg *raftcmd.RemoveNodeRequest) (*raftcmd.RemoveNodeResponse, error) {
	log.Info("Received RemoveNode %v\n", msg.NodeId)
}
