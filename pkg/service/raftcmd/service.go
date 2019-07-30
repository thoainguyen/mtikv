package service

import (
	"context"
	"github.com/thoainguyen/mtikv/pkg/api/raftcmd"
	"github.com/thoainguyen/mtikv/pkg/core/raftstore"

	log "github.com/sirupsen/logrus"
)

type RaftService struct {
	raftlayer *raftstore.RaftLayer
}

func NewRaftService(raftLayer *raftapi.RaftLayer) *raftcmd.RaftServiceServer {
	return &RaftService{raftlayer: raftLayer}
}

func (service RaftService) Put(ctx context.Context, msg *raftcmd.PutRequest) (*raftcmd.PutResponse, error) {
	log.Info("Received Put: Key: %#v Value: %#v\n", msg.Key, msg.Value)
	err := service.raftlayer.rPutData(msg.Key, msg.Value)
	if err != nil {
		log.Fatalf("Can't Put Key-Value: %v", err)
		return &raftcmd.PutResponse{Error: "failure"}, err
	}
	return &raftcmd.PutResponse{Error: "success"}, nil
}

func (service RaftService) Get(ctx context.Context, msg *raftcmd.GetRequest) (*raftcmd.GetResponse, error) {
	log.Info("Received Get: Key: %#v\n", msg.Key)
	data, err := service.raftlayer.rGetData(msg.Key)
	if err != nil {
		log.Fatalf("Can't Get Value: %v", err)
		return &raftcmd.GetResponse{Error: "failure"}, err
	}
	return &raftcmd.GetResponse{Value: data, Error: "success"}, nil
}

func (service RaftService) Delete(ctx context.Context, msg *raftcmd.DeleteRequest) (*raftcmd.DeleteResponse, error) {
	log.Info("Received Delete Key: %#v\n", msg.Key)
	err := service.raftlayer.rDeleteData(msg.Key)
	if err != nil {
		log.Fatalf("Can't Delete Key: %v", err)
		return &raftcmd.DeleteResponse{Error: "failure"}, err
	}
	return &raftcmd.DeleteResponse{Error: "success"}, nil
}

func (service RaftService) AddNode(ctx context.Context, msg *raftcmd.AddNodeRequest) (*raftcmd.AddNodeResponse, error) {
	log.Info("Received AddNode %v - %v\n", msg.NodeId, msg.Url)
	service.raftlayer.rAddNode(msg.NodeId, msg.Url)
	return &raftcmd.AddNodeResponse{Error: "success"}, nil
}

func (service RaftService) RemoveNode(ctx context.Context, msg *raftcmd.RemoveNodeRequest) (*raftcmd.RemoveNodeResponse, error) {
	log.Info("Received RemoveNode %v\n", msg.NodeId)
	service.raftlayer.rRemoveNode(msg.NodeId, msg.Url)
	return &raftcmd.RemoveNodeResponse{Error: "success"}, nil
}
