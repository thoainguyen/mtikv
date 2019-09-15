package service

import (
	"context"
	"github.com/thoainguyen/mtikv/pkg/core/raftstore/raft"

	pb "github.com/thoainguyen/mtikv/pkg/pb/raftkvpb"

	log "github.com/sirupsen/logrus"
)

type RaftService struct {
	raftLayer *raft.RaftLayer
}

func NewRaftService(raftLayer *raft.RaftLayer) pb.RaftServiceServer {
	return &RaftService{raftLayer: raftLayer}
}

func (service RaftService) Put(ctx context.Context, msg *pb.PutRequest) (*pb.PutResponse, error) {
	log.Info("Received Put: Key: %v Value: %v\n", msg.Key, msg.Value)

	err := service.raftLayer.PutData(msg.Key, msg.Value)
	if err != nil {
		log.Fatalf("Can't Put Key-Value: %v", err)
		return &pb.PutResponse{Error: "failure"}, err
	}
	return &pb.PutResponse{Error: "success"}, nil
}

func (service RaftService) Get(ctx context.Context, msg *pb.GetRequest) (*pb.GetResponse, error) {
	log.Info("Received Get: Key: %v\n", msg.Key)
	data, err := service.raftLayer.GetData(msg.Key)
	if err != nil {
		log.Fatalf("Can't Get Value: %v", err)
		return &pb.GetResponse{Error: "failure"}, err
	}
	return &pb.GetResponse{Value: data, Error: "success"}, nil
}

func (service RaftService) Delete(ctx context.Context, msg *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Info("Received Delete Key: %v\n", msg.Key)
	err := service.raftLayer.DeleteData(msg.Key)
	if err != nil {
		log.Fatalf("Can't Delete Key: %v", err)
		return &pb.DeleteResponse{Error: "failure"}, err
	}
	return &pb.DeleteResponse{Error: "success"}, nil
}

func (service RaftService) AddNode(ctx context.Context, msg *pb.AddNodeRequest) (*pb.AddNodeResponse, error) {
	log.Info("Received AddNode %v - %v\n", msg.NodeId, msg.Url)
	service.raftLayer.AddNode(msg.NodeId, msg.Url)
	return &pb.AddNodeResponse{Error: "success"}, nil
}

func (service RaftService) RemoveNode(ctx context.Context, msg *pb.RemoveNodeRequest) (*pb.RemoveNodeResponse, error) {
	log.Info("Received RemoveNode %v\n", msg.NodeId)
	service.raftLayer.RemoveNode(msg.NodeId)
	return &pb.RemoveNodeResponse{Error: "success"}, nil
}
