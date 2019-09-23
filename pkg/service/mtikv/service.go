package service

import (
	"context"

	"github.com/thoainguyen/mtikv/pkg/core/utils"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/thoainguyen/mtikv/pkg/core/mvcc"
	"github.com/thoainguyen/mtikv/pkg/core/store"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
)

type MTiKvService struct {
	store   *store.Store
	regions cmap.ConcurrentMap
}

func NewMTiKvService(store *store.Store, regions cmap.ConcurrentMap) *MTiKvService {
	mtikv := &MTiKvService{store, regions}
	return mtikv
}

func (serv MTiKvService) Prewrite(ctx context.Context, in *pb.PrewriteRequest) (*pb.PrewriteResponse, error) {
	clusterId := in.GetContext().GetClusterId()
	reg, ok := serv.regions.Get(clusterId)
	if !ok {
		return &pb.PrewriteResponse{Error: pb.Error_RegionNotFound}, nil
	}
	_, err := reg.(*mvcc.Mvcc).Prewrite(in.GetMutation(), in.GetStartVersion(), in.GetPrimaryLock())

	return &pb.PrewriteResponse{Error: err}, nil
}

func (serv MTiKvService) Commit(ctx context.Context, in *pb.CommitRequest) (*pb.CommitResponse, error) {
	clusterId := in.GetContext().GetClusterId()
	reg, ok := serv.regions.Get(clusterId)
	if !ok {
		return &pb.CommitResponse{Error: pb.Error_RegionNotFound}, nil
	}
	err := reg.(*mvcc.Mvcc).Commit(in.GetStartVersion(), in.GetCommitVersion(), in.GetKeys())
	return &pb.CommitResponse{Error: err}, nil
}

func (serv MTiKvService) ResolveLock(ctx context.Context, in *pb.ResolveLockRequest) (*pb.ResolveLockResponse, error) {
	// TODO:
	return nil, nil
}

func (serv MTiKvService) GC(ctx context.Context, in *pb.GCRequest) (*pb.GetResponse, error) {
	// TODO:
	return nil, nil
}

func (serv MTiKvService) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	clusterId := in.GetContext().GetClusterId()
	reg, ok := serv.regions.Get(clusterId)
	if !ok {
		return &pb.GetResponse{Error: pb.Error_RegionNotFound}, nil
	}
	value := reg.(*mvcc.Mvcc).Get(in.GetVersion(), in.GetKey())
	data := &pb.MvccObject{}

	if len(value) != 0 {
		utils.Unmarshal(value, data)
		value = data.GetValue()
	}

	return &pb.GetResponse{Value: value, Error: pb.Error_ErrOk}, nil
}

func (serv MTiKvService) RawPut(ctx context.Context, in *pb.RawPutRequest) (*pb.RawPutResponse, error) {
	clusterId := in.GetContext().GetClusterId()
	reg, ok := serv.regions.Get(clusterId)
	if !ok {
		return &pb.RawPutResponse{Error: pb.Error_RegionNotFound}, nil
	}

	err := reg.(*mvcc.Mvcc).RawPut(&pb.MvccObject{
		Key:   in.GetKey(),
		Value: in.GetValue(),
		Op:    pb.Op_PUT,
	}, in.GetVersion())

	return &pb.RawPutResponse{Error: err}, nil
}

func (serv MTiKvService) RawDelete(ctx context.Context, in *pb.RawDeleteRequest) (*pb.RawDeleteResponse, error) {
	clusterId := in.GetContext().GetClusterId()
	reg, ok := serv.regions.Get(clusterId)
	if !ok {
		return &pb.RawDeleteResponse{Error: pb.Error_RegionNotFound}, nil
	}
	err := reg.(*mvcc.Mvcc).RawPut(&pb.MvccObject{
		Key: in.GetKey(),
		Op:  pb.Op_DEL,
	}, in.GetVersion())
	return &pb.RawDeleteResponse{Error: err}, nil
}
