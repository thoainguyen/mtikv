package service

import (
	"context"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/thoainguyen/mtikv/pkg/core/store"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
)

type MTiKvService struct {
	store   *store.Store
	regions *cmap.ConcurrentMap
}

func NewMTiKvService(store *store.Store, regions *cmap.ConcurrentMap) *MTiKvService {
	mtikv := &MTiKvService{store, regions}
	return mtikv
}

func (serv MTiKvService) Prewrite(ctx context.Context, in *pb.PrewriteRequest) (*pb.PrewriteReponse, error) {

	return nil, nil
}

func (serv MTiKvService) Commit(ctx context.Context, in *pb.CommitRequest) (*pb.CommitReponse, error) {
	return nil, nil
}

func (serv MTiKvService) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetReponse, error) {
	return nil, nil
}

func (serv MTiKvService) ResolveLock(ctx context.Context, in *pb.ResolveLockRequest) (*pb.ResolveLockResponse, error) {
	return nil, nil
}

func (serv MTiKvService) GC(ctx context.Context, in *pb.GCRequest) (*pb.GetReponse, error) {
	return nil, nil
}

func (serv MTiKvService) RawGet(ctx context.Context, in *pb.RawGetRequest) (*pb.RawGetResponse, error) {
	return nil, nil
}

func (serv MTiKvService) RawPut(ctx context.Context, in *pb.RawPutRequest) (*pb.RawPutResponse, error) {
	return nil, nil
}

func (serv MTiKvService) RawDelete(ctx context.Context, in *pb.RawDeleteRequest) (*pb.RawDeleteResponse, error) {
	return nil, nil
}
