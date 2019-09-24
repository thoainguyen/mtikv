package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"

	"github.com/thoainguyen/mtikv/config"
	"go.etcd.io/etcd/raft/raftpb"

	"log"

	cmap "github.com/orcaman/concurrent-map"
	"github.com/thoainguyen/mtikv/pkg/mvcc"
	pb "github.com/thoainguyen/mtikv/proto/mtikvpb"

	"github.com/thoainguyen/mtikv/pkg/store"
	"github.com/thoainguyen/mtikv/utils"

	"google.golang.org/grpc"
)

var (
	fNode = flag.String("node", "1", "mtikv node id")
)

func main() {

	flag.Parse()

	cfg := config.LoadMtikvNodeV2(*fNode)

	var (
		proposeC    = make([]chan []byte, len(cfg.RaftID))
		confChangeC = make([]chan raftpb.ConfChange, len(cfg.RaftID))
	)

	for i := range cfg.RaftID {
		proposeC[i] = make(chan []byte)
		confChangeC[i] = make(chan raftpb.ConfChange)
	}

	defer func() {
		for i := range cfg.RaftID {
			if proposeC[i] != nil {
				close(proposeC[i])
			}
			if confChangeC[i] != nil {
				close(confChangeC[i])
			}
		}
	}()

	st := store.CreateStore(cfg.DataDir)
	defer st.Destroy()

	regions := cmap.New()

	for idx := range cfg.RaftID {
		regions.Set(cfg.RaftGroup[idx], mvcc.CreateMvcc(st, proposeC[idx], confChangeC[idx], cfg.RaftID[idx],
			strings.Split(cfg.Peers[idx], ","), false, fmt.Sprintf("wal-%02s", cfg.RaftGroup[idx])))
	}

	server := grpc.NewServer()
	pb.RegisterMTikvServer(server, newMtikvServ(st, regions))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	ctx := context.TODO()

	go func() {
		for range c {
			log.Println("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	log.Println("Start mtikv on " + cfg.Host + " ...")

	listen, err := net.Listen("tcp", cfg.Host)
	if err != nil {
		log.Fatal("run server err: ", err)
		os.Exit(1)
	}

	err = server.Serve(listen)
	if err != nil {
		log.Fatal("run server err: ", err)
		os.Exit(1)
	}
}

type mtikvServ struct {
	store   *store.Store
	regions cmap.ConcurrentMap
}

func newMtikvServ(store *store.Store, regions cmap.ConcurrentMap) *mtikvServ {
	mtikv := &mtikvServ{store, regions}
	return mtikv
}

func (serv *mtikvServ) Prewrite(ctx context.Context, in *pb.PrewriteRequest) (*pb.PrewriteResponse, error) {
	clusterId := in.GetContext().GetClusterId()
	reg, ok := serv.regions.Get(clusterId)
	if !ok {
		return &pb.PrewriteResponse{Error: pb.Error_RegionNotFound}, nil
	}
	_, err := reg.(*mvcc.Mvcc).Prewrite(in.GetMutation(), in.GetStartVersion(), in.GetPrimaryLock())

	return &pb.PrewriteResponse{Error: err}, nil
}

func (serv *mtikvServ) Commit(ctx context.Context, in *pb.CommitRequest) (*pb.CommitResponse, error) {
	clusterId := in.GetContext().GetClusterId()
	reg, ok := serv.regions.Get(clusterId)
	if !ok {
		return &pb.CommitResponse{Error: pb.Error_RegionNotFound}, nil
	}
	err := reg.(*mvcc.Mvcc).Commit(in.GetStartVersion(), in.GetCommitVersion(), in.GetKeys())
	return &pb.CommitResponse{Error: err}, nil
}

func (serv *mtikvServ) ResolveLock(ctx context.Context, in *pb.ResolveLockRequest) (*pb.ResolveLockResponse, error) {
	// TODO:
	return nil, nil
}

func (serv *mtikvServ) GC(ctx context.Context, in *pb.GCRequest) (*pb.GetResponse, error) {
	// TODO:
	return nil, nil
}

func (serv *mtikvServ) PingPong(ctx context.Context, in *pb.PingRequest) (*pb.PongReponse, error) { // check health of grpc Server
	return &pb.PongReponse{}, nil
}

func (serv *mtikvServ) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {

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

func (serv *mtikvServ) RawPut(ctx context.Context, in *pb.RawPutRequest) (*pb.RawPutResponse, error) {
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

func (serv *mtikvServ) RawDelete(ctx context.Context, in *pb.RawDeleteRequest) (*pb.RawDeleteResponse, error) {
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
