package cmd

import (
	"context"
	"net"
	"os"
	"os/signal"
	"sync"

	cmap "github.com/orcaman/concurrent-map"
	log "github.com/sirupsen/logrus"
	"github.com/thoainguyen/mtikv/pkg/core/mvcc"
	"github.com/thoainguyen/mtikv/pkg/core/store"
	pb "github.com/thoainguyen/mtikv/pkg/pb/mtikvpb"
	mtikv "github.com/thoainguyen/mtikv/pkg/service/mtikv"
	"go.etcd.io/etcd/raft/raftpb"
	"google.golang.org/grpc"
)

type Cluster struct {
	ClusterID   string
	Peers       []string
	ProposeC    []chan []byte
	ConfChangeC []chan raftpb.ConfChange
}

//RunServer run gRPC server
func RunServer(clus *Cluster, dir, port string) error {
	ctx := context.Background()

	st := store.CreateStore(dir)
	defer st.Destroy()
	regions := cmap.New()

	var wg sync.WaitGroup

	for idx, _ := range clus.Peers {
		wg.Add(1)
		go createMvccStore(&regions, idx, st, clus, &wg)
	}
	wg.Wait()

	serv := mtikv.NewMTiKvService(st, &regions)
	return RunMTiKvService(ctx, serv, port)
}

func createMvccStore(regions *cmap.ConcurrentMap, idx int, st *store.Store, clus *Cluster, wg *sync.WaitGroup) {

	region := mvcc.CreateMvcc(st, clus.ProposeC[idx], clus.ConfChangeC[idx], idx+1, clus.Peers, false)
	regions.Set(clus.ClusterID, region)
	wg.Done()
}

//RunRaftService run gRPC service
func RunMTiKvService(ctx context.Context, mtikvServer pb.MTikvServer, port string) error {
	listen, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	server := grpc.NewServer()
	pb.RegisterMTikvServer(server, mtikvServer)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		for range c {
			log.Info("Shutting down gRPC server...")
			server.GracefulStop()
			<-ctx.Done()
		}
	}()

	log.Info("Start mtikv service port " + port + " ...")
	return server.Serve(listen)
}
