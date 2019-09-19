package cmd

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"

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
	Cluster     []string
	ID          []int
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

	for idx := range clus.ID {
		regions.Set(clus.Cluster[idx], mvcc.CreateMvcc(st, clus.ProposeC[idx], clus.ConfChangeC[idx], clus.ID[idx],
			strings.Split(clus.Peers[idx], ","), false, fmt.Sprintf("wal-%02s", clus.Cluster[idx])))
	}

	serv := mtikv.NewMTiKvService(st, &regions)
	return RunMTiKvService(ctx, serv, port)
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
