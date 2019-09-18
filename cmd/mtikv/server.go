package main

import (
	"flag"
	"os"
	"strings"

	log "github.com/sirupsen/logrus"
	cmd "github.com/thoainguyen/mtikv/pkg/cmd/mtikv"
	"go.etcd.io/etcd/raft/raftpb"
)

var (
	pd        = flag.String("pd", "127.0.0.1:2379", "placement driver for mtikv")
	data_dir  = flag.String("data-dir", "dump", "data directory")
	cluster   = flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	clusterID = flag.String("cluster-id", "1", "region cluster id")
	port      = flag.String("port", "12380", "key-value server port")
)

func main() {

	flag.Parse()

	var (
		peers       = strings.Split(*cluster, ",")
		proposeC    = make([]chan []byte, len(peers))
		confChangeC = make([]chan raftpb.ConfChange, len(peers))
	)

	for i, _ := range peers {
		proposeC[i] = make(chan []byte)
		confChangeC[i] = make(chan raftpb.ConfChange)
	}

	clus := &cmd.Cluster{*clusterID, peers, proposeC, confChangeC}

	defer func() {
		for i, _ := range peers {
			if proposeC[i] != nil {
				close(proposeC[i])
			}
			if confChangeC[i] != nil {
				close(confChangeC[i])
			}
		}
	}()

	if err := cmd.RunServer(clus, *data_dir, *port); err != nil {
		log.Fatal("run server err: ", err)
		os.Exit(1)
	}
}
