package main

import (
	"flag"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	cmd "github.com/thoainguyen/mtikv/pkg/cmd/mtikv"
	"go.etcd.io/etcd/raft/raftpb"
)

var (
	fDir     = flag.String("dir", "dump", "data directory")
	fPeers   = flag.String("peers", "http://127.0.0.1:9021", "sermi colon separated cluster peers")
	fId      = flag.String("id", "1", "comma separated region id")
	fCluster = flag.String("cluster", "1", "comma separated cluster id")
	fPort    = flag.String("port", "12380", "key-value server port")
)

func main() {

	flag.Parse()

	var (
		id          = strings.Split(*fId, ",")
		cluster     = strings.Split(*fCluster, ",")
		peers       = strings.Split(*fPeers, ";")
		intID       = make([]int, len(id))
		proposeC    = make([]chan []byte, len(id))
		confChangeC = make([]chan raftpb.ConfChange, len(id))
	)

	for i := 0; i < len(id); i++ {
		intID[i], _ = strconv.Atoi(id[i])
		proposeC[i] = make(chan []byte)
		confChangeC[i] = make(chan raftpb.ConfChange)
	}

	clus := &cmd.Cluster{cluster, intID, peers, proposeC, confChangeC}

	defer func() {
		for i, _ := range id {
			if proposeC[i] != nil {
				close(proposeC[i])
			}
			if confChangeC[i] != nil {
				close(confChangeC[i])
			}
		}
	}()

	if err := cmd.RunServer(clus, *fDir, *fPort); err != nil {
		log.Fatal("run server err: ", err)
		os.Exit(1)
	}
}
