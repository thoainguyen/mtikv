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
	fDir    = flag.String("data-dir", "dump", "data directory")
	fPeers  = flag.String("peers", "http://127.0.0.1:9021", "sermi colon separated raft peers")
	fID     = flag.String("raft-id", "1", "comma separated region id")
	fRGroup = flag.String("raft-group", "1", "comma separated cluster id")
	fHost   = flag.String("host", ":12380", "key-value server host")
)

func main() {

	flag.Parse()

	var (
		id          = strings.Split(*fID, ",")
		raftGroup   = strings.Split(*fRGroup, ",")
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

	cfg := &cmd.MtikvConfig{raftGroup, intID, peers, proposeC, confChangeC}

	defer func() {
		for i := range id {
			if proposeC[i] != nil {
				close(proposeC[i])
			}
			if confChangeC[i] != nil {
				close(confChangeC[i])
			}
		}
	}()

	if err := cmd.RunServer(cfg, *fDir, *fHost); err != nil {
		log.Fatal("run server err: ", err)
		os.Exit(1)
	}
}
