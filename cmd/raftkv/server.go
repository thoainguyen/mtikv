package main

import (
	"flag"
	"os"

	cmd "github.com/thoainguyen/mtikv/pkg/cmd"

	log "github.com/sirupsen/logrus"
)

func main() {

	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	port := flag.String("port", "12380", "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	if err := cmd.RunServer(cluster, id, port, join); err != nil {
		log.Fatal("run server err: ", err)
		os.Exit(1)
	}
}
