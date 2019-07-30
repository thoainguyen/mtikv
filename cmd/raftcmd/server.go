package main

import (
	cmd "mtikv/pkg/cmd/raftcmd"
	"os"

	log "github.com/sirupsen/logrus"
)

func main() {
	if err := cmd.RunServer(); err != nil {
		log.Fatal("run server err: ", err)
		os.Exit(1)
	}
}
