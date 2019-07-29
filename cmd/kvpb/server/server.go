package main

import (
	"os"
	cmd "mtikv/pkg/cmd/kvpb"

	log "github.com/sirupsen/logrus"
)

func main() {
	if err := cmd.RunServer(); err != nil {
		log.Fatal("run server err: ", err)
		os.Exit(1)
	}
}
