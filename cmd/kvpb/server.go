package main

import (
	"os"

	cmd "github.com/thoainguyen/mtikv/pkg/cmd/kvpb"

	log "github.com/sirupsen/logrus"
)

func main() {
	if err := cmd.RunServer(); err != nil {
		log.Fatal("run server err: ", err)
		os.Exit(1)
	}
}
