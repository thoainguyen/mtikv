package main

import (
	"flag"
)

var (
	pd = flag.String("pd", "127.0.0.1:2379", "placement driver for mtikv")
)

func main() {

	flag.Parse()

}
