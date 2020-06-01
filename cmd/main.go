package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ashniu123/raft-grpc/cmd/server"
)

var (
	addr = flag.String("addr", ":20001", "Set the gRPC server bind address")
	id   = flag.Uint("id", 1, "Set the Node identifier")
	join = flag.String("join", "", "Set the Join address. Use to join a cluster, not to start one")
	hb   = flag.Uint("hb", 500, "Set Heartbeat (in ms)")
	et   = flag.Uint("timeout", 1000, "Min election timeout duration (in ms). Max is 2 times this value")
	t    = flag.Uint("t", 300, "Max ticker duration (in ms)")
)

// Usage function for "flag" lib
func Usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [options] \n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	server := server.New(*addr, *join, uint32(*id), uint32(*et), uint32(*hb), uint32(*t))
	server.Serve(*join)
}
