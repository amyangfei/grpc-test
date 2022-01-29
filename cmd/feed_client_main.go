package main

import (
	"flag"
	_ "net/http/pprof"

	"github.com/amyangfei/grpc-test/feedservice"
)

var (
	serverAddr = flag.String("addr", "127.0.0.1:27000", "feed server address")
	statusPort = flag.Int("status", 28000, "status port")
	clientNum  = flag.Int("c", 10, "grpc-client connnect num")
	workerNum  = flag.Int("g", 10, "goroutine nums")
	requestNum = flag.Int("n", 5000000, "requests number of per worker")
)

func main() {
	flag.Parse()
	feedservice.RunClient(
		*serverAddr,
		*statusPort,
		*clientNum,
		*workerNum,
		*requestNum,
	)
}
