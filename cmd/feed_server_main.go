package main

import (
	"flag"

	"github.com/amyangfei/grpc-test/feedservice"
)

var (
	port = flag.Int("port", 27000, "server bind port")
)

func main() {
	flag.Parse()
	feedservice.RunServer(*port)
}
