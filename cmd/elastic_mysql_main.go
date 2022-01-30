package main

import (
	"context"
	"flag"
	"log"
	_ "net/http/pprof"

	"github.com/amyangfei/grpc-test/mysqlop"
)

var (
	dsn        = flag.String("dsn", "mysql://root@127.0.0.1:3306/", "mysql dsn")
	batchSize  = flag.Int("batch", 20, "sql batch size")
	workerSize = flag.Int("worker", 20, "worker size")
	maxPending = flag.Int("pending", 20, "max pending background db executor")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	exec, err := mysqlop.NewExecutor(ctx, *dsn, *batchSize, *workerSize, *maxPending)
	if err != nil {
		log.Panicf("create executor failed: %s", err)
	}
	err = exec.Run(ctx)
	log.Printf("executor run error: %s", err)
}
