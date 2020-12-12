package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/amyangfei/grpc-test/pb"
	"google.golang.org/grpc"
)

var (
	addr         = flag.String("addr", "127.0.0.1:50051", "input server addr")
	clientNum    = flag.Int("c", 10, "grpc-client connnect num")
	workerNum    = flag.Int("g", 10, "goroutine nums")
	totalCount   = flag.Int("n", 200000, "total requests")
	pingInterval = flag.Duration("i", time.Millisecond, "ping interval")
)

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:13200", nil)
	}()

	flag.Parse()
	log.Printf("server addr: %s, totalCount: %d, multi client: %d, worker num: %d, ping interval: %s",
		*addr,
		*totalCount,
		*clientNum,
		*workerNum,
		*pingInterval,
	)

	ctx := context.Background()
	startTime := time.Now()
	handleMultiClient(ctx, *addr, *totalCount, *clientNum, *workerNum)
	duration := time.Now().Sub(startTime)
	costTime := float64(duration.Nanoseconds()) / float64(1000) / float64(1000) / float64(1000)

	qps := float64(*totalCount) / costTime
	log.Printf("duration: %s, qps is %.0f", duration, qps)
}

func handleMultiClient(ctx context.Context, addr string, totalCount, clientNum, workerNum int) {
	var wg sync.WaitGroup

	clientPool := []*grpc.ClientConn{}
	for index := 0; index < clientNum; index++ {
		clientPool = append(clientPool, newClient(addr))
	}

	for index := 0; index < workerNum; index++ {
		wg.Add(1)
		go func(index int) {
			cidx := index % clientNum
			c := pb.NewPingPongClient(clientPool[cidx])

			roundCount := totalCount / workerNum
			for idx := 0; idx < roundCount; idx++ {
				_, err := c.Ping(ctx, &pb.PingRequest{Id: uint64(idx)})
				if err != nil {
					log.Panicf("ping with error: %v", err)
				}
				time.Sleep(*pingInterval)
			}
			wg.Done()
		}(index)
	}
	wg.Wait()
}

func newClient(addr string) *grpc.ClientConn {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Panicf("grpc dial failed: %v\n", err)
	}
	return conn
}
