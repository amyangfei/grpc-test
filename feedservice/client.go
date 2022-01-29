package feedservice

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/amyangfei/grpc-test/pb"
	"google.golang.org/grpc"
)

func newClient(addr string) *grpc.ClientConn {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Panicf("grpc dial failed: %v\n", err)
	}
	return conn
}

func RunClient(
	serverAddr string,
	statusPort int,
	clientNum int,
	workerNum int,
	requestNum int,
) {
	go func() {
		http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", statusPort), nil)
	}()

	log.Printf(
		"server addr: %s, status port: %d, client num: %d, worker num: %d, requests per worker: %d",
		serverAddr,
		statusPort,
		clientNum,
		workerNum,
		requestNum,
	)

	ctx := context.Background()
	startTime := time.Now()
	handleMultiClient(ctx, serverAddr, clientNum, workerNum, requestNum)
	duration := time.Now().Sub(startTime)

	qps := float64(requestNum*workerNum) / (float64(duration.Nanoseconds()) / 1e9)
	log.Printf("duration: %s, qps is %.0f", duration, qps)
}

func handleMultiClient(
	ctx context.Context,
	addr string,
	clientNum,
	workerNum int,
	requestNum int,
) {
	var wg sync.WaitGroup

	clientPool := []*grpc.ClientConn{}
	for index := 0; index < clientNum; index++ {
		clientPool = append(clientPool, newClient(addr))
	}

	for index := 0; index < workerNum; index++ {
		wg.Add(1)
		go func(index int) {
			cidx := index % clientNum
			c := pb.NewStreamServiceClient(clientPool[cidx])

			req := &pb.FeedRequest{
				Start: 0,
				End:   uint64(requestNum),
			}
			stream, err := c.Feed(ctx, req)
			if err != nil {
				log.Panicf("create stream client failed: %s", err)
			}
			receivedEvents := 0
		recvLoop:
			for {
				ev, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						log.Printf("stream %d end, received events: %d",
							index, receivedEvents)
						break recvLoop
					}
					log.Panicf("stream recv with error: %s", err)
				}
				receivedEvents += len(ev.GetEvents())
			}
			wg.Done()
		}(index)
	}
	wg.Wait()
}
