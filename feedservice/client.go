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
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

var receivedEvents = atomic.NewUint64(0)

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
	reportIval int,
	requestNum int,
) {
	go func() {
		http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", statusPort), nil)
	}()

	log.Printf(
		"server addr: %s, status port: %d, client num: %d, worker num: %d, "+
			"report interval: %ds, requests per worker: %d,",
		serverAddr,
		statusPort,
		clientNum,
		workerNum,
		reportIval,
		requestNum,
	)

	ctx, cancel := context.WithCancel(context.Background())
	startTime := time.Now()
	go reporter(ctx, reportIval)
	handleMultiClient(ctx, serverAddr, clientNum, workerNum, requestNum)
	cancel()
	duration := time.Now().Sub(startTime)

	received := receivedEvents.Load()
	qps := float64(received) / (float64(duration.Nanoseconds()) / 1e9)
	log.Printf("total events: %d, duration: %s, qps is %.0f", received, duration, qps)
}

func reporter(ctx context.Context, reportIval int) {
	ticker := time.NewTicker(time.Second * time.Duration(reportIval))
	defer ticker.Stop()
	lastReportTime := time.Now()
	lastReceived := uint64(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			duration := now.Sub(lastReportTime)
			received := receivedEvents.Load()
			qps := float64(received-lastReceived) / (float64(duration.Nanoseconds()) / 1e9)
			log.Printf("duration: %s, qps is %.0f", duration, qps)
			lastReceived = received
			lastReportTime = now
		}
	}
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
		recvLoop:
			for {
				ev, err := stream.Recv()
				if err != nil {
					if err == io.EOF {
						log.Printf("stream %d end", index)
						break recvLoop
					}
					log.Panicf("stream recv with error: %s", err)
				}
				receivedEvents.Add(uint64(len(ev.GetEvents())))
			}
			wg.Done()
		}(index)
	}
	wg.Wait()
}
