package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/amyangfei/grpc-test/pb"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

var (
	cmuxReadTimeout   = 10 * time.Second
	useOfClosedErrMsg = "use of closed network connection"
)

type server struct{}

// Ping implements pb.Ping
func (s *server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PongResult, error) {
	ts1 := time.Now()
	result := &pb.PongResult{
		Id:          req.GetId(),
		ReceiveTime: ts1.UnixNano(),
	}
	ts2 := time.Now()
	result.SendTime = ts2.UnixNano()
	return result, nil
}

func isErrNetClosing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), useOfClosedErrMsg)
}

func runServer() {
	addr := "0.0.0.0:13100"
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Panicf("failed to listen: %v", err)
	}

	m := cmux.New(lis)
	m.SetReadTimeout(cmuxReadTimeout)
	grpcL := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpL := m.Match(cmux.HTTP1Fast())

	grpcSrv := grpc.NewServer(
		grpc.MaxConcurrentStreams(4096),
		grpc.WriteBufferSize(1024*1024),
	)
	pb.RegisterPingPongServer(grpcSrv, &server{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("starts grpc server at %s\n", addr)
		err := grpcSrv.Serve(grpcL)
		if err != nil && !isErrNetClosing(err) && err != cmux.ErrListenerClosed {
			log.Printf("gRPC server returned error: %v\n", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		mux := http.NewServeMux()

		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		srv := &http.Server{
			Handler: mux,
		}
		log.Printf("starts http server at %s\n", addr)
		err := srv.Serve(httpL)
		if err != nil && !isErrNetClosing(err) && err != http.ErrServerClosed {
			log.Printf("http server returned error: %v\n", err)
		}
	}()

	err = m.Serve()
	if err != nil {
		log.Printf("cmux server error: %s\n", err)
	}
	wg.Wait()
}

func main() {
	runServer()
}
