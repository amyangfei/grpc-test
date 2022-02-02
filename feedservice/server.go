package feedservice

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/amyangfei/grpc-test/pb"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
)

var (
	cmuxReadTimeout   = 10 * time.Second
	useOfClosedErrMsg = "use of closed network connection"
	batchSize         = 500
	defaultPayload    = "abcdefghij"
)

type server struct{}

// Ping implements pb.FeedEvent
func (s *server) Feed(req *pb.FeedRequest, stream pb.StreamService_FeedServer) error {
	start := req.Start
	end := req.End
	if end <= start {
		end = math.MaxUint64
	}
	log.Printf("receive feed request, start:%d, end: %d\n", start, end)
	for start < end {
		events := make([]*pb.Event, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			events = append(events, &pb.Event{
				Ts:      start,
				Payload: defaultPayload,
			})
			start++
		}
		feedEv := &pb.FeedEvent{Events: events}
		err := stream.Send(feedEv)
		if err != nil {
			return err
		}
	}
	return nil
}

func isErrNetClosing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), useOfClosedErrMsg)
}

func RunServer(port int) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
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
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
	)
	pb.RegisterStreamServiceServer(grpcSrv, &server{})
	grpc_prometheus.Register(grpcSrv)

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
		mux.Handle("/metrics", promhttp.Handler())

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
