package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"time"

	"github.com/amyangfei/grpc-test/mysqlop"
	"golang.org/x/sync/errgroup"
)

var (
	dsn        = flag.String("dsn", "root@tcp(127.0.0.1:3306)/test", "mysql dsn")
	schema     = flag.String("schema", "elastic", "benchmark schema name")
	tableFmt   = flag.String("table", "test-%d", "table name format")
	batchSize  = flag.Int("batch", 20, "sql batch size")
	workerSize = flag.Int("worker", 20, "worker size")
	maxPending = flag.Int("pending", 80, "max pending background db executor")
	reportI    = flag.Int("interval", 10, "report interval in seconds")
	dataSize   = flag.Int("data", 500000, "per table data size")
)

var ErrAllDataExecuted = errors.New("all data has been executed")

func reporter(ctx context.Context, exec *mysqlop.Executor, reportIval int) error {
	ticker := time.NewTicker(time.Second * time.Duration(reportIval))
	defer ticker.Stop()
	lastReportTime := time.Now()
	lastExecuted := uint64(0)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			now := time.Now()
			duration := now.Sub(lastReportTime)
			executed := uint64(0)
			pendingWorkers := uint32(0)
			for i := 0; i < *workerSize; i++ {
				executed += exec.Executed(i)
				pendingWorkers += exec.Pending(i)
			}
			qps := float64(executed-lastExecuted) / (float64(duration.Nanoseconds()) / 1e9)
			log.Printf("duration: %s, qps is %.0f, pending workers: %d",
				duration, qps, pendingWorkers)
			lastExecuted = executed
			lastReportTime = now
			if executed == uint64(*dataSize**workerSize) {
				return ErrAllDataExecuted
			}
		}
	}
}

func generator(ctx context.Context, exec *mysqlop.Executor, index int) error {
	table := fmt.Sprintf(*tableFmt, index)
	gen := mysqlop.NewTableGen(*schema, table, 0)
	for j := 0; j < *dataSize; j++ {
		dml := gen.Next()
		exec.AddJob(dml, index)
	}
	return nil
}

func main() {
	flag.Parse()
	ctx := context.Background()
	exec, err := mysqlop.NewExecutor(ctx, *dsn, *batchSize, *workerSize, *maxPending)
	if err != nil {
		log.Panicf("create executor failed: %s", err)
	}

	ddls := make([]string, 0, *workerSize)
	for i := 0; i < *workerSize; i++ {
		table := fmt.Sprintf(*tableFmt, i)
		ddls = append(ddls, fmt.Sprintf(mysqlop.CreateTableTpl, *schema, table))
	}
	err = exec.ExecuteDDLs(ctx, ddls)
	if err != nil {
		log.Panicf("execute ddls with error: %s", err)
	}

	errg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < *workerSize; i++ {
		i := i
		errg.Go(func() error {
			return generator(ctx, exec, i)
		})
	}
	errg.Go(func() error {
		return exec.Run(ctx)
	})
	errg.Go(func() error {
		return reporter(ctx, exec, *reportI)
	})
	err = errg.Wait()
	if err == ErrAllDataExecuted {
		log.Printf("all data executed successfully")
	} else {
		log.Printf("executor run error: %s", err)
	}
}
