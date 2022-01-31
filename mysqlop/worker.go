package mysqlop

import (
	"context"
	"database/sql"
	"log"
	"sync"
	"time"

	"github.com/edwingeng/deque"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

type DML struct {
	sql  string
	args []interface{}
}

type workerQueue struct {
	sync.Mutex
	queue    deque.Deque
	pending  atomic.Uint32
	executed atomic.Uint64
	errCh    chan error
}

type Executor struct {
	db         *sql.DB
	queues     []*workerQueue
	batchSize  int
	workerSize int
	maxPending int
}

func (q *workerQueue) sendError(err error) {
	select {
	case q.errCh <- err:
	default:
		log.Printf("duplicated error: %s", err)
	}
}

func openDB(
	ctx context.Context, dsn string, workerSize int, maxPending int,
) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	err = db.PingContext(ctx)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("close db failed: %s", err)
		}
		return nil, err
	}
	db.SetMaxIdleConns(workerSize)
	// Executor workers could use workerSize * maxPending database connections
	// at most.
	db.SetMaxOpenConns(workerSize * maxPending)
	return db, err
}

func NewExecutor(
	ctx context.Context, dsn string, batchSize int, workerSize int, maxPending int,
) (*Executor, error) {
	db, err := openDB(ctx, dsn, workerSize, maxPending)
	if err != nil {
		return nil, err
	}
	queues := make([]*workerQueue, 0, workerSize)
	for i := 0; i < workerSize; i++ {
		queues = append(queues, &workerQueue{
			queue: deque.NewDeque(),
			errCh: make(chan error, 1),
		})
	}
	return &Executor{
		db:         db,
		queues:     queues,
		batchSize:  batchSize,
		workerSize: workerSize,
		maxPending: maxPending,
	}, nil
}

func (e *Executor) checkIndex(index int) {
	if index < 0 || index >= e.workerSize {
		log.Panicf("invalid queue index: %d, should be [0, %d]",
			index, e.workerSize-1)
	}
}

func (e *Executor) AddJob(dml *DML, index int) {
	e.checkIndex(index)
	q := e.queues[index]
	q.Lock()
	q.queue.Enqueue(dml)
	q.Unlock()
}

func (e *Executor) ExecuteDDLs(ctx context.Context, ddls []string) error {
	for _, ddl := range ddls {
		_, err := e.db.ExecContext(ctx, ddl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Executor) singleWorker(ctx context.Context, index int) error {
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	q := e.queues[index]
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if q.pending.Load() == uint32(e.maxPending) {
				continue
			}
			q.Lock()
			dmls := q.queue.DequeueMany(e.batchSize)
			q.Unlock()
			if len(dmls) == 0 {
				continue
			}
			q.pending.Inc()
			go func() {
				defer q.pending.Dec()
				tx, err := e.db.BeginTx(ctx, nil)
				if err != nil {
					q.sendError(err)
				}
				for _, elem := range dmls {
					dml := elem.(*DML)
					_, err := tx.ExecContext(ctx, dml.sql, dml.args...)
					if err != nil {
						rbErr := tx.Rollback()
						if rbErr != nil {
							log.Printf("rollback error: %s", rbErr)
						}
						q.sendError(err)
					}
				}
				err = tx.Commit()
				if err != nil {
					q.sendError(err)
				}
				q.executed.Add(uint64(len(dmls)))
			}()
		}
	}
}

func (e *Executor) Run(ctx context.Context) error {
	wg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < e.workerSize; i++ {
		i := i
		wg.Go(func() error {
			return e.singleWorker(ctx, i)
		})
	}
	return wg.Wait()
}

func (e *Executor) Executed(index int) uint64 {
	e.checkIndex(index)
	return e.queues[index].executed.Load()
}

func (e *Executor) Pending(index int) uint32 {
	e.checkIndex(index)
	return e.queues[index].pending.Load()
}
