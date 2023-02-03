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

type DMLIface interface {
	Stmt() string
	Args() []interface{}
}

type DML struct {
	sql  string
	args []interface{}
}

func (dml *DML) Stmt() string {
	return dml.sql
}

func (dml *DML) Args() []interface{} {
	return dml.args
}

type workerQueue struct {
	sync.Mutex
	queue    deque.Deque
	pending  atomic.Uint32
	executed atomic.Uint64
	errCh    chan error
}

type Executor struct {
	db          *sql.DB
	queues      []*workerQueue
	batchSize   int
	workerSize  int
	maxPending  int
	batchUpdate bool
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

type NewExecutorOpt struct {
	batchUpdate bool
}

type NewExecutorOption func(*NewExecutorOpt)

func WithBatchUpdate() NewExecutorOption {
	return func(opt *NewExecutorOpt) {
		opt.batchUpdate = true
	}
}

func NewExecutor(
	ctx context.Context, dsn string,
	batchSize int, workerSize int, maxPending int, opts ...NewExecutorOption,
) (*Executor, error) {
	options := &NewExecutorOpt{}
	for _, opt := range opts {
		opt(options)
	}
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
		db:          db,
		queues:      queues,
		batchSize:   batchSize,
		workerSize:  workerSize,
		maxPending:  maxPending,
		batchUpdate: options.batchUpdate,
	}, nil
}

func (e *Executor) checkIndex(index int) {
	if index < 0 || index >= e.workerSize {
		log.Panicf("invalid queue index: %d, should be [0, %d]",
			index, e.workerSize-1)
	}
}

func (e *Executor) DB() *sql.DB {
	return e.db
}

func (e *Executor) AddJob(dml DMLIface, index int) {
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

	nonBatchUpdate := func(dmls []interface{}) ([]string, [][]interface{}) {
		sqls := make([]string, 0, len(dmls))
		args := make([][]interface{}, 0, len(dmls))
		for _, elem := range dmls {
			dml := elem.(DMLIface)
			sqls = append(sqls, dml.Stmt())
			args = append(args, dml.Args())
		}
		return sqls, args
	}

	batchUpdate := func(dmls []interface{}) ([]string, [][]interface{}) {
		// TODO
		return nil, nil
	}

	exec := func(sqls []string, args [][]interface{}) {
		defer q.pending.Dec()
		tx, err := e.db.BeginTx(ctx, nil)
		if err != nil {
			q.sendError(err)
			return
		}
		for i := range sqls {
			_, err := tx.ExecContext(ctx, sqls[i], args[i]...)
			if err != nil {
				rbErr := tx.Rollback()
				if rbErr != nil {
					log.Printf("rollback error: %s", rbErr)
				}
				q.sendError(err)
				return
			}
		}
		err = tx.Commit()
		if err != nil {
			q.sendError(err)
			return
		}
		q.executed.Add(uint64(len(sqls)))
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-q.errCh:
			return err
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
				var (
					sqls []string
					args [][]interface{}
				)
				if e.batchUpdate {
					sqls, args = batchUpdate(dmls)
				} else {
					sqls, args = nonBatchUpdate(dmls)
				}
				exec(sqls, args)
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
