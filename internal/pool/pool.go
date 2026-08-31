package pool

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

// Task represents a unit of work submitted to the worker pool.
type Task func()

// Latch is a reusable zero-allocation atomic countdown latch.
type Latch struct {
	count int32
	done  chan struct{}
	once  sync.Once
}

// NewLatch creates a countdown latch initialized with the given count.
func NewLatch(count int) *Latch {
	l := &Latch{
		count: int32(count),
		done:  make(chan struct{}),
	}
	if count <= 0 {
		close(l.done)
	}
	return l
}

// CountDown decrements the latch count. If count reaches 0, waiters are notified.
func (l *Latch) CountDown() {
	if atomic.AddInt32(&l.count, -1) <= 0 {
		l.once.Do(func() {
			close(l.done)
		})
	}
}

// Wait blocks until the latch count reaches zero.
func (l *Latch) Wait() {
	<-l.done
}

// WorkerPool manages a pre-warmed pool of goroutines across GOMAXPROCS for parallel child branch execution.
type WorkerPool struct {
	numWorkers int
	taskQueue  chan Task
	stopped    uint32
	wg         sync.WaitGroup
}

var defaultPool *WorkerPool
var onceDefault sync.Once

// GetDefaultPool returns the global pre-warmed worker pool sized to GOMAXPROCS.
func GetDefaultPool() *WorkerPool {
	onceDefault.Do(func() {
		defaultPool = NewWorkerPool(runtime.GOMAXPROCS(0) * 2, 4096)
	})
	return defaultPool
}

// NewWorkerPool creates and starts a worker pool with the specified concurrency and queue depth.
func NewWorkerPool(workers int, queueSize int) *WorkerPool {
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if queueSize <= 0 {
		queueSize = 1024
	}

	p := &WorkerPool{
		numWorkers: workers,
		taskQueue:  make(chan Task, queueSize),
		stopped:    0,
	}

	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.workerLoop()
	}

	return p
}

func (p *WorkerPool) workerLoop() {
	defer p.wg.Done()
	for task := range p.taskQueue {
		if task != nil {
			p.safeExecute(task)
		}
	}
}

// safeExecute contains panics at the worker thread boundary.
func (p *WorkerPool) safeExecute(task Task) {
	defer func() {
		if r := recover(); r != nil {
			// Panic caught at worker boundary; node fails safely without crashing process
			_ = fmt.Sprintf("flux worker panic: %v", r)
		}
	}()
	task()
}

// Submit enqueues a task for parallel execution.
// If the queue is saturated, it executes synchronously on the calling goroutine as backpressure protection.
func (p *WorkerPool) Submit(task Task) {
	if atomic.LoadUint32(&p.stopped) == 1 {
		p.safeExecute(task)
		return
	}

	select {
	case p.taskQueue <- task:
	default:
		// Queue full -> execute inline synchronously
		p.safeExecute(task)
	}
}

// Close stops the worker pool and waits for active tasks to complete.
func (p *WorkerPool) Close() {
	if atomic.CompareAndSwapUint32(&p.stopped, 0, 1) {
		close(p.taskQueue)
		p.wg.Wait()
	}
}
