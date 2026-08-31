package sink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrSinkNotFound = errors.New("flux sink: named sink not found")
	ErrSinkStopped  = errors.New("flux sink: sink dispatcher stopped")
)

// Sink defines the contract for external dispatch destinations (e.g. Kafka, Postgres, S3, Webhooks).
type Sink interface {
	Emit(ctx context.Context, payload any) error
}

// FuncSink allows plain Go functions to satisfy the Sink interface.
type FuncSink func(ctx context.Context, payload any) error

func (f FuncSink) Emit(ctx context.Context, payload any) error {
	return f(ctx, payload)
}

type sinkTask struct {
	sinkName string
	payload  any
}

// Registry manages named Sink dispatchers and an asynchronous dispatch pipeline.
type Registry struct {
	mu        sync.RWMutex
	sinks     map[string]Sink
	queue     chan sinkTask
	stopped   uint32
	wg        sync.WaitGroup
}

// New creates a new Sink Registry with an async worker pool.
func New(workers int, queueDepth int) *Registry {
	if workers <= 0 {
		workers = 4
	}
	if queueDepth <= 0 {
		queueDepth = 1024
	}

	r := &Registry{
		sinks:   make(map[string]Sink),
		queue:   make(chan sinkTask, queueDepth),
		stopped: 0,
	}

	r.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go r.workerLoop()
	}

	return r
}

func (r *Registry) workerLoop() {
	defer r.wg.Done()
	for task := range r.queue {
		r.mu.RLock()
		s, ok := r.sinks[task.sinkName]
		r.mu.RUnlock()

		if ok && s != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.Emit(ctx, task.payload)
			cancel()
		}
	}
}

// Register registers a named Sink implementation.
func (r *Registry) Register(name string, s Sink) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sinks[name] = s
}

// Dispatch sends a payload to a named Sink asynchronously with backpressure.
func (r *Registry) Dispatch(ctx context.Context, name string, payload any) error {
	if atomic.LoadUint32(&r.stopped) == 1 {
		return ErrSinkStopped
	}

	r.mu.RLock()
	s, ok := r.sinks[name]
	r.mu.RUnlock()
	if !ok || s == nil {
		return fmt.Errorf("%w: %s", ErrSinkNotFound, name)
	}

	select {
	case r.queue <- sinkTask{sinkName: name, payload: payload}:
		return nil
	default:
		// Queue full -> execute inline synchronously
		return s.Emit(ctx, payload)
	}
}

// DispatchSync executes the named sink synchronously.
func (r *Registry) DispatchSync(ctx context.Context, name string, payload any) error {
	r.mu.RLock()
	s, ok := r.sinks[name]
	r.mu.RUnlock()
	if !ok || s == nil {
		return fmt.Errorf("%w: %s", ErrSinkNotFound, name)
	}
	return s.Emit(ctx, payload)
}

// Close gracefully flushes the sink queue and shuts down worker threads.
func (r *Registry) Close() error {
	if atomic.CompareAndSwapUint32(&r.stopped, 0, 1) {
		close(r.queue)
		r.wg.Wait()
	}
	return nil
}
