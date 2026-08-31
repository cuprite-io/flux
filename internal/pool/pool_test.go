package pool_test

import (
	"sync/atomic"
	"testing"

	"github.com/cuprite-io/flux/internal/pool"
)

func TestCountdownLatch(t *testing.T) {
	count := 10
	latch := pool.NewLatch(count)

	var completed int32
	for i := 0; i < count; i++ {
		go func() {
			atomic.AddInt32(&completed, 1)
			latch.CountDown()
		}()
	}

	latch.Wait()

	if atomic.LoadInt32(&completed) != int32(count) {
		t.Errorf("expected %d completed, got %d", count, completed)
	}
}

func TestWorkerPool_StressAndPanicIsolation(t *testing.T) {
	wp := pool.NewWorkerPool(8, 512)
	defer wp.Close()

	var counter int64
	numTasks := 1000
	latch := pool.NewLatch(numTasks)

	for i := 0; i < numTasks; i++ {
		taskID := i
		wp.Submit(func() {
			defer latch.CountDown()
			if taskID%50 == 0 {
				// Intentionally panic to test panic isolation boundary
				panic("simulated worker anomaly")
			}
			atomic.AddInt64(&counter, 1)
		})
	}

	latch.Wait()

	// 1000 tasks - 20 panicked = 980 succeeded
	if atomic.LoadInt64(&counter) < 900 {
		t.Errorf("expected >= 900 successful tasks, got %d", counter)
	}
}
