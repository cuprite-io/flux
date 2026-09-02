package sink_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cuprite-io/flux/internal/sink"
)

func TestSinkRegistry_DispatchAndAsyncPipeline(t *testing.T) {
	reg := sink.New(4, 128)
	defer reg.Close()

	var counter int64
	mockSink := sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&counter, 1)
		return nil
	})

	reg.Register("kafka_alerts", mockSink)

	// Async Dispatch
	for i := 0; i < 50; i++ {
		err := reg.Dispatch(context.Background(), "kafka_alerts", map[string]any{"event_id": i})
		if err != nil {
			t.Fatalf("dispatch failed: %v", err)
		}
	}

	// Allow workers to process
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt64(&counter) != 50 {
		t.Errorf("expected 50 emitted events, got %d", counter)
	}

	// Synchronous Dispatch
	err := reg.DispatchSync(context.Background(), "kafka_alerts", map[string]any{"sync": true})
	if err != nil {
		t.Fatalf("dispatch sync failed: %v", err)
	}
	if atomic.LoadInt64(&counter) != 51 {
		t.Errorf("expected 51 emitted events after sync dispatch, got %d", counter)
	}

	// Unknown sink error
	err = reg.DispatchSync(context.Background(), "non_existent_sink", map[string]any{})
	if err == nil {
		t.Errorf("expected error for non existent sink, got nil")
	}
}
