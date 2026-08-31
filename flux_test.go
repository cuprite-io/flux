package flux_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/types"
)

type PaymentEvent struct {
	UserID string  `json:"user_id"`
	Amount float64 `json:"amount"`
	CardID string  `json:"card_id"`
}

func TestFlux_StrictInputValidation(t *testing.T) {
	eng, err := flux.New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	ctx := context.Background()

	// Valid inputs: Struct, Pointer to Struct, JSON string, Slice
	if err := flux.ValidateInput(PaymentEvent{UserID: "u1"}); err != nil {
		t.Errorf("struct should be valid, got err: %v", err)
	}
	if err := flux.ValidateInput(&PaymentEvent{UserID: "u1"}); err != nil {
		t.Errorf("struct ptr should be valid, got err: %v", err)
	}
	if err := flux.ValidateInput(`{"amount": 100}`); err != nil {
		t.Errorf("json string should be valid, got err: %v", err)
	}
	if err := flux.ValidateInput([]int{1, 2, 3}); err != nil {
		t.Errorf("slice should be valid, got err: %v", err)
	}

	// Invalid inputs: primitives, nil, map, channel
	if err := flux.ValidateInput(nil); err == nil {
		t.Errorf("nil should be invalid")
	}
	if err := flux.ValidateInput(42); err == nil {
		t.Errorf("int should be invalid")
	}
	if err := flux.ValidateInput(true); err == nil {
		t.Errorf("bool should be invalid")
	}

	// Spark call with invalid input should return ErrInvalidInputType
	_, err = eng.Spark(ctx, 12345, "payments")
	if err != flux.ErrInvalidInputType {
		t.Errorf("expected ErrInvalidInputType, got: %v", err)
	}
}

func TestFlux_SparkEndToEnd(t *testing.T) {
	var sinkFired int64
	mockSink := func(ctx context.Context, payload any) error {
		atomic.AddInt64(&sinkFired, 1)
		return nil
	}

	eng, err := flux.New(
		flux.WithWorkers(4),
	)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	eng.RegisterSink("fraud_alerts", sink.FuncSink(mockSink))

	// 1. Build a Tree-based Circuit with parallel child branches
	root := types.NewNode("filter_high_value").
		Step(flux.Volt(`amount >= 1000.0`))

	nodeA := types.NewNode("alert_branch").
		WithCondition(`amount >= 1000.0`).
		Step(flux.Sink("fraud_alerts", "kafka_sink")).
		Step(flux.Return(map[string]any{
			"status": "FLAGGED",
			"risk":   "HIGH",
		}))

	root.AddChildren(nodeA)

	circuit := types.NewCircuit("fraud_detection_circuit").
		WithTags("stream:payments").
		WithRoot(root)

	_ = eng.Registry().Put(context.Background(), circuit)

	// 2. Execute Spark with valid Go struct
	evt := PaymentEvent{UserID: "user_101", Amount: 5000.0, CardID: "card_999"}
	res, err := eng.Spark(context.Background(), evt, "stream:payments")
	if err != nil {
		t.Fatalf("Spark failed: %v", err)
	}

	if !res.Passed {
		t.Errorf("expected Spark to pass")
	}
	if len(res.ExecutedCircuits) != 1 || res.ExecutedCircuits[0] != "fraud_detection_circuit" {
		t.Errorf("expected executed circuit 'fraud_detection_circuit'")
	}
	if res.ReturnedData["status"] != "FLAGGED" {
		t.Errorf("expected status FLAGGED, got %v", res.ReturnedData["status"])
	}

	// Allow async sink worker to process
	time.Sleep(30 * time.Millisecond)
	if atomic.LoadInt64(&sinkFired) != 1 {
		t.Errorf("expected 1 sink dispatch, got %d", sinkFired)
	}
}

func TestFlux_ConductEndToEnd(t *testing.T) {
	eng, err := flux.New()
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	defer eng.Close()

	// Item 1: High level offer (Level >= 10)
	root1 := types.NewNode("titan_eval").
		WithCondition(`user.level >= 10`).
		Step(flux.Return(map[string]any{
			"title":  "Titan Slayer Bundle",
			"reward": 500,
		}))

	item1 := &types.Item{
		ID:   "offer_titan",
		Tags: []string{"offers:boss_killer"},
		Data: map[string]any{"name": "Titan Bundle"},
		Circuit: types.NewCircuit("titan_circuit").WithRoot(root1),
	}

	// Item 2: Beginner offer (Level < 10)
	root2 := types.NewNode("beginner_eval").
		WithCondition(`user.level < 10`).
		Step(flux.Return(map[string]any{
			"title":  "Starter Pack",
			"reward": 50,
		}))

	item2 := &types.Item{
		ID:   "offer_starter",
		Tags: []string{"offers:boss_killer"},
		Data: map[string]any{"name": "Starter Pack"},
		Circuit: types.NewCircuit("starter_circuit").WithRoot(root2),
	}

	_ = eng.Catalog().Put(context.Background(), item1)
	_ = eng.Catalog().Put(context.Background(), item2)

	// Conduct query for a Level 15 player (Starter offer should be SILENTLY OMITTED)
	req := &types.ConductRequest{
		Tags: []string{"offers:boss_killer"},
		Context: map[string]any{
			"level": 15,
		},
		TopK: 5,
	}

	result, err := eng.Conduct(context.Background(), req)
	if err != nil {
		t.Fatalf("Conduct failed: %v", err)
	}

	if result.EvaluatedCount != 2 {
		t.Errorf("expected 2 evaluated items, got %d", result.EvaluatedCount)
	}
	if len(result.Items) != 1 {
		t.Fatalf("expected exactly 1 qualified item, got %d", len(result.Items))
	}
	if result.Items[0].ID != "offer_titan" {
		t.Errorf("expected qualified item offer_titan, got %s", result.Items[0].ID)
	}
	if result.Items[0].ComputedOutput["title"] != "Titan Slayer Bundle" {
		t.Errorf("expected dynamic title 'Titan Slayer Bundle', got %v", result.Items[0].ComputedOutput["title"])
	}
}

func BenchmarkFlux_Spark(b *testing.B) {
	eng, _ := flux.New()
	defer eng.Close()

	root := types.NewNode("bench_node").
		Step(flux.Volt(`amount >= 100.0`))
	circuit := types.NewCircuit("bench_circuit").
		WithTags("stream:bench").
		WithRoot(root)

	_ = eng.Registry().Put(context.Background(), circuit)
	evt := PaymentEvent{UserID: "u1", Amount: 500.0}
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = eng.Spark(ctx, evt, "stream:bench")
	}
}
