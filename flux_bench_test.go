package flux_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/types"
)

// setupSimpleCircuitEngine creates an engine configured with a single-node linear rule.
func setupSimpleCircuitEngine() (*flux.Engine, error) {
	eng, err := flux.New(flux.WithWorkers(runtime.NumCPU()))
	if err != nil {
		return nil, err
	}

	root := types.NewNode("simple_root").
		WithCondition("payload.amount >= 100.0").
		Step(flux.Volt("set('processed', true)")).
		Step(flux.Return(map[string]any{
			"status": "APPROVED",
			"tier":   "BASIC",
		}))

	circuit := types.NewCircuit("simple_circuit").
		WithTags("stream:simple").
		WithRoot(root)

	_ = eng.Registry().Put(context.Background(), circuit)
	return eng, nil
}

// setupAdvancedCircuitEngine creates an engine configured with a hierarchical multi-branch DAG
// executing geo calculations, card masking, cryptographic hashes, and parallel branch pruning.
func setupAdvancedCircuitEngine() (*flux.Engine, error) {
	eng, err := flux.New(flux.WithWorkers(runtime.NumCPU()))
	if err != nil {
		return nil, err
	}

	// Root: calculate distance and mask card
	root := types.NewNode("advanced_root").
		Step(flux.Volt("set('dist_km', geo.distance_km(user.lat, user.lon, payload.lat, payload.lon))")).
		Step(flux.Volt("set('masked_card', mask.card(payload.card_id))"))

	// Child A: High Anomaly
	nodeA := types.NewNode("anomaly_branch").
		WithCondition("dist_km > 500.0 && payload.amount >= 1000.0").
		Step(flux.Volt("set('audit_hash', crypto.sha256(masked_card))")).
		Step(flux.Return(map[string]any{
			"status": "FLAGGED_ANOMALY",
			"risk":   "HIGH",
			"hash":   "audit_hash",
		}))

	// Child B: High Value Standard
	nodeB := types.NewNode("high_value_branch").
		WithCondition("dist_km <= 500.0 && payload.amount >= 1000.0").
		Step(flux.Return(map[string]any{
			"status": "REQUIRES_REVIEW",
			"risk":   "MEDIUM",
		}))

	// Child C: Fast-path Cleared
	nodeC := types.NewNode("fast_path_branch").
		WithCondition("payload.amount < 1000.0").
		Step(flux.Return(map[string]any{
			"status": "AUTO_CLEARED",
			"risk":   "LOW",
		}))

	root.AddChildren(nodeA, nodeB, nodeC)

	circuit := types.NewCircuit("advanced_circuit").
		WithTags("stream:advanced").
		WithRoot(root)

	_ = eng.Registry().Put(context.Background(), circuit)
	return eng, nil
}

// setupConductCatalogEngine creates an engine with 100 candidate items registered in a category partition.
func setupConductCatalogEngine() (*flux.Engine, error) {
	eng, err := flux.New()
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		item := &types.Item{
			ID:       fmt.Sprintf("item_%03d", i),
			Category: "rewards:tier_a",
			Data:     map[string]any{"sku": fmt.Sprintf("SKU-%d", i)},
			Circuit: types.NewCircuit(fmt.Sprintf("circuit_%d", i)).WithRoot(
				types.NewNode("qualify").
					WithCondition(fmt.Sprintf("user.level >= %f", float64(i%20))).
					Step(flux.Return(map[string]any{
						"bonus": float64(i * 10),
					})),
			),
		}
		_ = eng.Catalog().Put(ctx, item)
	}

	return eng, nil
}

// BenchmarkSpark_SimpleDAG_Sequential measures single-threaded sequential execution of a simple DAG.
func BenchmarkSpark_SimpleDAG_Sequential(b *testing.B) {
	eng, err := setupSimpleCircuitEngine()
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	ctx := context.Background()
	payload := map[string]any{"amount": 250.0}

	// Warm up
	for i := 0; i < 1000; i++ {
		_, _ = eng.Spark(ctx, payload, "stream:simple")
	}

	b.ReportAllocs()

	for b.Loop() {
		_, _ = eng.Spark(ctx, payload, "stream:simple")
	}
}

// BenchmarkSpark_SimpleDAG_Parallel measures multi-threaded concurrent execution of a simple DAG across CPU cores.
func BenchmarkSpark_SimpleDAG_Parallel(b *testing.B) {
	eng, err := setupSimpleCircuitEngine()
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	ctx := context.Background()
	payload := map[string]any{"amount": 250.0}

	// Warm up
	for i := 0; i < 1000; i++ {
		_, _ = eng.Spark(ctx, payload, "stream:simple")
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = eng.Spark(ctx, payload, "stream:simple")
		}
	})
}

// BenchmarkSpark_AdvancedDAG_Sequential measures single-threaded execution of a complex multi-branch DAG
// with geo calculations, card masking, SHA-256 hashing, and parallel condition pruning.
func BenchmarkSpark_AdvancedDAG_Sequential(b *testing.B) {
	eng, err := setupAdvancedCircuitEngine()
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	ctx := context.Background()
	payload := map[string]any{
		"amount":  2500.0,
		"card_id": "4111-2222-3333-4444",
		"lat":     37.7749,
		"lon":     -122.4194,
		"user": map[string]any{
			"lat": 40.7128,
			"lon": -74.0060,
		},
	}

	// Warm up
	for i := 0; i < 1000; i++ {
		_, _ = eng.Spark(ctx, payload, "stream:advanced")
	}

	b.ReportAllocs()

	for b.Loop() {
		_, _ = eng.Spark(ctx, payload, "stream:advanced")
	}
}

// BenchmarkSpark_AdvancedDAG_Parallel measures multi-threaded concurrent execution of an advanced DAG across CPU cores.
func BenchmarkSpark_AdvancedDAG_Parallel(b *testing.B) {
	eng, err := setupAdvancedCircuitEngine()
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	ctx := context.Background()
	payload := map[string]any{
		"amount":  2500.0,
		"card_id": "4111-2222-3333-4444",
		"lat":     37.7749,
		"lon":     -122.4194,
		"user": map[string]any{
			"lat": 40.7128,
			"lon": -74.0060,
		},
	}

	// Warm up
	for i := 0; i < 1000; i++ {
		_, _ = eng.Spark(ctx, payload, "stream:advanced")
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = eng.Spark(ctx, payload, "stream:advanced")
		}
	})
}

// BenchmarkConduct_CandidateScoring_Sequential measures single-threaded Conduct evaluation across 100 items.
func BenchmarkConduct_CandidateScoring_Sequential(b *testing.B) {
	eng, err := setupConductCatalogEngine()
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	ctx := context.Background()
	req := &types.ConductRequest{
		Category: "rewards:tier_a",
		Context:  map[string]any{"level": 15.0},
		TopK:     10,
	}

	// Warm up
	for i := 0; i < 100; i++ {
		_, _ = eng.Conduct(ctx, req)
	}

	b.ReportAllocs()

	for b.Loop() {
		_, _ = eng.Conduct(ctx, req)
	}
}

// BenchmarkConduct_CandidateScoring_Parallel measures multi-threaded concurrent Conduct evaluation across 100 items.
func BenchmarkConduct_CandidateScoring_Parallel(b *testing.B) {
	eng, err := setupConductCatalogEngine()
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	ctx := context.Background()
	req := &types.ConductRequest{
		Category: "rewards:tier_a",
		Context:  map[string]any{"level": 15.0},
		TopK:     10,
	}

	// Warm up
	for i := 0; i < 100; i++ {
		_, _ = eng.Conduct(ctx, req)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = eng.Conduct(ctx, req)
		}
	})
}
