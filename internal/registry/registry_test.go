package registry_test

import (
	"context"
	"testing"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/registry"
	"github.com/cuprite-io/flux/internal/vm"
	"github.com/cuprite-io/flux/types"
)

func TestFBWF_EncodeDecode(t *testing.T) {
	steps := []vm.Step{
		{Op: vm.OpAddInt, InRegs: [3]uint8{0, 1, 0}, OutReg: 2},
		{Op: vm.OpReturn, InRegs: [3]uint8{2, 0, 0}},
	}
	constPool := []byte("flux_test_constants")
	prog := vm.NewProgram("test_circuit_1", steps, nil, constPool, nil)

	// Encode to FBWF binary
	encoded := registry.EncodeFBWF(prog)
	if len(encoded) < 32 {
		t.Fatalf("encoded FBWF binary too small: %d bytes", len(encoded))
	}

	// Decode back
	decoded, err := registry.DecodeFBWF("test_circuit_1", encoded)
	if err != nil {
		t.Fatalf("failed to decode FBWF binary: %v", err)
	}

	if len(decoded.Steps) != len(steps) {
		t.Errorf("expected %d steps, got %d", len(steps), len(decoded.Steps))
	}
	if string(decoded.ConstantPool) != string(constPool) {
		t.Errorf("expected const pool %s, got %s", constPool, decoded.ConstantPool)
	}
	if decoded.Steps[0].Op != vm.OpAddInt {
		t.Errorf("expected first step OpAddInt, got %v", decoded.Steps[0].Op)
	}
}

func TestRegistry_HotSwappingAndTags(t *testing.T) {
	memCache := cache.NewMemoryCache()
	reg := registry.New(memCache)

	c1 := types.NewCircuit("c1").WithTags("payments:us", "tier:gold")
	c2 := types.NewCircuit("c2").WithTags("payments:eu", "tier:gold")
	c3 := types.NewCircuit("c3").WithTags("payments:asia")

	_ = reg.Put(context.Background(), c1)
	_ = reg.Put(context.Background(), c2)
	_ = reg.Put(context.Background(), c3)

	// Query matching tags
	goldCircuits := reg.GetMatching(context.Background(), "tier:gold")
	if len(goldCircuits) != 2 {
		t.Errorf("expected 2 circuits for tier:gold, got %d", len(goldCircuits))
	}

	asiaCircuits := reg.GetMatching(context.Background(), "payments:asia")
	if len(asiaCircuits) != 1 || asiaCircuits[0].ID != "c3" {
		t.Errorf("expected circuit c3 for payments:asia")
	}

	circuitC2, err := reg.Get(context.Background(), "c2")
	if err != nil || circuitC2 == nil || circuitC2.ID != "c2" {
		t.Fatalf("expected to get c2, got err: %v", err)
	}

	_, err = reg.Get(context.Background(), "non_existent")
	if err == nil {
		t.Errorf("expected error for non existent circuit, got nil")
	}

	// Hot-swap delete c1
	_ = reg.Delete(context.Background(), "c1")
	goldAfterDelete := reg.GetMatching(context.Background(), "tier:gold")
	if len(goldAfterDelete) != 1 || goldAfterDelete[0].ID != "c2" {
		t.Errorf("expected only c2 for tier:gold after deleting c1")
	}
}

func TestRegistry_TagReconciliationOnPut(t *testing.T) {
	memCache := cache.NewMemoryCache()
	reg := registry.New(memCache)

	// 1. Initial Put with tags tagA and tagB
	c := types.NewCircuit("c_dyn").WithTags("tagA", "tagB")
	if err := reg.Put(context.Background(), c); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	if len(reg.GetMatching(context.Background(), "tagA")) != 1 {
		t.Errorf("expected 1 match for tagA")
	}
	if len(reg.GetMatching(context.Background(), "tagB")) != 1 {
		t.Errorf("expected 1 match for tagB")
	}

	// 2. Re-register c_dyn with ONLY tagC (tagA and tagB should be reconciled/purged)
	cUpdated := types.NewCircuit("c_dyn").WithTags("tagC")
	if err := reg.Put(context.Background(), cUpdated); err != nil {
		t.Fatalf("updated put failed: %v", err)
	}

	if len(reg.GetMatching(context.Background(), "tagA")) != 0 {
		t.Errorf("expected 0 matches for stale tagA after update")
	}
	if len(reg.GetMatching(context.Background(), "tagB")) != 0 {
		t.Errorf("expected 0 matches for stale tagB after update")
	}
	if len(reg.GetMatching(context.Background(), "tagC")) != 1 {
		t.Errorf("expected 1 match for new tagC")
	}

	// 3. Delete c_dyn and verify tagC is cleaned up
	if err := reg.Delete(context.Background(), "c_dyn"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if len(reg.GetMatching(context.Background(), "tagC")) != 0 {
		t.Errorf("expected 0 matches for tagC after delete")
	}
}

func BenchmarkFBWF_Decode(b *testing.B) {
	steps := []vm.Step{
		{Op: vm.OpAddInt, InRegs: [3]uint8{0, 1, 0}, OutReg: 2},
		{Op: vm.OpMulInt, InRegs: [3]uint8{2, 1, 0}, OutReg: 3},
		{Op: vm.OpGtInt, InRegs: [3]uint8{3, 0, 0}, OutReg: 4},
		{Op: vm.OpReturn, InRegs: [3]uint8{4, 0, 0}},
	}
	prog := vm.NewProgram("bench_prog", steps, nil, []byte("benchmark_constants"), nil)
	encoded := registry.EncodeFBWF(prog)

	b.ReportAllocs()

	for b.Loop() {
		_, err := registry.DecodeFBWF("bench_prog", encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
