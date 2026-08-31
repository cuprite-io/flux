package vm_test

import (
	"testing"

	"github.com/cuprite-io/flux/internal/vm"
)

type mockVMContext struct {
	state  map[string]any
	pruned uint64
}

func (m *mockVMContext) GetState(key string) (any, bool) {
	v, ok := m.state[key]
	return v, ok
}

func (m *mockVMContext) SetState(key string, val any) {
	m.state[key] = val
}

func (m *mockVMContext) CallNative(fnIdx uint32, args []vm.Value, frame *vm.FrameArena) (vm.Value, error) {
	return vm.NullValue, nil
}

func (m *mockVMContext) IsPruned(bit uint64) bool {
	return (m.pruned & bit) != 0
}

func (m *mockVMContext) SetPruned(bit uint64) {
	m.pruned |= bit
}

func TestFluxVM_Arithmetic(t *testing.T) {
	// Program: R[2] = R[0] + R[1]; R[3] = R[2] * R[1]; return R[3]
	steps := []vm.Step{
		{Op: vm.OpAddInt, InRegs: [3]uint8{0, 1, 0}, OutReg: 2},
		{Op: vm.OpMulInt, InRegs: [3]uint8{2, 1, 0}, OutReg: 3},
		{Op: vm.OpReturn, InRegs: [3]uint8{3, 0, 0}},
	}
	prog := vm.NewProgram("arithmetic_test", steps, nil, nil, nil)

	frame := vm.AcquireFrame()
	defer vm.ReleaseFrame(frame)

	frame.Regs.Registers[0] = vm.NewInt64(10)
	frame.Regs.Registers[1] = vm.NewInt64(5)

	res, err := vm.Run(prog, frame, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// (10 + 5) * 5 = 75
	if res.Int64() != 75 {
		t.Errorf("expected 75, got %d", res.Int64())
	}
}

func TestFluxVM_BranchingAndPruning(t *testing.T) {
	// Program:
	// R[2] = R[0] > R[1]  (100 > 50 -> true)
	// BranchGuard: if !R[2], prune with NeedMask=0x01 and jump 2 steps
	// R[3] = R[0] + 10
	// return R[3]
	steps := []vm.Step{
		{Op: vm.OpGtInt, InRegs: [3]uint8{0, 1, 0}, OutReg: 2},
		{Op: vm.OpBranchGuard, InRegs: [3]uint8{2, 0, 0}, NeedMask: 0x01, BranchOffset: 2},
		{Op: vm.OpAddInt, InRegs: [3]uint8{0, 0, 0}, OutReg: 3, AuxIdx: 0}, // Placeholder
		{Op: vm.OpReturn, InRegs: [3]uint8{3, 0, 0}},
	}
	prog := vm.NewProgram("branch_test", steps, nil, nil, nil)

	frame := vm.AcquireFrame()
	defer vm.ReleaseFrame(frame)

	frame.Regs.Registers[0] = vm.NewInt64(100)
	frame.Regs.Registers[1] = vm.NewInt64(50)

	mctx := &mockVMContext{state: make(map[string]any)}
	_, err := vm.Run(prog, frame, mctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mctx.pruned != 0 {
		t.Errorf("expected pruned=0, got %d", mctx.pruned)
	}
}

func BenchmarkFluxVM_Execution(b *testing.B) {
	// 5-step arithmetic and comparison pipeline
	steps := []vm.Step{
		{Op: vm.OpAddInt, InRegs: [3]uint8{0, 1, 0}, OutReg: 2},
		{Op: vm.OpMulInt, InRegs: [3]uint8{2, 1, 0}, OutReg: 3},
		{Op: vm.OpGtInt, InRegs: [3]uint8{3, 0, 0}, OutReg: 4},
		{Op: vm.OpReturn, InRegs: [3]uint8{4, 0, 0}},
	}
	prog := vm.NewProgram("bench_prog", steps, nil, nil, nil)

	frame := vm.AcquireFrame()
	defer vm.ReleaseFrame(frame)

	frame.Regs.Registers[0] = vm.NewInt64(42)
	frame.Regs.Registers[1] = vm.NewInt64(10)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res, err := vm.Run(prog, frame, nil)
		if err != nil || !res.Bool() {
			b.Fatal("execution failed")
		}
	}
}
