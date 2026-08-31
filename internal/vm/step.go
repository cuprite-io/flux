package vm

import "unsafe"

// OpCode defines a virtual instruction executed by FluxVM.
type OpCode uint8

const (
	OpNop OpCode = iota

	// Register & Constant Movement
	OpMov      // Move R[dst] = R[src]
	OpLoadK    // Load R[dst] = ConstantPool[AuxIdx]
	OpLoadNull // Load R[dst] = Null

	// Arithmetic (Integer & Float)
	OpAddInt
	OpSubInt
	OpMulInt
	OpDivInt
	OpModInt
	OpNegInt

	OpAddFloat
	OpSubFloat
	OpMulFloat
	OpDivFloat
	OpNegFloat

	// Comparisons
	OpEq
	OpNeq
	OpLtInt
	OpLteInt
	OpGtInt
	OpGteInt

	OpLtFloat
	OpLteFloat
	OpGtFloat
	OpGteFloat

	OpEqStr
	OpNeqStr

	// Logical & Bitwise
	OpNot
	OpAnd
	OpOr
	OpBitAnd
	OpBitOr
	OpBitXor
	OpBitShl
	OpBitShr

	// Control Flow & Branching
	OpJump        // Unconditional jump by BranchOffset
	OpJumpIfTrue  // Jump if R[InRegs[0]] is Truthy
	OpJumpIfFalse // Jump if R[InRegs[0]] is Falsy
	OpBranchGuard // If R[InRegs[0]] is false, set DeadMask bit and skip

	// State & Context Interaction
	OpStateGet // Read variable from StateContext.Scratchpad/Input into R[dst]
	OpStateSet // Write R[src] to StateContext.Scratchpad[key]

	// Function Invocation
	OpCall // Invoke native operator function at FnPtr / AuxIdx

	// Terminal Instructions
	OpReturn // Emit StepReturn output
	OpAbort  // Terminate execution with abort status
	OpHalt   // Normal execution termination
)

// StepFlags modifies instruction execution behavior.
type StepFlags uint8

const (
	FlagNone StepFlags = 0
	// FlagShortCircuit skips downstream evaluation on boolean identity.
	FlagShortCircuit StepFlags = 1 << iota
	// FlagSideEffect marks instruction as having external side-effects.
	FlagSideEffect
)

// Step occupies exactly 64 bytes (1 CPU cache line).
// It contains all parameters needed to execute a virtual instruction without pointer chasing.
type Step struct {
	Op           OpCode    // 1 B: Virtual instruction opcode
	Flags        StepFlags // 1 B: Execution flags
	InDegree     uint8     // 1 B: Unresolved prerequisites
	OutDegree    uint8     // 1 B: Dependents count
	InRegs       [3]uint8  // 3 B: Input register indices (R0..R63)
	OutReg       uint8     // 1 B: Output register index (R0..R63)
	NeedMask     uint64    // 8 B: Bitmask of prerequisite Step/Node IDs
	NotDeadMask  uint64    // 8 B: Bitmask of branch liveness (skipped if dead)
	AuxIdx       uint32    // 4 B: Index into constant pool, bytecode array, or string table
	BranchOffset int32     // 4 B: Fast-forward step jump offset
	FnPtr        uintptr   // 8 B: Direct function pointer for native extensions
	UserData     [24]byte  // 24 B: Inline parameters, numeric thresholds, and lookup cache
}

// Static compile-time assertion: Step must be exactly 64 bytes (1 CPU cache line).
const (
	_ = uint(unsafe.Sizeof(Step{}) - 64)
	_ = uint(64 - unsafe.Sizeof(Step{}))
)
