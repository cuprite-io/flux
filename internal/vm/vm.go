package vm

import (
	"errors"
	"fmt"
)

var (
	ErrDivisionByZero  = errors.New("fluxvm: division by zero")
	ErrInvalidOpCode   = errors.New("fluxvm: invalid opcode")
	ErrExecutionAborted = errors.New("fluxvm: execution aborted")
	ErrStackOverflow   = errors.New("fluxvm: maximum step limit exceeded")
)

// VMContext provides runtime environmental context to a FluxVM execution instance.
type VMContext interface {
	GetState(key string) (any, bool)
	SetState(key string, val any)
	CallNative(fnIdx uint32, args []Value, frame *FrameArena) (Value, error)
	IsPruned(bit uint64) bool
	SetPruned(bit uint64)
}

// Run executes a compiled Program within the provided FrameArena and VMContext.
func Run(prog *Program, frame *FrameArena, vctx VMContext) (Value, error) {
	steps := prog.Steps
	numSteps := len(steps)
	regs := &frame.Regs.Registers

	pc := 0
	for pc < numSteps {
		step := &steps[pc]

		// Bitmask Pruning Check: If step's branch is pruned, skip it
		if step.NotDeadMask != 0 && vctx != nil && vctx.IsPruned(step.NotDeadMask) {
			if step.BranchOffset > 0 {
				pc += int(step.BranchOffset)
			} else {
				pc++
			}
			continue
		}

		switch step.Op {
		case OpNop:
			pc++

		// --- Movement & Constants ---
		case OpMov:
			regs[step.OutReg] = regs[step.InRegs[0]]
			pc++

		case OpLoadK:
			if int(step.AuxIdx) < len(prog.Constants) {
				regs[step.OutReg] = prog.Constants[step.AuxIdx]
			}
			pc++

		case OpLoadNull:
			regs[step.OutReg] = NullValue
			pc++

		// --- Integer Arithmetic ---
		case OpAddInt:
			regs[step.OutReg] = NewInt64(regs[step.InRegs[0]].Int64() + regs[step.InRegs[1]].Int64())
			pc++

		case OpSubInt:
			regs[step.OutReg] = NewInt64(regs[step.InRegs[0]].Int64() - regs[step.InRegs[1]].Int64())
			pc++

		case OpMulInt:
			regs[step.OutReg] = NewInt64(regs[step.InRegs[0]].Int64() * regs[step.InRegs[1]].Int64())
			pc++

		case OpDivInt:
			divisor := regs[step.InRegs[1]].Int64()
			if divisor == 0 {
				return NullValue, ErrDivisionByZero
			}
			regs[step.OutReg] = NewInt64(regs[step.InRegs[0]].Int64() / divisor)
			pc++

		case OpModInt:
			divisor := regs[step.InRegs[1]].Int64()
			if divisor == 0 {
				return NullValue, ErrDivisionByZero
			}
			regs[step.OutReg] = NewInt64(regs[step.InRegs[0]].Int64() % divisor)
			pc++

		case OpNegInt:
			regs[step.OutReg] = NewInt64(-regs[step.InRegs[0]].Int64())
			pc++

		// --- Float Arithmetic ---
		case OpAddFloat:
			regs[step.OutReg] = NewFloat64(regs[step.InRegs[0]].Float64() + regs[step.InRegs[1]].Float64())
			pc++

		case OpSubFloat:
			regs[step.OutReg] = NewFloat64(regs[step.InRegs[0]].Float64() - regs[step.InRegs[1]].Float64())
			pc++

		case OpMulFloat:
			regs[step.OutReg] = NewFloat64(regs[step.InRegs[0]].Float64() * regs[step.InRegs[1]].Float64())
			pc++

		case OpDivFloat:
			regs[step.OutReg] = NewFloat64(regs[step.InRegs[0]].Float64() / regs[step.InRegs[1]].Float64())
			pc++

		case OpNegFloat:
			regs[step.OutReg] = NewFloat64(-regs[step.InRegs[0]].Float64())
			pc++

		// --- Integer Comparisons ---
		case OpEq:
			v1, v2 := regs[step.InRegs[0]], regs[step.InRegs[1]]
			regs[step.OutReg] = NewBool(v1.Tag == v2.Tag && v1.Raw == v2.Raw)
			pc++

		case OpNeq:
			v1, v2 := regs[step.InRegs[0]], regs[step.InRegs[1]]
			regs[step.OutReg] = NewBool(v1.Tag != v2.Tag || v1.Raw != v2.Raw)
			pc++

		case OpLtInt:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].Int64() < regs[step.InRegs[1]].Int64())
			pc++

		case OpLteInt:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].Int64() <= regs[step.InRegs[1]].Int64())
			pc++

		case OpGtInt:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].Int64() > regs[step.InRegs[1]].Int64())
			pc++

		case OpGteInt:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].Int64() >= regs[step.InRegs[1]].Int64())
			pc++

		// --- Float Comparisons ---
		case OpLtFloat:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].Float64() < regs[step.InRegs[1]].Float64())
			pc++

		case OpLteFloat:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].Float64() <= regs[step.InRegs[1]].Float64())
			pc++

		case OpGtFloat:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].Float64() > regs[step.InRegs[1]].Float64())
			pc++

		case OpGteFloat:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].Float64() >= regs[step.InRegs[1]].Float64())
			pc++

		// --- String Comparisons ---
		case OpEqStr:
			s1 := frame.GetString(regs[step.InRegs[0]].ArenaView())
			s2 := frame.GetString(regs[step.InRegs[1]].ArenaView())
			regs[step.OutReg] = NewBool(s1 == s2)
			pc++

		case OpNeqStr:
			s1 := frame.GetString(regs[step.InRegs[0]].ArenaView())
			s2 := frame.GetString(regs[step.InRegs[1]].ArenaView())
			regs[step.OutReg] = NewBool(s1 != s2)
			pc++

		// --- Logical & Bitwise ---
		case OpNot:
			regs[step.OutReg] = NewBool(!regs[step.InRegs[0]].IsTruthy())
			pc++

		case OpAnd:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].IsTruthy() && regs[step.InRegs[1]].IsTruthy())
			pc++

		case OpOr:
			regs[step.OutReg] = NewBool(regs[step.InRegs[0]].IsTruthy() || regs[step.InRegs[1]].IsTruthy())
			pc++

		case OpBitAnd:
			regs[step.OutReg] = NewUint64(regs[step.InRegs[0]].Uint64() & regs[step.InRegs[1]].Uint64())
			pc++

		case OpBitOr:
			regs[step.OutReg] = NewUint64(regs[step.InRegs[0]].Uint64() | regs[step.InRegs[1]].Uint64())
			pc++

		case OpBitXor:
			regs[step.OutReg] = NewUint64(regs[step.InRegs[0]].Uint64() ^ regs[step.InRegs[1]].Uint64())
			pc++

		case OpBitShl:
			regs[step.OutReg] = NewUint64(regs[step.InRegs[0]].Uint64() << regs[step.InRegs[1]].Uint64())
			pc++

		case OpBitShr:
			regs[step.OutReg] = NewUint64(regs[step.InRegs[0]].Uint64() >> regs[step.InRegs[1]].Uint64())
			pc++

		// --- Control Flow ---
		case OpJump:
			pc += int(step.BranchOffset)

		case OpJumpIfTrue:
			if regs[step.InRegs[0]].IsTruthy() {
				pc += int(step.BranchOffset)
			} else {
				pc++
			}

		case OpJumpIfFalse:
			if !regs[step.InRegs[0]].IsTruthy() {
				pc += int(step.BranchOffset)
			} else {
				pc++
			}

		case OpBranchGuard:
			cond := regs[step.InRegs[0]].IsTruthy()
			if !cond {
				if vctx != nil && step.NeedMask != 0 {
					vctx.SetPruned(step.NeedMask)
				}
				if step.BranchOffset != 0 {
					pc += int(step.BranchOffset)
				} else {
					pc++
				}
			} else {
				pc++
			}

		// --- State Operations ---
		case OpStateGet:
			key := prog.ConstantString(step.AuxIdx)
			if vctx != nil {
				if val, ok := vctx.GetState(key); ok {
					regs[step.OutReg] = ToValue(val, frame)
				} else {
					regs[step.OutReg] = NullValue
				}
			} else {
				regs[step.OutReg] = NullValue
			}
			pc++

		case OpStateSet:
			key := prog.ConstantString(step.AuxIdx)
			if vctx != nil {
				vctx.SetState(key, FromValue(regs[step.InRegs[0]], frame))
			}
			pc++

		// --- Function Invocation ---
		case OpCall:
			if vctx != nil {
				numArgs := int(step.InDegree)
				args := make([]Value, numArgs)
				for i := 0; i < numArgs && i < 3; i++ {
					args[i] = regs[step.InRegs[i]]
				}
				res, err := vctx.CallNative(step.AuxIdx, args, frame)
				if err != nil {
					return NullValue, err
				}
				regs[step.OutReg] = res
			}
			pc++

		// --- Terminal ---
		case OpReturn:
			return regs[step.InRegs[0]], nil

		case OpAbort:
			return NullValue, ErrExecutionAborted

		case OpHalt:
			return regs[step.OutReg], nil

		default:
			return NullValue, fmt.Errorf("%w: %d", ErrInvalidOpCode, step.Op)
		}
	}

	return regs[0], nil
}

// ToValue converts an arbitrary Go value into a zero-allocation 16-byte Value.
func ToValue(v any, frame *FrameArena) Value {
	if v == nil {
		return NullValue
	}
	switch val := v.(type) {
	case bool:
		return NewBool(val)
	case int:
		return NewInt64(int64(val))
	case int8:
		return NewInt64(int64(val))
	case int16:
		return NewInt64(int64(val))
	case int32:
		return NewInt64(int64(val))
	case int64:
		return NewInt64(val)
	case uint:
		return NewUint64(uint64(val))
	case uint8:
		return NewUint64(uint64(val))
	case uint16:
		return NewUint64(uint64(val))
	case uint32:
		return NewUint64(uint64(val))
	case uint64:
		return NewUint64(val)
	case float32:
		return NewFloat64(float64(val))
	case float64:
		return NewFloat64(val)
	case string:
		off, l := frame.AllocString(val)
		return NewStringView(off, l)
	case []byte:
		off, l := frame.AllocBytes(val)
		return NewBytesView(off, l)
	default:
		h := frame.AllocHandle(v)
		return NewHandle(h)
	}
}

// FromValue converts a 16-byte Value back to a Go native interface.
func FromValue(v Value, frame *FrameArena) any {
	switch v.Tag {
	case TagNull:
		return nil
	case TagBool:
		return v.Bool()
	case TagInt64:
		return v.Int64()
	case TagUint64:
		return v.Uint64()
	case TagFloat64:
		return v.Float64()
	case TagStringView:
		off, len := v.ArenaView()
		return frame.GetString(off, len)
	case TagBytesView:
		off, len := v.ArenaView()
		return frame.GetBytes(off, len)
	case TagHandle:
		return frame.GetHandle(v.Handle())
	default:
		return nil
	}
}
