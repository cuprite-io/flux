package compiler

import (
	"errors"
	"fmt"

	"github.com/cuprite-io/flux/internal/vm"
)

var (
	ErrEmptyProgram         = errors.New("flux verifier: program contains no steps")
	ErrRegisterOutOfBounds  = errors.New("flux verifier: register index exceeds max 64")
	ErrInvalidOpCodeBounds  = errors.New("flux verifier: unknown opcode")
	ErrCorruptChecksum      = errors.New("flux verifier: program CRC64 checksum verification failed")
)

// Verifier validates bytecode programs for bounds safety, register limits, and checksum integrity.
type Verifier struct{}

// NewVerifier creates a new Verifier.
func NewVerifier() *Verifier {
	return &Verifier{}
}

// Verify checks a compiled *vm.Program for instruction and memory safety.
func (v *Verifier) Verify(prog *vm.Program) error {
	if prog == nil || len(prog.Steps) == 0 {
		return ErrEmptyProgram
	}

	// Verify CRC64 Checksum if present
	if prog.CRC64 != 0 && !prog.VerifyCRC64() {
		return ErrCorruptChecksum
	}

	numSteps := len(prog.Steps)
	for i, step := range prog.Steps {
		// Verify Register Bounds (< 64)
		if step.OutReg >= vm.MaxRegisters {
			return fmt.Errorf("%w: out register R%d at step %d", ErrRegisterOutOfBounds, step.OutReg, i)
		}
		for rIdx, inReg := range step.InRegs {
			if inReg >= vm.MaxRegisters {
				return fmt.Errorf("%w: in register R%d (slot %d) at step %d", ErrRegisterOutOfBounds, inReg, rIdx, i)
			}
		}

		// Verify Jump Offsets
		if step.BranchOffset != 0 {
			targetPC := i + int(step.BranchOffset)
			if targetPC < 0 || targetPC > numSteps {
				return fmt.Errorf("flux verifier: jump offset %d at step %d points out of bounds (target %d, total %d)", step.BranchOffset, i, targetPC, numSteps)
			}
		}
	}

	return nil
}
