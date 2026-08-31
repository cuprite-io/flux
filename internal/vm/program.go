package vm

import (
	"hash/crc64"
	"unsafe"
)

var crcTable = crc64.MakeTable(crc64.ECMA)

// Program represents an immutable compiled bytecode artifact ready for execution.
type Program struct {
	ID           string
	Steps        []Step
	ConstantPool []byte
	Constants    []Value
	StringTable  []string
	CRC64        uint64
}

// NewProgram creates a new Program with the given instructions and constant pool.
func NewProgram(id string, steps []Step, constants []Value, constPool []byte, stringTable []string) *Program {
	p := &Program{
		ID:           id,
		Steps:        steps,
		Constants:    constants,
		ConstantPool: constPool,
		StringTable:  stringTable,
	}
	p.CRC64 = p.ComputeCRC64()
	return p
}

// ComputeCRC64 calculates the structural checksum of the Program.
func (p *Program) ComputeCRC64() uint64 {
	h := crc64.New(crcTable)
	h.Write([]byte(p.ID))
	h.Write(p.ConstantPool)
	for i := range p.Steps {
		stepBytes := (*[64]byte)(unsafe.Pointer(&p.Steps[i]))
		h.Write(stepBytes[:])
	}
	return h.Sum64()
}

// VerifyCRC64 returns true if the Program's internal checksum is valid.
func (p *Program) VerifyCRC64() bool {
	return p.CRC64 == p.ComputeCRC64()
}

// ConstantString retrieves a string constant from the Program's string table or constant pool.
func (p *Program) ConstantString(idx uint32) string {
	if int(idx) < len(p.StringTable) {
		return p.StringTable[idx]
	}
	return ""
}
