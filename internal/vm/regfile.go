package vm

import (
	"sync"
	"unsafe"
)

const (
	// MaxRegisters defines the number of registers per frame (fits in L1 CPU cache: 64 * 16B = 1024B).
	MaxRegisters = 64
	// DefaultArenaCapacity is the default size for scratch string/byte allocations per execution.
	DefaultArenaCapacity = 4096
)

// RegFile is a contiguous 1024-byte array of 64 scalar registers.
type RegFile struct {
	Registers [MaxRegisters]Value
}

// Static compile-time assertion: RegFile must be exactly 1024 bytes (1 KB).
const (
	_ = uint(unsafe.Sizeof(RegFile{}) - 1024)
	_ = uint(1024 - unsafe.Sizeof(RegFile{}))
)

// Reset zeroes out all registers in the RegFile.
func (r *RegFile) Reset() {
	for i := range r.Registers {
		r.Registers[i] = NullValue
	}
}

// FrameArena provides zero-allocation scratch memory for an execution instance.
type FrameArena struct {
	Regs    RegFile
	Arena   []byte
	ArenaLen int
	Handles []any
}

// NewFrameArena allocates a fresh FrameArena with the default capacity.
func NewFrameArena() *FrameArena {
	return &FrameArena{
		Arena:   make([]byte, DefaultArenaCapacity),
		Handles: make([]any, 0, 16),
	}
}

// Reset resets the FrameArena for reuse without re-allocating memory.
func (fa *FrameArena) Reset() {
	fa.Regs.Reset()
	fa.ArenaLen = 0
	fa.Handles = fa.Handles[:0]
}

// AllocBytes copies a slice into the Arena and returns its (offset, length).
func (fa *FrameArena) AllocBytes(src []byte) (offset, length uint32) {
	l := len(src)
	if l == 0 {
		return 0, 0
	}
	need := fa.ArenaLen + l
	if need > len(fa.Arena) {
		// Grow arena with doubling
		newCap := len(fa.Arena) * 2
		if newCap < need {
			newCap = need + 1024
		}
		newArena := make([]byte, newCap)
		copy(newArena, fa.Arena[:fa.ArenaLen])
		fa.Arena = newArena
	}
	off := fa.ArenaLen
	copy(fa.Arena[off:], src)
	fa.ArenaLen += l
	return uint32(off), uint32(l)
}

// AllocString copies a string into the Arena and returns its (offset, length).
func (fa *FrameArena) AllocString(src string) (offset, length uint32) {
	return fa.AllocBytes(unsafe.Slice(unsafe.StringData(src), len(src)))
}

// GetString extracts a string view from the Arena at the given offset and length.
func (fa *FrameArena) GetString(offset, length uint32) string {
	if length == 0 || int(offset+length) > fa.ArenaLen {
		return ""
	}
	return unsafe.String(&fa.Arena[offset], length)
}

// GetBytes extracts a byte slice view from the Arena at the given offset and length.
func (fa *FrameArena) GetBytes(offset, length uint32) []byte {
	if length == 0 || int(offset+length) > fa.ArenaLen {
		return nil
	}
	return fa.Arena[offset : offset+length]
}

// AllocHandle stores a host object and returns its handle index.
func (fa *FrameArena) AllocHandle(obj any) uint32 {
	idx := len(fa.Handles)
	fa.Handles = append(fa.Handles, obj)
	return uint32(idx)
}

// GetHandle retrieves a host object by handle index.
func (fa *FrameArena) GetHandle(idx uint32) any {
	if int(idx) < len(fa.Handles) {
		return fa.Handles[idx]
	}
	return nil
}

// FramePool manages a pre-warmed pool of reusable FrameArena instances.
var FramePool = sync.Pool{
	New: func() any {
		return NewFrameArena()
	},
}

// AcquireFrame borrows a reset FrameArena from the pool.
func AcquireFrame() *FrameArena {
	fa := FramePool.Get().(*FrameArena)
	fa.Reset()
	return fa
}

// ReleaseFrame returns a FrameArena to the pool for reuse.
func ReleaseFrame(fa *FrameArena) {
	if fa != nil {
		FramePool.Put(fa)
	}
}
