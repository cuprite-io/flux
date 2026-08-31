package registry

import (
	"context"
	"errors"
	"fmt"
	"hash/crc64"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/vm"
	"github.com/cuprite-io/flux/types"
)

var (
	ErrCircuitNotFound  = errors.New("flux registry: circuit not found")
	ErrCorruptFBWFHeader = errors.New("flux registry: invalid or corrupt FBWF binary header")
	ErrChecksumMismatch  = errors.New("flux registry: FBWF CRC64 checksum mismatch")
)

const (
	// FBWFMagic: "FLUX" in ASCII (0x464C5558)
	FBWFMagic uint32 = 0x464C5558
	// FBWFVersion is the current binary format version.
	FBWFVersion uint16 = 1
	// FBWFHeaderSize is the fixed 32-byte header size.
	FBWFHeaderSize = 32
)

var crcTable = crc64.MakeTable(crc64.ECMA)

// FBWFHeader represents the 32-byte fixed binary header of a compiled Circuit.
type FBWFHeader struct {
	Magic            uint32  // 4 B: 0x464C5558 ('FLUX')
	Version          uint16  // 2 B: Wire format version
	Flags            uint16  // 2 B: Compression / encoding flags
	StepCount        uint32  // 4 B: Number of contiguous 64-byte Steps
	ConstPoolLen     uint32  // 4 B: Byte length of constant string arena pool
	StringTableCount uint32  // 4 B: Count of string entries
	Reserved         uint32  // 4 B: Future expansion padding
	CRC64Checksum    uint64  // 8 B: ECMA-182 polynomial checksum
}

// Static compile-time assertion: FBWFHeader must be exactly 32 bytes.
const (
	_ = uint(unsafe.Sizeof(FBWFHeader{}) - 32)
	_ = uint(32 - unsafe.Sizeof(FBWFHeader{}))
)

// EncodeFBWF serializes a compiled *vm.Program into a contiguous FBWF binary byte slice.
func EncodeFBWF(prog *vm.Program) []byte {
	numSteps := len(prog.Steps)
	constLen := len(prog.ConstantPool)
	strTableLen := len(prog.StringTable)

	// Calculate total bytes: 32B Header + StringTable + ConstantPool + Steps(64B * N)
	stepsSize := numSteps * 64
	totalSize := FBWFHeaderSize + constLen + stepsSize

	buf := make([]byte, totalSize)

	// Write Constant Pool
	constOffset := FBWFHeaderSize
	copy(buf[constOffset:constOffset+constLen], prog.ConstantPool)

	// Write Steps
	stepsOffset := constOffset + constLen
	for i := 0; i < numSteps; i++ {
		stepBytes := (*[64]byte)(unsafe.Pointer(&prog.Steps[i]))
		copy(buf[stepsOffset+i*64:stepsOffset+(i+1)*64], stepBytes[:])
	}

	// Compute Checksum over payload (after header)
	h := crc64.New(crcTable)
	h.Write(buf[FBWFHeaderSize:])
	checksum := h.Sum64()

	// Write Header
	hdr := FBWFHeader{
		Magic:            FBWFMagic,
		Version:          FBWFVersion,
		Flags:            0,
		StepCount:        uint32(numSteps),
		ConstPoolLen:     uint32(constLen),
		StringTableCount: uint32(strTableLen),
		Reserved:         0,
		CRC64Checksum:    checksum,
	}

	hdrBytes := (*[32]byte)(unsafe.Pointer(&hdr))
	copy(buf[:32], hdrBytes[:])

	return buf
}

// DecodeFBWF deserializes a byte slice into an immutable *vm.Program with zero heap copying.
func DecodeFBWF(id string, data []byte) (*vm.Program, error) {
	if len(data) < FBWFHeaderSize {
		return nil, ErrCorruptFBWFHeader
	}

	hdr := (*FBWFHeader)(unsafe.Pointer(&data[0]))
	if hdr.Magic != FBWFMagic {
		return nil, fmt.Errorf("%w: invalid magic 0x%X", ErrCorruptFBWFHeader, hdr.Magic)
	}

	// Verify CRC64 Checksum
	h := crc64.New(crcTable)
	h.Write(data[FBWFHeaderSize:])
	if h.Sum64() != hdr.CRC64Checksum {
		return nil, ErrChecksumMismatch
	}

	constLen := int(hdr.ConstPoolLen)
	numSteps := int(hdr.StepCount)
	expectedSize := FBWFHeaderSize + constLen + numSteps*64
	if len(data) < expectedSize {
		return nil, fmt.Errorf("%w: payload truncated (expected %d, got %d)", ErrCorruptFBWFHeader, expectedSize, len(data))
	}

	constOffset := FBWFHeaderSize
	constPool := data[constOffset : constOffset+constLen]

	stepsOffset := constOffset + constLen
	steps := make([]vm.Step, numSteps)
	for i := 0; i < numSteps; i++ {
		stepPtr := (*vm.Step)(unsafe.Pointer(&data[stepsOffset+i*64]))
		steps[i] = *stepPtr
	}

	return vm.NewProgram(id, steps, nil, constPool, nil), nil
}

// RegistrySnapshot represents an immutable point-in-time snapshot of registered Circuits.
type RegistrySnapshot struct {
	circuits map[string]*types.Circuit
	tagIndex map[string][]string // tag -> []circuitID
}

// Registry manages standalone Circuit storage, tag indexing, and atomic hot-swapping.
type Registry struct {
	cacheBackend cache.CacheBackend
	mu           sync.RWMutex
	current      atomic.Pointer[RegistrySnapshot]
}

// New creates a new Registry connected to an optional CacheBackend.
func New(backend cache.CacheBackend) *Registry {
	r := &Registry{
		cacheBackend: backend,
	}
	r.current.Store(&RegistrySnapshot{
		circuits: make(map[string]*types.Circuit),
		tagIndex: make(map[string][]string),
	})
	return r
}

// Put registers or updates a Circuit with zero-downtime atomic hot-swapping.
func (r *Registry) Put(ctx context.Context, circuit *types.Circuit) error {
	if circuit == nil || circuit.ID == "" {
		return errors.New("flux registry: circuit ID required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	cur := r.current.Load()

	// Clone map for copy-on-write snapshot
	newCircuits := make(map[string]*types.Circuit, len(cur.circuits)+1)
	for k, v := range cur.circuits {
		newCircuits[k] = v
	}
	newCircuits[circuit.ID] = circuit

	// Rebuild tag index
	newTagIndex := make(map[string][]string)
	for id, c := range newCircuits {
		for _, tag := range c.Tags {
			newTagIndex[tag] = append(newTagIndex[tag], id)
		}
	}

	// Atomically hot-swap the pointer (Spark execution threads see new state with 0 locks)
	r.current.Store(&RegistrySnapshot{
		circuits: newCircuits,
		tagIndex: newTagIndex,
	})

	// Optionally persist to CacheBackend if present
	if r.cacheBackend != nil {
		_ = r.cacheBackend.Set(ctx, "circuit:"+circuit.ID, circuit, 0)
	}

	return nil
}

// Get retrieves a Circuit by its ID.
func (r *Registry) Get(ctx context.Context, id string) (*types.Circuit, error) {
	snap := r.current.Load()
	c, ok := snap.circuits[id]
	if !ok {
		return nil, ErrCircuitNotFound
	}
	return c, nil
}

// GetMatching retrieves all Circuits that match any of the provided query tags.
func (r *Registry) GetMatching(ctx context.Context, tags ...string) []*types.Circuit {
	if len(tags) == 0 {
		return nil
	}

	snap := r.current.Load()
	matchedIDs := make(map[string]struct{})

	for _, tag := range tags {
		if ids, ok := snap.tagIndex[tag]; ok {
			for _, id := range ids {
				matchedIDs[id] = struct{}{}
			}
		}
	}

	if len(matchedIDs) == 0 {
		return nil
	}

	res := make([]*types.Circuit, 0, len(matchedIDs))
	for id := range matchedIDs {
		if c, ok := snap.circuits[id]; ok {
			res = append(res, c)
		}
	}
	return res
}

// Delete removes a Circuit by ID.
func (r *Registry) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cur := r.current.Load()
	if _, ok := cur.circuits[id]; !ok {
		return ErrCircuitNotFound
	}

	newCircuits := make(map[string]*types.Circuit, len(cur.circuits))
	for k, v := range cur.circuits {
		if k != id {
			newCircuits[k] = v
		}
	}

	newTagIndex := make(map[string][]string)
	for cID, c := range newCircuits {
		for _, tag := range c.Tags {
			newTagIndex[tag] = append(newTagIndex[tag], cID)
		}
	}

	r.current.Store(&RegistrySnapshot{
		circuits: newCircuits,
		tagIndex: newTagIndex,
	})

	if r.cacheBackend != nil {
		_ = r.cacheBackend.Delete(ctx, "circuit:"+id)
	}

	return nil
}
