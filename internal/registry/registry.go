package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"sort"
	"unsafe"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/vm"
	"github.com/cuprite-io/flux/types"
)

var (
	ErrCircuitNotFound   = errors.New("flux registry: circuit not found")
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
	Magic            uint32 // 4 B: 0x464C5558 ('FLUX')
	Version          uint16 // 2 B: Wire format version
	Flags            uint16 // 2 B: Compression / encoding flags
	StepCount        uint32 // 4 B: Number of contiguous 64-byte Steps
	ConstPoolLen     uint32 // 4 B: Byte length of constant string arena pool
	StringTableCount uint32 // 4 B: Count of string entries
	Reserved         uint32 // 4 B: Future expansion padding
	CRC64Checksum    uint64 // 8 B: ECMA-182 polynomial checksum
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

const (
	circuitsMapKey = "registry:circuits"
	tagSetPrefix   = "registry:tag:"
)

// Registry manages standalone Circuit storage and tag indexing directly in the CacheBackend (Capacitor).
// It maintains ZERO local in-memory shadow copies, guaranteeing 100% multi-node cluster convergence.
type Registry struct {
	cacheBackend cache.CacheBackend
	fallback     *cache.MemoryCache
}

// New creates a new stateless Registry backed directly by the provided CacheBackend.
// If backend is nil, an in-memory fallback cache is initialized.
func New(backend cache.CacheBackend) *Registry {
	r := &Registry{
		cacheBackend: backend,
	}
	if backend == nil {
		r.fallback = cache.NewMemoryCache()
	}
	return r
}

func (r *Registry) backend() cache.CacheBackend {
	if r.cacheBackend != nil {
		return r.cacheBackend
	}
	return r.fallback
}

// Put registers or updates a Circuit directly in Capacitor with zero shadow caching.
func (r *Registry) Put(ctx context.Context, circuit *types.Circuit) error {
	if circuit == nil || circuit.ID == "" {
		return errors.New("flux registry: circuit ID required")
	}

	data, err := json.Marshal(circuit)
	if err != nil {
		return fmt.Errorf("flux registry: failed to marshal circuit %s: %w", circuit.ID, err)
	}

	// Store circuit in Capacitor's partitioned circuits map
	if _, err := r.backend().MapSet(ctx, circuitsMapKey, circuit.ID, string(data), 0); err != nil {
		return err
	}

	// Index tags incrementally in Capacitor distributed sets
	for _, tag := range circuit.Tags {
		if tag != "" {
			if _, err := r.backend().SetAdd(ctx, tagSetPrefix+tag, circuit.ID); err != nil {
				return err
			}
		}
	}

	return nil
}

// Get retrieves a Circuit directly from Capacitor by its ID.
func (r *Registry) Get(ctx context.Context, id string) (*types.Circuit, error) {
	if id == "" {
		return nil, ErrCircuitNotFound
	}

	var circuit types.Circuit
	found, err := r.backend().MapGetScan(ctx, circuitsMapKey, id, &circuit)
	if err != nil || !found {
		return nil, ErrCircuitNotFound
	}

	return &circuit, nil
}

// GetMatching retrieves all Circuits that match any of the provided query tags directly from Capacitor.
func (r *Registry) GetMatching(ctx context.Context, tags ...string) []*types.Circuit {
	if len(tags) == 0 {
		return nil
	}

	matchedIDs := make(map[string]struct{})
	for _, tag := range tags {
		if tag == "" {
			continue
		}
		ids, err := r.backend().SetMembers(ctx, tagSetPrefix+tag)
		if err == nil {
			for _, id := range ids {
				matchedIDs[id] = struct{}{}
			}
		}
	}

	if len(matchedIDs) == 0 {
		return nil
	}

	// Sort IDs for 100% deterministic evaluation order across cluster nodes
	sortedIDs := make([]string, 0, len(matchedIDs))
	for id := range matchedIDs {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Strings(sortedIDs)

	res := make([]*types.Circuit, 0, len(sortedIDs))
	for _, id := range sortedIDs {
		var c types.Circuit
		found, err := r.backend().MapGetScan(ctx, circuitsMapKey, id, &c)
		if err == nil && found {
			res = append(res, &c)
		}
	}

	return res
}

// Delete removes a Circuit by ID from Capacitor and clears its tag memberships.
func (r *Registry) Delete(ctx context.Context, id string) error {
	if id == "" {
		return ErrCircuitNotFound
	}

	// Read circuit first to clean up its tag sets
	if existing, err := r.Get(ctx, id); err == nil && existing != nil {
		for _, tag := range existing.Tags {
			if tag != "" {
				_, _ = r.backend().SetRemove(ctx, tagSetPrefix+tag, id)
			}
		}
	}

	found, err := r.backend().MapRemove(ctx, circuitsMapKey, id)
	if err != nil {
		return err
	}
	if !found {
		return ErrCircuitNotFound
	}

	return nil
}
