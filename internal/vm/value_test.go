package vm_test

import (
	"math"
	"testing"

	"github.com/cuprite-io/flux/internal/vm"
	"github.com/stretchr/testify/assert"
)

func TestVM_ValueScalarsAndViews(t *testing.T) {
	// Null
	nullVal := vm.NullValue
	assert.True(t, nullVal.IsNull())
	assert.False(t, nullVal.IsTruthy())
	assert.Equal(t, "null", vm.TagNull.String())

	// Bool
	bTrue := vm.NewBool(true)
	bFalse := vm.NewBool(false)
	assert.Equal(t, vm.TagBool, bTrue.Tag)
	assert.True(t, bTrue.Bool())
	assert.True(t, bTrue.IsTruthy())
	assert.False(t, bFalse.Bool())
	assert.False(t, bFalse.IsTruthy())
	assert.Equal(t, "bool", vm.TagBool.String())

	// Int64
	iVal := vm.NewInt64(-42)
	assert.Equal(t, vm.TagInt64, iVal.Tag)
	assert.Equal(t, int64(-42), iVal.Int64())
	assert.True(t, iVal.IsTruthy())
	assert.False(t, vm.NewInt64(0).IsTruthy())
	assert.Equal(t, "int64", vm.TagInt64.String())

	// Uint64
	uVal := vm.NewUint64(100)
	assert.Equal(t, vm.TagUint64, uVal.Tag)
	assert.Equal(t, uint64(100), uVal.Uint64())
	assert.True(t, uVal.IsTruthy())
	assert.False(t, vm.NewUint64(0).IsTruthy())
	assert.Equal(t, "uint64", vm.TagUint64.String())

	// Float64
	fVal := vm.NewFloat64(3.14159)
	assert.Equal(t, vm.TagFloat64, fVal.Tag)
	assert.Equal(t, 3.14159, fVal.Float64())
	assert.True(t, fVal.IsTruthy())
	assert.False(t, vm.NewFloat64(0.0).IsTruthy())
	assert.False(t, vm.NewFloat64(math.NaN()).IsTruthy())
	assert.Equal(t, "float64", vm.TagFloat64.String())

	// StringView
	sView := vm.NewStringView(128, 64)
	assert.Equal(t, vm.TagStringView, sView.Tag)
	off, lenVal := sView.ArenaView()
	assert.Equal(t, uint32(128), off)
	assert.Equal(t, uint32(64), lenVal)
	assert.True(t, sView.IsTruthy())
	assert.False(t, vm.NewStringView(0, 0).IsTruthy())
	assert.Equal(t, "string", vm.TagStringView.String())

	// BytesView
	bView := vm.NewBytesView(256, 32)
	assert.Equal(t, vm.TagBytesView, bView.Tag)
	bOff, bLen := bView.ArenaView()
	assert.Equal(t, uint32(256), bOff)
	assert.Equal(t, uint32(32), bLen)
	assert.True(t, bView.IsTruthy())
	assert.False(t, vm.NewBytesView(0, 0).IsTruthy())
	assert.Equal(t, "bytes", vm.TagBytesView.String())

	// Handle
	hVal := vm.NewHandle(7)
	assert.Equal(t, vm.TagHandle, hVal.Tag)
	assert.Equal(t, uint32(7), hVal.Handle())
	assert.True(t, hVal.IsTruthy())
	assert.Equal(t, "handle", vm.TagHandle.String())

	// Unknown tag
	assert.Equal(t, "unknown", vm.Tag(99).String())
}

func TestVM_FrameArenaAllocation(t *testing.T) {
	arena := vm.NewFrameArena()
	defer arena.Reset()

	// AllocString & GetString
	sOff, sLen := arena.AllocString("flux_kernel_test")
	assert.Equal(t, "flux_kernel_test", arena.GetString(sOff, sLen))

	// AllocBytes & GetBytes
	raw := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	bOff, bLen := arena.AllocBytes(raw)
	assert.Equal(t, raw, arena.GetBytes(bOff, bLen))

	// AllocHandle & GetHandle
	type HostObj struct{ Value int }
	obj := &HostObj{Value: 42}
	hIdx := arena.AllocHandle(obj)
	assert.Equal(t, obj, arena.GetHandle(hIdx).(*HostObj))

	// Reset
	arena.Reset()
	assert.Equal(t, "", arena.GetString(sOff, sLen))
}

func TestVM_ProgramCRC64AndConstants(t *testing.T) {
	steps := []vm.Step{
		{Op: vm.OpReturn},
	}
	prog := vm.NewProgram("prog_1", steps, nil, []byte("const_pool_data"), []string{"str_0", "str_1"})
	assert.True(t, prog.VerifyCRC64())
	assert.Equal(t, "str_0", prog.ConstantString(0))
	assert.Equal(t, "str_1", prog.ConstantString(1))
	assert.Equal(t, "", prog.ConstantString(99))

	// Corrupt and verify failure
	prog.CRC64 = 12345
	assert.False(t, prog.VerifyCRC64())
}
