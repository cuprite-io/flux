package vm

import (
	"math"
	"unsafe"
)

// Tag identifies the runtime scalar type or arena reference of a Value.
type Tag uint8

const (
	TagNull Tag = iota
	TagBool
	TagInt64
	TagUint64
	TagFloat64
	TagStringView // Stored as (offset << 32 | length) into frame Arena
	TagBytesView  // Stored as (offset << 32 | length) into frame Arena
	TagHandle     // Dynamic handle index into Frame's host object table
)

// String returns a human-readable name of the Tag.
func (t Tag) String() string {
	switch t {
	case TagNull:
		return "null"
	case TagBool:
		return "bool"
	case TagInt64:
		return "int64"
	case TagUint64:
		return "uint64"
	case TagFloat64:
		return "float64"
	case TagStringView:
		return "string"
	case TagBytesView:
		return "bytes"
	case TagHandle:
		return "handle"
	default:
		return "unknown"
	}
}

// Value is a fixed 16-byte tagged union fitting directly into CPU registers.
// It is 100% pointerless to ensure the Go GC completely ignores it during heap scans.
type Value struct {
	Tag Tag     // 1 B: Type tag
	_   [7]byte // 7 B: Explicit alignment padding
	Raw uint64  // 8 B: Scalar value or packed (offset << 32 | length)
}

// Static compile-time assertion: Value must be exactly 16 bytes.
const (
	_ = uint(unsafe.Sizeof(Value{}) - 16)
	_ = uint(16 - unsafe.Sizeof(Value{}))
)

// NullValue returns an uninitialized Null Value.
var NullValue = Value{Tag: TagNull, Raw: 0}

// NewBool creates a boolean Value.
func NewBool(v bool) Value {
	var raw uint64
	if v {
		raw = 1
	}
	return Value{Tag: TagBool, Raw: raw}
}

// NewInt64 creates an int64 Value.
func NewInt64(v int64) Value {
	return Value{Tag: TagInt64, Raw: uint64(v)}
}

// NewUint64 creates a uint64 Value.
func NewUint64(v uint64) Value {
	return Value{Tag: TagUint64, Raw: v}
}

// NewFloat64 creates a float64 Value.
func NewFloat64(v float64) Value {
	return Value{Tag: TagFloat64, Raw: math.Float64bits(v)}
}

// NewStringView creates a Value representing a string slice in the byte Arena.
func NewStringView(offset, length uint32) Value {
	raw := (uint64(offset) << 32) | uint64(length)
	return Value{Tag: TagStringView, Raw: raw}
}

// NewBytesView creates a Value representing a raw byte slice in the byte Arena.
func NewBytesView(offset, length uint32) Value {
	raw := (uint64(offset) << 32) | uint64(length)
	return Value{Tag: TagBytesView, Raw: raw}
}

// NewHandle creates a Value referencing a host object by index.
func NewHandle(idx uint32) Value {
	return Value{Tag: TagHandle, Raw: uint64(idx)}
}

// Bool returns the boolean value.
func (v Value) Bool() bool {
	return v.Raw != 0
}

// Int64 returns the int64 value.
func (v Value) Int64() int64 {
	return int64(v.Raw)
}

// Uint64 returns the uint64 value.
func (v Value) Uint64() uint64 {
	return v.Raw
}

// Float64 returns the float64 value.
func (v Value) Float64() float64 {
	return math.Float64frombits(v.Raw)
}

// ArenaView returns the offset and length for string/byte views in the Arena.
func (v Value) ArenaView() (offset, length uint32) {
	return uint32(v.Raw >> 32), uint32(v.Raw & 0xFFFFFFFF)
}

// Handle returns the host handle index.
func (v Value) Handle() uint32 {
	return uint32(v.Raw)
}

// IsNull returns true if the Value is Null.
func (v Value) IsNull() bool {
	return v.Tag == TagNull
}

// IsTruthy returns true if the value evaluates to true in conditional branches.
func (v Value) IsTruthy() bool {
	switch v.Tag {
	case TagBool:
		return v.Bool()
	case TagInt64:
		return v.Int64() != 0
	case TagUint64:
		return v.Uint64() != 0
	case TagFloat64:
		return v.Float64() != 0 && !math.IsNaN(v.Float64())
	case TagStringView, TagBytesView:
		_, len := v.ArenaView()
		return len > 0
	case TagHandle:
		return true
	default:
		return false
	}
}
