package flux

import (
	"errors"
	"reflect"
)

var (
	// ErrInvalidInputType is returned when Spark input is not strictly JSON, Slice, or Struct.
	ErrInvalidInputType = errors.New("flux: input payload must strictly be JSON ([]byte/string), Go slice, or Go struct")
)

// ValidateInput enforces that Spark inputs are strictly JSON, Slice, or Struct.
func ValidateInput(payload any) error {
	if payload == nil {
		return ErrInvalidInputType
	}

	switch payload.(type) {
	case []byte, string: // JSON byte stream or JSON string
		return nil
	}

	val := reflect.ValueOf(payload)
	kind := val.Kind()

	if kind == reflect.Ptr {
		val = val.Elem()
		kind = val.Kind()
	}

	if kind == reflect.Slice || kind == reflect.Struct {
		return nil
	}

	return ErrInvalidInputType
}
