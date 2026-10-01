package flux

import (
	"github.com/cuprite-io/flux/internal/declarative"
	"github.com/cuprite-io/flux/types"
)

// LoadCircuitJSON parses a raw JSON byte slice into an executable *types.Circuit.
func LoadCircuitJSON(data []byte) (*types.Circuit, error) {
	return declarative.LoadCircuitJSON(data)
}

// LoadCircuitYAML parses a raw YAML byte slice into an executable *types.Circuit.
func LoadCircuitYAML(data []byte) (*types.Circuit, error) {
	return declarative.LoadCircuitYAML(data)
}

// LoadCircuitFile reads and parses a .json or .yaml Circuit file.
func LoadCircuitFile(filePath string) (*types.Circuit, error) {
	return declarative.LoadCircuitFile(filePath)
}

// LoadItemJSON parses a raw JSON byte slice into a *types.Item.
func LoadItemJSON(data []byte) (*types.Item, error) {
	return declarative.LoadItemJSON(data)
}

// LoadItemFile reads and parses a candidate item file (.json or .yaml).
func LoadItemFile(filePath string) (*types.Item, error) {
	return declarative.LoadItemFile(filePath)
}

// LoadCircuitsFromDir loads all .json and .yaml circuit files from a directory.
func LoadCircuitsFromDir(dir string) ([]*types.Circuit, error) {
	return declarative.LoadCircuitsFromDir(dir)
}
