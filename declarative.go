package flux

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cuprite-io/flux/types"
	"gopkg.in/yaml.v3"
)

type rawStep struct {
	Type      string         `json:"type" yaml:"type"`
	Script    string         `json:"script,omitempty" yaml:"script,omitempty"`
	SinkName  string         `json:"sink,omitempty" yaml:"sink,omitempty"`
	SinkType  string         `json:"sink_type,omitempty" yaml:"sink_type,omitempty"`
	Condition string         `json:"condition,omitempty" yaml:"condition,omitempty"`
	Payload   string         `json:"payload,omitempty" yaml:"payload,omitempty"`
	ReturnMap map[string]any `json:"data,omitempty" yaml:"data,omitempty"`
}

type rawNode struct {
	ID        string     `json:"id,omitempty" yaml:"id,omitempty"`
	Name      string     `json:"name" yaml:"name"`
	Condition string     `json:"condition,omitempty" yaml:"condition,omitempty"`
	Steps     []rawStep  `json:"steps,omitempty" yaml:"steps,omitempty"`
	Children  []*rawNode `json:"children,omitempty" yaml:"children,omitempty"`
}

type rawCircuit struct {
	ID      string   `json:"id" yaml:"id"`
	Tags    []string `json:"tags,omitempty" yaml:"tags,omitempty"`
	Root    *rawNode `json:"root" yaml:"root"`
	Version uint32   `json:"version,omitempty" yaml:"version,omitempty"`
}

type rawItem struct {
	ID       string         `json:"id" yaml:"id"`
	Category string         `json:"category,omitempty" yaml:"category,omitempty"`
	Tags     []string       `json:"tags,omitempty" yaml:"tags,omitempty"`
	Data     map[string]any `json:"data,omitempty" yaml:"data,omitempty"`
	Circuit  *rawCircuit    `json:"circuit,omitempty" yaml:"circuit,omitempty"`
}

func parseStepType(t string) (types.StepType, error) {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "volt", "stepvolt", "expr", "script":
		return types.StepVolt, nil
	case "sink", "stepsink":
		return types.StepSink, nil
	case "return", "stepreturn":
		return types.StepReturn, nil
	case "abort", "stepabort":
		return types.StepAbort, nil
	case "vm", "stepvm":
		return types.StepVM, nil
	default:
		return 0, fmt.Errorf("unknown step type %q", t)
	}
}

func convertNode(rn *rawNode) (*types.Node, error) {
	if rn == nil {
		return nil, nil
	}

	node := types.NewNode(rn.Name).WithCondition(rn.Condition)
	node.ID = rn.ID

	for _, rs := range rn.Steps {
		st, err := parseStepType(rs.Type)
		if err != nil {
			return nil, fmt.Errorf("node %s: %w", rn.Name, err)
		}
		node.Step(&types.StepDefinition{
			Type:      st,
			Script:    rs.Script,
			SinkName:  rs.SinkName,
			SinkType:  rs.SinkType,
			Condition: rs.Condition,
			Payload:   rs.Payload,
			ReturnMap: rs.ReturnMap,
		})
	}

	for _, rc := range rn.Children {
		cn, err := convertNode(rc)
		if err != nil {
			return nil, err
		}
		node.AddChildren(cn)
	}

	return node, nil
}

func convertCircuit(rc *rawCircuit) (*types.Circuit, error) {
	if rc == nil {
		return nil, errors.New("circuit definition is empty")
	}
	if rc.ID == "" {
		return nil, errors.New("circuit ID is required")
	}

	rootNode, err := convertNode(rc.Root)
	if err != nil {
		return nil, fmt.Errorf("circuit %s: %w", rc.ID, err)
	}

	c := types.NewCircuit(rc.ID).
		WithTags(rc.Tags...).
		WithRoot(rootNode)
	c.Version = rc.Version
	return c, nil
}

// LoadCircuitJSON parses a raw JSON byte slice into an executable *types.Circuit.
func LoadCircuitJSON(data []byte) (*types.Circuit, error) {
	var rc rawCircuit
	if err := json.Unmarshal(data, &rc); err != nil {
		return nil, fmt.Errorf("flux: failed to parse circuit JSON: %w", err)
	}
	return convertCircuit(&rc)
}

// LoadCircuitYAML parses a raw YAML byte slice into an executable *types.Circuit.
func LoadCircuitYAML(data []byte) (*types.Circuit, error) {
	var rc rawCircuit
	if err := yaml.Unmarshal(data, &rc); err != nil {
		return nil, fmt.Errorf("flux: failed to parse circuit YAML: %w", err)
	}
	return convertCircuit(&rc)
}

// LoadCircuitFile reads and parses a .json or .yaml Circuit file.
func LoadCircuitFile(filePath string) (*types.Circuit, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("flux: failed to read file %s: %w", filePath, err)
	}

	ext := strings.ToLower(filepath.Ext(filePath))
	if ext == ".yaml" || ext == ".yml" {
		return LoadCircuitYAML(data)
	}
	return LoadCircuitJSON(data)
}

// LoadItemJSON parses a raw JSON byte slice into a *types.Item.
func LoadItemJSON(data []byte) (*types.Item, error) {
	var ri rawItem
	if err := json.Unmarshal(data, &ri); err != nil {
		return nil, fmt.Errorf("flux: failed to parse item JSON: %w", err)
	}

	item := &types.Item{
		ID:       ri.ID,
		Category: ri.Category,
		Tags:     ri.Tags,
		Data:     ri.Data,
	}

	if ri.Circuit != nil {
		c, err := convertCircuit(ri.Circuit)
		if err != nil {
			return nil, fmt.Errorf("item %s embedded circuit: %w", ri.ID, err)
		}
		item.Circuit = c
	}

	return item, nil
}

// LoadItemFile reads and parses a candidate item file (.json or .yaml).
func LoadItemFile(filePath string) (*types.Item, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("flux: failed to read item file %s: %w", filePath, err)
	}

	ext := strings.ToLower(filepath.Ext(filePath))
	if ext == ".yaml" || ext == ".yml" {
		var ri rawItem
		if err := yaml.Unmarshal(data, &ri); err != nil {
			return nil, fmt.Errorf("flux: failed to parse item YAML: %w", err)
		}
		item := &types.Item{
			ID:       ri.ID,
			Category: ri.Category,
			Tags:     ri.Tags,
			Data:     ri.Data,
		}
		if ri.Circuit != nil {
			c, err := convertCircuit(ri.Circuit)
			if err != nil {
				return nil, err
			}
			item.Circuit = c
		}
		return item, nil
	}

	return LoadItemJSON(data)
}

// LoadCircuitsFromDir loads all .json and .yaml circuit files from a directory.
func LoadCircuitsFromDir(dir string) ([]*types.Circuit, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("flux: failed to read directory %s: %w", dir, err)
	}

	circuits := make([]*types.Circuit, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".circuit.json") || strings.HasSuffix(name, ".circuit.yaml") || strings.HasSuffix(name, ".circuit.yml") || strings.HasSuffix(name, ".json") {
			fullPath := filepath.Join(dir, name)
			c, err := LoadCircuitFile(fullPath)
			if err != nil {
				return nil, fmt.Errorf("flux: failed to load circuit file %s: %w", fullPath, err)
			}
			if c != nil && c.ID != "" {
				circuits = append(circuits, c)
			}
		}
	}
	return circuits, nil
}
