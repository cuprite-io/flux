package types

import (
	"context"
	"time"
)

// StepType defines the category of an execution step in a Circuit.
type StepType uint8

const (
	// StepVolt executes a VoltScript expression or script block.
	StepVolt StepType = iota
	// StepSink dispatches a projected payload to a registered named external sink.
	StepSink
	// StepReturn projects specific key-values from the Circuit state as final returned output.
	StepReturn
	// StepAbort explicitly aborts Circuit execution and marks the result as rejected/halted.
	StepAbort
)

// String returns the human-readable name of the StepType.
func (st StepType) String() string {
	switch st {
	case StepVolt:
		return "VOLT"
	case StepSink:
		return "SINK"
	case StepReturn:
		return "RETURN"
	case StepAbort:
		return "ABORT"
	default:
		return "UNKNOWN"
	}
}

// StepDefinition represents a single declarative step within a Circuit Node.
type StepDefinition struct {
	Type      StepType       `json:"type"`
	Script    string         `json:"script,omitempty"`
	SinkName  string         `json:"sink,omitempty"`
	Condition string         `json:"condition,omitempty"`
	Payload   string         `json:"payload,omitempty"`
	ReturnMap map[string]any `json:"data,omitempty"`
}

// Node represents a node in a hierarchical Circuit execution tree.
type Node struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Condition string            `json:"condition,omitempty"` // Volt boolean guard expression
	Steps     []*StepDefinition `json:"steps"`
	Children  []*Node           `json:"children,omitempty"`
}

// NewNode creates a new named Circuit Node.
func NewNode(name string) *Node {
	return &Node{
		Name:     name,
		Steps:    make([]*StepDefinition, 0, 4),
		Children: make([]*Node, 0, 2),
	}
}

// WithCondition attaches a Volt boolean guard condition to the node.
// If the condition evaluates to false, this node and all its children are pruned.
func (n *Node) WithCondition(cond string) *Node {
	n.Condition = cond
	return n
}

// StepProvider provides a StepDefinition.
type StepProvider interface {
	ToStep() *StepDefinition
}

// ToStep allows *StepDefinition to satisfy StepProvider.
func (s *StepDefinition) ToStep() *StepDefinition {
	return s
}

// Step appends one or more execution steps (or step builders) to the node.
func (n *Node) Step(steps ...StepProvider) *Node {
	for _, s := range steps {
		if s != nil {
			n.Steps = append(n.Steps, s.ToStep())
		}
	}
	return n
}

// AddChildren attaches child nodes that execute in parallel after this node completes.
func (n *Node) AddChildren(children ...*Node) *Node {
	n.Children = append(n.Children, children...)
	return n
}

// Circuit represents an executable Circuit tree pipeline.
type Circuit struct {
	ID        string    `json:"id"`
	Tags      []string  `json:"tags,omitempty"`
	Root      *Node     `json:"root"`
	Version   uint32    `json:"version,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
}

// NewCircuit creates a new Circuit with the given unique ID.
func NewCircuit(id string) *Circuit {
	return &Circuit{
		ID:        id,
		Tags:      make([]string, 0, 4),
		CreatedAt: time.Now(),
	}
}

// WithTags sets the indexing tags for the Circuit.
func (c *Circuit) WithTags(tags ...string) *Circuit {
	c.Tags = tags
	return c
}

// WithRoot sets the root execution node for the Circuit tree.
func (c *Circuit) WithRoot(root *Node) *Circuit {
	c.Root = root
	return c
}

// Item represents a candidate item in the Catalog (e.g. promotional offer, mission, pricing rule)
// with an embedded qualification and transformation Circuit.
type Item struct {
	ID        string         `json:"id"`
	Tags      []string       `json:"tags,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	Circuit   *Circuit       `json:"circuit"`
	ExpiresAt time.Time      `json:"expires_at,omitempty"`
}

// SparkResult represents the outcome of a Spark single-payload execution.
type SparkResult struct {
	OriginalInput    any            // Original immutable input payload
	ReturnedData     map[string]any // Populated if a StepReturn step is executed
	Passed           bool           // True if all non-aborted matching Circuits executed
	ExecutedCircuits []string       // IDs of Circuits that matched and executed
	TriggeredSinks   []string       // IDs of Sinks triggered during execution
	Errors           []error        // Any runtime/step errors encountered
}

// ConductRequest encapsulates query parameters for candidate item retrieval and evaluation.
type ConductRequest struct {
	EntityID string         `json:"entity_id"` // Target identifier (e.g. "user_101")
	Tags     []string       `json:"tags"`      // Candidate item pre-filter tags (e.g. ["offers:gaming"])
	Context  map[string]any `json:"context"`   // Ephemeral request context (e.g. {"cart_total": 150.0})
	TopK     int            `json:"top_k"`     // Max items to return (0 = all qualified)
	Timeout  time.Duration  `json:"timeout"`   // Execution timeout budget
}

// EvaluatedItem represents a qualified, dynamically modified candidate item.
type EvaluatedItem struct {
	ID             string         `json:"id"`
	Data           map[string]any `json:"data"`            // Original catalog item data
	ComputedOutput map[string]any `json:"computed_output"` // Dynamically modified output from StepReturn
	Score          float64        `json:"score"`           // Computed ranking priority
	Rank           int            `json:"rank"`            // 1-based rank in response
}

// ConductResult represents the outcome of a Conduct candidate evaluation query.
type ConductResult struct {
	EntityID       string          `json:"entity_id"`
	Items          []EvaluatedItem `json:"items"`
	EvaluatedCount int             `json:"evaluated_count"` // Total candidate items inspected
	Duration       time.Duration   `json:"duration"`
}

// StateContext holds the execution state for a single Circuit evaluation.
type StateContext struct {
	Ctx           context.Context
	OriginalInput any
	Scratchpad    map[string]any
	ReturnData    map[string]any
	DeadMask      uint64 // Bitmask tracking pruned branches (bit N == 1 -> node N is dead)
	Aborted       bool
	Errors        []error
}

// NewStateContext creates a fresh StateContext initialized with the original payload.
func NewStateContext(ctx context.Context, input any) *StateContext {
	return &StateContext{
		Ctx:           ctx,
		OriginalInput: input,
		Scratchpad:    make(map[string]any, 16),
		ReturnData:    nil,
		DeadMask:      0,
		Aborted:       false,
		Errors:        nil,
	}
}
