package types_test

import (
	"context"
	"testing"

	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyStepProvider struct {
	step *types.StepDefinition
}

func (d dummyStepProvider) ToStep() *types.StepDefinition {
	return d.step
}

func TestTypes_NodeAndCircuitBuilders(t *testing.T) {
	node := types.NewNode("root_node").
		WithCondition("payload.value > 10.0")

	stepDef := &types.StepDefinition{Type: types.StepVolt, Script: "set('a', 1)"}
	node.Step(stepDef)

	provider := dummyStepProvider{step: &types.StepDefinition{Type: types.StepAbort}}
	node.Step(provider)

	child1 := types.NewNode("child_1")
	child2 := types.NewNode("child_2")
	node.AddChildren(child1, child2)

	assert.Equal(t, "root_node", node.Name)
	assert.Equal(t, "payload.value > 10.0", node.Condition)
	assert.Equal(t, 2, len(node.Steps))
	assert.Equal(t, 2, len(node.Children))

	circuit := types.NewCircuit("test_circuit").
		WithTags("stream:events", "tier:gold").
		WithRoot(node)

	assert.Equal(t, "test_circuit", circuit.ID)
	assert.Equal(t, []string{"stream:events", "tier:gold"}, circuit.Tags)
	assert.Equal(t, node, circuit.Root)
}

func TestTypes_ItemPrimaryCategory(t *testing.T) {
	// 1. Explicit Category
	item1 := &types.Item{
		ID:       "item_1",
		Category: "offers:gaming",
		Tags:     []string{"tag_1", "tag_2"},
	}
	assert.Equal(t, "offers:gaming", item1.PrimaryCategory())

	// 2. Fallback to Tags[0]
	item2 := &types.Item{
		ID:   "item_2",
		Tags: []string{"offers:retail", "tag_2"},
	}
	assert.Equal(t, "offers:retail", item2.PrimaryCategory())

	// 3. Fallback to "default"
	item3 := &types.Item{
		ID: "item_3",
	}
	assert.Equal(t, "default", item3.PrimaryCategory())
}

func TestTypes_ConductRequestTargetCategory(t *testing.T) {
	// 1. Explicit Category
	req1 := &types.ConductRequest{
		Category: "offers:vip",
		Tags:     []string{"tag_1"},
	}
	assert.Equal(t, "offers:vip", req1.TargetCategory())

	// 2. Fallback to Tags[0]
	req2 := &types.ConductRequest{
		Tags: []string{"offers:starter"},
	}
	assert.Equal(t, "offers:starter", req2.TargetCategory())

	// 3. Fallback to "default"
	req3 := &types.ConductRequest{}
	assert.Equal(t, "default", req3.TargetCategory())
}

func TestTypes_StepTypeString(t *testing.T) {
	assert.Equal(t, "VOLT", types.StepVolt.String())
	assert.Equal(t, "SINK", types.StepSink.String())
	assert.Equal(t, "RETURN", types.StepReturn.String())
	assert.Equal(t, "ABORT", types.StepAbort.String())
	assert.Equal(t, "UNKNOWN", types.StepType(99).String())
}

func TestTypes_NewStateContext(t *testing.T) {
	ctx := context.Background()
	input := map[string]any{"user_id": "u123", "amount": 500.0}

	sctx := types.NewStateContext(ctx, input)
	require.NotNil(t, sctx)
	assert.Equal(t, ctx, sctx.Ctx)
	assert.Equal(t, input, sctx.OriginalInput)
	assert.NotNil(t, sctx.Scratchpad)
	assert.False(t, sctx.Aborted)
	assert.Equal(t, uint64(0), sctx.DeadMask)
}
