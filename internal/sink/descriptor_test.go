package sink_test

import (
	"context"
	"testing"

	"github.com/cuprite-io/flux/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSinkRegistry_Descriptors(t *testing.T) {
	reg := sink.New(2, 64)
	defer reg.Close()

	noop := sink.FuncSink(func(ctx context.Context, payload any) error {
		return nil
	})

	// 1. Register with explicit metadata options
	reg.Register("oncall_emergency", noop,
		sink.WithDescription("Emergency alert channel for fatal outages"),
		sink.WithSeverity(sink.SeverityCritical),
		sink.WithType("webhook"),
		sink.WithTags("oncall", "infrastructure"),
	)

	// 2. Register without options
	reg.Register("audit_logger", noop)
	reg.Register("metrics_counter", noop)

	// Test GetDescriptor for oncall_emergency
	desc, ok := reg.GetDescriptor("oncall_emergency")
	require.True(t, ok)
	assert.Equal(t, "oncall_emergency", desc.Name)
	assert.Equal(t, "Emergency alert channel for fatal outages", desc.Description)
	assert.Equal(t, sink.SeverityCritical, desc.Severity)
	assert.Equal(t, "webhook", desc.SinkType)
	assert.Equal(t, []string{"oncall", "infrastructure"}, desc.Tags)

	// Test GetDescriptor for audit_logger (unspecified metadata is empty)
	descAudit, ok := reg.GetDescriptor("audit_logger")
	require.True(t, ok)
	assert.Equal(t, "audit_logger", descAudit.Name)
	assert.Empty(t, descAudit.Description)
	assert.Empty(t, descAudit.SinkType)
	assert.Empty(t, descAudit.Severity)

	// Test non-existent descriptor
	_, ok = reg.GetDescriptor("non_existent")
	assert.False(t, ok)

	// Test ListDescriptors sorting and count
	descs := reg.ListDescriptors()
	require.Len(t, descs, 3)
	assert.Equal(t, "audit_logger", descs[0].Name)
	assert.Equal(t, "metrics_counter", descs[1].Name)
	assert.Equal(t, "oncall_emergency", descs[2].Name)
}
