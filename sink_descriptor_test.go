package flux_test

import (
	"context"
	"testing"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_SinkDescriptorsAndIntrospection(t *testing.T) {
	eng, err := flux.New()
	require.NoError(t, err)
	defer eng.Close()

	dummySink := sink.FuncSink(func(ctx context.Context, payload any) error {
		return nil
	})

	// 1. Register with explicit metadata options
	eng.RegisterSink("emergency_alert", dummySink,
		flux.WithSinkDescription("Alerts on-call engineers for critical system failures"),
		flux.WithSinkSeverity(flux.SinkSeverityCritical),
		flux.WithSinkType("webhook"),
		flux.WithSinkTags("tier1", "infrastructure"),
	)

	// 2. Register without options (unspecified metadata is empty)
	eng.RegisterSink("general_notifications", dummySink)

	// Introspect specific sink with metadata
	desc, ok := eng.SinkDescriptor("emergency_alert")
	require.True(t, ok)
	assert.Equal(t, "emergency_alert", desc.Name)
	assert.Equal(t, "Alerts on-call engineers for critical system failures", desc.Description)
	assert.Equal(t, flux.SinkSeverityCritical, desc.Severity)
	assert.Equal(t, "webhook", desc.SinkType)
	assert.Equal(t, []string{"tier1", "infrastructure"}, desc.Tags)

	// Introspect sink without options
	notifDesc, ok := eng.SinkDescriptor("general_notifications")
	require.True(t, ok)
	assert.Equal(t, "general_notifications", notifDesc.Name)
	assert.Empty(t, notifDesc.Description)
	assert.Empty(t, notifDesc.SinkType)
	assert.Empty(t, notifDesc.Severity)

	// List all sinks
	allSinks := eng.ListSinks()
	require.Len(t, allSinks, 2)
	assert.Equal(t, "emergency_alert", allSinks[0].Name)
	assert.Equal(t, "general_notifications", allSinks[1].Name)
}
