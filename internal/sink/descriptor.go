package sink

import "sort"

const (
	// SeverityInfo is used for informational, audit, or routine notifications.
	SeverityInfo = "info"

	// SeverityWarning is used for degraded performance, anomalies, or threshold warnings.
	SeverityWarning = "warning"

	// SeverityCritical is used for outages, fatal failures, security incidents, or paging on-call.
	SeverityCritical = "critical"
)

// Descriptor describes a registered external sink destination.
// This metadata enables autonomous agents to introspect available sinks and understand where to route events.
type Descriptor struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	SinkType    string   `json:"sink_type,omitempty"`
	Severity    string   `json:"severity,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

// Option configures metadata on a sink Descriptor.
type Option func(*Descriptor)

// WithDescription sets a human-readable explanation of when and why this sink should be invoked.
func WithDescription(desc string) Option {
	return func(d *Descriptor) {
		d.Description = desc
	}
}

// WithSeverity sets the severity tier associated with this sink (e.g. SeverityCritical, SeverityWarning, SeverityInfo).
func WithSeverity(sev string) Option {
	return func(d *Descriptor) {
		d.Severity = sev
	}
}

// WithType specifies a user-defined category or protocol string for the destination.
func WithType(sinkType string) Option {
	return func(d *Descriptor) {
		d.SinkType = sinkType
	}
}

// WithTags attaches descriptive tags to the sink.
func WithTags(tags ...string) Option {
	return func(d *Descriptor) {
		d.Tags = append(d.Tags, tags...)
	}
}

// SortDescriptors sorts a slice of descriptors alphabetically by name.
func SortDescriptors(descs []Descriptor) {
	sort.Slice(descs, func(i, j int) bool {
		return descs[i].Name < descs[j].Name
	})
}
