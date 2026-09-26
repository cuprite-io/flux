package flux

import (
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/internal/profiler"
	"github.com/cuprite-io/flux/internal/schematap"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
)

// Option configures an Engine instance.
type Option func(*Engine)

// WithCache sets the distributed CacheBackend for state and persistence.
func WithCache(backend cache.CacheBackend) Option {
	return func(e *Engine) {
		e.cache = backend
	}
}

// WithWorkers sets the concurrency worker count for the internal parallel pool.
func WithWorkers(numWorkers int) Option {
	return func(e *Engine) {
		e.workers = numWorkers
	}
}

// WithSink registers a named Sink with the engine.
func WithSink(name string, s sink.Sink) Option {
	return func(e *Engine) {
		e.sinks.Register(name, s)
	}
}

// WithSchemaLearning enables streaming schema inference over Spark payloads via assay.
func WithSchemaLearning(enable bool, cfg ...schematap.Config) Option {
	return func(e *Engine) {
		e.schemaLearning = enable
		if len(cfg) > 0 {
			e.schemaConfig = cfg[0]
		} else {
			e.schemaConfig = schematap.DefaultConfig()
		}
		e.schemaConfig.Enabled = enable
	}
}

// WithSchemaDiscriminators configures prioritized discriminator keys for schema partitioning.
func WithSchemaDiscriminators(keys ...string) Option {
	return func(e *Engine) {
		e.schemaConfig.DiscriminatorKeys = keys
	}
}

// WithSchemaAsync sets whether schema learning is offloaded asynchronously from Spark's hot path.
func WithSchemaAsync(async bool) Option {
	return func(e *Engine) {
		e.schemaConfig.Async = async
	}
}

// WithSchemaSampleRate sets the sampling ratio (0.0 to 1.0) for schema inference.
func WithSchemaSampleRate(rate float64) Option {
	return func(e *Engine) {
		e.schemaConfig.SampleRate = rate
	}
}

// WithSchemaQueueSize sets the bounded channel buffer size for asynchronous schema sampling.
func WithSchemaQueueSize(size int) Option {
	return func(e *Engine) {
		e.schemaConfig.QueueSize = size
	}
}

// WithTracer enables OpenTelemetry distributed tracing across Spark, Conduct, Nodes, and Sinks.
func WithTracer(tr trace.Tracer) Option {
	return func(e *Engine) {
		e.tracer = telemetry.NewTracer(tr, telemetry.AttrFluxVersion.String(Version))
	}
}

// WithProfiling enables streaming value profiling and exemplar retention directly in CacheBackend.
func WithProfiling(enable bool, cfg ...profiler.Config) Option {
	return func(e *Engine) {
		e.profiling = enable
		if len(cfg) > 0 {
			e.profilerConfig = cfg[0]
		} else {
			e.profilerConfig = profiler.DefaultConfig()
		}
		e.profilerConfig.Enabled = enable
	}
}

// WithProfilerSampleRate sets the sampling ratio (0.0 to 1.0) for value profiling.
func WithProfilerSampleRate(rate float64) Option {
	return func(e *Engine) {
		e.profilerConfig.SampleRate = rate
	}
}

// WithProfilerMaxExemplars sets the maximum number of distinct exemplars retained per tag in CacheBackend.
func WithProfilerMaxExemplars(max int) Option {
	return func(e *Engine) {
		e.profilerConfig.MaxExemplars = max
	}
}

// WithProfilerTTL sets the rolling TTL duration for profiler entries in CacheBackend.
func WithProfilerTTL(ttl time.Duration) Option {
	return func(e *Engine) {
		e.profilerConfig.TTL = ttl
	}
}

// WithProfilerDiscriminators sets prioritized discriminator keys for exemplar shape partitioning.
func WithProfilerDiscriminators(keys ...string) Option {
	return func(e *Engine) {
		e.profilerConfig.DiscriminatorKeys = keys
	}
}

// WithSinkDescription sets a human-readable explanation of when and why a sink should be invoked.
func WithSinkDescription(desc string) SinkOption {
	return sink.WithDescription(desc)
}

// WithSinkSeverity sets the severity tier associated with a sink (e.g. "critical", "warning", "info").
func WithSinkSeverity(sev string) SinkOption {
	return sink.WithSeverity(sev)
}

// WithSinkType specifies a user-defined category or protocol string for a sink.
func WithSinkType(st string) SinkOption {
	return sink.WithType(st)
}

// WithSinkTags attaches descriptive tags to a sink.
func WithSinkTags(tags ...string) SinkOption {
	return sink.WithTags(tags...)
}

// WithHistory enables historical alert retention directly in CacheBackend.
func WithHistory(enable bool, cfg ...history.Config) Option {
	return func(e *Engine) {
		e.historyEnabled = enable
		if len(cfg) > 0 {
			e.historyConfig = cfg[0]
		} else {
			e.historyConfig = history.DefaultConfig()
		}
		e.historyConfig.Enabled = enable
	}
}

// WithHistoryTTL sets the rolling TTL retention duration for historical alert records in CacheBackend.
func WithHistoryTTL(ttl time.Duration) Option {
	return func(e *Engine) {
		e.historyConfig.TTL = ttl
	}
}

// WithHistoryMaxRecords sets the maximum number of historical alert records retained per circuit.
func WithHistoryMaxRecords(max int) Option {
	return func(e *Engine) {
		e.historyConfig.MaxRecordsPerCircuit = max
	}
}

// WithHistoryAsync toggles whether alert persistence executes asynchronously off the hot path.
func WithHistoryAsync(async bool) Option {
	return func(e *Engine) {
		e.historyConfig.Async = async
	}
}


