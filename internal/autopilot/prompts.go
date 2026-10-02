package autopilot

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cuprite-io/assay"
	"github.com/cuprite-io/flux/internal/history"
	"github.com/cuprite-io/flux/internal/profiler"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/types"
)

const baseSystemPrompt = `You are Flux Autopilot, an expert autonomous compiler and streaming architect for the Flux Stream Engine.
Your task is to synthesize an optimal, high-performance, deterministic Circuit DAG in JSON format from the user's intent, the inferred data schema, value profiles, and registered sink destinations.

### FLUX CIRCUIT SCHEMA SPECIFICATION
A Flux Circuit is a hierarchical, declarative Directed Acyclic Graph (DAG) executed on streaming events.
The output MUST be a single, valid JSON object matching the following structure:
{
  "id": "<circuit_id_unique_string>",
  "tags": ["<stream_tag_1>", "<stream_tag_2>"],
  "root": {
    "name": "<node_name>",
    "condition": "<optional_volt_boolean_guard_expression>",
    "steps": [
      {
        "type": "volt | sink | return | abort",
        "script": "set('var_name', <volt_expression>)",
        "sink": "<registered_sink_name>",
        "payload": "payload",
        "data": {
          "status": "VALUE",
          "metric": "$var_name",
          "field": "payload.field"
        }
      }
    ],
    "children": [
      <child_nodes_with_same_structure>
    ]
  }
}

### NODE EXECUTION RULES
1. A Node executes its steps in sequential order.
2. "condition" is an optional Volt/CEL boolean expression. If false, the node and ALL its children are skipped.
3. If condition is omitted, the node always executes.
4. Children nodes execute in order after parent steps complete.

### STEP TYPES
- "volt": Executes data transformation and updates circuit state.
  - Use "script": "set('key', <expr>)".
  - Chain multiple assignments with " && ": "set('a', payload.x) && set('b', window.count('k', '60s'))".
- "sink": Dispatches payload or projected alert snapshot to an external sink destination.
  - "sink": MUST EXACTLY MATCH one of the available registered sink names provided in context. Do not invent sink names.
  - "payload": "payload" (dispatches current event payload) or expression.
- "return": Terminal step that outputs a structured JSON response map.
  - "data": map of returned keys. Reference state variables using "$var_name" or CEL expressions.
- "abort": Explicitly terminates execution of the branch and rejects further node processing.

### VOLT / CEL SYNTAX AND COMPLETE BUILT-IN OPERATOR CATALOG
Variables in scope:
- "payload.<field>": Incoming stream event fields.
- "state.<var_name>": Variables saved via set('var_name', expr).
- "context.<key>": Execution context parameters.

1. Sliding Window Aggregation & Cache:
- window.count('<key_prefix>' + payload.field, '<duration>') -> int
  Sliding window event frequency counter. E.g.: window.count('rate:5xx:' + payload.service, '60s')
  Duration formats: '10s', '30s', '60s', '5m', '1h', '24h'.
- cache.get('<key>') -> string
  Queries distributed CacheBackend state store.

2. State & Map Manipulation:
- get(map, '<key>', fallback) / map.get(map, '<key>', fallback) -> any
  Safe map key extraction with default fallback. E.g.: get(payload, 'priority', 'normal')
- map.merge(m1, m2) -> map
  Combines two maps into a single map.
- map.delete(m, '<key>') -> map
  Returns copy of map with specified key removed.

3. PII Detection & Data Sanitization:
- is_pii(string) -> bool
  Detects emails, credit cards, SSNs, phone numbers, IPv4 addresses, and UUIDs.
- mask.email(string) -> string
  Partially masks email (e.g. j***@domain.com).
- mask.card(string) -> string
  Masks credit card numbers, preserving only the last 4 digits (e.g. ************1234).
- mask(string, "email" | "card") -> string
  Generic masking helper.

4. Cryptography, Hashing & Encryption:
- crypto.sha256(string) / hash.sha256(string) -> string (hex SHA-256)
- crypto.sha512(string) / hash.sha512(string) -> string (hex SHA-512)
- crypto.md5(string) / hash.md5(string) -> string (hex MD5)
- crypto.crc32(string) / hash.crc32(string) -> uint (CRC32 checksum)
- crypto.hmac(string, secret) -> string (HMAC-SHA256 hex digest)
- crypto.encrypt(plaintext, secretKey) -> string
  Authenticated AES-256-GCM symmetric encryption returning hex ciphertext.
- crypto.decrypt(ciphertext, secretKey) -> string
  Authenticated AES-256-GCM decryption returning plaintext.

5. Geo-Spatial Navigation:
- geo.dist_km(lat1, lon1, lat2, lon2) / geo.distance_km(...) -> float
  Great-circle Haversine distance in kilometers between two GPS coordinates.
- geo.dist_m(lat1, lon1, lat2, lon2) -> float
  Haversine distance in meters between two GPS coordinates.

6. Machine Learning & Anomaly Scoring:
- ml.anomaly('<model_id>', [float_values]) -> float
  Computes real-time anomaly score (0.0 to 1.0) using heuristic vector scoring.
- ml.score('<model_id>', [float_values]) -> float
  Vector scoring function for real-time classification/ranking.

7. Math, Statistics & Lists:
- math.clamp(val, min, max) -> float
- math.stats([float_values]) -> map
  Returns map with "mean", "stddev", and "count". E.g.: math.stats([10.0, 20.0, 30.0]).stddev
- list.unique([items]) -> list
  Deduplicates elements in a list.
- Standard CEL math: math.greatest(a, b), math.least(a, b), math.abs(a), math.ceil(a), math.floor(a), math.round(a)

8. Encodings & UUID Generation:
- uuid() / uuid.v4() -> string
  Generates a new RFC 4122 v4 UUID string.
- base64.encode(string) / base64.decode(string) -> string
- hex.encode(string) / hex.decode(string) -> string

9. Strings & Text Processing (CEL Extensions):
- str.contains(substr) -> bool
- str.startsWith(prefix) -> bool
- str.endsWith(suffix) -> bool
- str.matches(regex) -> bool
- str.indexOf(substr) -> int
- str.replace(old, new) -> string
- str.split(delimiter) -> list
- str.lowerAscii() / str.upperAscii() -> string
- str.trim() -> string

10. Logical & Relational Operators:
- Standard comparisons: <, <=, >, >=, ==, !=
- Logical operators: &&, ||, !
- Membership: in (e.g. payload.level in ['ERROR', 'FATAL'])

### CRITICAL REQUIREMENTS & CONSTRAINTS
1. ONLY reference sink names that are explicitly listed in the available sinks.
2. ONLY reference fields that are confirmed in the inferred schema or exemplars.
3. Keep DAGs efficient: place fast filter conditions at higher nodes to prune uninteresting traffic early.
4. Use window.count appropriately for frequency, rate-limiting, and threshold alerts.
5. Output ONLY raw JSON. Do NOT wrap in markdown fences (no ` + "```json" + `). Do NOT include conversational text or explanations.
`

// PromptBuilder constructs system and user prompts for circuit synthesis and refinement.
type PromptBuilder struct{}

// NewPromptBuilder creates a new PromptBuilder instance.
func NewPromptBuilder() *PromptBuilder {
	return &PromptBuilder{}
}

// BuildSystemPrompt returns the static system instructions explaining Flux grammar and constraints.
func (b *PromptBuilder) BuildSystemPrompt() string {
	return baseSystemPrompt
}

// BuildUserPrompt constructs the rich contextual prompt containing user requirements,
// schemas, value distributions, exemplars, and sink descriptors.
func (b *PromptBuilder) BuildUserPrompt(
	userGoal string,
	targetTags []string,
	schemas map[string]*assay.SchemaNode,
	profiles map[string]*profiler.StreamProfile,
	sinks []sink.Descriptor,
	circuitID string,
) string {
	var sb strings.Builder

	sb.WriteString("### USER OBJECTIVE\n")
	sb.WriteString(strings.TrimSpace(userGoal))
	sb.WriteString("\n\n")

	sb.WriteString("### TARGET STREAM TAGS\n")
	if len(targetTags) == 0 {
		sb.WriteString("None specified (use default 'stream:default')\n\n")
	} else {
		for _, tag := range targetTags {
			sb.WriteString(fmt.Sprintf("- %s\n", tag))
		}
		sb.WriteString("\n")
	}

	if circuitID != "" {
		sb.WriteString(fmt.Sprintf("### CIRCUIT ID\nPlease use the ID: %q\n\n", circuitID))
	}

	sb.WriteString("### INFERRED STREAM SCHEMAS (from assay)\n")
	if len(schemas) == 0 {
		sb.WriteString("No schema observed yet for these tags. Infer structure from the user prompt and exemplars.\n\n")
	} else {
		keys := make([]string, 0, len(schemas))
		for k := range schemas {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			node := schemas[k]
			sb.WriteString(fmt.Sprintf("Tag / Variant: %s\n", k))
			b.formatSchemaNode(&sb, "payload", node, 1)
			sb.WriteString("\n")
		}
	}

	sb.WriteString("### STREAM VALUE PROFILES & EXEMPLARS (from profiler)\n")
	if len(profiles) == 0 {
		sb.WriteString("No value profile recorded yet.\n\n")
	} else {
		pKeys := make([]string, 0, len(profiles))
		for k := range profiles {
			pKeys = append(pKeys, k)
		}
		sort.Strings(pKeys)
		for _, k := range pKeys {
			prof := profiles[k]
			b.formatProfile(&sb, prof)
		}
	}

	sb.WriteString("### AVAILABLE SINK DESTINATIONS\n")
	if len(sinks) == 0 {
		sb.WriteString("No external sinks registered. Use return steps only.\n\n")
	} else {
		sink.SortDescriptors(sinks)
		for _, s := range sinks {
			desc := s.Description
			if desc == "" {
				desc = "No description provided"
			}
			sev := s.Severity
			if sev == "" {
				sev = "info"
			}
			stype := s.SinkType
			if stype == "" {
				stype = "generic"
			}
			sb.WriteString(fmt.Sprintf("- Sink Name: %q\n  Description: %s\n  Severity: %s\n  Type: %s\n",
				s.Name, desc, sev, stype))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("### SYNTHESIS TASK\n")
	sb.WriteString("Based on the objective, schemas, exemplars, and sinks above, synthesize a complete, valid Flux Circuit DAG.\n")
	sb.WriteString("Remember: Output strictly the JSON object and nothing else.\n")

	return sb.String()
}

func (b *PromptBuilder) formatSchemaNode(sb *strings.Builder, prefix string, node *assay.SchemaNode, depth int) {
	if node == nil {
		return
	}

	indent := strings.Repeat("  ", depth)
	if len(node.Children) == 0 {
		req := "optional"
		if node.Required {
			req = "required"
		}
		sb.WriteString(fmt.Sprintf("%s- %s: %s (%s)\n", indent, prefix, node.Type, req))
		return
	}

	// Sort children for deterministic output
	childKeys := make([]string, 0, len(node.Children))
	for k := range node.Children {
		childKeys = append(childKeys, k)
	}
	sort.Strings(childKeys)

	for _, k := range childKeys {
		child := node.Children[k]
		subPrefix := fmt.Sprintf("%s.%s", prefix, k)
		b.formatSchemaNode(sb, subPrefix, child, depth)
	}
}

func (b *PromptBuilder) formatProfile(sb *strings.Builder, prof *profiler.StreamProfile) {
	if prof == nil {
		return
	}
	sb.WriteString(fmt.Sprintf("Profile for Tag: %s (Total Events Sampled: %d)\n", prof.Tag, prof.TotalSampled))

	// Categoricals
	if len(prof.Categoricals) > 0 {
		sb.WriteString("  Categorical Value Distributions:\n")
		catKeys := make([]string, 0, len(prof.Categoricals))
		for k := range prof.Categoricals {
			catKeys = append(catKeys, k)
		}
		sort.Strings(catKeys)
		for _, field := range catKeys {
			valCounts := prof.Categoricals[field]
			items := make([]string, 0, len(valCounts))
			for val, count := range valCounts {
				items = append(items, fmt.Sprintf("%q: %d", val, count))
			}
			sort.Strings(items)
			sb.WriteString(fmt.Sprintf("    - payload.%s: [%s]\n", field, strings.Join(items, ", ")))
		}
	}

	// Numerical Ranges
	if len(prof.Ranges) > 0 {
		sb.WriteString("  Numerical Ranges:\n")
		rangeKeys := make([]string, 0, len(prof.Ranges))
		for k := range prof.Ranges {
			rangeKeys = append(rangeKeys, k)
		}
		sort.Strings(rangeKeys)
		for _, field := range rangeKeys {
			r := prof.Ranges[field]
			sb.WriteString(fmt.Sprintf("    - payload.%s: min=%.2f, max=%.2f (samples: %d)\n",
				field, r.Min, r.Max, r.Count))
		}
	}

	// Exemplars (Sanitized payloads)
	if len(prof.Exemplars) > 0 {
		sb.WriteString("  Representative Payload Exemplars:\n")
		limit := 3
		if len(prof.Exemplars) < limit {
			limit = len(prof.Exemplars)
		}
		for i := 0; i < limit; i++ {
			ex := prof.Exemplars[i]
			jsonBytes, err := json.Marshal(ex.Payload)
			if err == nil {
				sb.WriteString(fmt.Sprintf("    Exemplar %d: %s\n", i+1, string(jsonBytes)))
			}
		}
	}
	sb.WriteString("\n")
}

// BuildRefinementPrompt constructs a prompt to tune an existing active circuit based on
// recent alerts, user feedback, schema drift, or firing rate anomalies.
func (b *PromptBuilder) BuildRefinementPrompt(
	userGoal string,
	targetTags []string,
	activeCircuit *types.Circuit,
	trigger string,
	reason string,
	alerts []history.AlertRecord,
	schemas map[string]*assay.SchemaNode,
	profiles map[string]*profiler.StreamProfile,
	sinks []sink.Descriptor,
) string {
	var sb strings.Builder

	sb.WriteString("### REFINEMENT TASK\n")
	sb.WriteString("You are tuning an existing active Flux Circuit in response to real production telemetry and feedback.\n")
	sb.WriteString("Your goal is to modify the Circuit DAG to resolve the trigger/feedback while maintaining high detection accuracy.\n\n")

	if userGoal != "" {
		sb.WriteString("### ORIGINAL USER GOAL\n")
		sb.WriteString(strings.TrimSpace(userGoal))
		sb.WriteString("\n\n")
	}

	sb.WriteString("### REFINEMENT TRIGGER & MOTIVATION\n")
	sb.WriteString(fmt.Sprintf("Trigger: %s\n", trigger))
	sb.WriteString(fmt.Sprintf("Reason: %s\n\n", reason))

	if activeCircuit != nil {
		sb.WriteString("### CURRENT ACTIVE CIRCUIT (TO BE UPDATED)\n")
		circuitBytes, err := json.MarshalIndent(activeCircuit, "", "  ")
		if err == nil {
			sb.WriteString(string(circuitBytes))
		}
		sb.WriteString("\n\n")
	}

	sb.WriteString("### TARGET STREAM TAGS\n")
	if len(targetTags) == 0 && activeCircuit != nil {
		targetTags = activeCircuit.Tags
	}
	for _, tag := range targetTags {
		sb.WriteString(fmt.Sprintf("- %s\n", tag))
	}
	sb.WriteString("\n")

	sb.WriteString("### RECENT ALERTS & OPERATOR FEEDBACK\n")
	if len(alerts) == 0 {
		sb.WriteString("No recent alert history available.\n\n")
	} else {
		for i, al := range alerts {
			sb.WriteString(fmt.Sprintf("Alert #%d (ID: %s):\n", i+1, al.ID))
			sb.WriteString(fmt.Sprintf("  Timestamp: %s\n", al.Timestamp.Format(time.RFC3339)))
			sb.WriteString(fmt.Sprintf("  Node: %s, Sink: %s\n", al.NodeName, al.SinkName))
			if al.Condition != "" {
				sb.WriteString(fmt.Sprintf("  Condition: %s\n", al.Condition))
			}
			if al.Feedback != nil {
				sb.WriteString(fmt.Sprintf("  *** OPERATOR FEEDBACK ***: Classification=%q", al.Feedback.Classification))
				if al.Feedback.Reason != "" {
					sb.WriteString(fmt.Sprintf(", Reason=%q", al.Feedback.Reason))
				}
				sb.WriteString("\n")
			}
			if len(al.Payload) > 0 {
				pBytes, _ := json.Marshal(al.Payload)
				sb.WriteString(fmt.Sprintf("  Payload: %s\n", string(pBytes)))
			}
			sb.WriteString("\n")
		}
	}

	sb.WriteString("### INFERRED STREAM SCHEMAS (from assay)\n")
	if len(schemas) == 0 {
		sb.WriteString("No schema changes detected.\n\n")
	} else {
		keys := make([]string, 0, len(schemas))
		for k := range schemas {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			node := schemas[k]
			sb.WriteString(fmt.Sprintf("Tag / Variant: %s\n", k))
			b.formatSchemaNode(&sb, "payload", node, 1)
			sb.WriteString("\n")
		}
	}

	sb.WriteString("### STREAM VALUE PROFILES & EXEMPLARS (from profiler)\n")
	if len(profiles) > 0 {
		pKeys := make([]string, 0, len(profiles))
		for k := range profiles {
			pKeys = append(pKeys, k)
		}
		sort.Strings(pKeys)
		for _, k := range pKeys {
			prof := profiles[k]
			b.formatProfile(&sb, prof)
		}
	}

	sb.WriteString("### AVAILABLE SINK DESTINATIONS\n")
	if len(sinks) > 0 {
		sink.SortDescriptors(sinks)
		for _, s := range sinks {
			desc := s.Description
			if desc == "" {
				desc = "No description provided"
			}
			sev := s.Severity
			if sev == "" {
				sev = "info"
			}
			stype := s.SinkType
			if stype == "" {
				stype = "generic"
			}
			sb.WriteString(fmt.Sprintf("- Sink Name: %q\n  Description: %s\n  Severity: %s\n  Type: %s\n",
				s.Name, desc, sev, stype))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("### REFINEMENT INSTRUCTIONS\n")
	sb.WriteString("1. If alerts were marked 'false_positive' or 'noisy', adjust the condition(s) or add steps to eliminate those false alerts.\n")
	sb.WriteString("2. If new fields or types appeared in schema drift, leverage them if relevant to improve detection.\n")
	sb.WriteString("3. If an alert storm occurred, tighten the threshold or require additional corroborating signals.\n")
	sb.WriteString("4. Output ONLY valid raw JSON representing the complete refined Circuit DAG. Do NOT use markdown code fences.\n")

	return sb.String()
}

