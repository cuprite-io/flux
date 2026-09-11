# Flux

<p align="center">
  <b>Embedded reactive rule engine and candidate scoring kernel for Go.</b>
</p>

<p align="center">
  <img src="assets/mascot.png" alt="Flux Mascot" width="300" />
</p>

<p align="center">
  Flux is a high-throughput decision kernel designed for Go applications. It evaluates hierarchical decision trees (DAGs), processes reactive event streams, and scores candidate datasets against state layers.
</p>

---

## ⚡ Overview

Flux provides two primary execution paradigms:

1. **`Spark` (Reactive Stream Evaluation)**: Evaluates incoming streaming payloads against tagged Circuits. Features multi-circuit matching, step-level transformations, parallel branch evaluation, output projection, and asynchronous side-effect sinks.
2. **`Conduct` (Candidate Item Qualification & Scoring)**: Evaluates candidate items against entity state. Ineligible items are silently omitted, while qualified candidates have dynamic properties projected directly into caller responses.

---

## ✨ Key Features

- **FluxVM**: 64-bit register-based virtual machine utilizing 16-byte pointerless tagged `Value` unions to bypass Go GC heap scans.
- **Cache-Line Aligned Instructions**: Instructions are 64-byte aligned with compile-time assertions to maximize L1/L2 CPU cache residency.
- **Dual-Path Parallel Engine**: Adaptive execution engine running single-child paths inline and dispatching branching children ($\ge 2$) concurrently across a pre-warmed worker pool.
- **$O(1)$ Bitmask Branch Pruning**: Failed guard conditions instantly prune entire descendant sub-trees using atomic bitmask operations (`DeadMask`).
- **Stateless Category-Partitioned Catalog**: Eliminates duplicate in-memory caching by reading candidate item partitions directly from the underlying cache layer in a single batch read.
- **VoltScript Expression Engine**: An extended dialect of Google's Common Expression Language (CEL), equipped with an extensive standard library.

---

## 📦 Installation

```bash
go get github.com/cuprite-io/flux
```

---

## 🚀 Quickstart

### 1. Reactive Stream Pipeline (`Spark`)

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/types"
)

func main() {
	ctx := context.Background()

	// 1. Initialize Engine
	eng, err := flux.New(flux.WithWorkers(4))
	if err != nil {
		log.Fatal(err)
	}
	defer eng.Close()

	// 2. Build a Decision Tree Circuit
	root := types.NewNode("security_guard").
		Step(flux.Volt(`set('masked_card', mask.card(payload.card_id))`))

	// High Value Branch
	highValueNode := types.NewNode("high_value_branch").
		WithCondition(`payload.amount >= 1000.0`).
		Step(flux.Return(map[string]any{
			"status": "REQUIRES_REVIEW",
			"risk":   "HIGH",
			"card":   "masked_card",
		}))

	// Standard Branch
	standardNode := types.NewNode("standard_branch").
		WithCondition(`payload.amount < 1000.0`).
		Step(flux.Return(map[string]any{
			"status": "APPROVED",
			"risk":   "LOW",
			"card":   "masked_card",
		}))

	root.AddChildren(highValueNode, standardNode)

	circuit := types.NewCircuit("payment_evaluator").
		WithTags("stream:payments").
		WithRoot(root)

	_ = eng.Registry().Put(ctx, circuit)

	// 3. Evaluate an Incoming Event
	payload := map[string]any{
		"amount":  2500.0,
		"card_id": "4111-2222-3333-4444",
	}

	res, err := eng.Spark(ctx, payload, "stream:payments")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Decision: %+v\n", res.ReturnedData)
	// Output: Decision: map[card:4111-XXXX-XXXX-4444 risk:HIGH status:REQUIRES_REVIEW]
}
```

---

### 2. Candidate Item Scoring (`Conduct`)

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/types"
)

func main() {
	ctx := context.Background()
	eng, err := flux.New()
	if err != nil {
		log.Fatal(err)
	}
	defer eng.Close()

	// 1. Register candidate items in the catalog
	item := &types.Item{
		ID:       "vip_pass",
		Category: "memberships",
		Data: map[string]any{
			"base_price": 100.0,
		},
		Circuit: types.NewCircuit("vip_qualify").WithRoot(
			types.NewNode("check_eligibility").
				WithCondition("user.level >= 20.0").
				Step(flux.Return(map[string]any{
					"discount": 0.25,
					"tier":     "ELITE",
				})),
		),
	}
	_ = eng.Catalog().Put(ctx, item)

	// 2. Evaluate candidates against user context
	req := &types.ConductRequest{
		Category: "memberships",
		Context: map[string]any{
			"level": 25.0,
		},
	}

	result, err := eng.Conduct(ctx, req)
	if err != nil {
		log.Fatal(err)
	}

	for _, it := range result.Items {
		fmt.Printf("Qualified: %s -> Computed: %+v\n", it.ID, it.ComputedOutput)
	}
}
```

---

## 📄 Declarative JSON & YAML Loading

Flux supports defining and loading Circuits and Items from declarative **JSON** and **YAML** files.

### `security_check.circuit.json`
```json
{
  "id": "security_pipeline",
  "tags": ["stream:events"],
  "root": {
    "name": "root_guard",
    "steps": [
      {
        "type": "volt",
        "script": "set('dist_km', geo.distance_km(user.lat, user.lon, payload.lat, payload.lon))"
      }
    ],
    "children": [
      {
        "name": "anomaly_branch",
        "condition": "dist_km > 500.0",
        "steps": [
          {
            "type": "sink",
            "sink": "soc_incident_webhook"
          },
          {
            "type": "return",
            "data": {
              "status": "CHALLENGE_REQUIRED",
              "reason": "GEOGRAPHICAL_ANOMALY"
            }
          },
          {
            "type": "abort"
          }
        ]
      },
      {
        "name": "clear_branch",
        "condition": "dist_km <= 500.0",
        "steps": [
          {
            "type": "return",
            "data": {
              "status": "CLEARED"
            }
          }
        ]
      }
    ]
  }
}
```

### Loading Declarative Files

```go
// Load single Circuit file (JSON or YAML)
circuit, err := flux.LoadCircuitFile("security_check.circuit.json")
_ = eng.Registry().Put(ctx, circuit)

// Load single Item file (JSON or YAML)
item, err := flux.LoadItemFile("membership.item.json")
_ = eng.Catalog().Put(ctx, item)

// Batch load all circuits from a directory
circuits, err := flux.LoadCircuitsFromDir("./circuits")
for _, c := range circuits {
    _ = eng.Registry().Put(ctx, c)
}
```

---

## 📚 VoltScript Language Reference

**VoltScript** is built on top of **Google's Common Expression Language (CEL)**, extending standard CEL grammar with safe state mutation operators and an expanded standard library:

| Category | Operators |
| :--- | :--- |
| **State & Maps** | `set(k, v)`, `get(k, fallback)`, `map.merge(m1, m2)`, `map.delete(m, k)` |
| **Security & Masking** | `is_pii(str)`, `mask.email(email)`, `mask.card(card)` |
| **Cryptography** | `crypto.sha256(v)`, `crypto.sha512(v)`, `crypto.md5(v)`, `crypto.hmac(v, k)`, `crypto.encrypt(v, k)`, `crypto.decrypt(v, k)` |
| **Geo-Spatial** | `geo.distance_km(lat1, lon1, lat2, lon2)`, `geo.dist_km(...)`, `geo.dist_m(...)` |
| **Math & Lists** | `math.clamp(val, min, max)`, `list.unique(list)` |
| **Encoding & Identifiers**| `uuid.v4()`, `base64.encode(str)`, `base64.decode(str)`, `hex.encode(str)`, `hex.decode(str)` |
| **Stateful Cache** | `window.count(key, duration)`, `cache.get(key)` |

---

## 🌐 Distributed Architecture with Capacitor

Flux seamlessly integrates with **[Capacitor](https://github.com/cuprite-io/capacitor)** as an embedded, distributed caching and state synchronization plane:

- **Category-Partitioned Maps**: Catalog items are stored directly in Capacitor's partitioned maps (`MapSet`, `MapGetAll`), keeping Flux memory-free.
- **Instant Multi-Node State Convergence**: When state is modified on Node A, Capacitor's Delta Log streams replicate changes to Node B. Node B's Flux engine reflects the updated state on the next evaluation with zero local cache invalidation delays.

---

## 💡 Production Real-World Examples

Explore complete runnable examples with full Capacitor integration in the [`examples/`](examples/) directory:

1. **[`01_fraud_detection`](examples/01_fraud_detection/)**: Real-time fraud detection combining historical all-time high amounts, CRDT device sets, geo-spatial speed vectors, and multi-sink alerting.
2. **[`02_log_alerts`](examples/02_log_alerts/)**: Production log streaming and alerting engine demonstrating multi-DAG decision trees, rolling 60s 5xx error rate thresholds via `window.count`, noise suppression, and multi-channel alerting.
3. **[`03_gaming_boss_kill`](examples/03_gaming_boss_kill/)**: Parallel branch execution dispatching reward calculations and external Discord webhook notifications simultaneously.
4. **[`04_adaptive_mfa_access`](examples/04_adaptive_mfa_access/)**: Adaptive Zero-Trust Multi-Factor Authentication (MFA) and Risk-Based Access Control combining Spark risk assessment streams, Capacitor sliding-window IP burst velocity and failed login counters, Tor/Datacenter ASN reputation, headless browser anomaly detection, and Conduct dynamic policy pruning.
5. **[`05_distributed_rate_limiting`](examples/05_distributed_rate_limiting/)**: High-throughput multi-tenant API gateway rate limiter using Capacitor sliding window counters.

---

## 📄 License

Flux is released under the [MIT License](LICENSE).
