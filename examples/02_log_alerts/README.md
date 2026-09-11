# Example 2: Real-Time Production Log Stream & Multi-DAG Alerting Engine

This example demonstrates using **Flux** with **Capacitor** to ingest, analyze, and alert on a high-velocity production log stream in real time. It showcases multi-DAG decision routing, sliding window error rate calculation, and multi-channel alerting.

---

## 🏗️ Architecture Overview

```
                          Incoming Log Stream (Spark)
                                      │
                                      ▼
                        ┌───────────────────────────┐
                        │   log_ingress Root Node   │
                        │  (Extracts Level & Code)  │
                        └─────────────┬─────────────┘
                                      │
       ┌──────────────────────────────┼──────────────────────────────┐
       │                              │                              │
       ▼                              ▼                              ▼
┌──────────────────────────┐  ┌──────────────────────────┐  ┌──────────────────────────┐
│ critical_error_detector  │  │ http_5xx_rate_evaluator  │  │   healthy_log_baseline   │
│ (FATAL, CRITICAL, OOM,   │  │ (Status >= 500)          │  │ (Status < 500, INFO)     │
│ DatabaseConnectionError) │  │                          │  │                          │
└────────────┬─────────────┘  └─────────────┬────────────┘  └────────────┬─────────────┘
             │                              │                            │
             ▼                              ▼                            ▼
  PagerDuty Alert Sink          Sliding Window Count (60s)         Normal Ingest
  (Immediate Paging)           via Capacitor Window Engine        (Status: HEALTHY)
                                            │
                             ┌──────────────┴──────────────┐
                             ▼                             ▼
                 [Count >= 5 in 60s]              [Count < 5 in 60s]
                 Slack DevOps Alert               Transient Spike Ignored
                 (Rate Limit Exceeded)            (Metric Recorded Only)
```

---

## ⚡ Key Highlights

1. **Stateful Sliding Window Counter (`window.count`)**:
   Uses Capacitor's distributed sliding window engine to track 5xx error spikes per service over a rolling 60-second window.
2. **Noise Suppression / Alert Fatigue Prevention**:
   The first few transient 5xx errors (counts 1–4) are safely recorded without waking engineers. When error velocity breaches the threshold ($\ge 5$ in 60s), the Slack DevOps alert fires immediately.
3. **Instant Critical Escalation**:
   High-severity fatal errors (`OutOfMemory`, `DeadlockDetected`, `DatabaseConnectionError`, or `FATAL`/`CRITICAL` log levels) bypass rate counters and dispatch directly to the PagerDuty on-call sink.
4. **Declarative DAG Definition**:
   The entire alerting topology is defined declaratively in [`log_alerts.circuit.json`](log_alerts.circuit.json) using Google CEL and VoltScript.

---

## 🚀 Running the Example

Execute from the repository root:
```bash
go run ./examples/02_log_alerts/main.go
```

Or run directly within the directory:
```bash
cd examples/02_log_alerts
go run main.go
```

---

## 📊 Sample Output

```
================================================================================
   🚨 FLUX + CAPACITOR: REAL-TIME PRODUCTION LOG STREAM & ALERTING ENGINE
================================================================================
✔ Loaded and deployed circuit "production_log_alert_pipeline" to Flux Registry (Tags: [stream:logs stream:production_logs])

▶ Streaming 100 Production Logs into Flux Engine via Spark...

  [Log #013] [order-service  ] ERROR 503 | ⚠️ 5XX_TRANSIENT_SPIKE       | Window Count: 1/5.0 (Tolerated below threshold, no alert)
  [Log #025] [payment-gateway] INFO  200 | Status: HEALTHY    | Request handled successfully
  [Log #029] [order-service  ] ERROR 502 | ⚠️ 5XX_TRANSIENT_SPIKE       | Window Count: 2/5.0 (Tolerated below threshold, no alert)
      📟 [PAGERDUTY DISPATCH] High-severity incident on-call paged!
  [Log #046] [payment-gateway] FATAL 500 | 💥 CRITICAL_SEVERITY_INCIDENT | ACTION: PAGE_ONCALL -> OutOfMemory: Java heap space during batch reconciliation
  [Log #050] [order-service  ] INFO  200 | Status: HEALTHY    | Request handled successfully
  [Log #071] [order-service  ] ERROR 500 | ⚠️ 5XX_TRANSIENT_SPIKE       | Window Count: 3/5.0 (Tolerated below threshold, no alert)
  [Log #072] [order-service  ] ERROR 500 | ⚠️ 5XX_TRANSIENT_SPIKE       | Window Count: 4/5.0 (Tolerated below threshold, no alert)
  [Log #073] [order-service  ] ERROR 500 | 🚨 5XX_RATE_LIMIT_EXCEEDED   | Window Count: 5/5.0 -> ACTION: NOTIFY_SLACK_DEVOPS
      💬 [SLACK #devops-alerts] 5xx rate threshold breached! Alert sent to channel.
...
================================================================================
   📊 STREAM PROCESSING & ALERTING SUMMARY
================================================================================
Total Logs Processed:        100 logs in 44ms
Average Latency per Log:     440µs
Healthy Normal Logs:         90
Transient 5xx (Suppressed):  4 (Ignored as expected while below threshold)
5xx Rate Alerts Fired:       5 (Dispatched to Slack DevOps sink)
Critical Incidents Fired:    1 (Dispatched to PagerDuty sink)
================================================================================
```
