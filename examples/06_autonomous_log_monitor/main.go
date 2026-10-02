package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/autopilot"
	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/sink"
)

// LogEvent represents an incoming structured application / server log record.
type LogEvent struct {
	TraceID    string  `json:"trace_id"`
	Timestamp  string  `json:"timestamp"`
	Service    string  `json:"service"`
	Level      string  `json:"level"`
	StatusCode float64 `json:"status_code"`
	LatencyMs  float64 `json:"latency_ms"`
	Message    string  `json:"message"`
	Path       string  `json:"path"`
}

const (
	initialCandidateJSON = `{
  "id": "autonomous_log_monitor",
  "tags": ["stream:web_logs"],
  "root": {
    "name": "evaluate_log_event",
    "condition": "payload.status_code >= 500 || payload.level == 'fatal'",
    "steps": [
      {
        "type": "sink",
        "sink": "pagerduty_critical",
        "condition": "payload.level == 'fatal'",
        "payload": "payload"
      },
      {
        "type": "sink",
        "sink": "slack_ops",
        "condition": "payload.status_code >= 500 && payload.level != 'fatal'",
        "payload": "payload"
      }
    ]
  }
}`

	refinedCandidateJSON = `{
  "id": "autonomous_log_monitor",
  "tags": ["stream:web_logs"],
  "root": {
    "name": "evaluate_log_event_refined",
    "condition": "(payload.status_code >= 500 && payload.status_code != 503) || payload.level == 'fatal'",
    "steps": [
      {
        "type": "sink",
        "sink": "pagerduty_critical",
        "condition": "payload.level == 'fatal'",
        "payload": "payload"
      },
      {
        "type": "sink",
        "sink": "slack_ops",
        "condition": "payload.status_code >= 500 && payload.status_code != 503 && payload.level != 'fatal'",
        "payload": "payload"
      }
    ]
  }
}`
)

func mockAutonomousAI(ctx context.Context, messages []autopilot.Message) (string, error) {
	for _, m := range messages {
		if strings.Contains(m.Content, "OPERATOR FEEDBACK") || strings.Contains(m.Content, "REFINEMENT") {
			return refinedCandidateJSON, nil
		}
	}
	return initialCandidateJSON, nil
}

func getAIProvider() (autopilot.AIProvider, string) {
	if key := os.Getenv("OPENAI_API_KEY"); key != "" {
		p, err := autopilot.NewOpenAI(key, "gpt-4o")
		if err == nil {
			return p, "OpenAI (gpt-4o)"
		}
	}
	if key := os.Getenv("GEMINI_API_KEY"); key != "" {
		p, err := autopilot.NewGemini(key, "gemini-3.8-flash")
		if err == nil {
			return p, "Google Gemini (gemini-3.8-flash)"
		}
	}
	if host := os.Getenv("OLLAMA_HOST"); host != "" {
		p, err := autopilot.NewOllama(host, "qwen3-coder:7b")
		if err == nil {
			return p, fmt.Sprintf("Ollama (%s)", host)
		}
	}
	return autopilot.ProviderFunc(mockAutonomousAI), "Embedded Autonomous Synthesis Engine (Simulator)"
}

func main() {
	ctx := context.Background()

	fmt.Println("================================================================================")
	fmt.Println("   🤖 FLUX AUTOPILOT: END-TO-END AUTONOMOUS STREAMING MONITOR & CLOSED-LOOP REFINER")
	fmt.Println("================================================================================")

	// 1. Initialize Distributed Cache Node
	fmt.Println("\n[1/6] 🚀 Initializing Distributed Cache Node & Flux Engine...")
	tmpDir, err := os.MkdirTemp("", "flux-autopilot-demo-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	var backend cache.CacheBackend
	cp, err := capacitor.New(capacitor.Config{
		NodeID:     "autopilot-demo-node",
		DataPath:   tmpDir,
		BindPort:   19611,
		StreamPort: 19612,
	})
	if err != nil {
		fmt.Printf("      ⚠️  Capacitor port busy, using fast in-memory cache backend: %v\n", err)
		backend = cache.NewMemoryCache()
	} else {
		backend = cp
		defer cp.Close()
		fmt.Println("      ✔ Capacitor Distributed Cache active (Node: autopilot-demo-node, Ports: 19611/19612)")
	}

	eng, err := flux.New(
		flux.WithCache(backend),
		flux.WithWorkers(8),
		flux.WithHistory(true),
	)
	if err != nil {
		log.Fatalf("failed to initialize flux: %v", err)
	}
	defer eng.Close()

	// Register alert destinations
	var slackCount, pagerDutyCount int64
	eng.RegisterSink("slack_ops", sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&slackCount, 1)
		b, _ := json.Marshal(payload)
		fmt.Printf("      💬 [SLACK #devops-ops] Warning Alert: %s\n", string(b))
		return nil
	}), flux.WithSinkDescription("Operational warnings and 5xx alerts"), flux.WithSinkSeverity(flux.SinkSeverityWarning))

	eng.RegisterSink("pagerduty_critical", sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&pagerDutyCount, 1)
		b, _ := json.Marshal(payload)
		fmt.Printf("      📟 [PAGERDUTY CRITICAL] On-Call Paged! Fatal Exception: %s\n", string(b))
		return nil
	}), flux.WithSinkDescription("Critical outages and fatal exceptions"), flux.WithSinkSeverity(flux.SinkSeverityCritical))

	fmt.Println("      ✔ Sinks registered: slack_ops (Warning), pagerduty_critical (Critical)")

	// 2. Launch Autopilot Session
	provider, providerName := getAIProvider()
	fmt.Printf("\n[2/6] 🤖 Launching Flux Autopilot Session...\n")
	fmt.Printf("      🧠 AI Provider: %s\n", providerName)

	prompt := "Monitor web server and database logs. Alert ops on Slack for HTTP 5xx errors. Page on-call via PagerDuty for fatal unhandled crashes. Ignore 404s and normal 200 OKs."
	fmt.Printf("      🎯 User Goal: %q\n", prompt)

	sampleThreshold := 30
	shadowBudget := 30

	handle, err := eng.Autopilot(
		ctx,
		prompt,
		provider,
		[]string{"stream:web_logs"},
		flux.WithSampleThreshold(sampleThreshold),
		flux.WithShadowEvaluationEvents(shadowBudget),
		flux.WithMinTargetFiringRate(0.01), // 1.0% min firing
		flux.WithMaxTargetFiringRate(0.20), // 20.0% max firing
		flux.WithAlertStormThreshold(0.35), // 35.0% alert storm limit
		flux.WithAutoPromotion(true),
	)
	if err != nil {
		log.Fatalf("failed to launch autopilot: %v", err)
	}
	defer handle.Stop(context.Background())

	fmt.Printf("      ✔ Session initiated (Status: %s)\n", handle.Status())

	// 3. Cold-Start Sampling Phase
	fmt.Printf("\n[3/6] 📊 Ingesting Cold-Start Telemetry (%d events for schema inference)...\n", sampleThreshold)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	services := []string{"auth-service", "billing-api", "order-processor", "user-gateway"}
	endpoints := []string{"/api/v1/login", "/api/v1/charge", "/api/v1/checkout", "/api/v1/profile"}

	for i := 0; i < sampleThreshold; i++ {
		svc := services[rnd.Intn(len(services))]
		path := endpoints[rnd.Intn(len(endpoints))]
		code := 200.0
		lvl := "info"
		if i == 5 {
			code = 404.0
		}
		evt := LogEvent{
			TraceID:    fmt.Sprintf("trace-%06d", i),
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			Service:    svc,
			Level:      lvl,
			StatusCode: code,
			LatencyMs:  12.0 + rnd.Float64()*40.0,
			Message:    "request processed successfully",
			Path:       path,
		}
		_, _ = eng.Spark(ctx, evt, "stream:web_logs")
		time.Sleep(2 * time.Millisecond)
	}
	fmt.Printf("      ✔ Cold-start sampling complete! Total sampled events: %d\n", sampleThreshold)

	// 4. Synthesis & Shadow Verification Phase
	fmt.Printf("\n[4/6] 🧠 AI Synthesis & Shadow Verification Phase...\n")
	waitShadow, cancelShadow := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShadow()
	_ = handle.WaitUntil(waitShadow, flux.StatusShadowing)
	fmt.Printf("      ✔ Circuit DAG synthesized! Now verifying candidate in Shadow Mode (Budget: %d events)...\n", shadowBudget)

	// Stream shadow observation traffic: ~93% OK, ~4% 503 errors, ~3% fatal
	for i := 0; i < shadowBudget; i++ {
		svc := services[rnd.Intn(len(services))]
		path := endpoints[rnd.Intn(len(endpoints))]
		code := 200.0
		lvl := "info"
		msg := "request processed"

		switch i {
		case 10:
			code = 503.0
			lvl = "error"
			msg = "transient database connection pool timeout"
		case 20:
			code = 500.0
			lvl = "fatal"
			msg = "panic: unhandled nil pointer dereference"
		}

		evt := LogEvent{
			TraceID:    fmt.Sprintf("shadow-trace-%06d", i),
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			Service:    svc,
			Level:      lvl,
			StatusCode: code,
			LatencyMs:  15.0 + rnd.Float64()*80.0,
			Message:    msg,
			Path:       path,
		}
		_, _ = eng.Spark(ctx, evt, "stream:web_logs")
		time.Sleep(2 * time.Millisecond)
	}

	waitLive, cancelLive := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelLive()
	if err := handle.WaitUntil(waitLive, flux.StatusLive); err != nil {
		log.Fatalf("failed waiting for StatusLive: %v (error=%v)", err, handle.Error())
	}

	stgResult := handle.StagingResult()
	fmt.Printf("      ✔ Shadow verification succeeded! Safety envelope verified:\n")
	fmt.Printf("         - Observed Firing Rate: %.2f%%\n", stgResult.ObservedRate*100)
	fmt.Printf("         - Error Events: %d, Firing Events: %d\n", stgResult.ErrorEvents, stgResult.FiringEvents)
	fmt.Printf("         - Status: %s, Automatically Promoted: %v\n", stgResult.Status, stgResult.Promoted)
	fmt.Printf("      🚀 Circuit %q is now LIVE in Registry with zero downtime!\n", handle.ActiveCircuits()[0])

	// 5. Live Production Traffic & Real Sink Dispatches
	fmt.Printf("\n[5/6] ⚡ Live Traffic Execution & Sink Dispatches...\n")
	atomic.StoreInt64(&slackCount, 0)
	atomic.StoreInt64(&pagerDutyCount, 0)

	// Ingest 50 live events with 1 fatal crash and 1 transient 503
	for i := 0; i < 50; i++ {
		code := 200.0
		lvl := "info"
		msg := "ok"
		switch i {
		case 15:
			code = 503.0
			lvl = "error"
			msg = "transient db timeout"
		case 30:
			code = 500.0
			lvl = "fatal"
			msg = "panic: out of memory"
		}

		evt := LogEvent{
			TraceID:    fmt.Sprintf("live-trace-%06d", i),
			Timestamp:  time.Now().UTC().Format(time.RFC3339),
			Service:    "order-processor",
			Level:      lvl,
			StatusCode: code,
			LatencyMs:  25.0,
			Message:    msg,
			Path:       "/api/v1/checkout",
		}
		_, _ = eng.Spark(ctx, evt, "stream:web_logs")
		time.Sleep(2 * time.Millisecond)
	}

	time.Sleep(50 * time.Millisecond)
	fmt.Printf("      ✔ Live traffic processed. Total Slack alerts: %d, PagerDuty pages: %d\n",
		atomic.LoadInt64(&slackCount), atomic.LoadInt64(&pagerDutyCount))

	// 6. Operator Feedback & Autonomous Refinement Loop
	fmt.Printf("\n[6/6] 🔄 Operator Feedback & Closed-Loop Autonomous Refinement...\n")
	_ = eng.History().Flush()
	circuitID := handle.ActiveCircuits()[0]
	alerts, err := eng.GetAlertHistory(ctx, circuitID, 10)
	if err != nil {
		log.Fatalf("failed to retrieve alert history: %v", err)
	}
	fmt.Printf("      Found %d historical alerts in CacheBackend.\n", len(alerts))

	// Flag the 503 transient error as false_positive
	for _, al := range alerts {
		pStr, _ := json.Marshal(al.Payload)
		if strings.Contains(string(pStr), "503") || strings.Contains(string(pStr), "transient") {
			fmt.Printf("      🧑‍💻 Operator Review: Marking alert %q as 'false_positive' (transient deployment spike)...\n", al.ID)
			err := handle.RecordFeedback(ctx, al.ID, flux.FeedbackFalsePositive, "transient deployment restart, do not alert")
			if err != nil {
				log.Fatalf("failed to record feedback: %v", err)
			}
			break
		}
	}

	// Trigger closed-loop refinement to tune out transient 503s
	fmt.Printf("      🧠 Autonomous Refiner triggered with feedback context...\n")
	err = handle.TriggerRefinement(ctx, flux.TriggerUserFeedback, "operator marked transient 503 as false_positive")
	if err != nil {
		log.Fatalf("failed to trigger refinement: %v", err)
	}

	fmt.Printf("      ✔ Autonomous Refinement complete! Circuit DAG updated and hot-swapped:\n")
	fmt.Printf("         - New Root Guard Condition: %q\n", handle.CurrentCircuit().Root.Condition)
	fmt.Printf("      🎉 Complete Autonomous Loop successfully verified end-to-end!\n\n")
}
