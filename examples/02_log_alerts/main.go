package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/sink"
)

// LogEvent represents an incoming production application/system log stream record.
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

func resolveFilePath(filename string) string {
	if _, err := os.Stat(filename); err == nil {
		return filename
	}
	subPath := filepath.Join("examples", "02_log_alerts", filename)
	if _, err := os.Stat(subPath); err == nil {
		return subPath
	}
	return filename
}

func main() {
	ctx := context.Background()

	fmt.Println("================================================================================")
	fmt.Println("   🚨 FLUX + CAPACITOR: REAL-TIME PRODUCTION LOG STREAM & ALERTING ENGINE")
	fmt.Println("================================================================================")

	// 1. Initialize Capacitor distributed caching node for sliding window counters
	tmpDir, err := os.MkdirTemp("", "capacitor-logalerts-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cp, err := capacitor.New(capacitor.Config{
		NodeID:     "log-alert-node-01",
		DataPath:   tmpDir,
		BindPort:   19511,
		StreamPort: 19512,
	})
	if err != nil {
		log.Fatalf("failed to start capacitor: %v", err)
	}

	// 2. Initialize Flux Rule & Workflow Engine connected to Capacitor
	eng, err := flux.New(flux.WithCache(cp), flux.WithWorkers(8))
	if err != nil {
		log.Fatalf("failed to start flux: %v", err)
	}
	defer eng.Close()

	// 3. Register production alert notification sinks
	var pagerDutyCount, slackCount int64
	var verboseSinks atomic.Bool
	verboseSinks.Store(true)

	eng.RegisterSink("pagerduty_alerts", sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&pagerDutyCount, 1)
		if verboseSinks.Load() {
			fmt.Printf("      📟 [PAGERDUTY DISPATCH] High-severity incident on-call paged!\n")
		}
		return nil
	}))

	eng.RegisterSink("slack_devops_alerts", sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&slackCount, 1)
		if verboseSinks.Load() {
			fmt.Printf("      💬 [SLACK #devops-alerts] 5xx rate threshold breached! Alert sent to channel.\n")
		}
		return nil
	}))

	// 4. Load declarative Log Alerting Decision Circuit DAG
	circuitPath := resolveFilePath("log_alerts.circuit.json")
	circuit, err := flux.LoadCircuitFile(circuitPath)
	if err != nil {
		log.Fatalf("failed to load circuit from %s: %v", circuitPath, err)
	}
	if err := eng.Registry().Put(ctx, circuit); err != nil {
		log.Fatalf("failed to register circuit: %v", err)
	}
	fmt.Printf("✔ Loaded and deployed circuit %q to Flux Registry (Tags: %v)\n\n", circuit.ID, circuit.Tags)

	// 5. Generate a realistic production stream of 100 logs
	// Features:
	// - Mostly healthy 200 OK traffic (INFO)
	// - First few transient 5xx errors scattered early (below threshold of 5 in 60s, ignored/tolerated)
	// - Critical incident (OutOfMemory / FATAL) at log #45 triggering instant PagerDuty dispatch
	// - Concentrated surge of 5xx errors (logs #70-#78) on 'order-service' exceeding 5 in 60s threshold, triggering Slack alert
	const totalLogs = 100
	logs := make([]LogEvent, totalLogs)

	services := []string{"auth-service", "order-service", "payment-gateway", "inventory-api"}
	paths := []string{"/api/v1/login", "/api/v1/orders/checkout", "/api/v1/payments/charge", "/api/v1/items/search"}

	rnd := rand.New(rand.NewSource(42))
	baseTime := time.Now().Add(-100 * time.Second)

	for i := 0; i < totalLogs; i++ {
		svc := services[rnd.Intn(len(services))]
		pth := paths[rnd.Intn(len(paths))]
		ts := baseTime.Add(time.Duration(i) * time.Second).Format(time.RFC3339)
		traceID := fmt.Sprintf("trace-%04d", i+1)

		// Normal baseline: 200 OK
		logs[i] = LogEvent{
			TraceID:    traceID,
			Timestamp:  ts,
			Service:    svc,
			Level:      "INFO",
			StatusCode: 200,
			LatencyMs:  float64(20 + rnd.Intn(50)),
			Message:    "Request handled successfully",
			Path:       pth,
		}

		// Inject specific production test cases:
		switch i {
		case 12:
			// Transient 5xx #1 on order-service (Window count: 1 -> Ignored)
			logs[i].Service = "order-service"
			logs[i].Level = "ERROR"
			logs[i].StatusCode = 503
			logs[i].Message = "Service Unavailable: downstream inventory read timeout"
		case 28:
			// Transient 5xx #2 on order-service (Window count: 2 -> Ignored)
			logs[i].Service = "order-service"
			logs[i].Level = "ERROR"
			logs[i].StatusCode = 502
			logs[i].Message = "Bad Gateway: connection reset by peer"
		case 45:
			// Critical fatal incident on payment-gateway
			logs[i].Service = "payment-gateway"
			logs[i].Level = "FATAL"
			logs[i].StatusCode = 500
			logs[i].Message = "OutOfMemory: Java heap space during batch reconciliation"
		case 70, 71, 72, 73, 74, 75, 76:
			// Rapid 5xx storm on order-service:
			// Logs 70, 71 (counts 3, 4) -> below threshold (Ignored)
			// Logs 72-76 (counts 5, 6, 7, 8, 9) -> threshold >= 5 met! -> Alert fired!
			logs[i].Service = "order-service"
			logs[i].Level = "ERROR"
			logs[i].StatusCode = 500
			logs[i].Message = "InternalServerError: Postgres transaction connection pool exhausted"
		case 88:
			// Client error (401 Unauthorized, not a 5xx or fatal error)
			logs[i].Level = "WARN"
			logs[i].StatusCode = 401
			logs[i].Message = "Unauthorized: invalid JWT signature"
		}
	}

	fmt.Printf("▶ Streaming %d Production Logs into Flux Engine via Spark...\n\n", totalLogs)

	var normalCount, transient5xxCount, surgeAlertCount, criticalAlertCount int

	streamStart := time.Now()

	for i, logItem := range logs {
		res, err := eng.Spark(ctx, logItem, "stream:logs")
		if err != nil {
			log.Fatalf("spark error on log #%d: %v", i+1, err)
		}

		status := fmt.Sprintf("%v", res.ReturnedData["status"])
		action := fmt.Sprintf("%v", res.ReturnedData["action"])
		alertType := fmt.Sprintf("%v", res.ReturnedData["alert_type"])

		switch status {
		case "HEALTHY":
			normalCount++
			if (i+1)%25 == 0 {
				fmt.Printf("  [Log #%03d] [%-15s] %-5s %3.0f | Status: %-10s | %s\n",
					i+1, logItem.Service, logItem.Level, logItem.StatusCode, status, logItem.Message)
			}
		case "IGNORED_BELOW_THRESHOLD":
			transient5xxCount++
			windowCount := res.ReturnedData["current_error_count_in_window"]
			fmt.Printf("  [Log #%03d] [%-15s] %-5s %3.0f | ⚠️ %-25s | Window Count: %v/5.0 (Tolerated below threshold, no alert)\n",
				i+1, logItem.Service, logItem.Level, logItem.StatusCode, alertType, windowCount)
		case "ALERT_TRIGGERED":
			if alertType == "5XX_RATE_LIMIT_EXCEEDED" {
				surgeAlertCount++
				windowCount := res.ReturnedData["current_error_count_in_window"]
				fmt.Printf("  [Log #%03d] [%-15s] %-5s %3.0f | 🚨 %-25s | Window Count: %v/5.0 -> ACTION: %s\n",
					i+1, logItem.Service, logItem.Level, logItem.StatusCode, alertType, windowCount, action)
			} else if alertType == "CRITICAL_SEVERITY_INCIDENT" {
				criticalAlertCount++
				fmt.Printf("  [Log #%03d] [%-15s] %-5s %3.0f | 💥 %-25s | ACTION: %s -> %s\n",
					i+1, logItem.Service, logItem.Level, logItem.StatusCode, alertType, action, logItem.Message)
			}
		}
	}

	streamDuration := time.Since(streamStart)
	time.Sleep(50 * time.Millisecond) // brief pause for async sink queue flush

	fmt.Println("\n================================================================================")
	fmt.Println("   📊 STREAM PROCESSING & ALERTING SUMMARY")
	fmt.Println("================================================================================")
	fmt.Printf("Total Logs Processed:        %d logs in %v\n", totalLogs, streamDuration)
	fmt.Printf("Average Latency per Log:     %v\n", streamDuration/time.Duration(totalLogs))
	fmt.Printf("Healthy Normal Logs:         %d\n", normalCount)
	fmt.Printf("Transient 5xx (Suppressed):  %d (Ignored as expected while below threshold)\n", transient5xxCount)
	fmt.Printf("5xx Rate Alerts Fired:       %d (Dispatched to Slack DevOps sink)\n", surgeAlertCount)
	fmt.Printf("Critical Incidents Fired:    %d (Dispatched to PagerDuty sink)\n", criticalAlertCount)
	fmt.Printf("PagerDuty Sinks Dispatched:  %d\n", atomic.LoadInt64(&pagerDutyCount))
	fmt.Printf("Slack Sinks Dispatched:      %d\n", atomic.LoadInt64(&slackCount))

	// 6. High-Throughput Micro-Benchmark: 5,000 Log Ingestions
	fmt.Println("\n================================================================================")
	fmt.Println("   ⚡ HIGH-THROUGHPUT LOG STREAMING BENCHMARK (5,000 OPS)")
	fmt.Println("================================================================================")

	verboseSinks.Store(false)

	const benchIters = 5000
	benchLog := LogEvent{
		TraceID:    "trace-bench-001",
		Timestamp:  time.Now().Format(time.RFC3339),
		Service:    "order-service",
		Level:      "INFO",
		StatusCode: 200,
		LatencyMs:  35.0,
		Message:    "Benchmark request evaluation",
		Path:       "/api/v1/orders/checkout",
	}

	latencies := make([]float64, benchIters)
	tBenchStart := time.Now()
	for i := 0; i < benchIters; i++ {
		t0 := time.Now()
		_, _ = eng.Spark(ctx, benchLog, "stream:logs")
		latencies[i] = float64(time.Since(t0).Nanoseconds())
	}
	tBench := time.Since(tBenchStart)

	sort.Float64s(latencies)
	p50 := time.Duration(latencies[int(float64(benchIters)*0.50)]) * time.Nanosecond
	p90 := time.Duration(latencies[int(float64(benchIters)*0.90)]) * time.Nanosecond
	p99 := time.Duration(latencies[int(float64(benchIters)*0.99)]) * time.Nanosecond
	var sum float64
	for _, l := range latencies {
		sum += l
	}
	avg := time.Duration(sum/float64(benchIters)) * time.Nanosecond
	opsSec := float64(benchIters) / tBench.Seconds()

	fmt.Printf("Total Iterations:      %d logs\n", benchIters)
	fmt.Printf("Total Elapsed Time:    %v\n", tBench)
	fmt.Printf("Throughput:            %.2f logs/sec\n", opsSec)
	fmt.Printf("Mean Processing Time:  %v\n", avg)
	fmt.Printf("P50 (Median) Latency:  %v\n", p50)
	fmt.Printf("P90 Latency:           %v\n", p90)
	fmt.Printf("P99 Latency:           %v\n", p99)
	fmt.Println("================================================================================")
}
