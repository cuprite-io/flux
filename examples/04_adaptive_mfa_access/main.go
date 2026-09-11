package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/sink"
	"github.com/cuprite-io/flux/types"
)

// AuthEvent represents an incoming sign-in or authorization attempt.
type AuthEvent struct {
	AttemptID           string  `json:"attempt_id"`
	UserID              string  `json:"user_id"`
	Email               string  `json:"email"`
	DeviceID            string  `json:"device_id"`
	IsKnownDevice       bool    `json:"is_known_device"`
	IP                  string  `json:"ip"`
	ASNType             string  `json:"asn_type"` // e.g. "RESIDENTIAL", "COMMERCIAL_VPN", "DATACENTER", "TOR_EXIT"
	ClientHeaderAnomaly bool    `json:"client_header_anomaly"`
	UserAgent           string  `json:"user_agent"`
	Timestamp           float64 `json:"timestamp"`
	RequestedTier       string  `json:"requested_tier"` // e.g., "CUSTOMER_PORTAL", "ADMIN_FINANCE_CONSOLE"
}

// UserSecurityProfile is hydrated in Flux's distributed cache (Capacitor).
type UserSecurityProfile struct {
	UserID        string  `json:"user_id"`
	Email         string  `json:"email"`
	RiskScore     float64 `json:"risk_score"`
	IsKnownDevice bool    `json:"is_known_device"`
	ASNType       string  `json:"asn_type"`
	LastIP        string  `json:"last_ip"`
	LastTimestamp float64 `json:"last_timestamp"`
	RecentFails   float64 `json:"recent_fails"`
	IPVelocity    float64 `json:"ip_velocity"`
}

func resolveFilePath(filename string) string {
	if _, err := os.Stat(filename); err == nil {
		return filename
	}
	subPath := filepath.Join("examples", "04_adaptive_mfa_access", filename)
	if _, err := os.Stat(subPath); err == nil {
		return subPath
	}
	return filename
}

func main() {
	ctx := context.Background()

	fmt.Println("================================================================================")
	fmt.Println("   🛡️ FLUX + CAPACITOR: ADAPTIVE MFA & ZERO-TRUST RISK-BASED ACCESS CONTROL")
	fmt.Println("================================================================================")

	// 1. Initialize Capacitor distributed node
	tmpDir, err := os.MkdirTemp("", "capacitor-mfa-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cp, err := capacitor.New(capacitor.Config{
		NodeID:     "zero-trust-node-01",
		DataPath:   tmpDir,
		BindPort:   19531,
		StreamPort: 19532,
	})
	if err != nil {
		log.Fatalf("failed to start capacitor: %v", err)
	}

	// 2. Initialize Flux Engine with Capacitor backend
	eng, err := flux.New(flux.WithCache(cp), flux.WithWorkers(8))
	if err != nil {
		log.Fatalf("failed to start flux: %v", err)
	}
	defer eng.Close()

	// 3. Register Security Sinks
	var socAlertCount, stateUpdateCount int64
	var verboseSinks atomic.Bool
	verboseSinks.Store(true)

	eng.RegisterSink("soc_security_incident", sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&socAlertCount, 1)
		if verboseSinks.Load() {
			fmt.Printf("      🚨 [SOC INCIDENT SINK] Critical anomaly reported! Security Operations Center dispatched.\n")
		}
		return nil
	}))

	eng.RegisterSink("user_security_state_sink", sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&stateUpdateCount, 1)
		m, ok := payload.(map[string]any)
		if !ok {
			return nil
		}
		rawPayload, _ := m["payload"].(map[string]any)
		userID, _ := rawPayload["user_id"].(string)
		if userID == "" {
			return nil
		}

		riskScore, _ := m["risk_score"].(float64)
		isKnown, _ := rawPayload["is_known_device"].(bool)
		ip, _ := rawPayload["ip"].(string)
		asnType, _ := rawPayload["asn_type"].(string)
		ts, _ := rawPayload["timestamp"].(float64)
		email, _ := rawPayload["email"].(string)
		fails, _ := m["recent_fails"].(float64)
		ipVel, _ := m["ip_velocity"].(float64)

		profile := UserSecurityProfile{
			UserID:        userID,
			Email:         email,
			RiskScore:     riskScore,
			IsKnownDevice: isKnown,
			ASNType:       asnType,
			LastIP:        ip,
			LastTimestamp: ts,
			RecentFails:   fails,
			IPVelocity:    ipVel,
		}

		data, _ := json.Marshal(profile)
		return cp.Set(ctx, "entity:"+userID, string(data), 0)
	}))

	// 4. Load & Register Declarative Circuit DAG
	circuitPath := resolveFilePath("auth_risk_assessment.circuit.json")
	circuit, err := flux.LoadCircuitFile(circuitPath)
	if err != nil {
		log.Fatalf("failed to load circuit from %s: %v", circuitPath, err)
	}
	if err := eng.Registry().Put(ctx, circuit); err != nil {
		log.Fatalf("failed to register circuit: %v", err)
	}
	fmt.Printf("✔ Deployed Circuit: %s (Tags: %v)\n", circuit.ID, circuit.Tags)

	// 5. Load & Index Candidate MFA Policies Catalog
	catalogPath := resolveFilePath("security_policies.catalog.json")
	catData, err := os.ReadFile(catalogPath)
	if err != nil {
		log.Fatalf("failed to read catalog file: %v", err)
	}
	var rawItems []json.RawMessage
	_ = json.Unmarshal(catData, &rawItems)
	for _, raw := range rawItems {
		item, err := flux.LoadItemJSON(raw)
		if err != nil {
			log.Fatalf("failed to parse catalog item: %v", err)
		}
		_ = eng.Catalog().Put(ctx, item)
	}
	fmt.Printf("✔ Indexed %d candidate MFA authentication policies into 'auth:mfa_methods'\n\n", len(rawItems))

	// 6. Execute Realistic Auth Scenarios
	now := float64(time.Now().Unix())

	scenarios := []struct {
		Title       string
		Event       AuthEvent
		PreloadFail int // Preload sliding window failed logins for user
		PreloadIP   int // Preload sliding window requests for IP burst velocity
		ExpectRisk  string
	}{
		{
			Title: "Scenario 1: Trusted Everyday Employee Sign-In (Corporate Residential Fiber)",
			Event: AuthEvent{
				AttemptID:           "att_001_trusted",
				UserID:              "usr_alice_sec",
				Email:               "alice.smith@enterprise.corp",
				DeviceID:            "dev_corp_macbook_99",
				IsKnownDevice:       true,
				IP:                  "198.51.100.24",
				ASNType:             "RESIDENTIAL",
				ClientHeaderAnomaly: false,
				UserAgent:           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
				Timestamp:           now,
				RequestedTier:       "CUSTOMER_PORTAL",
			},
			PreloadFail: 0,
			PreloadIP:   1, // Normal traffic
			ExpectRisk:  "LOW",
		},
		{
			Title: "Scenario 2: Remote Employee on Commercial VPN & Unmanaged Mobile Device (1 Password Typo)",
			Event: AuthEvent{
				AttemptID:           "att_002_vpn_travel",
				UserID:              "usr_bob_sales",
				Email:               "bob.jones@enterprise.corp",
				DeviceID:            "dev_hotel_kiosk_01",
				IsKnownDevice:       false, // New unmanaged device
				IP:                  "203.0.113.88",
				ASNType:             "COMMERCIAL_VPN",
				ClientHeaderAnomaly: false,
				UserAgent:           "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X)",
				Timestamp:           now,
				RequestedTier:       "CUSTOMER_PORTAL",
			},
			PreloadFail: 1, // 1 recent password typo
			PreloadIP:   2, // Low normal rate
			ExpectRisk:  "ELEVATED",
		},
		{
			Title: "Scenario 3: Automated Credential Stuffing & Bot Attack (Tor Exit Node + Headless Browser + Burst IP Rate)",
			Event: AuthEvent{
				AttemptID:           "att_003_bot_attack",
				UserID:              "usr_charlie_admin",
				Email:               "charlie.root@enterprise.corp",
				DeviceID:            "dev_unrecognized_headless",
				IsKnownDevice:       false, // Foreign device
				IP:                  "185.220.101.5",
				ASNType:             "TOR_EXIT",
				ClientHeaderAnomaly: true, // Missing Sec-CH headers / headless python script
				UserAgent:           "python-requests/2.31.0",
				Timestamp:           now,
				RequestedTier:       "ADMIN_FINANCE_CONSOLE",
			},
			PreloadFail: 3,  // 3 failed brute force attempts
			PreloadIP:   12, // 12 authentication attempts/min from this Tor IP across user accounts
			ExpectRisk:  "CRITICAL",
		},
	}

	for idx, sc := range scenarios {
		fmt.Printf("================================================================================\n")
		fmt.Printf("▶ [STEP %d/3] %s\n", idx+1, sc.Title)
		fmt.Printf("================================================================================\n")

		// Preload failed attempts into Capacitor sliding window if scenario specifies
		if sc.PreloadFail > 0 {
			for f := 0; f < sc.PreloadFail; f++ {
				_, _ = cp.IncrementSlidingWindow(ctx, "auth:failed:"+sc.Event.UserID, 5*time.Minute)
			}
		}

		// Preload IP velocity into Capacitor sliding window
		if sc.PreloadIP > 0 {
			for p := 0; p < sc.PreloadIP; p++ {
				_, _ = cp.IncrementSlidingWindow(ctx, "auth:ip_rate:"+sc.Event.IP, 1*time.Minute)
			}
		}

		// A. Spark Authentication Event through Decision DAG
		tSparkStart := time.Now()
		sparkRes, err := eng.Spark(ctx, sc.Event, "stream:auth")
		tSpark := time.Since(tSparkStart)
		if err != nil {
			log.Fatalf("spark failed: %v", err)
		}

		// Wait briefly for state sink to persist
		for w := 0; w < 50; w++ {
			val, _ := cp.Get(ctx, "entity:"+sc.Event.UserID)
			if val != "" {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}

		action := sparkRes.ReturnedData["action"]
		riskLevel := sparkRes.ReturnedData["risk_level"]
		riskScore := sparkRes.ReturnedData["risk_score"]
		asnType := sparkRes.ReturnedData["asn_type"]
		ipVelocity := sparkRes.ReturnedData["ip_velocity_1m"]
		maskedEmail := sparkRes.ReturnedData["user_email"]
		recentFails := sparkRes.ReturnedData["recent_failed_logins"]

		fmt.Printf("  ⚡ [Spark Risk Engine] Evaluated in %v\n", tSpark)
		fmt.Printf("     Account: %s | Risk: %v (Score: %v/100)\n", maskedEmail, riskLevel, riskScore)
		fmt.Printf("     Network ASN: %v | IP Velocity (1m): %v req/min | Failed Passwords (5m): %v\n", asnType, ipVelocity, recentFails)
		fmt.Printf("     Engine Action: %v\n\n", action)

		// B. Conduct Zero-Trust MFA Candidate Evaluation against Hydrated Capacitor State
		tConductStart := time.Now()
		conductRes, err := eng.Conduct(ctx, &types.ConductRequest{
			EntityID: sc.Event.UserID,
			Category: "auth:mfa_methods",
			TopK:     5,
		})
		tConduct := time.Since(tConductStart)
		if err != nil {
			log.Fatalf("conduct failed: %v", err)
		}

		fmt.Printf("  🎯 [Conduct Zero-Trust Policy Resolution] Evaluated in %v\n", tConduct)
		fmt.Printf("     Candidate Methods Evaluated: %d | Qualified Policies: %d\n",
			conductRes.EvaluatedCount, len(conductRes.Items))

		for rIdx, it := range conductRes.Items {
			fmt.Printf("     ✨ [Option %d] %-28s | Policy Decision: %+v\n",
				rIdx+1, it.ID, it.ComputedOutput)
		}
		fmt.Println()
	}

	// 7. High-Throughput Real-Time Security Benchmark
	fmt.Println("================================================================================")
	fmt.Println("   📊 HIGH-THROUGHPUT ZERO-TRUST SECURITY BENCHMARK (5,000 OPS)")
	fmt.Println("================================================================================")

	verboseSinks.Store(false)

	const benchIters = 5000
	benchEvent := scenarios[0].Event

	// Spark Benchmark
	sparkLatencies := make([]float64, benchIters)
	tBenchSparkStart := time.Now()
	for i := 0; i < benchIters; i++ {
		t0 := time.Now()
		_, _ = eng.Spark(ctx, benchEvent, "stream:auth")
		sparkLatencies[i] = float64(time.Since(t0).Nanoseconds())
	}
	tBenchSpark := time.Since(tBenchSparkStart)

	sort.Float64s(sparkLatencies)
	p50Spark := time.Duration(sparkLatencies[int(float64(benchIters)*0.50)]) * time.Nanosecond
	p90Spark := time.Duration(sparkLatencies[int(float64(benchIters)*0.90)]) * time.Nanosecond
	p99Spark := time.Duration(sparkLatencies[int(float64(benchIters)*0.99)]) * time.Nanosecond
	var sumSpark float64
	for _, l := range sparkLatencies {
		sumSpark += l
	}
	avgSpark := time.Duration(sumSpark/float64(benchIters)) * time.Nanosecond
	sparkOps := float64(benchIters) / tBenchSpark.Seconds()

	fmt.Println("--- ⚡ Spark Risk Assessment Throughput & Latency ---")
	fmt.Printf("Total Iterations:      %d auth evaluations\n", benchIters)
	fmt.Printf("Total Elapsed Time:    %v\n", tBenchSpark)
	fmt.Printf("Throughput:            %.2f evals/sec\n", sparkOps)
	fmt.Printf("Mean Spark Latency:    %v\n", avgSpark)
	fmt.Printf("P50 (Median) Latency:  %v\n", p50Spark)
	fmt.Printf("P90 Latency:           %v\n", p90Spark)
	fmt.Printf("P99 Latency:           %v\n", p99Spark)

	// Conduct Policy Benchmark
	conductReq := &types.ConductRequest{
		EntityID: "usr_alice_sec",
		Category: "auth:mfa_methods",
		TopK:     5,
	}
	conductLatencies := make([]float64, benchIters)
	tBenchConductStart := time.Now()
	for i := 0; i < benchIters; i++ {
		t0 := time.Now()
		_, _ = eng.Conduct(ctx, conductReq)
		conductLatencies[i] = float64(time.Since(t0).Nanoseconds())
	}
	tBenchConduct := time.Since(tBenchConductStart)

	sort.Float64s(conductLatencies)
	p50Conduct := time.Duration(conductLatencies[int(float64(benchIters)*0.50)]) * time.Nanosecond
	p90Conduct := time.Duration(conductLatencies[int(float64(benchIters)*0.90)]) * time.Nanosecond
	p99Conduct := time.Duration(conductLatencies[int(float64(benchIters)*0.99)]) * time.Nanosecond
	var sumConduct float64
	for _, l := range conductLatencies {
		sumConduct += l
	}
	avgConduct := time.Duration(sumConduct/float64(benchIters)) * time.Nanosecond
	conductOps := float64(benchIters) / tBenchConduct.Seconds()

	fmt.Println("\n--- 🎯 Conduct Zero-Trust Policy Resolution Throughput & Latency ---")
	fmt.Printf("Total Iterations:      %d policy queries\n", benchIters)
	fmt.Printf("Total Elapsed Time:    %v\n", tBenchConduct)
	fmt.Printf("Throughput:            %.2f queries/sec\n", conductOps)
	fmt.Printf("Mean Conduct Latency:  %v\n", avgConduct)
	fmt.Printf("P50 (Median) Latency:  %v\n", p50Conduct)
	fmt.Printf("P90 Latency:           %v\n", p90Conduct)
	fmt.Printf("P99 Latency:           %v\n", p99Conduct)
	fmt.Printf("SOC Alerts Dispatched: %d incidents\n", atomic.LoadInt64(&socAlertCount))
	fmt.Printf("User States Persisted: %d writes\n", atomic.LoadInt64(&stateUpdateCount))
	fmt.Println("================================================================================")
}
