package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/sink"
)

type PaymentTransaction struct {
	TxID      string  `json:"tx_id"`
	UserID    string  `json:"user_id"`
	Amount    float64 `json:"amount"`
	CardID    string  `json:"card_id"`
	DeviceID  string  `json:"device_id"`
	Timestamp float64 `json:"timestamp"`
	Lat       float64 `json:"lat"`
	Lon       float64 `json:"lon"`
	City      string  `json:"city"`
}

type FraudEvaluationPayload struct {
	Payload PaymentTransaction `json:"payload"`
	User    UserProfileState   `json:"user"`
}

type UserProfileState struct {
	AllTimeMax    float64 `json:"all_time_max"`
	LastLat       float64 `json:"last_lat"`
	LastLon       float64 `json:"last_lon"`
	LastTS        float64 `json:"last_ts"`
	IsKnownDevice bool    `json:"is_known_device"`
}

func main() {
	ctx := context.Background()

	// 1. Boot local Capacitor node for historical baseline tracking
	tmpDir, _ := os.MkdirTemp("", "capacitor-fraud-*")
	defer os.RemoveAll(tmpDir)

	cp, err := capacitor.New(capacitor.Config{
		NodeID:     "fraud-sentinel-01",
		DataPath:   tmpDir,
		BindPort:   19501,
		StreamPort: 19502,
	})
	if err != nil {
		log.Fatalf("failed to start capacitor: %v", err)
	}

	// 2. Boot Flux Engine connected to Capacitor
	eng, err := flux.New(flux.WithCache(cp), flux.WithWorkers(4))
	if err != nil {
		log.Fatalf("failed to start flux: %v", err)
	}
	defer eng.Close()

	// 3. Register Asynchronous Sinks (for operational SOC telemetry and ledger clearance)
	eng.RegisterSink("incident_response_webhook", sink.FuncSink(func(ctx context.Context, payload any) error {
		fmt.Printf("  🚨 [ASYNC SINK: SOC TELEMETRY] Broadcasted Anomaly Incident Vector to Security Center!\n")
		return nil
	}))

	eng.RegisterSink("kafka_cleared_payments", sink.FuncSink(func(ctx context.Context, payload any) error {
		fmt.Printf("  💸 [ASYNC SINK: SETTLEMENT] Emitted cleared payment event to ledger clearinghouse.\n")
		return nil
	}))

	// 4. Load declarative Combined Circuit
	circuit, err := flux.LoadCircuitFile("fraud_check.circuit.json")
	if err != nil {
		log.Fatalf("failed to load circuit: %v", err)
	}
	_ = eng.Registry().Put(ctx, circuit)
	fmt.Printf("🛡️  Loaded Circuit %q [Tags: %v]\n\n", circuit.ID, circuit.Tags)

	// 5. Initialize user history in Capacitor: Alice starts with a known iPhone in San Francisco
	userID := "usr_alice"
	baseTime := float64(time.Now().Unix())

	_ = cp.Set(ctx, "max_amount:"+userID, "45.0", 0)
	_ = cp.Set(ctx, "last_lat:"+userID, "37.7749", 0)
	_ = cp.Set(ctx, "last_lon:"+userID, "-122.4194", 0)
	_ = cp.Set(ctx, "last_ts:"+userID, fmt.Sprintf("%.0f", baseTime), 0)
	_, _ = cp.SetAdd(ctx, "devices:"+userID, "device_iphone_15")

	// 6. Stream transactions simulating normal activity -> impossible travel -> historical baseline surge
	transactions := []PaymentTransaction{
		{
			TxID: "tx_001", UserID: userID, Amount: 45.0, CardID: "4111-2222-3333-4444",
			DeviceID: "device_iphone_15", Timestamp: baseTime + 300, Lat: 37.7749, Lon: -122.4194, City: "San Francisco",
		},
		{
			TxID: "tx_002", UserID: userID, Amount: 150.0, CardID: "4111-2222-3333-4444",
			DeviceID: "device_iphone_15", Timestamp: baseTime + 3600, Lat: 37.7749, Lon: -122.4194, City: "San Francisco",
		},
		{
			TxID: "tx_003", UserID: userID, Amount: 450.0, CardID: "4111-2222-3333-4444",
			DeviceID: "device_iphone_15", Timestamp: baseTime + 18000, Lat: 37.3382, Lon: -121.8863, City: "San Jose (65 km away)",
		},
		{
			TxID: "tx_004", UserID: userID, Amount: 20.0, CardID: "4111-2222-3333-4444",
			DeviceID: "device_iphone_15", Timestamp: baseTime + 20700, Lat: 51.5074, Lon: -0.1278, City: "London, UK (8,600 km away, 45m later)",
		},
		{
			TxID: "tx_005", UserID: userID, Amount: 4800.0, CardID: "4111-2222-3333-4444",
			DeviceID: "device_tor_exit_node", Timestamp: baseTime + 21000, Lat: 37.7749, Lon: -122.4194, City: "San Francisco (TOR Network)",
		},
	}

	for idx, tx := range transactions {
		fmt.Printf("================================================================================\n")
		fmt.Printf("▶ [TX #%d] %s | Amount: $%.2f | City: %s | Device: %s\n", idx+1, tx.TxID, tx.Amount, tx.City, tx.DeviceID)

		// Hydrate user profile from Capacitor
		allTimeMaxStr, _ := cp.Get(ctx, "max_amount:"+userID)
		lastLatStr, _ := cp.Get(ctx, "last_lat:"+userID)
		lastLonStr, _ := cp.Get(ctx, "last_lon:"+userID)
		lastTSStr, _ := cp.Get(ctx, "last_ts:"+userID)
		isKnown, _ := cp.SetIsMember(ctx, "devices:"+userID, tx.DeviceID)

		allTimeMax, _ := strconv.ParseFloat(allTimeMaxStr, 64)
		lastLat, _ := strconv.ParseFloat(lastLatStr, 64)
		lastLon, _ := strconv.ParseFloat(lastLonStr, 64)
		lastTS, _ := strconv.ParseFloat(lastTSStr, 64)

		userProfile := UserProfileState{
			AllTimeMax:    allTimeMax,
			LastLat:       lastLat,
			LastLon:       lastLon,
			LastTS:        lastTS,
			IsKnownDevice: isKnown,
		}

		fmt.Printf("   Capacitor Baseline: [All-Time Max: $%.2f, Last Loc: (%.2f, %.2f), Known Device: %v]\n",
			userProfile.AllTimeMax, userProfile.LastLat, userProfile.LastLon, userProfile.IsKnownDevice)

		evalPayload := FraudEvaluationPayload{
			Payload: tx,
			User:    userProfile,
		}

		// Execute Spark evaluation
		sparkRes, _ := eng.Spark(ctx, evalPayload, "stream:payments")

		// Response payload is self-contained and returned directly to the Spark invoker!
		action := sparkRes.ReturnedData["action"]
		risk := sparkRes.ReturnedData["risk_level"]
		challengeType := sparkRes.ReturnedData["challenge_type"]
		reason := sparkRes.ReturnedData["reason"]

		if sparkRes.Passed && action == "APPROVED" {
			fmt.Printf("   ✅ Spark Response: APPROVED (Risk Level: %v)\n", risk)

			// Update Capacitor Baseline
			if tx.Amount > allTimeMax {
				_ = cp.Set(ctx, "max_amount:"+userID, fmt.Sprintf("%.2f", tx.Amount), 0)
			}
			_ = cp.Set(ctx, "last_lat:"+userID, fmt.Sprintf("%.4f", tx.Lat), 0)
			_ = cp.Set(ctx, "last_lon:"+userID, fmt.Sprintf("%.4f", tx.Lon), 0)
			_ = cp.Set(ctx, "last_ts:"+userID, fmt.Sprintf("%.0f", tx.Timestamp), 0)
			_, _ = cp.SetAdd(ctx, "devices:"+userID, tx.DeviceID)
		} else {
			fmt.Printf("   ⛔ Spark Response: %v [Challenge: %v, Risk: %v, Reason: %v]\n",
				action, challengeType, risk, reason)
		}

		// Allow async worker sinks to flush logs cleanly
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Printf("================================================================================\n")
}
