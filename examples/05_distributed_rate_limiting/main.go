package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/cuprite-io/flux"
)

type APIRequest struct {
	TenantID         string  `json:"tenant_id"`
	Path             string  `json:"path"`
	RequestsInWindow float64 `json:"requests_in_window"`
}

func main() {
	ctx := context.Background()

	tmpDir, _ := os.MkdirTemp("", "capacitor-ratelimit-*")
	defer os.RemoveAll(tmpDir)

	cp, err := capacitor.New(capacitor.Config{
		NodeID:     "gateway-limiter-01",
		DataPath:   tmpDir,
		BindPort:   19541,
		StreamPort: 19542,
	})
	if err != nil {
		log.Fatalf("failed to start capacitor: %v", err)
	}

	eng, err := flux.New(flux.WithCache(cp))
	if err != nil {
		log.Fatalf("failed to start flux: %v", err)
	}
	defer eng.Close()

	circuit, err := flux.LoadCircuitFile("rate_limiter.circuit.json")
	if err != nil {
		log.Fatalf("failed to load circuit: %v", err)
	}
	_ = eng.Registry().Put(ctx, circuit)

	// Simulate 8 requests from tenant_acme within 1 second sliding window (Limit is 5)
	tenantKey := "ratelimit:tenant_acme"
	fmt.Printf("🚀 Firing burst of 8 requests for tenant_acme (Sliding window limit = 5 req/sec)...\n\n")

	for i := 1; i <= 8; i++ {
		// Increment sliding window in Capacitor
		count, err := cp.IncrementSlidingWindow(ctx, tenantKey, 1*time.Second)
		if err != nil {
			log.Fatalf("failed to update sliding window: %v", err)
		}

		req := APIRequest{
			TenantID:         "tenant_acme",
			Path:             "/api/v1/orders",
			RequestsInWindow: float64(count),
		}

		res, _ := eng.Spark(ctx, req, "stream:api_gateway")

		status := res.ReturnedData["status"]
		if status == "200_OK" {
			fmt.Printf("  [Req #%d] Status: 200 OK (Window count: %d)\n", i, count)
		} else {
			retryAfter := res.ReturnedData["retry_after_ms"]
			fmt.Printf("  [Req #%d] ⛔ Status: 429 Too Many Requests (Rate Limit Exceeded! Retry After: %vms, Window count: %d)\n",
				i, retryAfter, count)
		}
	}
}
