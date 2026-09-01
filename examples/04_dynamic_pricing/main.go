package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/cuprite-io/capacitor"
	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/types"
)

func main() {
	ctx := context.Background()

	tmpDir, _ := os.MkdirTemp("", "capacitor-pricing-*")
	defer os.RemoveAll(tmpDir)

	cp, err := capacitor.New(capacitor.Config{
		NodeID:     "pricing-engine-01",
		DataPath:   tmpDir,
		BindPort:   19531,
		StreamPort: 19532,
	})
	if err != nil {
		log.Fatalf("failed to start capacitor: %v", err)
	}

	eng, err := flux.New(flux.WithCache(cp))
	if err != nil {
		log.Fatalf("failed to start flux: %v", err)
	}
	defer eng.Close()

	// Load delivery pricing rules
	data, err := os.ReadFile("pricing_rules.json")
	if err != nil {
		log.Fatalf("failed to read pricing rules: %v", err)
	}

	var rawItems []json.RawMessage
	_ = json.Unmarshal(data, &rawItems)
	for _, raw := range rawItems {
		item, err := flux.LoadItemJSON(raw)
		if err != nil {
			log.Fatalf("failed to load pricing item: %v", err)
		}
		_ = eng.Catalog().Put(ctx, item)
	}

	// Cart 1: Total $25 -> Only Ground delivery available (Drone omitted)
	_ = cp.Set(ctx, "entity:cart_small", `{"cart_total": 25.0}`, 0)

	// Cart 2: Total $120 -> Both Drone and Ground delivery available with discounted fees
	_ = cp.Set(ctx, "entity:cart_large", `{"cart_total": 120.0}`, 0)

	carts := []string{"cart_small", "cart_large"}
	for _, cartID := range carts {
		fmt.Printf("▶ Evaluating Delivery Options for [%s]...\n", cartID)
		res, err := eng.Conduct(ctx, &types.ConductRequest{
			EntityID: cartID,
			Tags:     []string{"pricing:delivery"},
		})
		if err != nil {
			log.Fatalf("conduct failed: %v", err)
		}

		for _, item := range res.Items {
			fmt.Printf("  🚚 Option: %s | Dynamic Pricing: %+v\n", item.ID, item.ComputedOutput)
		}
		fmt.Println()
	}
}
