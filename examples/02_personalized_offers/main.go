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

	// 1. Initialize Capacitor
	tmpDir, _ := os.MkdirTemp("", "capacitor-offers-*")
	defer os.RemoveAll(tmpDir)

	cp, err := capacitor.New(capacitor.Config{
		NodeID:     "offers-engine-01",
		DataPath:   tmpDir,
		BindPort:   19511,
		StreamPort: 19512,
	})
	if err != nil {
		log.Fatalf("failed to start capacitor: %v", err)
	}

	// 2. Initialize Flux
	eng, err := flux.New(flux.WithCache(cp))
	if err != nil {
		log.Fatalf("failed to start flux: %v", err)
	}
	defer eng.Close()

	// 3. Load candidate offers from JSON
	data, err := os.ReadFile("catalog_offers.json")
	if err != nil {
		log.Fatalf("failed to read offers: %v", err)
	}

	var rawItems []json.RawMessage
	_ = json.Unmarshal(data, &rawItems)
	for _, raw := range rawItems {
		item, err := flux.LoadItemJSON(raw)
		if err != nil {
			log.Fatalf("failed to parse item: %v", err)
		}
		_ = eng.Catalog().Put(ctx, item)
		fmt.Printf("📦 Catalogued Item: %s (Tags: %v)\n", item.ID, item.Tags)
	}

	// 4. Hydrate Player Profiles in Capacitor
	// Player 1: Level 15, Spend $50 -> Qualifies for Gold Pack only (Diamond is silently omitted)
	_ = cp.Set(ctx, "entity:player_noob", `{"level": 15.0, "spend": 50.0}`, 0)

	// Player 2: Level 60, Spend $2500 -> Qualifies for both Gold and Diamond
	_ = cp.Set(ctx, "entity:player_whale", `{"level": 60.0, "spend": 2500.0}`, 0)

	// 5. Evaluate Conduct queries
	players := []string{"player_noob", "player_whale"}
	for _, playerID := range players {
		fmt.Printf("\n▶ Conducting Offer Evaluation for [%s]...\n", playerID)
		res, err := eng.Conduct(ctx, &types.ConductRequest{
			EntityID: playerID,
			Tags:     []string{"offers:gaming_vip"},
			TopK:     5,
		})
		if err != nil {
			log.Fatalf("conduct failed: %v", err)
		}

		fmt.Printf("  Evaluated Items: %d, Qualified Items: %d\n", res.EvaluatedCount, len(res.Items))
		for _, item := range res.Items {
			fmt.Printf("   ✨ [Rank %d] Offer: %s | Computed Output: %+v\n", item.Rank, item.ID, item.ComputedOutput)
		}
	}
}
