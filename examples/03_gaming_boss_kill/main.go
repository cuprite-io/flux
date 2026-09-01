package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/cuprite-io/capacitor"
	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/internal/sink"
)

type BossFightResult struct {
	PlayerID string  `json:"player_id"`
	BossType string  `json:"boss_type"`
	Damage   float64 `json:"damage"`
}

func main() {
	ctx := context.Background()

	tmpDir, _ := os.MkdirTemp("", "capacitor-gaming-*")
	defer os.RemoveAll(tmpDir)

	cp, err := capacitor.New(capacitor.Config{
		NodeID:     "gaming-engine-01",
		DataPath:   tmpDir,
		BindPort:   19521,
		StreamPort: 19522,
	})
	if err != nil {
		log.Fatalf("failed to start capacitor: %v", err)
	}

	eng, err := flux.New(flux.WithCache(cp), flux.WithWorkers(4))
	if err != nil {
		log.Fatalf("failed to start flux: %v", err)
	}
	defer eng.Close()

	// Register Discord Webhook sink
	eng.RegisterSink("discord_webhooks", sink.FuncSink(func(ctx context.Context, payload any) error {
		fmt.Printf("📣 [DISCORD SINK] Broadcasted to guild: %+v\n", payload)
		return nil
	}))

	// Load Circuit
	circuit, err := flux.LoadCircuitFile("boss_encounter.circuit.json")
	if err != nil {
		log.Fatalf("failed to load circuit: %v", err)
	}
	_ = eng.Registry().Put(ctx, circuit)
	fmt.Printf("✅ Loaded Circuit %s\n\n", circuit.ID)

	events := []BossFightResult{
		{PlayerID: "hero_arthur", BossType: "MINION", Damage: 1200.0},
		{PlayerID: "hero_valkyrie", BossType: "RAID_BOSS", Damage: 8500.0},
	}

	for _, evt := range events {
		fmt.Printf("▶ Processing Encounter for [%s] against %s (Damage: %.1f)...\n", evt.PlayerID, evt.BossType, evt.Damage)
		res, err := eng.Spark(ctx, evt, "stream:game_events")
		if err != nil {
			log.Fatalf("spark error: %v", err)
		}
		fmt.Printf("  Spark Passed: %v, Returned Data: %+v\n\n", res.Passed, res.ReturnedData)
	}
}
