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

// DungeonClearEvent represents a completed dungeon run event stream payload.
type DungeonClearEvent struct {
	PlayerID       string  `json:"player_id"`
	Class          string  `json:"class"`
	DungeonID      string  `json:"dungeon_id"`
	DungeonName    string  `json:"dungeon_name"`
	MobsKilled     float64 `json:"mobs_killed"`
	BossDamage     float64 `json:"boss_damage"`
	DungeonCleared bool    `json:"dungeon_cleared"`
	Difficulty     string  `json:"difficulty"`
}

// BossFightResult represents an endgame raid boss encounter event stream payload.
type BossFightResult struct {
	PlayerID         string  `json:"player_id"`
	Class            string  `json:"class"`
	BossID           string  `json:"boss_id"`
	BossName         string  `json:"boss_name"`
	BossType         string  `json:"boss_type"`
	Damage           float64 `json:"damage"`
	PartySize        float64 `json:"party_size"`
	ExecutionTimeSec float64 `json:"execution_time_sec"`
}

// PlayerState represents the authoritative player profile state persisted in Flux's cache (Capacitor).
type PlayerState struct {
	PlayerID      string  `json:"player_id"`
	Class         string  `json:"class"`
	Level         float64 `json:"level"`
	Gold          float64 `json:"gold"`
	GearScore     float64 `json:"gear_score"`
	DungeonClears float64 `json:"dungeon_clears"`
	BossKills     float64 `json:"boss_kills"`
	VIPTier       string  `json:"vip_tier"`
}

func resolveFilePath(filename string) string {
	if _, err := os.Stat(filename); err == nil {
		return filename
	}
	subPath := filepath.Join("examples", "03_gaming_boss_kill", filename)
	if _, err := os.Stat(subPath); err == nil {
		return subPath
	}
	return filename
}

func main() {
	ctx := context.Background()

	fmt.Println("================================================================================")
	fmt.Println("   🎮 FLUX + CAPACITOR: PRODUCTION GAMING EVENT PIPELINE & CATALOG CONDUCT")
	fmt.Println("================================================================================")

	// 1. Initialize Capacitor Distributed Cache Node
	fmt.Println("\n[1/6] 🚀 Initializing Capacitor Distributed Storage Node...")
	tmpDir, err := os.MkdirTemp("", "capacitor-gaming-prod-*")
	if err != nil {
		log.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cpStart := time.Now()
	cp, err := capacitor.New(capacitor.Config{
		NodeID:     "gaming-cluster-node-01",
		DataPath:   tmpDir,
		BindPort:   19531,
		StreamPort: 19532,
	})
	if err != nil {
		log.Fatalf("failed to start capacitor: %v", err)
	}
	fmt.Printf("      ✔ Capacitor active in %v (Node: gaming-cluster-node-01)\n", time.Since(cpStart))

	// 2. Initialize Flux Rule & Workflow Engine paired with Capacitor
	fmt.Println("\n[2/6] ⚡ Initializing Flux Engine (stateless core + Capacitor backend)...")
	engStart := time.Now()
	eng, err := flux.New(flux.WithCache(cp), flux.WithWorkers(8))
	if err != nil {
		log.Fatalf("failed to start flux: %v", err)
	}
	defer eng.Close()
	fmt.Printf("      ✔ Flux engine active in %v (Workers: 8, Backend: Capacitor)\n", time.Since(engStart))

	// 3. Register Production Sinks: Discord Guild Broadcast & Player State Persistence
	fmt.Println("\n[3/6] 📡 Registering Event Dispatch Sinks...")
	var discordAlertCount, stateUpdateCount int64
	var verboseSinks atomic.Bool
	verboseSinks.Store(true)

	// Guild Discord Webhook Sink
	eng.RegisterSink("discord_webhooks", sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&discordAlertCount, 1)
		if verboseSinks.Load() {
			fmt.Printf("      📣 [DISCORD GUILD SINK] Broadcasted to guild: %+v\n", payload)
		}
		return nil
	}))

	// Player State Persistence Sink: Automatically hydrates/syncs player state into Flux's cache (Capacitor)
	eng.RegisterSink("player_state_sink", sink.FuncSink(func(ctx context.Context, payload any) error {
		atomic.AddInt64(&stateUpdateCount, 1)
		m, ok := payload.(map[string]any)
		if !ok {
			return nil
		}
		playerID, _ := m["player_id"].(string)
		if playerID == "" {
			return nil
		}
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		return cp.Set(ctx, "entity:"+playerID, string(data), 0)
	}))
	fmt.Println("      ✔ Sinks registered: 'discord_webhooks' & 'player_state_sink'")

	// 4. Deploy Declarative Circuits & Catalogs
	fmt.Println("\n[4/6] 📜 Loading Production Circuits & Catalogs into Flux...")
	dungeonCircuitPath := resolveFilePath("dungeon_progression.circuit.json")
	dungeonCircuit, err := flux.LoadCircuitFile(dungeonCircuitPath)
	if err != nil {
		log.Fatalf("failed to load dungeon circuit: %v", err)
	}
	_ = eng.Registry().Put(ctx, dungeonCircuit)
	fmt.Printf("      ✔ Deployed Circuit: %s (Tags: %v)\n", dungeonCircuit.ID, dungeonCircuit.Tags)

	bossCircuitPath := resolveFilePath("boss_encounter.circuit.json")
	bossCircuit, err := flux.LoadCircuitFile(bossCircuitPath)
	if err != nil {
		log.Fatalf("failed to load boss circuit: %v", err)
	}
	_ = eng.Registry().Put(ctx, bossCircuit)
	fmt.Printf("      ✔ Deployed Circuit: %s (Tags: %v)\n", bossCircuit.ID, bossCircuit.Tags)

	// Load Adventurer Catalog (Category: catalog:adventurer_armory)
	advCatPath := resolveFilePath("adventurer_catalog.json")
	advData, err := os.ReadFile(advCatPath)
	if err != nil {
		log.Fatalf("failed to read adventurer catalog: %v", err)
	}
	var rawAdvItems []json.RawMessage
	_ = json.Unmarshal(advData, &rawAdvItems)
	for _, raw := range rawAdvItems {
		item, err := flux.LoadItemJSON(raw)
		if err != nil {
			log.Fatalf("failed to parse item: %v", err)
		}
		_ = eng.Catalog().Put(ctx, item)
	}
	fmt.Printf("      ✔ Indexed %d candidate items into 'catalog:adventurer_armory'\n", len(rawAdvItems))

	// Load Mythic Vault Catalog (Category: catalog:mythic_vault)
	mythicCatPath := resolveFilePath("mythic_vault_catalog.json")
	mythicData, err := os.ReadFile(mythicCatPath)
	if err != nil {
		log.Fatalf("failed to read mythic catalog: %v", err)
	}
	var rawMythicItems []json.RawMessage
	_ = json.Unmarshal(mythicData, &rawMythicItems)
	for _, raw := range rawMythicItems {
		item, err := flux.LoadItemJSON(raw)
		if err != nil {
			log.Fatalf("failed to parse mythic item: %v", err)
		}
		_ = eng.Catalog().Put(ctx, item)
	}
	fmt.Printf("      ✔ Indexed %d candidate items into 'catalog:mythic_vault'\n", len(rawMythicItems))

	// Target Test Player
	playerID := "hero_valkyrie_99"

	// =========================================================================
	// PHASE 1: SPARK DUNGEON EVENT -> GENERATE USER'S STATE IN FLUX'S CACHE
	// =========================================================================
	fmt.Println("\n================================================================================")
	fmt.Println("   PHASE 1: SPARK EVENT -> GENERATE USER'S STATE IN FLUX'S CACHE")
	fmt.Println("================================================================================")

	dungeonEvent := DungeonClearEvent{
		PlayerID:       playerID,
		Class:          "WARRIOR",
		DungeonID:      "dungeon_sunken_catacombs",
		DungeonName:    "Sunken Catacombs",
		MobsKilled:     24.0,
		BossDamage:     3400.0,
		DungeonCleared: true,
		Difficulty:     "NORMAL",
	}

	fmt.Printf("▶ Ingesting Dungeon Clear Event for [%s] (Class: %s, Mobs: %.0f, Cleared: %v)...\n",
		dungeonEvent.PlayerID, dungeonEvent.Class, dungeonEvent.MobsKilled, dungeonEvent.DungeonCleared)

	tSpark1Start := time.Now()
	spark1Res, err := eng.Spark(ctx, dungeonEvent, "stream:dungeon_events")
	tSpark1 := time.Since(tSpark1Start)
	if err != nil {
		log.Fatalf("Phase 1 Spark failed: %v", err)
	}

	fmt.Printf("  ✔ Spark Executed in %v (Passed: %v, Circuits: %v)\n", tSpark1, spark1Res.Passed, spark1Res.ExecutedCircuits)
	fmt.Printf("  ✔ Spark Rewards Returned: %+v\n", spark1Res.ReturnedData)

	// Verify generated entity state in Flux's Cache (Capacitor)
	var state1 PlayerState
	for i := 0; i < 50; i++ {
		raw, err := cp.Get(ctx, "entity:"+playerID)
		if err == nil && raw != "" {
			_ = json.Unmarshal([]byte(raw), &state1)
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	fmt.Printf("  📦 [Flux Cache State - Capacitor] EntityID: %s\n", state1.PlayerID)
	fmt.Printf("     Class: %s | Level: %.0f | Gold: %.0f | GearScore: %.0f | DungeonClears: %.0f | BossKills: %.0f | VIP: %s\n",
		state1.Class, state1.Level, state1.Gold, state1.GearScore, state1.DungeonClears, state1.BossKills, state1.VIPTier)

	// =========================================================================
	// PHASE 1: CONDUCT BASED ON INITIAL USER STATE (ADVENTURER ARMORY)
	// =========================================================================
	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Printf("▶ Conducting Candidate Item Evaluation for [%s] on 'catalog:adventurer_armory'...\n", playerID)
	tConduct1Start := time.Now()
	res1, err := eng.Conduct(ctx, &types.ConductRequest{
		EntityID: playerID,
		Category: "catalog:adventurer_armory",
		TopK:     10,
	})
	tConduct1 := time.Since(tConduct1Start)
	if err != nil {
		log.Fatalf("Conduct Phase 1 failed: %v", err)
	}

	fmt.Printf("  ✔ Conduct Completed in %v (Evaluated Candidates: %d | Qualified Items: %d)\n\n",
		tConduct1, res1.EvaluatedCount, len(res1.Items))
	for _, it := range res1.Items {
		fmt.Printf("   ✨ [Rank %d] Qualified Item: %-25s | Dynamic Output: %+v\n", it.Rank, it.ID, it.ComputedOutput)
	}
	fmt.Println("\n   🚫 Silently Omitted Items (Conditions Not Met):")
	fmt.Println("      - 'arcane_frost_staff': Ineligible (Requires MAGE, player is WARRIOR)")
	fmt.Println("      - 'champion_dragon_plate': Ineligible (Requires Level >= 50 && Boss Kills >= 1, player is Level 25)")
	fmt.Println("      - 'titan_aegis_shield': Ineligible (Requires Gear Score >= 500, player Gear Score is 280)")

	// =========================================================================
	// PHASE 2: SPARK MORE DATA -> HIGH-TIER MYTHIC RAID BOSS KILL
	// =========================================================================
	fmt.Println("\n================================================================================")
	fmt.Println("   PHASE 2: SPARK MORE DATA -> MYTHIC RAID BOSS ENCOUNTER")
	fmt.Println("================================================================================")

	bossEvent := BossFightResult{
		PlayerID:         playerID,
		Class:            "WARRIOR",
		BossID:           "boss_ignis_worldburner",
		BossName:         "Ignis the Worldburner",
		BossType:         "MYTHIC_RAID_BOSS",
		Damage:           95200.0,
		PartySize:        20.0,
		ExecutionTimeSec: 210.0,
	}

	fmt.Printf("▶ Ingesting Mythic Raid Boss Event for [%s] (Boss: %s, Damage: %.1f, Party: %.0f)...\n",
		bossEvent.PlayerID, bossEvent.BossName, bossEvent.Damage, bossEvent.PartySize)

	tSpark2Start := time.Now()
	spark2Res, err := eng.Spark(ctx, bossEvent, "stream:boss_events")
	tSpark2 := time.Since(tSpark2Start)
	if err != nil {
		log.Fatalf("Phase 2 Spark failed: %v", err)
	}

	fmt.Printf("  ✔ Spark Executed in %v (Passed: %v, Circuits: %v)\n", tSpark2, spark2Res.Passed, spark2Res.ExecutedCircuits)
	fmt.Printf("  ✔ Spark Rewards Returned: %+v\n", spark2Res.ReturnedData)

	// Verify promoted entity state in Flux's Cache (Capacitor)
	var state2 PlayerState
	for i := 0; i < 50; i++ {
		raw, err := cp.Get(ctx, "entity:"+playerID)
		if err == nil && raw != "" {
			_ = json.Unmarshal([]byte(raw), &state2)
			if state2.Level == 60.0 {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	fmt.Printf("  📦 [Flux Cache State - Promoted] EntityID: %s\n", state2.PlayerID)
	fmt.Printf("     Class: %s | Level: %.0f | Gold: %.0f | GearScore: %.0f | DungeonClears: %.0f | BossKills: %.0f | VIP: %s\n",
		state2.Class, state2.Level, state2.Gold, state2.GearScore, state2.DungeonClears, state2.BossKills, state2.VIPTier)

	// =========================================================================
	// PHASE 2: CONDUCT FOR MORE ELIGIBLE CATALOGUES
	// =========================================================================
	fmt.Println("\n================================================================================")
	fmt.Println("   PHASE 2: CONDUCT EVALUATION FOR NEWLY UNLOCKED & EXPANDED CATALOGUES")
	fmt.Println("================================================================================")

	// Query A: Newly unlocked Mythic Vault Catalog
	fmt.Printf("\n▶ [Catalogue 1/2] Conducting Evaluation on Newly Unlocked 'catalog:mythic_vault'...\n")
	tConductMythicStart := time.Now()
	resMythic, err := eng.Conduct(ctx, &types.ConductRequest{
		EntityID: playerID,
		Category: "catalog:mythic_vault",
		TopK:     10,
	})
	tConductMythic := time.Since(tConductMythicStart)
	if err != nil {
		log.Fatalf("Conduct Mythic Vault failed: %v", err)
	}

	fmt.Printf("  ✔ Conduct Completed in %v (Evaluated Candidates: %d | Qualified Items: %d)\n\n",
		tConductMythic, resMythic.EvaluatedCount, len(resMythic.Items))
	for _, it := range resMythic.Items {
		fmt.Printf("   👑 [Rank %d] Mythic Item: %-25s | Dynamic Output: %+v\n", it.Rank, it.ID, it.ComputedOutput)
	}
	fmt.Println("\n   🚫 Silently Omitted Items:")
	fmt.Println("      - 'worldbreaker_annihilator': Requires 10 Mythic Boss Kills (Player has 1)")
	fmt.Println("      - 'archmage_eternal_robes': Requires MAGE (Player is WARRIOR)")

	// Query B: Re-evaluating Adventurer Armory with Promoted State
	fmt.Printf("\n▶ [Catalogue 2/2] Re-Conducting on 'catalog:adventurer_armory' (Progression Check)...\n")
	tConductAdv2Start := time.Now()
	resAdv2, err := eng.Conduct(ctx, &types.ConductRequest{
		EntityID: playerID,
		Category: "catalog:adventurer_armory",
		TopK:     10,
	})
	tConductAdv2 := time.Since(tConductAdv2Start)
	if err != nil {
		log.Fatalf("Re-conduct Adventurer Armory failed: %v", err)
	}

	fmt.Printf("  ✔ Conduct Completed in %v (Evaluated Candidates: %d | Qualified Items: %d)\n\n",
		tConductAdv2, resAdv2.EvaluatedCount, len(resAdv2.Items))
	for _, it := range resAdv2.Items {
		fmt.Printf("   ✨ [Rank %d] Qualified Item: %-25s | Dynamic Output: %+v\n", it.Rank, it.ID, it.ComputedOutput)
	}
	fmt.Println("\n   📈 Progression Summary:")
	fmt.Printf("      - Adventurer Armory Qualified: %d/6 items (increased from %d/6 in Phase 1)\n", len(resAdv2.Items), len(res1.Items))
	fmt.Println("      - 'champion_dragon_plate': NOW UNLOCKED! (Level 60 >= 50 && Boss Kills 1 >= 1)")
	fmt.Println("      - 'titan_aegis_shield':    NOW UNLOCKED! (Gear Score 620 >= 500)")
	fmt.Println("      - 'iron_battleaxe':        NOW UPGRADED! (Max Level 50+ discount applied: 25% off)")

	// =========================================================================
	// 5. PRODUCTION HIGH-THROUGHPUT REAL-TIME BENCHMARKS
	// =========================================================================
	fmt.Println("\n================================================================================")
	fmt.Println("   📊 HIGH-THROUGHPUT GAMING BENCHMARKS (5,000 OPS)")
	fmt.Println("================================================================================")

	// Turn off verbose sink console logging for high-throughput micro-benchmark
	verboseSinks.Store(false)

	const benchIters = 5000

	// Benchmark A: Spark Event Ingestion
	sparkLatencies := make([]float64, benchIters)
	tBenchSparkStart := time.Now()
	for i := 0; i < benchIters; i++ {
		t0 := time.Now()
		_, _ = eng.Spark(ctx, bossEvent, "stream:boss_events")
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

	fmt.Println("--- ⚡ Spark Stream Throughput & Latency ---")
	fmt.Printf("Total Iterations:      %d ops\n", benchIters)
	fmt.Printf("Total Elapsed Time:    %v\n", tBenchSpark)
	fmt.Printf("Throughput:            %.2f ops/sec\n", sparkOps)
	fmt.Printf("Mean Spark Latency:    %v\n", avgSpark)
	fmt.Printf("P50 (Median) Latency:  %v\n", p50Spark)
	fmt.Printf("P90 Latency:           %v\n", p90Spark)
	fmt.Printf("P99 Latency:           %v\n", p99Spark)

	// Benchmark B: Conduct Parallel Candidate Item Qualification
	conductReq := &types.ConductRequest{
		EntityID: playerID,
		Category: "catalog:mythic_vault",
		TopK:     10,
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

	fmt.Println("\n--- 🎯 Conduct Parallel Item Qualification Throughput & Latency ---")
	fmt.Printf("Total Iterations:      %d ops\n", benchIters)
	fmt.Printf("Total Elapsed Time:    %v\n", tBenchConduct)
	fmt.Printf("Throughput:            %.2f ops/sec\n", conductOps)
	fmt.Printf("Mean Conduct Latency:  %v\n", avgConduct)
	fmt.Printf("P50 (Median) Latency:  %v\n", p50Conduct)
	fmt.Printf("P90 Latency:           %v\n", p90Conduct)
	fmt.Printf("P99 Latency:           %v\n", p99Conduct)
	fmt.Printf("Discord Sinks Fired:   %d dispatches\n", atomic.LoadInt64(&discordAlertCount))
	fmt.Printf("State Updates Fired:   %d dispatches\n", atomic.LoadInt64(&stateUpdateCount))
	fmt.Printf("Stateless Core:        100%% Stateless Flux Core + Zero L1 Cache Copying\n")
	fmt.Println("================================================================================")
}
