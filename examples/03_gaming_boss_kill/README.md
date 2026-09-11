# Example 3: Production Gaming Event Pipeline & Dynamic Catalog Conduct

This example demonstrates an end-to-end production gaming architecture pairing **Flux** (the high-throughput rule & workflow engine) with **Capacitor** (distributed state storage). It showcases real-time game event stream ingestion via `Spark`, reactive player state accumulation in Flux's distributed cache, and candidate item evaluation and pruning via `Conduct`.

---

## Architecture Overview

```
                        +---------------------------------------------+
                        |        Real-Time Game Event Stream          |
                        | (Dungeon Clears, Boss Encounters, Combat)   |
                        +---------------------------------------------+
                                               |
                                               v
                        +---------------------------------------------+
                        |            Flux Engine (Spark)              |
                        |  - Declarative Circuits (CEL / Volt)        |
                        |  - Parallel Child Branch DAG                |
                        +---------------------------------------------+
                                       /              \
                          (State Sink)/                \(Guild Sinks)
                                     v                  v
    +-----------------------------------------+    +-----------------------+
    |           Capacitor Storage             |    | Discord Webhook Sink  |
    |   `entity:<player_id>` (Distributed)    |    | (Real-time Broadcast) |
    +-----------------------------------------+    +-----------------------+
                         |
                         | (Zero-Allocation Layered Hydration)
                         v
    +-----------------------------------------------------------------------+
    |                         Flux Engine (Conduct)                         |
    |   - Partitioned Candidate Catalog Fetch (`catalog:group:<category>`)  |
    |   - Lock-Free Concurrent Qualification of Embedded Item Circuits      |
    |   - Dynamic Price Discounts, Stat Buffs & Rarity Unlocks              |
    |   - Silent Omission of Ineligible Items (Level, Class, Raid Kills)    |
    +-----------------------------------------------------------------------+
```

---

## Workflow Progression

### Phase 1: Dungeon Clear Spark & Initial State Accumulation
1. **Ingest Event**: `DungeonClearEvent` is sparked (`valkyrie_hero_01`, Warrior, 24 mobs killed, Sunken Catacombs cleared).
2. **Circuit Evaluation**: `dungeon_progression_pipeline` calculates initial character progression and dispatches `player_state_sink`.
3. **State Persistence**: Flux's distributed cache (Capacitor) is updated with:
   - `level`: 25
   - `gold`: 2,450
   - `gear_score`: 280
   - `dungeon_clears`: 5
   - `boss_kills`: 0
   - `vip_tier`: `BRONZE`
4. **Conduct Pass 1 (`catalog:adventurer_armory`)**:
   - 6 candidate items evaluated in parallel.
   - **Qualified (3)**:
     - `iron_battleaxe`: Warrior level 10+ satisfied, 10% level discount applied (405 Gold).
     - `stamina_runic_ring`: Level 20+ satisfied, 15% discount applied (680 Gold).
     - `dungeon_conqueror_crest`: 5 dungeon clears satisfied, awarded free "Dungeon Delver" title.
   - **Silently Omitted (3)**:
     - `arcane_frost_staff`: Pruned (Requires MAGE, player is WARRIOR).
     - `champion_dragon_plate`: Pruned (Requires Level 50+ & 1+ Boss Kills).
     - `titan_aegis_shield`: Pruned (Requires Gear Score 500+).

---

### Phase 2: Mythic Raid Boss Spark & Catalog Expansion
1. **Spark More Data**: Player participates in an endgame 20-person guild raid, dealing 95,200 damage to *Ignis the Worldburner* (`MYTHIC_RAID_BOSS`).
2. **Circuit Execution**: `boss_kill_pipeline` triggers Discord guild announcement webhook, awards 18,500 bonus gold, boosts Gear Score to 620, increments Boss Kills to 1, and promotes player to Level 60 (`MYTHIC_LEGEND`).
3. **State Persistence**: Updated state is written to Capacitor under `entity:hero_valkyrie_99`.
4. **Conduct Pass 2A (`catalog:mythic_vault`)**:
   - Evaluates newly unlocked endgame vault.
   - Player qualifies for **Mythic Artifacts**:
     - `titanforged_thunderfury`: Level 60 Warrior with 1 Boss Kill qualifies, 30% VIP discount applied (10,500 Gold, Chain Lightning proc).
     - `celestial_phoenix_mount`: Unlocked by `MYTHIC_LEGEND` VIP status with 350% mount speed and guild banner.
     - `crown_of_the_immortals`: Unlocked by Gear Score 620 and Level 60.
   - Ineligible items like `worldbreaker_annihilator` (requires 10 kills) and `archmage_eternal_robes` (requires Mage) are pruned.
5. **Conduct Pass 2B (Re-Conduct on `catalog:adventurer_armory`)**:
   - **Progression**: Qualified items increase from 3/6 to **5/6**!
   - `champion_dragon_plate`: **Now Unlocked** (Level 60 >= 50 && Boss Kills 1 >= 1).
   - `titan_aegis_shield`: **Now Unlocked** (Gear Score 620 >= 500).
   - `iron_battleaxe`: **Upgraded** (Max level 50+ discount applied: 25% off).

---

## File Structure

- [`main.go`](file:///home/shantanu/Projects/cuprite-io/flux/examples/03_gaming_boss_kill/main.go): Complete runnable demonstration with Capacitor initialization, sinks, progressive Spark and Conduct phases, and 5,000-op micro-benchmark.
- [`dungeon_progression.circuit.json`](file:///home/shantanu/Projects/cuprite-io/flux/examples/03_gaming_boss_kill/dungeon_progression.circuit.json): Phase 1 dungeon clear circuit with Volt state calculation, state persistence sink, and Discord announcement.
- [`boss_encounter.circuit.json`](file:///home/shantanu/Projects/cuprite-io/flux/examples/03_gaming_boss_kill/boss_encounter.circuit.json): Phase 2 mythic raid boss circuit with damage verification and VIP promotion.
- [`adventurer_catalog.json`](file:///home/shantanu/Projects/cuprite-io/flux/examples/03_gaming_boss_kill/adventurer_catalog.json): Starter / Adept armory catalog items with embedded qualification circuits.
- [`mythic_vault_catalog.json`](file:///home/shantanu/Projects/cuprite-io/flux/examples/03_gaming_boss_kill/mythic_vault_catalog.json): Endgame Legendary / Mythic catalog items with high-level condition guards.

---

## Running the Example

Execute from repository root:
```bash
go run examples/03_gaming_boss_kill/main.go
```

Or execute directly from the example directory:
```bash
cd examples/03_gaming_boss_kill
go run main.go
```
