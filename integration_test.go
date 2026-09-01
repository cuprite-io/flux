package flux_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cuprite-io/capacitor"
	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type BossKillEvent struct {
	UserID    string `json:"user_id"`
	BossKills int    `json:"boss_kills"`
	XPAdded   int    `json:"xp_added"`
}

// TestDistributed_CapacitorReplicationConvergence verifies that two separate Flux instances,
// each backed by an independent Capacitor with zero direct communication between Flux instances,
// achieve instantaneous distributed state convergence across Spark and Conduct.
func TestDistributed_CapacitorReplicationConvergence(t *testing.T) {
	ctx := context.Background()

	// 1. Boot Capacitor Node 1 (Peer A)
	dir1, err := os.MkdirTemp("", "capacitor-peer-1-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir1)

	cfg1 := capacitor.Config{
		NodeID:     "node-1",
		DataPath:   dir1,
		BindPort:   19411,
		StreamPort: 19412,
	}
	cp1, err := capacitor.New(cfg1)
	require.NoError(t, err)

	// 2. Boot Capacitor Node 2 (Peer B) and join Node 1
	dir2, err := os.MkdirTemp("", "capacitor-peer-2-*")
	require.NoError(t, err)
	defer os.RemoveAll(dir2)

	cfg2 := capacitor.Config{
		NodeID:     "node-2",
		DataPath:   dir2,
		BindPort:   19413,
		StreamPort: 19414,
		Peers:      []string{"127.0.0.1:19411"},
	}
	cp2, err := capacitor.New(cfg2)
	require.NoError(t, err)

	// 3. Initialize Flux Instance 1 (connected exclusively to Capacitor Node 1)
	flux1, err := flux.New(flux.WithCache(cp1))
	require.NoError(t, err)
	defer flux1.Close()

	// 4. Initialize Flux Instance 2 (connected exclusively to Capacitor Node 2)
	// NOTICE: Flux 1 and Flux 2 have ZERO direct linkage or shared memory!
	flux2, err := flux.New(flux.WithCache(cp2))
	require.NoError(t, err)
	defer flux2.Close()

	// 5. Register Circuit on Flux 1 from declarative JSON
	circuitJSON := `{
		"id": "boss_progression_circuit",
		"tags": ["stream:gameplay"],
		"root": {
			"name": "record_kill",
			"steps": [
				{ "type": "volt", "script": "payload.xp_added > 0.0" }
			]
		}
	}`
	circuit, err := flux.LoadCircuitJSON([]byte(circuitJSON))
	require.NoError(t, err)
	err = flux1.Registry().Put(ctx, circuit)
	require.NoError(t, err)

	// 6. Register Candidate Item on Flux 2 from declarative JSON
	itemJSON := `{
		"id": "offer_titan_slayer",
		"category": "offers:boss_rewards",
		"data": {
			"sku": "SKU-TITAN-01"
		},
		"circuit": {
			"id": "titan_qualify",
			"root": {
				"name": "check_level",
				"condition": "user.level >= 10.0",
				"steps": [
					{ "type": "return", "data": { "tier": "TITAN_SLAYER", "bonus_gems": 500 } }
				]
			}
		}
	}`
	item, err := flux.LoadItemJSON([]byte(itemJSON))
	require.NoError(t, err)
	err = flux2.Catalog().Put(ctx, item)
	require.NoError(t, err)

	// 7. Store initial player state in Capacitor Node 1 (Player is Level 10, XP 9000)
	err = cp1.Set(ctx, "entity:player_42", `{"level": 10.0, "xp": 9000.0}`, 0)
	require.NoError(t, err)

	// 8. Execute Spark on Flux Instance 1 (Processing boss kill event)
	evt := BossKillEvent{UserID: "player_42", BossKills: 1, XPAdded: 1500}
	sparkRes, err := flux1.Spark(ctx, evt, "stream:gameplay")
	require.NoError(t, err)
	if len(sparkRes.Errors) > 0 {
		t.Fatalf("spark had errors: %v", sparkRes.Errors)
	}
	assert.True(t, sparkRes.Passed)

	// Update entity state in Capacitor Node 1
	err = cp1.Set(ctx, "entity:player_42", `{"level": 10.0, "xp": 10500.0}`, 0)
	require.NoError(t, err)

	// 9. Wait for Delta Log replication from Node 1 to Node 2
	require.Eventually(t, func() bool {
		val, err := cp2.Get(ctx, "entity:player_42")
		return err == nil && val != ""
	}, 5*time.Second, 20*time.Millisecond, "delta log replication failed to converge on Node 2")

	// 10. Execute Conduct on Flux Instance 2 (Querying offers for player_42 via Node 2)
	req := &types.ConductRequest{
		EntityID: "player_42",
		Tags:     []string{"offers:boss_rewards"},
		TopK:     5,
		Timeout:  100 * time.Millisecond,
	}

	conductRes, err := flux2.Conduct(ctx, req)
	require.NoError(t, err)
	t.Logf("Evaluated count: %d, items: %+v", conductRes.EvaluatedCount, conductRes.Items)

	// Verify distributed qualification
	require.Equal(t, 1, len(conductRes.Items), "player_42 should qualify on Node 2")
	assert.Equal(t, "offer_titan_slayer", conductRes.Items[0].ID)
	assert.Equal(t, "TITAN_SLAYER", conductRes.Items[0].ComputedOutput["tier"])
	assert.Equal(t, float64(500), conductRes.Items[0].ComputedOutput["bonus_gems"])
}
