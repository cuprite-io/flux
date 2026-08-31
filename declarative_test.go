package flux_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cuprite-io/flux"
	"github.com/cuprite-io/flux/types"
)

func TestDeclarative_LoadCircuitJSON(t *testing.T) {
	rawJSON := `{
		"id": "payment_fraud_check",
		"tags": ["stream:payments", "region:us"],
		"root": {
			"name": "initial_filter",
			"condition": "amount >= 1000.0",
			"steps": [
				{ "type": "volt", "script": "amount >= 1000.0" }
			],
			"children": [
				{
					"name": "alert_branch",
					"condition": "amount > 5000.0",
					"steps": [
						{ "type": "sink", "sink": "kafka_alerts" },
						{ "type": "return", "data": { "status": "FLAGGED" } }
					]
				}
			]
		}
	}`

	c, err := flux.LoadCircuitJSON([]byte(rawJSON))
	if err != nil {
		t.Fatalf("failed to load circuit JSON: %v", err)
	}

	if c.ID != "payment_fraud_check" {
		t.Errorf("expected id payment_fraud_check, got %s", c.ID)
	}
	if len(c.Tags) != 2 || c.Tags[0] != "stream:payments" {
		t.Errorf("tags not parsed properly: %v", c.Tags)
	}
	if c.Root.Name != "initial_filter" {
		t.Errorf("expected root name initial_filter, got %s", c.Root.Name)
	}
	if len(c.Root.Children) != 1 || c.Root.Children[0].Name != "alert_branch" {
		t.Errorf("expected 1 child node named alert_branch")
	}
	if c.Root.Children[0].Steps[0].Type != types.StepSink {
		t.Errorf("expected child step 0 to be StepSink")
	}
	if c.Root.Children[0].Steps[1].Type != types.StepReturn {
		t.Errorf("expected child step 1 to be StepReturn")
	}
}

func TestDeclarative_LoadItemYAML(t *testing.T) {
	rawYAML := `
id: offer_titan_pack
tags:
  - "offers:boss_killer"
  - "store:special"
data:
  name: "Titan Slayer Bundle"
circuit:
  id: "titan_eval"
  root:
    name: "qualification"
    condition: "user.level >= 10"
    steps:
      - type: "return"
        data:
          badge: "TITAN_SLAYER"
`
	tmpFile := filepath.Join(t.TempDir(), "offer.yaml")
	_ = os.WriteFile(tmpFile, []byte(rawYAML), 0644)

	item, err := flux.LoadItemFile(tmpFile)
	if err != nil {
		t.Fatalf("failed to load item YAML: %v", err)
	}

	if item.ID != "offer_titan_pack" {
		t.Errorf("expected id offer_titan_pack, got %s", item.ID)
	}
	if item.Data["name"] != "Titan Slayer Bundle" {
		t.Errorf("expected name Titan Slayer Bundle, got %v", item.Data["name"])
	}
	if item.Circuit == nil || item.Circuit.Root.Name != "qualification" {
		t.Errorf("expected embedded circuit root qualification")
	}
}
