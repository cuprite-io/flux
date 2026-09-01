package catalog_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/catalog"
	"github.com/cuprite-io/flux/types"
)

func TestCatalog_CategoryPartitionAndFilter(t *testing.T) {
	ctx := context.Background()
	cat := catalog.New(cache.NewMemoryCache())

	item1 := &types.Item{
		ID:       "item_1",
		Category: "offers:gaming",
		Data:     map[string]any{"discount": 0.15},
	}
	item2 := &types.Item{
		ID:       "item_2",
		Category: "offers:gaming",
		Data:     map[string]any{"discount": 0.20},
	}
	item3 := &types.Item{
		ID:        "item_3",
		Category:  "offers:gaming",
		Data:      map[string]any{"discount": 0.50},
		ExpiresAt: time.Now().Add(-1 * time.Hour), // Expired!
	}
	itemOther := &types.Item{
		ID:       "item_other",
		Category: "offers:retail",
		Data:     map[string]any{"discount": 0.05},
	}

	_ = cat.Put(ctx, item1)
	_ = cat.Put(ctx, item2)
	_ = cat.Put(ctx, item3)
	_ = cat.Put(ctx, itemOther)

	// Get by category & ID
	it, err := cat.Get(ctx, "offers:gaming", "item_1")
	if err != nil || it.ID != "item_1" {
		t.Fatalf("expected item_1, got err: %v", err)
	}

	// Expired item should return ErrItemNotFound
	_, err = cat.Get(ctx, "offers:gaming", "item_3")
	if err == nil {
		t.Errorf("expected expired item_3 to return error")
	}

	// Fetch entire category bucket (item3 expired, itemOther in different partition -> returns 2)
	gamingItems := cat.GetByCategory(ctx, "offers:gaming")
	if len(gamingItems) != 2 {
		t.Fatalf("expected 2 active gaming items, got %d", len(gamingItems))
	}

	// Fetch retail category bucket
	retailItems := cat.GetByCategory(ctx, "offers:retail")
	if len(retailItems) != 1 || retailItems[0].ID != "item_other" {
		t.Fatalf("expected 1 retail item, got %+v", retailItems)
	}

	// Delete item1 from gaming category
	err = cat.Delete(ctx, "offers:gaming", "item_1")
	if err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}

	remaining := cat.GetByCategory(ctx, "offers:gaming")
	if len(remaining) != 1 || remaining[0].ID != "item_2" {
		t.Fatalf("expected 1 item after deletion, got %+v", remaining)
	}
}

func BenchmarkCatalog_CategoryQuery(b *testing.B) {
	ctx := context.Background()
	cat := catalog.New(cache.NewMemoryCache())

	for i := 0; i < 1000; i++ {
		_ = cat.Put(ctx, &types.Item{
			ID:       fmt.Sprintf("item_%d", i),
			Category: "tag:bench",
			Data:     map[string]any{"idx": i},
		})
	}

	b.ReportAllocs()

	for b.Loop() {
		items := cat.GetByCategory(ctx, "tag:bench")
		if len(items) != 1000 {
			b.Fatal("missing items in bench")
		}
	}
}
