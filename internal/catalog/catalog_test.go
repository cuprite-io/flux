package catalog_test

import (
	"context"
	"testing"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/internal/catalog"
	"github.com/cuprite-io/flux/types"
)

func TestCatalog_PutGetAndTagFilter(t *testing.T) {
	cat := catalog.New(cache.NewMemoryCache())

	item1 := &types.Item{
		ID:   "item_1",
		Tags: []string{"offers:gaming", "region:us"},
		Data: map[string]any{"discount": 0.15},
	}
	item2 := &types.Item{
		ID:   "item_2",
		Tags: []string{"offers:gaming", "region:eu"},
		Data: map[string]any{"discount": 0.20},
	}
	item3 := &types.Item{
		ID:        "item_3",
		Tags:      []string{"offers:gaming"},
		Data:      map[string]any{"discount": 0.50},
		ExpiresAt: time.Now().Add(-1 * time.Hour), // Expired!
	}

	_ = cat.Put(context.Background(), item1)
	_ = cat.Put(context.Background(), item2)
	_ = cat.Put(context.Background(), item3)

	// Get by ID
	it, err := cat.Get(context.Background(), "item_1")
	if err != nil || it.ID != "item_1" {
		t.Fatalf("expected item_1, got err: %v", err)
	}

	// Expired item should return ErrItemNotFound
	_, err = cat.Get(context.Background(), "item_3")
	if err == nil {
		t.Errorf("expected expired item_3 to return error")
	}

	// Filter by tag: offers:gaming (item3 expired, so only 2 return)
	gamingItems := cat.GetByTags(context.Background(), "offers:gaming")
	if len(gamingItems) != 2 {
		t.Errorf("expected 2 active gaming items, got %d", len(gamingItems))
	}
}

func BenchmarkCatalog_TagQuery(b *testing.B) {
	cat := catalog.New(cache.NewMemoryCache())

	for i := 0; i < 1000; i++ {
		_ = cat.Put(context.Background(), &types.Item{
			ID:   string(rune(i)),
			Tags: []string{"offers:all", "tag:bench"},
			Data: map[string]any{"idx": i},
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		items := cat.GetByTags(context.Background(), "tag:bench")
		if len(items) == 0 {
			b.Fatal("no items returned")
		}
	}
}
