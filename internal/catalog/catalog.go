package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/types"
)

var (
	ErrItemNotFound = errors.New("flux catalog: item not found")
)

const (
	categoryMapPrefix = "catalog:group:"
)

// Catalog manages candidate items directly in the CacheBacken
type Catalog struct {
	cacheBackend cache.CacheBackend
	fallback     *cache.MemoryCache
}

// New creates a new stateless Catalog backed directly by the provided CacheBackend.
// If backend is nil, an in-memory fallback cache is initialized.
func New(backend cache.CacheBackend) *Catalog {
	c := &Catalog{
		cacheBackend: backend,
	}
	if backend == nil {
		c.fallback = cache.NewMemoryCache()
	}
	return c
}

func (c *Catalog) backend() cache.CacheBackend {
	if c.cacheBackend != nil {
		return c.cacheBackend
	}
	return c.fallback
}

// Put inserts or updates a candidate Item directly in Capacitor's Category-Partitioned Map.
func (c *Catalog) Put(ctx context.Context, item *types.Item) error {
	if item == nil || item.ID == "" {
		return errors.New("flux catalog: item ID required")
	}

	cat := item.PrimaryCategory()
	mapKey := categoryMapPrefix + cat

	data, err := json.Marshal(item)
	if err != nil {
		return err
	}

	_, err = c.backend().MapSet(ctx, mapKey, item.ID, string(data), 0)
	return err
}

// Get retrieves a candidate item by primary category and ID.
func (c *Catalog) Get(ctx context.Context, category, id string) (*types.Item, error) {
	if category == "" {
		category = "default"
	}
	mapKey := categoryMapPrefix + category

	var item types.Item
	found, err := c.backend().MapGetScan(ctx, mapKey, id, &item)
	if err != nil || !found {
		return nil, ErrItemNotFound
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		return nil, ErrItemNotFound
	}
	return &item, nil
}

// GetByCategory retrieves all active candidate items for a category partition in a single 1-call Map read.
func (c *Catalog) GetByCategory(ctx context.Context, category string) []*types.Item {
	if category == "" {
		category = "default"
	}
	mapKey := categoryMapPrefix + category

	rawMap, err := c.backend().MapGetAll(ctx, mapKey)
	if err != nil || len(rawMap) == 0 {
		return nil
	}

	now := time.Now()
	res := make([]*types.Item, 0, len(rawMap))
	for _, raw := range rawMap {
		var it types.Item
		if errJSON := json.Unmarshal([]byte(raw), &it); errJSON == nil {
			if it.ExpiresAt.IsZero() || it.ExpiresAt.After(now) {
				res = append(res, &it)
			}
		}
	}
	return res
}

// GetByTags retrieves candidate items for the primary tag category.
func (c *Catalog) GetByTags(ctx context.Context, tags ...string) []*types.Item {
	if len(tags) == 0 {
		return nil
	}
	return c.GetByCategory(ctx, tags[0])
}

// Delete removes an item from its Category-Partitioned Map.
func (c *Catalog) Delete(ctx context.Context, category, id string) error {
	if category == "" {
		category = "default"
	}
	mapKey := categoryMapPrefix + category
	_, err := c.backend().MapRemove(ctx, mapKey, id)
	return err
}
