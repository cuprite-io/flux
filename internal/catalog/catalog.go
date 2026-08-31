package catalog

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cuprite-io/flux/internal/cache"
	"github.com/cuprite-io/flux/types"
)

var (
	ErrItemNotFound = errors.New("flux catalog: item not found")
)

// CatalogSnapshot represents an immutable point-in-time snapshot of candidate items.
type CatalogSnapshot struct {
	items    map[string]*types.Item
	tagIndex map[string][]string // tag -> []itemID
}

// Catalog manages candidate items (offers, missions, rules) with embedded qualification Circuits.
type Catalog struct {
	cacheBackend cache.CacheBackend
	mu           sync.RWMutex
	current      atomic.Pointer[CatalogSnapshot]
}

// New creates a new Catalog connected to an optional CacheBackend.
func New(backend cache.CacheBackend) *Catalog {
	c := &Catalog{
		cacheBackend: backend,
	}
	c.current.Store(&CatalogSnapshot{
		items:    make(map[string]*types.Item),
		tagIndex: make(map[string][]string),
	})
	return c
}

// Put inserts or updates a candidate Item with atomic snapshot hot-swapping.
func (c *Catalog) Put(ctx context.Context, item *types.Item) error {
	if item == nil || item.ID == "" {
		return errors.New("flux catalog: item ID required")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cur := c.current.Load()

	// Clone map for copy-on-write snapshot
	newItems := make(map[string]*types.Item, len(cur.items)+1)
	for k, v := range cur.items {
		newItems[k] = v
	}
	newItems[item.ID] = item

	// Rebuild tag index
	newTagIndex := make(map[string][]string)
	for id, it := range newItems {
		for _, tag := range it.Tags {
			newTagIndex[tag] = append(newTagIndex[tag], id)
		}
	}

	c.current.Store(&CatalogSnapshot{
		items:    newItems,
		tagIndex: newTagIndex,
	})

	if c.cacheBackend != nil {
		_ = c.cacheBackend.Set(ctx, "catalog:"+item.ID, item, 0)
	}

	return nil
}

// Get retrieves a candidate item by ID.
func (c *Catalog) Get(ctx context.Context, id string) (*types.Item, error) {
	snap := c.current.Load()
	it, ok := snap.items[id]
	if !ok {
		return nil, ErrItemNotFound
	}
	// Check expiration
	if !it.ExpiresAt.IsZero() && time.Now().After(it.ExpiresAt) {
		return nil, ErrItemNotFound
	}
	return it, nil
}

// GetByTags retrieves all candidate items matching any of the specified filter tags.
func (c *Catalog) GetByTags(ctx context.Context, tags ...string) []*types.Item {
	if len(tags) == 0 {
		return nil
	}

	snap := c.current.Load()
	matchedIDs := make(map[string]struct{})

	for _, tag := range tags {
		if ids, ok := snap.tagIndex[tag]; ok {
			for _, id := range ids {
				matchedIDs[id] = struct{}{}
			}
		}
	}

	if len(matchedIDs) == 0 {
		return nil
	}

	now := time.Now()
	res := make([]*types.Item, 0, len(matchedIDs))
	for id := range matchedIDs {
		if it, ok := snap.items[id]; ok {
			if it.ExpiresAt.IsZero() || it.ExpiresAt.After(now) {
				res = append(res, it)
			}
		}
	}
	return res
}

// Delete removes an item by ID.
func (c *Catalog) Delete(ctx context.Context, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cur := c.current.Load()
	if _, ok := cur.items[id]; !ok {
		return ErrItemNotFound
	}

	newItems := make(map[string]*types.Item, len(cur.items))
	for k, v := range cur.items {
		if k != id {
			newItems[k] = v
		}
	}

	newTagIndex := make(map[string][]string)
	for itID, it := range newItems {
		for _, tag := range it.Tags {
			newTagIndex[tag] = append(newTagIndex[tag], itID)
		}
	}

	c.current.Store(&CatalogSnapshot{
		items:    newItems,
		tagIndex: newTagIndex,
	})

	if c.cacheBackend != nil {
		_ = c.cacheBackend.Delete(ctx, "catalog:"+id)
	}

	return nil
}
