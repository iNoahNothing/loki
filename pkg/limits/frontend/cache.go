package frontend

import (
	"sync"
	"time"
)

// PartitionConsumersCachable is a cache of partition IDs for a given instance.
// It is used to avoid querying the same instance multiple times for the same
// partition IDs.
type PartitionConsumersCachable interface {
	Get(addr string) (*PartitionConsumersCacheEntry, bool)
	GetAll() map[string]*PartitionConsumersCacheEntry
	Set(addr string, partitions []int32, assignedAt map[int32]int64)
	Delete(addr string)
	DeleteAll()
}

// PartitionConsumersCache is a cache of partition IDs for a given instance.
// It is used to avoid querying the same instance multiple times for the same
// partition IDs.
type PartitionConsumersCache struct {
	mtx     sync.RWMutex
	entries map[string]*PartitionConsumersCacheEntry
	ttl     time.Duration
}

type PartitionConsumersCacheEntry struct {
	partitions []int32
	assignedAt map[int32]int64 // timestamp when each partition was assigned
	expiration time.Time
}

func NewPartitionConsumersCache(ttl time.Duration) PartitionConsumersCachable {
	// If TTL is zero or negative, return a no-op cache
	if ttl <= 0 {
		return &PartitionConsumersCache{
			ttl: ttl,
		}
	}

	cache := &PartitionConsumersCache{
		entries: make(map[string]*PartitionConsumersCacheEntry),
		ttl:     ttl,
	}

	// Start cleanup goroutine
	go cache.evict(ttl)
	return cache
}

func (c *PartitionConsumersCache) evict(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		c.mtx.Lock()
		now := time.Now()
		for key, entry := range c.entries {
			if now.After(entry.expiration) {
				delete(c.entries, key)
			}
		}
		c.mtx.Unlock()
	}
}

func (c *PartitionConsumersCache) Get(addr string) (*PartitionConsumersCacheEntry, bool) {
	// If TTL is zero or negative, cache is disabled
	if c.ttl <= 0 {
		return nil, false
	}

	c.mtx.RLock()
	defer c.mtx.RUnlock()

	entry, exists := c.entries[addr]
	if !exists {
		return nil, false
	}

	if time.Now().After(entry.expiration) {
		return nil, false
	}

	return entry, true
}

func (c *PartitionConsumersCache) GetAll() map[string]*PartitionConsumersCacheEntry {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	return c.entries
}

func (c *PartitionConsumersCache) Set(addr string, partitions []int32, assignedAt map[int32]int64) {
	// If TTL is zero or negative, cache is disabled
	if c.ttl <= 0 {
		return
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.entries[addr] = &PartitionConsumersCacheEntry{
		partitions: partitions,
		assignedAt: assignedAt,
		expiration: time.Now().Add(c.ttl),
	}
}

func (c *PartitionConsumersCache) Delete(addr string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.entries, addr)
}

func (c *PartitionConsumersCache) DeleteAll() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.entries = make(map[string]*PartitionConsumersCacheEntry)
}
