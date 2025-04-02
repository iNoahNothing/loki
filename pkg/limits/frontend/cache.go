package frontend

import (
	"time"

	"github.com/jellydator/ttlcache/v3"
)

type PartitionConsumersCache = *ttlcache.Cache[string, *PartitionConsumersCacheEntry]

type PartitionConsumersCacheEntry struct {
	partitions []int32
	assignedAt map[int32]int64 // timestamp when each partition was assigned
	expiration time.Time
}

func NewPartitionConsumerCache(ttl time.Duration) PartitionConsumersCache {
	return ttlcache.New(
		ttlcache.WithTTL[string, *PartitionConsumersCacheEntry](ttl),
		ttlcache.WithDisableTouchOnHit[string, *PartitionConsumersCacheEntry](),
	)
}
