package frontend

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/loki/v3/pkg/logproto"
)

const (
	RingKey  = "ingest-limits-frontend"
	RingName = "ingest-limits-frontend"
)

var (
	LimitsRead = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
)

// partitionConsumersCachable is a cache of partition consumers for a given instance.
// It is used to avoid querying the same instance multiple times for the same
// partition IDs.
type partitionConsumersCachable interface {
	get(addr string) (*partitionConsumersCacheEntry, bool)
	set(addr string, partitions []int32, assignedAt map[int32]int64)
}

// partitionConsumersCache is a cache of partition consumers for a given instance.
// It is used to avoid querying the same instance multiple times for the same
// partition IDs.
type partitionConsumersCache struct {
	sync.RWMutex
	entries map[string]*partitionConsumersCacheEntry
	ttl     time.Duration
}

type partitionConsumersCacheEntry struct {
	partitions []int32
	assignedAt map[int32]int64 // timestamp when each partition was assigned
	expiration time.Time
}

func newPartitionConsumersCache(ttl time.Duration) partitionConsumersCachable {
	// If TTL is zero or negative, return a no-op cache
	if ttl <= 0 {
		return &partitionConsumersCache{
			ttl: ttl,
		}
	}

	cache := &partitionConsumersCache{
		entries: make(map[string]*partitionConsumersCacheEntry),
		ttl:     ttl,
	}

	// Start cleanup goroutine
	go cache.cleanup(ttl)
	return cache
}

func (c *partitionConsumersCache) cleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		c.Lock()
		now := time.Now()
		for key, entry := range c.entries {
			if now.After(entry.expiration) {
				delete(c.entries, key)
			}
		}
		c.Unlock()
	}
}

func (c *partitionConsumersCache) get(addr string) (*partitionConsumersCacheEntry, bool) {
	// If TTL is zero or negative, cache is disabled
	if c.ttl <= 0 {
		return nil, false
	}

	c.RLock()
	defer c.RUnlock()

	entry, exists := c.entries[addr]
	if !exists {
		return nil, false
	}

	if time.Now().After(entry.expiration) {
		return nil, false
	}

	return entry, true
}

func (c *partitionConsumersCache) set(addr string, partitions []int32, assignedAt map[int32]int64) {
	// If TTL is zero or negative, cache is disabled
	if c.ttl <= 0 {
		return
	}

	c.Lock()
	defer c.Unlock()

	c.entries[addr] = &partitionConsumersCacheEntry{
		partitions: partitions,
		assignedAt: assignedAt,
		expiration: time.Now().Add(c.ttl),
	}
}

// RingStreamUsageGatherer implements StreamUsageGatherer. It uses a ring to find
// limits instances.
type RingStreamUsageGatherer struct {
	logger        log.Logger
	ring          ring.ReadRing
	pool          *ring_client.Pool
	numPartitions int
	cache         partitionConsumersCachable
}

// NewRingStreamUsageGatherer returns a new RingStreamUsageGatherer.
func NewRingStreamUsageGatherer(ring ring.ReadRing, pool *ring_client.Pool, logger log.Logger, cache partitionConsumersCachable, numPartitions int) *RingStreamUsageGatherer {
	return &RingStreamUsageGatherer{
		logger:        logger,
		ring:          ring,
		pool:          pool,
		numPartitions: numPartitions,
		cache:         cache,
	}
}

// GetStreamUsage implements StreamUsageGatherer.
func (g *RingStreamUsageGatherer) GetStreamUsage(ctx context.Context, r GetStreamUsageRequest) ([]GetStreamUsageResponse, error) {
	if len(r.StreamHashes) == 0 {
		return nil, nil
	}
	return g.forAllBackends(ctx, r)
}

// TODO(grobinson): Need to rename this to something more accurate.
func (g *RingStreamUsageGatherer) forAllBackends(ctx context.Context, r GetStreamUsageRequest) ([]GetStreamUsageResponse, error) {
	rs, err := g.ring.GetAllHealthy(LimitsRead)
	if err != nil {
		return nil, err
	}
	return g.forGivenReplicaSet(ctx, rs, r)
}

func (g *RingStreamUsageGatherer) forGivenReplicaSet(ctx context.Context, rs ring.ReplicationSet, r GetStreamUsageRequest) ([]GetStreamUsageResponse, error) {
	partitions, err := g.getPartitionConsumers(ctx, rs)
	if err != nil {
		return nil, err
	}

	errg, ctx := errgroup.WithContext(ctx)
	var owningInstances []ring.InstanceDesc

outer:
	for _, hash := range r.StreamHashes {
		for _, instance := range rs.Instances {
			partitionID := int32(hash % uint64(g.numPartitions))

			if !slices.Contains(partitions[instance.Addr], partitionID) {
				continue
			}

			for _, owning := range owningInstances {
				if owning.Addr == instance.Addr {
					continue outer
				}
			}

			owningInstances = append(owningInstances, instance)
			continue outer
		}
	}

	responses := make([]GetStreamUsageResponse, len(owningInstances))

	// TODO: We shouldn't query all instances since we know which instance holds which stream.
	for i, instance := range owningInstances {
		errg.Go(func() error {
			client, err := g.pool.GetClientFor(instance.Addr)
			if err != nil {
				return err
			}
			protoReq := &logproto.GetStreamUsageRequest{
				Tenant:       r.Tenant,
				StreamHashes: r.StreamHashes,
				Partitions:   partitions[instance.Addr],
			}

			resp, err := client.(logproto.IngestLimitsClient).GetStreamUsage(ctx, protoReq)
			if err != nil {
				return err
			}
			responses[i] = GetStreamUsageResponse{Addr: instance.Addr, Response: resp}
			return nil
		})
	}

	if err := errg.Wait(); err != nil {
		return nil, err
	}

	return responses, nil
}

type getAssignedPartitionsResponse struct {
	Addr     string
	Response *logproto.GetAssignedPartitionsResponse
}

func (g *RingStreamUsageGatherer) getPartitionConsumers(ctx context.Context, rs ring.ReplicationSet) (map[string][]int32, error) {
	// Initialize result maps
	result := make(map[string][]int32)
	highestTimestamp := make(map[int32]int64)
	assigned := make(map[int32]string)

	// Track which instances need to be queried
	toQuery := make([]ring.InstanceDesc, 0, len(rs.Instances))

	// Try to get cached entries first
	for _, instance := range rs.Instances {
		if g.cache == nil {
			toQuery = append(toQuery, instance)
			continue
		}

		if cached, ok := g.cache.get(instance.Addr); ok {
			// Use cached partitions, but still participate in conflict resolution
			for _, partition := range cached.partitions {
				if t := highestTimestamp[partition]; t < cached.assignedAt[partition] {
					highestTimestamp[partition] = cached.assignedAt[partition]
					assigned[partition] = instance.Addr
				}
			}
		} else {
			toQuery = append(toQuery, instance)
		}
	}

	// Query uncached instances
	if len(toQuery) > 0 {
		errg, ctx := errgroup.WithContext(ctx)
		responses := make([]getAssignedPartitionsResponse, len(toQuery))

		for i, instance := range toQuery {
			i, instance := i, instance // capture loop variables
			errg.Go(func() error {
				client, err := g.pool.GetClientFor(instance.Addr)
				if err != nil {
					return err
				}
				resp, err := client.(logproto.IngestLimitsClient).GetAssignedPartitions(ctx, &logproto.GetAssignedPartitionsRequest{})
				if err != nil {
					return err
				}
				responses[i] = getAssignedPartitionsResponse{Addr: instance.Addr, Response: resp}
				return nil
			})
		}
		if err := errg.Wait(); err != nil {
			return nil, err
		}

		// Process and cache new responses
		for _, resp := range responses {
			instancePartitions := make([]int32, 0, len(resp.Response.AssignedPartitions))
			for partition, assignedAt := range resp.Response.AssignedPartitions {
				instancePartitions = append(instancePartitions, partition)
				if t := highestTimestamp[partition]; t < assignedAt {
					highestTimestamp[partition] = assignedAt
					assigned[partition] = resp.Addr
				}
			}
			// Cache the instance's partitions
			if g.cache != nil {
				g.cache.set(resp.Addr, instancePartitions, resp.Response.AssignedPartitions)
			}
		}
	}

	// Build final result based on conflict resolution
	for partition, addr := range assigned {
		result[addr] = append(result[addr], partition)
	}

	// Sort the partition IDs
	for instance := range result {
		slices.Sort(result[instance])
	}

	return result, nil
}
