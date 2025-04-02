package frontend

import (
	"context"
	"slices"
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

// RingStreamUsageGatherer implements StreamUsageGatherer. It uses a ring to find
// limits instances.
type RingStreamUsageGatherer struct {
	logger        log.Logger
	ring          ring.ReadRing
	pool          *ring_client.Pool
	numPartitions int
	cache         PartitionConsumersCache
	cacheTTL      time.Duration
}

// NewRingStreamUsageGatherer returns a new RingStreamUsageGatherer.
func NewRingStreamUsageGatherer(ring ring.ReadRing, pool *ring_client.Pool, logger log.Logger, cache PartitionConsumersCache, cacheTTL time.Duration, numPartitions int) *RingStreamUsageGatherer {
	return &RingStreamUsageGatherer{
		logger:        logger,
		ring:          ring,
		pool:          pool,
		numPartitions: numPartitions,
		cache:         cache,
		cacheTTL:      cacheTTL,
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

		if cached := g.cache.Get(instance.Addr); cached != nil {
			// Use cached partitions, but still participate in conflict resolution
			for _, partition := range cached.Value().partitions {
				if t := highestTimestamp[partition]; t < cached.Value().assignedAt[partition] {
					highestTimestamp[partition] = cached.Value().assignedAt[partition]
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
				//g.cache.Set(resp.Addr, instancePartitions, resp.Response.AssignedPartitions)
				g.cache.Set(resp.Addr, &PartitionConsumersCacheEntry{
					partitions: instancePartitions,
					assignedAt: resp.Response.AssignedPartitions,
				}, g.cacheTTL)
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
