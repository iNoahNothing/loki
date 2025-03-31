package frontend

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/logproto"
)

func TestRingStreamUsageGatherer_GetStreamUsage(t *testing.T) {
	const numPartitions = 2 // Using 2 partitions for simplicity in tests
	tests := []struct {
		name                              string
		getStreamUsageRequest             GetStreamUsageRequest
		expectedAssignedPartitionsRequest []*logproto.GetAssignedPartitionsRequest
		getAssignedPartitionsResponses    []*logproto.GetAssignedPartitionsResponse
		expectedStreamUsageRequest        []*logproto.GetStreamUsageRequest
		getStreamUsageResponses           []*logproto.GetStreamUsageResponse
		expectedResponses                 []GetStreamUsageResponse
	}{{
		// When there are no streams, no RPCs should be sent.
		name: "no streams",
		getStreamUsageRequest: GetStreamUsageRequest{
			Tenant:       "test",
			StreamHashes: []uint64{},
		},
	}, {
		// When there is one stream, and one instance, the stream usage for that
		// stream should be queried from that instance.
		name: "one stream",
		getStreamUsageRequest: GetStreamUsageRequest{
			Tenant:       "test",
			StreamHashes: []uint64{1}, // Hash 1 maps to partition 1
		},
		expectedAssignedPartitionsRequest: []*logproto.GetAssignedPartitionsRequest{{}},
		getAssignedPartitionsResponses: []*logproto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				1: time.Now().UnixNano(),
			},
		}},
		expectedStreamUsageRequest: []*logproto.GetStreamUsageRequest{{
			Tenant:       "test",
			StreamHashes: []uint64{1},
			Partitions:   []int32{1},
		}},
		getStreamUsageResponses: []*logproto.GetStreamUsageResponse{{
			Tenant:        "test",
			ActiveStreams: 1,
			Rate:          10,
		}},
		expectedResponses: []GetStreamUsageResponse{{
			Addr: "instance-0",
			Response: &logproto.GetStreamUsageResponse{
				Tenant:        "test",
				ActiveStreams: 1,
				Rate:          10,
			},
		}},
	}, {
		// When there is one stream, and two instances each owning separate
		// partitions, only the instance owning the partition for the stream hash
		// should be queried.
		name: "one stream two instances",
		getStreamUsageRequest: GetStreamUsageRequest{
			Tenant:       "test",
			StreamHashes: []uint64{1}, // Hash 1 maps to partition 1
		},
		expectedAssignedPartitionsRequest: []*logproto.GetAssignedPartitionsRequest{{}, {}},
		getAssignedPartitionsResponses: []*logproto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				0: time.Now().UnixNano(),
			},
		}, {
			AssignedPartitions: map[int32]int64{
				1: time.Now().UnixNano(),
			},
		}},
		expectedStreamUsageRequest: []*logproto.GetStreamUsageRequest{{}, {
			Tenant:       "test",
			StreamHashes: []uint64{1},
			Partitions:   []int32{1},
		}},
		getStreamUsageResponses: []*logproto.GetStreamUsageResponse{{}, {
			Tenant:        "test",
			ActiveStreams: 1,
			Rate:          10,
		}},
		expectedResponses: []GetStreamUsageResponse{{
			Addr: "instance-1",
			Response: &logproto.GetStreamUsageResponse{
				Tenant:        "test",
				ActiveStreams: 1,
				Rate:          10,
			},
		}},
	}, {
		// When there is one stream, and two instances owning overlapping
		// partitions, only the instance with the latest timestamp for the relevant
		// partition should be queried.
		name: "one stream two instances, overlapping partition ownership",
		getStreamUsageRequest: GetStreamUsageRequest{
			Tenant:       "test",
			StreamHashes: []uint64{1}, // Hash 1 maps to partition 1
		},
		expectedAssignedPartitionsRequest: []*logproto.GetAssignedPartitionsRequest{{}, {}},
		getAssignedPartitionsResponses: []*logproto.GetAssignedPartitionsResponse{{
			AssignedPartitions: map[int32]int64{
				1: time.Now().Add(-time.Second).UnixNano(),
			},
		}, {
			AssignedPartitions: map[int32]int64{
				1: time.Now().UnixNano(),
			},
		}},
		expectedStreamUsageRequest: []*logproto.GetStreamUsageRequest{{}, {
			Tenant:       "test",
			StreamHashes: []uint64{1},
			Partitions:   []int32{1},
		}},
		getStreamUsageResponses: []*logproto.GetStreamUsageResponse{{}, {
			Tenant:        "test",
			ActiveStreams: 1,
			Rate:          10,
		}},
		expectedResponses: []GetStreamUsageResponse{{
			Addr: "instance-1",
			Response: &logproto.GetStreamUsageResponse{
				Tenant:        "test",
				ActiveStreams: 1,
				Rate:          10,
			},
		}},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Set up the mock clients, one for each pair of mock RPC responses.
			clients := make([]logproto.IngestLimitsClient, len(test.expectedAssignedPartitionsRequest))
			instances := make([]ring.InstanceDesc, len(clients))

			for i := 0; i < len(test.expectedAssignedPartitionsRequest); i++ {
				expectedStreamUsageReq := (*logproto.GetStreamUsageRequest)(nil)
				getStreamUsageResp := (*logproto.GetStreamUsageResponse)(nil)

				if i < len(test.expectedStreamUsageRequest) {
					expectedStreamUsageReq = test.expectedStreamUsageRequest[i]
				}
				if i < len(test.getStreamUsageResponses) {
					getStreamUsageResp = test.getStreamUsageResponses[i]
				}

				clients[i] = &mockIngestLimitsClient{
					expectedAssignedPartitionsRequest: test.expectedAssignedPartitionsRequest[i],
					getAssignedPartitionsResponse:     test.getAssignedPartitionsResponses[i],
					expectedStreamUsageRequest:        expectedStreamUsageReq,
					getStreamUsageResponse:            getStreamUsageResp,
					t:                                 t,
				}
				instances[i] = ring.InstanceDesc{
					Addr: fmt.Sprintf("instance-%d", i),
				}
			}

			// Set up the mocked ring and client pool for the tests.
			readRing, clientPool := newMockRingWithClientPool(t, "test", clients, instances)
			// Disable caching for these tests
			g := NewRingStreamUsageGatherer(readRing, clientPool, log.NewNopLogger(), nil, numPartitions)

			// Set a maximum upper bound on the test execution time.
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			resps, err := g.GetStreamUsage(ctx, test.getStreamUsageRequest)
			require.NoError(t, err)
			require.Equal(t, test.expectedResponses, resps)
		})
	}
}

func TestRingStreamUsageGatherer_CacheDisabled(t *testing.T) {
	const numPartitions = 2

	// Set up test case with one instance
	now := time.Now()
	clients := []logproto.IngestLimitsClient{
		&mockIngestLimitsClient{
			expectedAssignedPartitionsRequest: &logproto.GetAssignedPartitionsRequest{},
			getAssignedPartitionsResponse: &logproto.GetAssignedPartitionsResponse{
				AssignedPartitions: map[int32]int64{
					1: now.UnixNano(),
				},
			},
			expectedStreamUsageRequest: &logproto.GetStreamUsageRequest{
				Tenant:       "test",
				StreamHashes: []uint64{1},
				Partitions:   []int32{1},
			},
			getStreamUsageResponse: &logproto.GetStreamUsageResponse{
				Tenant:        "test",
				ActiveStreams: 1,
				Rate:          10,
			},
			t: t,
		},
	}
	instances := []ring.InstanceDesc{
		{Addr: "instance-0"},
	}

	testCases := []struct {
		name                   string
		ttl                    time.Duration
		expectedCacheGetCalled int
		expectedCacheSetCalled int
	}{
		{
			name: "cache disabled with zero TTL",
			ttl:  0,
		},
		{
			name: "cache disabled with negative TTL",
			ttl:  -1 * time.Second,
		},
		{
			name:                   "cache enabled with positive TTL",
			ttl:                    time.Minute,
			expectedCacheGetCalled: 2,
			expectedCacheSetCalled: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			readRing, clientPool := newMockRingWithClientPool(t, "test", clients, instances)
			cache := newMockPartitionConsumersCache()
			g := NewRingStreamUsageGatherer(readRing, clientPool, log.NewNopLogger(), cache, numPartitions)

			ctx := context.Background()
			req := GetStreamUsageRequest{
				Tenant:       "test",
				StreamHashes: []uint64{1},
			}

			// First call
			resp1, err := g.GetStreamUsage(ctx, req)
			require.NoError(t, err)
			require.Len(t, resp1, 1)
			require.Equal(t, "instance-0", resp1[0].Addr)

			// Second immediate call
			resp2, err := g.GetStreamUsage(ctx, req)
			require.NoError(t, err)
			require.Len(t, resp2, 1)
			require.Equal(t, "instance-0", resp2[0].Addr)

			// For enabled cache, responses should be identical
			if tc.ttl > 0 {
				require.Equal(t, cache.getCalled, tc.expectedCacheGetCalled)
				require.Equal(t, cache.setCalled, tc.expectedCacheSetCalled)
				require.Equal(t, resp1, resp2, "with enabled cache, responses should be identical")
			}
		})
	}
}
