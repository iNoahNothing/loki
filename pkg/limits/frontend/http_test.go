package frontend

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/dskit/limiter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/logproto"
)

func TestFrontend_ServeHTTP(t *testing.T) {
	tests := []struct {
		name                          string
		limits                        Limits
		expectedGetStreamUsageRequest *GetStreamUsageRequest
		getStreamUsageResponses       []GetStreamUsageResponse
		request                       httpExceedsLimitsRequest
		expected                      httpExceedsLimitsResponse
	}{{
		name: "within limits",
		limits: &mockLimits{
			maxGlobalStreams: 1,
			ingestionRate:    100,
		},
		expectedGetStreamUsageRequest: &GetStreamUsageRequest{
			Tenant:       "test",
			StreamHashes: []uint64{0x1},
		},
		getStreamUsageResponses: []GetStreamUsageResponse{{
			Response: &logproto.GetStreamUsageResponse{
				Tenant:        "test",
				ActiveStreams: 1,
				Rate:          10,
			},
		}},
		request: httpExceedsLimitsRequest{
			TenantID:     "test",
			StreamHashes: []uint64{0x1},
		},
		// expected should be default value (no rejected streams).
	}, {
		name: "exceeds limits",
		limits: &mockLimits{
			maxGlobalStreams: 1,
			ingestionRate:    100,
		},
		expectedGetStreamUsageRequest: &GetStreamUsageRequest{
			Tenant:       "test",
			StreamHashes: []uint64{0x1},
		},
		getStreamUsageResponses: []GetStreamUsageResponse{{
			Response: &logproto.GetStreamUsageResponse{
				Tenant:        "test",
				ActiveStreams: 2,
				Rate:          200,
			},
		}},
		request: httpExceedsLimitsRequest{
			TenantID:     "test",
			StreamHashes: []uint64{0x1},
		},
		expected: httpExceedsLimitsResponse{
			RejectedStreams: []*logproto.RejectedStream{{
				StreamHash: 0x1,
				Reason:     "exceeds_rate_limit",
			}},
		},
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := Frontend{
				limits:      test.limits,
				rateLimiter: limiter.NewRateLimiter(newRateLimitsAdapter(test.limits), time.Second),
				streamUsage: &mockStreamUsageGatherer{
					t:               t,
					expectedRequest: test.expectedGetStreamUsageRequest,
					responses:       test.getStreamUsageResponses,
				},
				metrics: newMetrics(prometheus.NewRegistry()),
			}
			ts := httptest.NewServer(&f)
			defer ts.Close()

			b, err := json.Marshal(test.request)
			require.NoError(t, err)

			resp, err := http.Post(ts.URL, "application/json", bytes.NewReader(b))
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			defer resp.Body.Close()
			b, err = io.ReadAll(resp.Body)
			require.NoError(t, err)

			var actual httpExceedsLimitsResponse
			require.NoError(t, json.Unmarshal(b, &actual))
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestRingStreamUsageGatherer_ServeHTTP(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		formData       map[string]string
		initialCache   map[string]*partitionConsumersCacheEntry
		expectedCache  map[string]*partitionConsumersCacheEntry
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "GET with empty cache",
			method:         http.MethodGet,
			initialCache:   map[string]*partitionConsumersCacheEntry{},
			expectedStatus: http.StatusOK,
			expectedBody:   "Ring Stream Usage Cache",
		},
		{
			name:   "GET with populated cache",
			method: http.MethodGet,
			initialCache: map[string]*partitionConsumersCacheEntry{
				"instance1:8080": {
					partitions: []int32{1, 2, 3},
					assignedAt: map[int32]int64{
						1: time.Now().Unix(),
						2: time.Now().Unix(),
						3: time.Now().Unix(),
					},
					expiration: time.Now().Add(time.Hour),
				},
				"instance2:8080": {
					partitions: []int32{4, 5, 6},
					assignedAt: map[int32]int64{
						4: time.Now().Unix(),
						5: time.Now().Unix(),
						6: time.Now().Unix(),
					},
					expiration: time.Now().Add(time.Hour),
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody:   "instance1:8080",
		},
		{
			name:   "POST clear specific instance",
			method: http.MethodPost,
			formData: map[string]string{
				"instance": "instance1:8080",
			},
			initialCache: map[string]*partitionConsumersCacheEntry{
				"instance1:8080": {
					partitions: []int32{1, 2, 3},
					assignedAt: map[int32]int64{
						1: time.Now().Unix(),
						2: time.Now().Unix(),
						3: time.Now().Unix(),
					},
					expiration: time.Now().Add(time.Hour),
				},
				"instance2:8080": {
					partitions: []int32{4, 5, 6},
					assignedAt: map[int32]int64{
						4: time.Now().Unix(),
						5: time.Now().Unix(),
						6: time.Now().Unix(),
					},
					expiration: time.Now().Add(time.Hour),
				},
			},
			expectedCache: map[string]*partitionConsumersCacheEntry{
				"instance2:8080": {
					partitions: []int32{4, 5, 6},
					assignedAt: map[int32]int64{
						4: time.Now().Unix(),
						5: time.Now().Unix(),
						6: time.Now().Unix(),
					},
					expiration: time.Now().Add(time.Hour),
				},
			},
			expectedStatus: http.StatusSeeOther,
		},
		{
			name:   "POST clear all cache",
			method: http.MethodPost,
			initialCache: map[string]*partitionConsumersCacheEntry{
				"instance1:8080": {
					partitions: []int32{1, 2, 3},
					assignedAt: map[int32]int64{
						1: time.Now().Unix(),
						2: time.Now().Unix(),
						3: time.Now().Unix(),
					},
					expiration: time.Now().Add(time.Hour),
				},
				"instance2:8080": {
					partitions: []int32{4, 5, 6},
					assignedAt: map[int32]int64{
						4: time.Now().Unix(),
						5: time.Now().Unix(),
						6: time.Now().Unix(),
					},
					expiration: time.Now().Add(time.Hour),
				},
			},
			expectedCache:  map[string]*partitionConsumersCacheEntry{},
			expectedStatus: http.StatusSeeOther,
		},
		{
			name:           "Invalid method",
			method:         http.MethodPut,
			expectedStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new cache with test data
			cache := &partitionConsumersCache{
				entries: make(map[string]*partitionConsumersCacheEntry),
				ttl:     time.Hour,
			}
			for k, v := range tt.initialCache {
				cache.entries[k] = v
			}

			// Create the gatherer with our test cache
			gatherer := &RingStreamUsageGatherer{
				cache: cache,
			}

			// Create request
			req := httptest.NewRequest(tt.method, "/ring-usage", nil)
			if tt.formData != nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				form := make(map[string][]string)
				for k, v := range tt.formData {
					form[k] = []string{v}
				}
				req.PostForm = form
				req.Form = form
			}

			// Create response recorder
			w := httptest.NewRecorder()

			// Call the handler
			gatherer.ServeHTTP(w, req)

			// Check status code
			require.Equal(t, tt.expectedStatus, w.Code)

			// For GET requests, check if the response contains expected content
			if tt.method == http.MethodGet {
				require.Contains(t, w.Body.String(), tt.expectedBody)
			}

			// For POST requests, verify cache state
			if tt.method == http.MethodPost {
				require.Equal(t, len(tt.expectedCache), len(cache.entries))
				for k := range tt.expectedCache {
					_, exists := cache.entries[k]
					require.True(t, exists)
				}
			}
		})
	}
}
