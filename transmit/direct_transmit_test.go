package transmit

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
	"github.com/vmihailenco/msgpack/v5"

	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

type testDirectAPIServer struct {
	server       *httptest.Server
	maxBatchSize int

	events []receivedEvent
	mutex  sync.RWMutex
	t      testing.TB
}

type receivedEvent struct {
	APIKey     string         `msgpack:"-"`
	Dataset    string         `msgpack:"-"`
	Time       time.Time      `msgpack:"time"`
	SampleRate int64          `msgpack:"samplerate"`
	Data       map[string]any `msgpack:"data"`
}

func newTestDirectAPIServer(t testing.TB, maxBatchSize int) *testDirectAPIServer {
	server := &testDirectAPIServer{
		maxBatchSize: maxBatchSize,
		t:            t,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/1/batch/", server.handleBatch)

	server.server = httptest.NewServer(mux)
	t.Cleanup(server.server.Close)
	return server
}

func (t *testDirectAPIServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	var reader io.Reader = r.Body

	// Handle zstd compression (even though we don't expect it, keep for completeness)
	if encoding := r.Header.Get("Content-Encoding"); encoding == "zstd" {
		zr, err := zstd.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			assert.NoError(t.t, err)
			return
		}
		defer zr.Close()
		reader = zr
	}

	var events []receivedEvent
	contentType := r.Header.Get("Content-Type")
	assert.Equal(t.t, "application/msgpack", contentType)
	decoder := msgpack.NewDecoder(reader)
	decoder.UseLooseInterfaceDecoding(true)
	if err := decoder.Decode(&events); err != nil {
		http.Error(w, fmt.Sprintf("Msgpack decode error: %v", err), http.StatusBadRequest)
		assert.NoError(t.t, err)
		return
	}
	assert.LessOrEqual(t.t, len(events), t.maxBatchSize)

	dataset := strings.TrimPrefix(r.URL.Path, "/1/batch/")
	apiKey := r.Header.Get("X-Honeycomb-Team")

	t.mutex.Lock()
	defer t.mutex.Unlock()

	responses := make([]map[string]any, 0, len(events))
	for i := range events {
		events[i].APIKey = apiKey
		events[i].Dataset = dataset

		t.events = append(t.events, events[i])
		responses = append(responses, map[string]any{"status": http.StatusAccepted})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Send back proper responses for each event
	responseBytes, err := json.Marshal(responses)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(responseBytes)
}

func (t *testDirectAPIServer) getEvents() []receivedEvent {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return slices.Clone(t.events)
}

// setupDirectTransmissionTest creates a configured DirectTransmission with mocks for testing
func setupDirectTransmissionTest(t *testing.T) (*DirectTransmission, *metrics.MockMetrics, *logger.MockLogger) {
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()

	mockLogger := &logger.MockLogger{}

	dt := NewDirectTransmission(mockMetrics, "test", 10, 20*time.Millisecond)
	dt.Logger = mockLogger
	dt.Version = "test-version"
	dt.Transport = http.DefaultTransport.(*http.Transport)

	err := dt.Start()
	require.NoError(t, err)
	t.Cleanup(func() { dt.Stop() })

	return dt, mockMetrics, mockLogger
}

// sendTestEvents sends n events to the DirectTransmission with the given server URL
func sendTestEvents(dt *DirectTransmission, serverURL string, count int, apiKey string) {
	now := time.Now().UTC()
	for i := range count {
		eventData := types.NewPayload(map[string]any{
			"event_id": i,
		})

		event := &types.Event{
			Context:     context.Background(),
			APIHost:     serverURL,
			APIKey:      apiKey,
			Dataset:     "test-dataset",
			Environment: "test",
			SampleRate:  10,
			Timestamp:   now.Add(time.Duration(i) * time.Millisecond),
			Data:        eventData,
		}
		dt.EnqueueEvent(event)
	}
}

// getErrorEvents filters MockLogger events for error entries
func getErrorEvents(mockLogger *logger.MockLogger) []*logger.MockLoggerEvent {
	var errorEvents []*logger.MockLoggerEvent
	for _, event := range mockLogger.Events {
		if event.Fields["error"] != nil {
			errorEvents = append(errorEvents, event)
		}
	}
	return errorEvents
}

func TestDirectTransmissionErrorHandling(t *testing.T) {
	t.Run("individual event errors", func(t *testing.T) {
		// Create a test server that returns individual errors in batch format
		errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			// First event succeeds, second fails
			w.Write([]byte(`[{"status": 202}, {"status": 400}]`))
		}))
		defer errorServer.Close()

		dt, mockMetrics, mockLogger := setupDirectTransmissionTest(t)
		sendTestEvents(dt, errorServer.URL, 2, "test-api-key")

		// Wait for events to be processed
		assert.Eventually(t, func() bool {
			success, _ := mockMetrics.Get(counterResponse20x)
			errors, _ := mockMetrics.Get(counterResponseErrors)
			return success == 1 && errors == 1
		}, 100*time.Millisecond, 10*time.Millisecond)

		// Verify metrics: One success (202) and one error (400)
		success, _ := mockMetrics.Get(counterResponse20x)
		errors, _ := mockMetrics.Get(counterResponseErrors)
		assert.Equal(t, float64(1), success)
		assert.Equal(t, float64(1), errors)

		// Verify error log message
		errorEvents := getErrorEvents(mockLogger)
		require.Len(t, errorEvents, 1, "Expected exactly one error log for the failed event")

		errorEvent := errorEvents[0]
		assert.Equal(t, "error when sending event", errorEvent.Fields["error"])
		assert.Equal(t, http.StatusBadRequest, errorEvent.Fields["status_code"])
		assert.Equal(t, errorServer.URL, errorEvent.Fields["api_host"])
		assert.Equal(t, "test-dataset", errorEvent.Fields["dataset"])
		assert.Equal(t, "test", errorEvent.Fields["environment"])
		assert.Contains(t, errorEvent.Fields, "roundtrip_usec")
	})

	t.Run("http status error affects all events", func(t *testing.T) {
		// Create a test server that returns HTTP 500 error
		errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "internal server error"}`))
		}))
		defer errorServer.Close()

		dt, mockMetrics, mockLogger := setupDirectTransmissionTest(t)
		sendTestEvents(dt, errorServer.URL, 2, "test-api-key")

		// Wait for events to be processed
		assert.Eventually(t, func() bool {
			errors, _ := mockMetrics.Get(counterResponseErrors)
			return errors == 2
		}, 100*time.Millisecond, 10*time.Millisecond)

		// Both events should be errors, no successes
		success, _ := mockMetrics.Get(counterResponse20x)
		errors, _ := mockMetrics.Get(counterResponseErrors)
		assert.Equal(t, float64(0), success)
		assert.Equal(t, float64(2), errors)

		// Should have 2 error log messages
		errorEvents := getErrorEvents(mockLogger)
		require.Len(t, errorEvents, 2, "Expected error logs for both events")

		for _, errorEvent := range errorEvents {
			assert.Equal(t, "error when sending event", errorEvent.Fields["error"])
			assert.Equal(t, http.StatusInternalServerError, errorEvent.Fields["status_code"])
			assert.Contains(t, errorEvent.Fields, "response_body")
		}
	})

	t.Run("unauthorized status special handling", func(t *testing.T) {
		// Create a test server that returns HTTP 401 Unauthorized
		unauthorizedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error": "unauthorized"}`))
		}))
		defer unauthorizedServer.Close()

		dt, mockMetrics, mockLogger := setupDirectTransmissionTest(t)
		sendTestEvents(dt, unauthorizedServer.URL, 1, "invalid-key")

		// Wait for events to be processed
		assert.Eventually(t, func() bool {
			errors, _ := mockMetrics.Get(counterResponseErrors)
			return errors == 1
		}, 100*time.Millisecond, 10*time.Millisecond)

		// Should be an error
		success, _ := mockMetrics.Get(counterResponse20x)
		errors, _ := mockMetrics.Get(counterResponseErrors)
		assert.Equal(t, float64(0), success)
		assert.Equal(t, float64(1), errors)

		// Look for the special unauthorized log message
		var unauthorizedFound bool
		for _, event := range mockLogger.Events {
			if errorMsg, ok := event.Fields["error"].(string); ok && strings.Contains(errorMsg, "APIKey was rejected") {
				unauthorizedFound = true
				assert.Equal(t, "invalid-key", event.Fields["api_key"])
				break
			}
		}
		require.True(t, unauthorizedFound, "Expected special unauthorized log message")
	})

	t.Run("msgpack response handling", func(t *testing.T) {
		// Create a test server that returns msgpack responses
		msgpackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/msgpack")
			w.WriteHeader(http.StatusOK)

			// Create msgpack response for 2 events
			responses := []batchResponse{
				{Status: http.StatusAccepted},
				{Status: http.StatusBadRequest},
			}

			packed, err := msgpack.Marshal(responses)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.Write(packed)
		}))
		defer msgpackServer.Close()

		dt, mockMetrics, _ := setupDirectTransmissionTest(t)
		sendTestEvents(dt, msgpackServer.URL, 2, "test-api-key")

		// Wait for events to be processed
		assert.Eventually(t, func() bool {
			success, _ := mockMetrics.Get(counterResponse20x)
			errors, _ := mockMetrics.Get(counterResponseErrors)
			return success == 1 && errors == 1
		}, 100*time.Millisecond, 10*time.Millisecond)

		// One success (202) and one error (400)
		success, _ := mockMetrics.Get(counterResponse20x)
		errors, _ := mockMetrics.Get(counterResponseErrors)
		assert.Equal(t, float64(1), success)
		assert.Equal(t, float64(1), errors)
	})

	t.Run("insufficient responses from server", func(t *testing.T) {
		// Create a test server that returns fewer responses than events sent
		insufficientServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			// Send only 1 response for 2 events
			w.Write([]byte(`[{"status": 202}]`))
		}))
		defer insufficientServer.Close()

		dt, mockMetrics, mockLogger := setupDirectTransmissionTest(t)
		sendTestEvents(dt, insufficientServer.URL, 2, "test-api-key")

		// Wait for events to be processed
		assert.Eventually(t, func() bool {
			success, _ := mockMetrics.Get(counterResponse20x)
			errors, _ := mockMetrics.Get(counterResponseErrors)
			return success == 1 && errors == 1
		}, 100*time.Millisecond, 10*time.Millisecond)

		// One success and one error (for missing response)
		success, _ := mockMetrics.Get(counterResponse20x)
		errors, _ := mockMetrics.Get(counterResponseErrors)
		assert.Equal(t, float64(1), success)
		assert.Equal(t, float64(1), errors)

		// Verify error log message mentions insufficient responses
		errorEvents := getErrorEvents(mockLogger)
		require.Len(t, errorEvents, 1, "Expected exactly one error log for the missing response")

		errorEvent := errorEvents[0]
		assert.Equal(t, "error when sending event", errorEvent.Fields["error"])
		assert.Equal(t, http.StatusInternalServerError, errorEvent.Fields["status_code"])
		assert.Contains(t, errorEvent.Fields, "roundtrip_usec")
	})
}

func TestDirectTransmission(t *testing.T) {
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()

	// Use max batch size of 3 for testing
	testServer := newTestDirectAPIServer(t, 3)
	dt := NewDirectTransmission(mockMetrics, "test", 3, 50*time.Millisecond)
	dt.Logger = &logger.NullLogger{}
	dt.Version = "test-version"
	dt.Transport = http.DefaultTransport.(*http.Transport)

	err := dt.Start()
	require.NoError(t, err)
	defer dt.Stop()

	now := time.Now().UTC()

	// Create events for multiple datasets and scenarios
	var allEvents []*types.Event

	// Dataset A: 5 events (should create 2 batches: 3 + 2)
	for i := range 5 {
		eventData := types.NewPayload(map[string]any{
			"trace.trace_id": fmt.Sprintf("trace-a-%d", i),
			"dataset":        "A",
			"event_id":       i,
		})

		event := &types.Event{
			Context:     context.Background(),
			APIHost:     testServer.server.URL,
			APIKey:      "api-key-a",
			Dataset:     "dataset-a",
			Environment: "test",
			SampleRate:  10,
			Timestamp:   now.Add(time.Duration(i) * time.Millisecond),
			Data:        eventData,
		}
		allEvents = append(allEvents, event)
	}

	// Dataset B: 4 events (should create 2 batches: 3 + 1)
	for i := range 4 {
		eventData := types.NewPayload(map[string]any{
			"trace.trace_id": fmt.Sprintf("trace-b-%d", i),
			"dataset":        "B",
			"event_id":       i,
		})

		event := &types.Event{
			Context:     context.Background(),
			APIHost:     testServer.server.URL,
			APIKey:      "api-key-b",
			Dataset:     "dataset-b",
			Environment: "test",
			SampleRate:  20,
			Timestamp:   now.Add(time.Duration(i+100) * time.Millisecond),
			Data:        eventData,
		}
		allEvents = append(allEvents, event)
	}

	// Dataset C: 2 events (should create 1 batch via timeout)
	for i := range 2 {
		eventData := types.NewPayload(map[string]any{
			"trace.trace_id": fmt.Sprintf("trace-c-%d", i),
			"dataset":        "C",
			"event_id":       i,
		})

		event := &types.Event{
			Context:     context.Background(),
			APIHost:     testServer.server.URL,
			APIKey:      "api-key-c",
			Dataset:     "dataset-c",
			Environment: "test",
			SampleRate:  30,
			Timestamp:   now.Add(time.Duration(i+200) * time.Millisecond),
			Data:        eventData,
		}
		allEvents = append(allEvents, event)
	}

	// Send all events
	for _, event := range allEvents {
		dt.EnqueueEvent(event)
	}

	// Wait for all events to be processed (11 total)
	require.Eventually(t, func() bool {
		receivedEvents := testServer.getEvents()
		return len(receivedEvents) == 11
	}, 200*time.Millisecond, 10*time.Millisecond)

	// Group events by dataset
	receivedEvents := testServer.getEvents()
	eventsByDataset := make(map[string][]receivedEvent)
	for _, event := range receivedEvents {
		eventsByDataset[event.Dataset] = append(eventsByDataset[event.Dataset], event)
	}

	datasetA := eventsByDataset["dataset-a"]
	require.Len(t, datasetA, 5)
	for i, event := range datasetA {
		assert.Equal(t, "api-key-a", event.APIKey)
		assert.Equal(t, "dataset-a", event.Dataset)
		assert.Equal(t, int64(10), event.SampleRate)
		assert.Equal(t, "A", event.Data["dataset"])
		assert.Equal(t, fmt.Sprintf("trace-a-%d", i), event.Data["trace.trace_id"])
		assert.Equal(t, int64(i), event.Data["event_id"])
	}

	datasetB := eventsByDataset["dataset-b"]
	require.Len(t, datasetB, 4)
	for i, event := range datasetB {
		assert.Equal(t, "api-key-b", event.APIKey)
		assert.Equal(t, "dataset-b", event.Dataset)
		assert.Equal(t, int64(20), event.SampleRate)
		assert.Equal(t, "B", event.Data["dataset"])
		assert.Equal(t, fmt.Sprintf("trace-b-%d", i), event.Data["trace.trace_id"])
		assert.Equal(t, int64(i), event.Data["event_id"])
	}

	datasetC := eventsByDataset["dataset-c"]
	require.Len(t, datasetC, 2)
	for i, event := range datasetC {
		assert.Equal(t, "api-key-c", event.APIKey)
		assert.Equal(t, "dataset-c", event.Dataset)
		assert.Equal(t, int64(30), event.SampleRate)
		assert.Equal(t, "C", event.Data["dataset"])
		assert.Equal(t, fmt.Sprintf("trace-c-%d", i), event.Data["trace.trace_id"])
		assert.Equal(t, int64(i), event.Data["event_id"])
	}

	// Verify metrics for all 11 events
	success, _ := mockMetrics.Get(counterResponse20x)
	errors, _ := mockMetrics.Get(counterResponseErrors)
	enqueueErrors, _ := mockMetrics.Get(counterEnqueueErrors)
	queuedItems, _ := mockMetrics.Get(updownQueuedItems)

	assert.Equal(t, float64(11), success)
	assert.Equal(t, float64(0), errors)
	assert.Equal(t, float64(0), enqueueErrors)
	// Verify all events were queued (+11) and dequeued (-11), net = 0
	assert.Equal(t, float64(0), queuedItems)
	// Verify queue time histogram was updated for all events
	assert.Equal(t, 11, mockMetrics.GetHistogramCount(histogramQueueTime))
}

// Lightweight benchmark HTTP server
type benchmarkAPIServer struct {
	server     *httptest.Server
	eventCount atomic.Int64
	t          testing.TB
	bufferPool sync.Pool
}

func newBenchmarkAPIServer(t testing.TB) *benchmarkAPIServer {
	server := &benchmarkAPIServer{
		t: t,
		bufferPool: sync.Pool{
			New: func() interface{} {
				buf := make([]byte, 0, 1024) // Pre-allocate capacity
				return &buf
			},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/1/batch/", server.handleBatch)

	server.server = httptest.NewServer(mux)
	t.Cleanup(server.server.Close)

	return server
}

func (s *benchmarkAPIServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	// Accept both msgpack content types for compatibility
	contentType := r.Header.Get("Content-Type")
	encoding := r.Header.Get("Content-Encoding")

	// Support both msgpack content types (libhoney uses x-msgpack)
	if contentType != "application/msgpack" && contentType != "application/x-msgpack" {
		s.t.Errorf("unsupported content type: %s", contentType)
		http.Error(w, "unsupported content type", http.StatusBadRequest)
		return
	}

	if encoding != "" {
		s.t.Errorf("unsupported compression: %s", encoding)
		http.Error(w, "unsupported compression", http.StatusBadRequest)
		return
	}

	// Read the entire request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.t.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Use msgp to efficiently count array elements without full decoding
	var arraySize uint32
	arraySize, _, err = msgp.ReadArrayHeaderBytes(body)
	if err != nil {
		s.t.Error(err.Error())
		http.Error(w, fmt.Sprintf("Expected msgpack array: %v", err), http.StatusBadRequest)
		return
	}

	// Update total event count atomically
	s.eventCount.Add(int64(arraySize))

	// Return appropriate number of 202 responses
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Optimized JSON construction using pooled []byte slice to avoid allocations
	bufPtr := s.bufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0] // Reset slice to zero length but keep capacity
	defer s.bufferPool.Put(bufPtr)

	buf = append(buf, '[')
	for i := range arraySize {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, `{"status":202}`...)
	}
	buf = append(buf, ']')
	w.Write(buf)
}

func (s *benchmarkAPIServer) GetEventCount() int64 {
	return s.eventCount.Load()
}

func (s *benchmarkAPIServer) ResetEventCount() {
	s.eventCount.Store(0)
}

// createBenchmarkEvents creates a shared pool of events for benchmarking
func createBenchmarkEvents(t testing.TB, serverURL string, numEvents, datasets, apiHosts int) []*types.Event {
	events := make([]*types.Event, numEvents)

	// Pre-create static strings for commonly used values
	traceIDs := []string{"trace-1", "trace-2", "trace-3", "trace-4", "trace-5"}
	spanIDs := []string{"span-1", "span-2", "span-3", "span-4", "span-5"}
	parentIDs := []string{"parent-1", "parent-2", "parent-3", "parent-4", "parent-5"}
	serviceNames := []string{"service-auth", "service-api", "service-db", "service-cache", "service-queue"}
	operationNames := []string{"GET /users", "POST /login", "SELECT users", "cache.get", "queue.publish"}
	urls := []string{"https://example.com/users", "https://example.com/login", "https://example.com/products", "https://example.com/orders", "https://example.com/api"}
	userIDs := []string{"user-12345", "user-67890", "user-11111", "user-22222", "user-33333"}
	emails := []string{"alice@example.com", "bob@example.com", "charlie@example.com", "david@example.com", "eve@example.com"}
	versions := []string{"v1.2.0", "v1.2.1", "v1.2.2", "v1.2.3", "v1.2.4"}
	customValues := []string{"value-alpha", "value-beta", "value-gamma", "value-delta", "value-epsilon"}

	// Pre-create msgpack payloads and unmarshal them to simulate real event processing
	msgpackPayloads := make([][]byte, 5) // Create 5 different payloads to cycle through
	for j := range 5 {
		payload := map[string]any{
			"trace.trace_id":   traceIDs[j%len(traceIDs)],
			"trace.span_id":    spanIDs[j%len(spanIDs)],
			"trace.parent_id":  parentIDs[j%len(parentIDs)],
			"service.name":     serviceNames[j%len(serviceNames)],
			"name":             operationNames[j%len(operationNames)],
			"duration_ms":      float64(j*200) + 0.123,
			"http.status_code": 200 + (j % 5),
			"http.method":      []string{"GET", "POST", "PUT", "DELETE"}[j%4],
			"http.url":         urls[j%len(urls)],
			"user.id":          userIDs[j%len(userIDs)],
			"user.email":       emails[j%len(emails)],
			"error":            j%5 == 0,
			"error.message":    []string{"", "timeout", "connection refused", "internal error"}[j%4],
			"db.statement":     "SELECT * FROM users WHERE id = ?",
			"db.rows_affected": j * 20,
			"region":           []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}[j%4],
			"environment":      []string{"production", "staging", "development"}[j%3],
			"version":          versions[j%len(versions)],
			"custom.field1":    customValues[j%len(customValues)],
			"custom.field2":    float64(j) * 3.14159,
		}

		packed, err := msgpack.Marshal(payload)
		if err != nil {
			t.Fatalf("failed to marshal payload: %v", err)
		}
		msgpackPayloads[j] = packed
	}

	for i := range numEvents {
		datasetNum := i % datasets
		apiHostNum := i % apiHosts

		events[i] = &types.Event{
			Context:           context.Background(),
			APIHost:           serverURL,
			APIKey:            fmt.Sprintf("api-key-%d", apiHostNum),
			Dataset:           fmt.Sprintf("dataset-%d", datasetNum),
			Environment:       "benchmark",
			SampleRate:        uint(1 + (i % 100)),
			Timestamp:         time.Now().Add(time.Duration(i) * time.Microsecond),
			EnqueuedUnixMicro: time.Now().UnixMicro(),
		}
		err := events[i].Data.UnmarshalMsgpack(msgpackPayloads[i%len(msgpackPayloads)])
		if err != nil {
			t.Fatalf("failed to unmarshal payload: %v", err)
		}
	}

	return events
}

func BenchmarkTransmissionComparison(b *testing.B) {
	server := newBenchmarkAPIServer(b)

	// Benchmark scenarios
	scenarios := []struct {
		name     string
		datasets int
		apiHosts int
	}{
		{"high_card", 1000, 100},
		{"low_card", 1, 1},
	}

	// Pre-create shared event pools for each scenario
	eventPools := make(map[string][]*types.Event)
	const poolSize = 100000

	for _, scenario := range scenarios {
		eventPools[scenario.name] = createBenchmarkEvents(b, server.server.URL, poolSize, scenario.datasets, scenario.apiHosts)
	}

	// This benchmark can create too many simultaneous socket connections,
	// causing them to fail. To avoid this, limit the number of concurrent
	// connections. This value seems to give optimal performance.
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.MaxConnsPerHost = 25

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			// Get the pre-created event pool for this scenario
			eventPool := eventPools[scenario.name]

			b.Run("direct", func(b *testing.B) {
				mockMetrics := &metrics.MockMetrics{}
				mockMetrics.Start()

				dt := NewDirectTransmission(
					mockMetrics,
					"benchmark",
					libhoney.DefaultMaxBatchSize,
					libhoney.DefaultBatchTimeout,
				)
				dt.Logger = &logger.NullLogger{}
				dt.Version = "benchmark"
				dt.Transport = httpTransport
				dt.RegisterMetrics()

				err := dt.Start()
				if err != nil {
					b.Fatal(err)
				}

				// Reset server event count before test
				server.ResetEventCount()

				b.ResetTimer()
				for n := range b.N {
					// Replay events from the shared pool
					dt.EnqueueEvent(eventPool[n%poolSize])
				}
				err = dt.Stop()
				if err != nil {
					b.Fatal(err.Error())
				}

				// Wait for all events to be processed
				expectedCount := int64(b.N)
				if !assert.Eventually(b, func() bool {
					return server.GetEventCount() == expectedCount
				}, 2*time.Second, 10*time.Millisecond) {
					receivedCount := server.GetEventCount()
					b.Errorf("Expected %d events, but server received %d", expectedCount, receivedCount)
				}
				b.StopTimer()
			})

			b.Run("default", func(b *testing.B) {
				mockMetrics := &metrics.MockMetrics{}
				mockMetrics.Start()

				// Create a real HTTP transmission that sends to the test server
				libhoneyTransmission := &transmission.Honeycomb{
					MaxBatchSize:          libhoney.DefaultMaxBatchSize,
					BatchTimeout:          libhoney.DefaultBatchTimeout,
					MaxConcurrentBatches:  libhoney.DefaultMaxConcurrentBatches,
					Transport:             httpTransport,
					PendingWorkCapacity:   10000,
					BlockOnSend:           true,
					EnableMsgpackEncoding: true,
					DisableCompression:    true,
				}

				client, err := libhoney.NewClient(libhoney.ClientConfig{
					Transmission: libhoneyTransmission,
				})
				if err != nil {
					b.Fatal(err)
				}

				// Create a simple mock config
				mockConfig := &config.MockConfig{
					GetHoneycombAPIVal: server.server.URL,
				}

				dt := NewDefaultTransmission(client, mockMetrics, "benchmark")
				dt.Logger = &logger.NullLogger{}
				dt.Version = "benchmark"
				dt.Config = mockConfig
				dt.RegisterMetrics()

				err = dt.Start()
				if err != nil {
					b.Fatal(err)
				}

				// Reset server event count before test
				server.ResetEventCount()

				b.ResetTimer()
				for n := range b.N {
					// Replay events from the shared pool
					dt.EnqueueEvent(eventPool[n%poolSize])
				}
				err = dt.Stop()
				if err != nil {
					b.Fatal(err.Error())
				}
				err = libhoneyTransmission.Flush()
				if err != nil {
					b.Fatal(err.Error())
				}
				client.Close()

				// Wait for all events to be processed
				expectedCount := int64(b.N)
				if !assert.Eventually(b, func() bool {
					return server.GetEventCount() == expectedCount
				}, 2*time.Second, 10*time.Millisecond) {
					receivedCount := server.GetEventCount()
					b.Errorf("Expected %d events, but server received %d", expectedCount, receivedCount)
				}
				b.StopTimer()
			})
		})
	}
}
