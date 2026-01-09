package transmit

import (
	"bytes"
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

	"github.com/facebookgo/inject"
	"github.com/jonboulle/clockwork"
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

func readHTTPBody(t testing.TB, r *http.Request) []byte {
	t.Helper()

	body, err := io.ReadAll(r.Body)
	assert.NoError(t, err)
	assert.Len(t, body, int(r.ContentLength))

	if r.Header.Get("Content-Encoding") == "zstd" {
		zr, err := zstd.NewReader(bytes.NewReader(body))
		assert.NoError(t, err)
		body, err = io.ReadAll(zr)
		assert.NoError(t, err)
	}
	return body
}

func (t *testDirectAPIServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	t.t.Helper()
	defer r.Body.Close()

	body := readHTTPBody(t.t, r)
	assert.Less(t.t, len(body), apiMaxBatchSize)

	var events []receivedEvent
	contentType := r.Header.Get("Content-Type")
	assert.Equal(t.t, "application/msgpack", contentType)
	decoder := msgpack.NewDecoder(bytes.NewReader(body))
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
	return setupDirectTransmissionTestWithBatchSize(t, 10, 20*time.Millisecond)
}

// setupDirectTransmissionTestWithBatchSize creates a configured DirectTransmission with specific batch size
func setupDirectTransmissionTestWithBatchSize(t *testing.T, batchSize int, batchTimeout time.Duration) (*DirectTransmission, *metrics.MockMetrics, *logger.MockLogger) {
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()

	mockLogger := &logger.MockLogger{}

	dt := NewDirectTransmission(types.TransmitTypeUpstream, http.DefaultTransport.(*http.Transport), 10, 100*time.Millisecond, 10*time.Second, true, nil)
	dt.Logger = mockLogger
	dt.Version = "test-version"
	dt.Metrics = mockMetrics

	err := dt.Start()
	require.NoError(t, err)
	t.Cleanup(func() { dt.Stop() })

	return dt, mockMetrics, mockLogger
}

// sendTestEvents sends n events to the DirectTransmission with the given server URL
func sendTestEvents(dt *DirectTransmission, serverURL string, count int, apiKey string) {
	now := time.Now().UTC()
	mockCfg := &config.MockConfig{}
	for i := range count {
		eventData := types.NewPayload(mockCfg, map[string]any{
			"event_id": i,
		})
		eventData.ExtractMetadata()

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

func TestDirectTransmitDependencyInjection(t *testing.T) {
	var g inject.Graph
	err := g.Provide(
		&inject.Object{Value: &DirectTransmission{
			Transport: http.DefaultTransport.(*http.Transport),
		}},

		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: "test", Name: "version"},
		&inject.Object{Value: &metrics.MockMetrics{}, Name: "metrics"},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

func TestDirectTransmissionErrorHandling(t *testing.T) {
	t.Run("individual event errors", func(t *testing.T) {
		// Create a test server that returns individual errors in batch format
		errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Read the request to determine how many events were sent
			body := readHTTPBody(t, r)
			// DirectTransmission only sends msgpack
			eventCount, _, _ := msgp.ReadArrayHeaderBytes(body)

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

			// Build response based on actual event count
			// Half succeed, half fail
			responses := make([]map[string]int, eventCount)
			for i := range eventCount {
				if i%2 == 0 {
					responses[i] = map[string]int{"status": 202}
				} else {
					responses[i] = map[string]int{"status": 400}
				}
			}

			respBytes, _ := json.Marshal(responses)
			w.Write(respBytes)
		}))
		defer errorServer.Close()

		dt, mockMetrics, mockLogger := setupDirectTransmissionTest(t)
		// Send 4 events to ensure we get 2 successes and 2 errors
		sendTestEvents(dt, errorServer.URL, 4, "test-api-key")
		err := dt.Stop()
		require.NoError(t, err)

		// Verify metrics: 2 successes and 2 errors
		success, _ := mockMetrics.Get(dt.metricKeys.counterResponse20x)
		errors, _ := mockMetrics.Get(dt.metricKeys.counterResponseErrors)
		batchesSent, _ := mockMetrics.Get(dt.metricKeys.counterBatchesSent)
		messagesSent, _ := mockMetrics.Get(dt.metricKeys.counterMessagesSent)

		assert.Equal(t, float64(2), success)
		assert.Equal(t, float64(2), errors)
		assert.Equal(t, float64(1), batchesSent)  // Single batch containing 4 events
		assert.Equal(t, float64(4), messagesSent) // All 4 events were sent

		// Verify we got error log messages
		errorEvents := getErrorEvents(mockLogger)
		assert.GreaterOrEqual(t, len(errorEvents), 2, "Expected at least 2 error logs")

		// Check that error events have the expected fields
		for _, errorEvent := range errorEvents {
			assert.Equal(t, "error when sending event", errorEvent.Fields["error"])
			assert.Equal(t, http.StatusBadRequest, errorEvent.Fields["status_code"])
			assert.Equal(t, errorServer.URL, errorEvent.Fields["api_host"])
			assert.Equal(t, "test-dataset", errorEvent.Fields["dataset"])
			assert.Equal(t, "test", errorEvent.Fields["environment"])
			assert.Contains(t, errorEvent.Fields, "roundtrip_usec")
		}
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
		err := dt.Stop()
		require.NoError(t, err)

		// Both events should be errors, no successes
		success, _ := mockMetrics.Get(dt.metricKeys.counterResponse20x)
		errors, _ := mockMetrics.Get(dt.metricKeys.counterResponseErrors)
		batchesSent, _ := mockMetrics.Get(dt.metricKeys.counterBatchesSent)
		messagesSent, _ := mockMetrics.Get(dt.metricKeys.counterMessagesSent)

		assert.Equal(t, float64(0), success)
		assert.Equal(t, float64(2), errors)
		assert.Equal(t, float64(1), batchesSent) // Batch was sent despite error
		assert.Equal(t, float64(2), messagesSent)

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
		err := dt.Stop()
		require.NoError(t, err)

		// Should be an error
		success, _ := mockMetrics.Get(dt.metricKeys.counterResponse20x)
		errors, _ := mockMetrics.Get(dt.metricKeys.counterResponseErrors)
		batchesSent, _ := mockMetrics.Get(dt.metricKeys.counterBatchesSent)
		messagesSent, _ := mockMetrics.Get(dt.metricKeys.counterMessagesSent)

		assert.Equal(t, float64(0), success)
		assert.Equal(t, float64(1), errors)
		assert.Equal(t, float64(1), batchesSent) // Batch was sent despite error
		assert.Equal(t, float64(1), messagesSent)

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
		err := dt.Stop()
		require.NoError(t, err)

		// One success (202) and one error (400)
		success, _ := mockMetrics.Get(dt.metricKeys.counterResponse20x)
		errors, _ := mockMetrics.Get(dt.metricKeys.counterResponseErrors)
		batchesSent, _ := mockMetrics.Get(dt.metricKeys.counterBatchesSent)
		messagesSent, _ := mockMetrics.Get(dt.metricKeys.counterMessagesSent)

		assert.Equal(t, float64(1), success)
		assert.Equal(t, float64(1), errors)
		assert.Equal(t, float64(1), batchesSent) // Single batch containing 2 events
		assert.Equal(t, float64(2), messagesSent)
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
		err := dt.Stop()
		require.NoError(t, err)

		// One success and one error (for missing response)
		success, _ := mockMetrics.Get(dt.metricKeys.counterResponse20x)
		errors, _ := mockMetrics.Get(dt.metricKeys.counterResponseErrors)
		batchesSent, _ := mockMetrics.Get(dt.metricKeys.counterBatchesSent)
		messagesSent, _ := mockMetrics.Get(dt.metricKeys.counterMessagesSent)

		assert.Equal(t, float64(1), success)
		assert.Equal(t, float64(1), errors)
		assert.Equal(t, float64(1), batchesSent) // Single batch containing 2 events
		assert.Equal(t, float64(2), messagesSent)

		// Verify error log message mentions insufficient responses
		errorEvents := getErrorEvents(mockLogger)
		require.Len(t, errorEvents, 1, "Expected exactly one error log for the missing response")

		errorEvent := errorEvents[0]
		assert.Equal(t, "error when sending event", errorEvent.Fields["error"])
		assert.Equal(t, http.StatusInternalServerError, errorEvent.Fields["status_code"])
		assert.Contains(t, errorEvent.Fields, "roundtrip_usec")
	})

	t.Run("response decode errors", func(t *testing.T) {
		// Create a test server that returns invalid msgpack/JSON
		decodeErrorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/msgpack")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("invalid msgpack data"))
		}))
		defer decodeErrorServer.Close()

		dt, mockMetrics, _ := setupDirectTransmissionTest(t)
		sendTestEvents(dt, decodeErrorServer.URL, 1, "test-api-key")
		err := dt.Stop()
		require.NoError(t, err)

		// Should have decode error
		decodeErrors, _ := mockMetrics.Get(dt.metricKeys.counterResponseDecodeErrors)
		batchesSent, _ := mockMetrics.Get(dt.metricKeys.counterBatchesSent)
		messagesSent, _ := mockMetrics.Get(dt.metricKeys.counterMessagesSent)

		assert.Equal(t, float64(1), decodeErrors)
		assert.Equal(t, float64(1), batchesSent)
		assert.Equal(t, float64(1), messagesSent)
	})

	t.Run("event over 1M size", func(t *testing.T) {
		// Create a test server that accepts requests
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[{"status": 202}]`))
		}))
		defer server.Close()

		dt, mockMetrics, mockLogger := setupDirectTransmissionTest(t)

		// Create an event with data over 1M
		eventData := types.NewPayload(&config.MockConfig{}, map[string]any{
			"large_field": strings.Repeat("a", 1024*1024+1000),
			"event_id":    1,
		})
		eventData.ExtractMetadata()

		event := &types.Event{
			Context:     context.Background(),
			APIHost:     server.URL,
			APIKey:      "test-api-key",
			Dataset:     "test-dataset",
			Environment: "test",
			SampleRate:  10,
			Timestamp:   time.Now().UTC(),
			Data:        eventData,
		}
		dt.EnqueueEvent(event)
		err := dt.Stop()
		require.NoError(t, err)

		// Verify event was dropped due to size and metrics were updated
		success, _ := mockMetrics.Get(dt.metricKeys.counterResponse20x)
		errors, _ := mockMetrics.Get(dt.metricKeys.counterResponseErrors)
		assert.Equal(t, float64(0), success)
		assert.Equal(t, float64(1), errors)

		// Verify error log message about oversized event
		var oversizedFound bool
		for _, event := range mockLogger.Events {
			if errorMsg, ok := event.Fields["err"].(string); ok && strings.Contains(errorMsg, "exceeds max event size") {
				oversizedFound = true
				break
			}
		}
		require.True(t, oversizedFound, "Expected error log for oversized event")
	})
}

func TestDirectTransmission(t *testing.T) {
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()
	mockLogger := &logger.MockLogger{}

	// Use max batch size of 3 for testing
	testServer := newTestDirectAPIServer(t, 3)
	dt := NewDirectTransmission(types.TransmitTypeUpstream, http.DefaultTransport.(*http.Transport), 3, 50*time.Millisecond, 10*time.Second, true, nil)
	dt.Logger = mockLogger
	dt.Version = "test-version"
	dt.Metrics = mockMetrics

	clock := clockwork.NewFakeClock()
	dt.Clock = clock

	err := dt.Start()
	require.NoError(t, err)
	defer dt.Stop()
	assert.Equal(t, "libhoney_upstream_queued_items", dt.metricKeys.updownQueuedItems)
	assert.Equal(t, "libhoney_upstream_response_20x", dt.metricKeys.counterResponse20x)
	assert.Equal(t, "libhoney_upstream_response_errors", dt.metricKeys.counterResponseErrors)
	assert.Equal(t, "libhoney_upstream_enqueue_errors", dt.metricKeys.counterEnqueueErrors)
	assert.Equal(t, "libhoney_upstream_queue_time", dt.metricKeys.histogramQueueTime)

	assert.Equal(t, "libhoney_upstream_send_errors", dt.metricKeys.counterSendErrors)
	assert.Equal(t, "libhoney_upstream_send_retries", dt.metricKeys.counterSendRetries)
	assert.Equal(t, "libhoney_upstream_batches_sent", dt.metricKeys.counterBatchesSent)
	assert.Equal(t, "libhoney_upstream_messages_sent", dt.metricKeys.counterMessagesSent)
	assert.Equal(t, "libhoney_upstream_queue_length", dt.metricKeys.gaugeQueueLength)
	assert.Equal(t, "libhoney_upstream_response_decode_errors", dt.metricKeys.counterResponseDecodeErrors)

	assert.Equal(t, "libhoney_upstream_stale_dispatch_time", dt.metricKeys.staleDispatchTime)

	now := time.Now().UTC()
	cfg := &config.MockConfig{
		TraceIdFieldNames: []string{"trace.trace_id"},
	}

	// Create events for multiple datasets and scenarios
	var allEvents []*types.Event

	// Dataset A: 5 events (should create 2 batches: 3 + 2)
	for i := range 5 {
		eventData := types.NewPayload(cfg, map[string]any{
			"trace.trace_id": fmt.Sprintf("trace-a-%d", i),
			"dataset":        "A",
			"event_id":       i,
		})
		eventData.ExtractMetadata()

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

		// Set metadata fields, these should all be serialized as well
		event.Data.MetaSignalType = "trace"
		event.Data.MetaTraceID = fmt.Sprintf("trace-a-%d", i)
		event.Data.MetaAnnotationType = "span_event"
		event.Data.MetaRefineryProbe.Set(true)
		event.Data.MetaRefineryRoot.Set(true)
		event.Data.MetaRefineryIncomingUserAgent = "refinery"

		allEvents = append(allEvents, event)
	}

	// Dataset B: 7 events, one too large (should create 3 batches: 2 + 3 + 1)
	for i := range 7 {
		eventData := types.NewPayload(cfg, map[string]any{
			"trace.trace_id": fmt.Sprintf("trace-b-%d", i),
			"dataset":        "B",
			"event_id":       i,
		})
		eventData.ExtractMetadata()

		if i == 0 {
			eventData.Set("huge", strings.Repeat("a", 1024*1024))
		}

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

	// Dataset C: 2 events (1 batch via timeout, 1 via stop)
	mockCfg := &config.MockConfig{
		TraceIdFieldNames: []string{"trace.trace_id"},
	}
	for i := range 2 {
		eventData := types.NewPayload(mockCfg, map[string]any{
			"trace.trace_id": fmt.Sprintf("trace-c-%d", i),
			"dataset":        "C",
			"event_id":       i,
		})
		eventData.ExtractMetadata()

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

	// Send all but one of our events.
	for _, event := range allEvents[:len(allEvents)-1] {
		dt.EnqueueEvent(event)
	}

	// At this point, time hasn't advanced so we should't get any incomplete batches.
	require.Eventually(t, func() bool {
		// 4 events waiting for timeout
		// 1 event yet to be enqueued
		// 1 event is too large to send
		return len(testServer.getEvents()) == len(allEvents)-6
	}, 100*time.Millisecond, time.Millisecond)

	// Now everything should dispatch.
	clock.Advance(50 * time.Millisecond)

	// Now the ticker should fire and we should send old events.
	require.Eventually(t, func() bool {
		// 1 event yet to be enqueued
		// 1 event is too large to send
		return len(testServer.getEvents()) == len(allEvents)-2
	}, 100*time.Millisecond, time.Millisecond)

	// Send the last event - should get dispatched during Stop()
	dt.EnqueueEvent(allEvents[len(allEvents)-1])
	err = dt.Stop()
	require.NoError(t, err)

	// Group events by dataset
	// 1 event is too large to send
	expectedEvents := len(allEvents) - 1
	receivedEvents := testServer.getEvents()
	require.Len(t, receivedEvents, expectedEvents)
	eventsByDataset := make(map[string][]receivedEvent)
	for _, event := range receivedEvents {
		eventsByDataset[event.Dataset] = append(eventsByDataset[event.Dataset], event)
	}

	datasetA := eventsByDataset["dataset-a"]
	require.Len(t, datasetA, 5)
	// Check each event has the correct fields, regardless of order
	for _, event := range datasetA {
		assert.Equal(t, "api-key-a", event.APIKey)
		assert.Equal(t, "dataset-a", event.Dataset)
		assert.Equal(t, int64(10), event.SampleRate)
		assert.Equal(t, "A", event.Data["dataset"])

		// Extract event_id and verify trace ID matches
		eventID := int(event.Data["event_id"].(int64))
		assert.GreaterOrEqual(t, eventID, 0)
		assert.Less(t, eventID, 5)
		expectedTraceID := fmt.Sprintf("trace-a-%d", eventID)
		assert.Equal(t, expectedTraceID, event.Data["trace.trace_id"])

		assert.Equal(t, "trace", event.Data[types.MetaSignalType])
		assert.Equal(t, "span_event", event.Data[types.MetaAnnotationType])
		assert.Equal(t, true, event.Data[types.MetaRefineryProbe])
		assert.Equal(t, true, event.Data[types.MetaRefineryRoot])
		assert.Equal(t, "refinery", event.Data[types.MetaRefineryIncomingUserAgent])
	}

	datasetB := eventsByDataset["dataset-b"]
	require.Len(t, datasetB, 6)
	// Dataset B had the first event (index 0) skipped due to size
	seenEventIDs := make(map[int]bool)
	for _, event := range datasetB {
		assert.Equal(t, "api-key-b", event.APIKey)
		assert.Equal(t, "dataset-b", event.Dataset)
		assert.Equal(t, int64(20), event.SampleRate)
		assert.Equal(t, "B", event.Data["dataset"])

		// Extract event_id and verify trace ID matches
		eventID := int(event.Data["event_id"].(int64))
		assert.Equal(t, fmt.Sprintf("trace-b-%d", eventID), event.Data["trace.trace_id"])
		assert.GreaterOrEqual(t, eventID, 1) // First event (0) was skipped
		assert.Less(t, eventID, 7)
		seenEventIDs[eventID] = true
	}
	// Verify we got events 1-6 (event 0 was too large)
	for i := 1; i <= 6; i++ {
		assert.True(t, seenEventIDs[i], "Missing event_id %d", i)
	}

	datasetC := eventsByDataset["dataset-c"]
	require.Len(t, datasetC, 2)
	for _, event := range datasetC {
		assert.Equal(t, "api-key-c", event.APIKey)
		assert.Equal(t, "dataset-c", event.Dataset)
		assert.Equal(t, int64(30), event.SampleRate)
		assert.Equal(t, "C", event.Data["dataset"])

		// Extract event_id and verify trace ID matches
		eventID := int(event.Data["event_id"].(int64))
		assert.Equal(t, fmt.Sprintf("trace-c-%d", eventID), event.Data["trace.trace_id"])
		assert.GreaterOrEqual(t, eventID, 0)
		assert.Less(t, eventID, 2)
	}

	// Verify metrics for all events
	success, _ := mockMetrics.Get(dt.metricKeys.counterResponse20x)
	errors, _ := mockMetrics.Get(dt.metricKeys.counterResponseErrors)
	enqueueErrors, _ := mockMetrics.Get(dt.metricKeys.counterEnqueueErrors)
	queuedItems, _ := mockMetrics.Get(dt.metricKeys.updownQueuedItems)
	batchesSent, _ := mockMetrics.Get(dt.metricKeys.counterBatchesSent)
	messagesSent, _ := mockMetrics.Get(dt.metricKeys.counterMessagesSent)
	queueLength, _ := mockMetrics.Get(dt.metricKeys.gaugeQueueLength)

	assert.Equal(t, float64(expectedEvents), success)
	assert.Equal(t, float64(len(allEvents)-expectedEvents), errors)
	assert.Equal(t, float64(0), enqueueErrors)
	// Verify all events were queued and dequeued, net = 0
	assert.Equal(t, float64(0), queuedItems)
	// Verify queue time histogram was updated for all events
	assert.Equal(t, expectedEvents, mockMetrics.GetHistogramCount(dt.metricKeys.histogramQueueTime))

	// Verify batch and message counts
	// Dataset A: 5 events -> 2 batches (3+2)
	// Dataset B: 6 events -> 2 batches (3+3) (1 event skipped due to size)
	// Dataset C: 2 events -> may create multiple batches depending on timing
	// The exact number of batches depends on timing, but should be at least 4
	assert.GreaterOrEqual(t, batchesSent, float64(4), "Should have sent at least 4 batches")
	assert.Equal(t, float64(expectedEvents), messagesSent)

	// Queue length may not be exactly 0 due to gauge update timing
	assert.LessOrEqual(t, queueLength, float64(expectedEvents), "Queue length should be reasonable")

	assert.Equal(t, 2, mockMetrics.GetHistogramCount(dt.metricKeys.staleDispatchTime))
}

func TestDirectTransmissionBatchSizeLimit(t *testing.T) {
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()
	mockLogger := &logger.MockLogger{}

	testServer := newTestDirectAPIServer(t, 50)
	dt := NewDirectTransmission(types.TransmitTypeUpstream, http.DefaultTransport.(*http.Transport), 50, 50*time.Millisecond, 10*time.Second, true, nil)
	dt.Logger = mockLogger
	dt.Version = "test-version"
	dt.Metrics = mockMetrics

	err := dt.Start()
	require.NoError(t, err)
	defer dt.Stop()

	now := time.Now().UTC()

	var allEvents []*types.Event

	bigString := strings.Repeat("a", 700_000)
	mockCfg := &config.MockConfig{
		TraceIdFieldNames: []string{"trace.trace_id"},
	}
	for i := range 50 {
		eventData := types.NewPayload(mockCfg, map[string]any{
			"trace.trace_id": fmt.Sprintf("trace-a-%d", i),
			"dataset":        "A",
			"event_id":       i,
			"big":            bigString,
		})
		eventData.ExtractMetadata()

		// Some events are too big to send at all, that shouldn't foul up the logic here.
		if i == 0 || i == 9 || i == 10 || i == 49 {
			eventData.Set("also_big", bigString)
		}

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

	// Send all events
	for _, event := range allEvents {
		dt.EnqueueEvent(event)
	}
	err = dt.Stop()
	require.NoError(t, err)

	// We expect all events minus any that are too large to send
	expectedEvents := len(allEvents) - 4
	gotEvents := testServer.getEvents()
	assert.Len(t, gotEvents, expectedEvents)

	success, _ := mockMetrics.Get(dt.metricKeys.counterResponse20x)
	errors, _ := mockMetrics.Get(dt.metricKeys.counterResponseErrors)
	queuedItems, _ := mockMetrics.Get(dt.metricKeys.updownQueuedItems)
	batchesSent, _ := mockMetrics.Get(dt.metricKeys.counterBatchesSent)
	messagesSent, _ := mockMetrics.Get(dt.metricKeys.counterMessagesSent)

	assert.Equal(t, float64(expectedEvents), success)
	assert.Equal(t, float64(len(allEvents)-expectedEvents), errors)
	assert.Equal(t, float64(0), queuedItems)
	assert.Equal(t, expectedEvents, mockMetrics.GetHistogramCount(dt.metricKeys.histogramQueueTime))

	// Verify batch and message counts - events are large so batches will be smaller
	assert.Greater(t, batchesSent, float64(0), "Should have sent at least one batch")
	assert.Equal(t, float64(expectedEvents), messagesSent, "Should have sent all successful events")
}

func TestDirectTransmissionBatchTiming(t *testing.T) {
	testServer := newTestDirectAPIServer(t, 100)
	metrics := &metrics.MockMetrics{}
	metrics.Start()

	// Create a fake clock
	fakeClock := clockwork.NewFakeClock()

	// Use a 400ms batch timeout for testing
	dt := NewDirectTransmission(types.TransmitTypeUpstream, http.DefaultTransport.(*http.Transport), 100, 400*time.Millisecond, 10*time.Second, true, nil)
	dt.Config = &config.MockConfig{}
	dt.Logger = &logger.NullLogger{}
	dt.Version = "test-version"
	dt.Clock = fakeClock // Use the fake clock
	dt.Metrics = metrics

	err := dt.Start()
	require.NoError(t, err)
	mockCfg := &config.MockConfig{}

	// Send first event at time 0
	event1 := &types.Event{
		Context:    context.Background(),
		APIHost:    testServer.server.URL,
		APIKey:     "test-key",
		Dataset:    "test-dataset",
		SampleRate: 1,
		Timestamp:  fakeClock.Now(),
		Data:       types.NewPayload(mockCfg, map[string]any{"event": "first", "time": 0}),
	}
	dt.EnqueueEvent(event1)

	fakeClock.BlockUntilContext(t.Context(), 1)
	// Advance time by 100ms (1/4 of batch timeout) - ticker should fire but batch should not be sent
	fakeClock.Advance(100 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(testServer.getEvents()) == 0
	}, 100*time.Millisecond, time.Millisecond, "Batch should not be sent yet (only 100ms old)")

	// Send second event at 100ms
	event2 := &types.Event{
		Context:    context.Background(),
		APIHost:    testServer.server.URL,
		APIKey:     "test-key",
		Dataset:    "test-dataset",
		SampleRate: 1,
		Timestamp:  fakeClock.Now(),
		Data:       types.NewPayload(mockCfg, map[string]any{"event": "second", "time": 100}),
	}
	dt.EnqueueEvent(event2)

	// Advance time by another 100ms (200ms total) - ticker fires again, batch still not old enough
	fakeClock.BlockUntilContext(t.Context(), 1)
	fakeClock.Advance(100 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(testServer.getEvents()) == 0
	}, 100*time.Millisecond, time.Millisecond, "Batch should not be sent yet (only 200ms old)")

	// Advance time by another 100ms (300ms total) - ticker fires, still not old enough
	fakeClock.BlockUntilContext(t.Context(), 1)
	fakeClock.Advance(100 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(testServer.getEvents()) == 0
	}, 100*time.Millisecond, time.Millisecond, "Batch should not be sent yet (only 300ms old)")

	// Advance time by another 100ms (400ms total) - ticker fires, batch is now old enough
	fakeClock.BlockUntilContext(t.Context(), 1)
	fakeClock.Advance(100 * time.Millisecond)

	// Wait for batch to be sent
	assert.Eventually(t, func() bool {
		return len(testServer.getEvents()) == 2
	}, 100*time.Millisecond, time.Millisecond, "Batch should be sent after 400ms")

	receivedEvents := testServer.getEvents()
	assert.Len(t, receivedEvents, 2, "Both events should be sent together")

	// Verify both events were received
	eventData := make([]string, 0)
	for _, e := range receivedEvents {
		eventData = append(eventData, e.Data["event"].(string))
	}
	assert.Contains(t, eventData, "first")
	assert.Contains(t, eventData, "second")

	// Send a third event - this should start a new batch
	event3 := &types.Event{
		Context:    context.Background(),
		APIHost:    testServer.server.URL,
		APIKey:     "test-key",
		Dataset:    "test-dataset",
		SampleRate: 1,
		Timestamp:  fakeClock.Now(),
		Data:       types.NewPayload(mockCfg, map[string]any{"event": "third", "time": 400}),
	}
	dt.EnqueueEvent(event3)

	// Advance time by 300ms - not enough for the new batch to be sent
	fakeClock.BlockUntilContext(t.Context(), 1)
	fakeClock.Advance(300 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(testServer.getEvents()) == 2
	}, 100*time.Millisecond, time.Millisecond, "Third event should not be sent yet")

	// Advance time by another 100ms (400ms since third event) - now it should be sent
	fakeClock.BlockUntilContext(t.Context(), 1)
	fakeClock.Advance(100 * time.Millisecond)

	assert.Eventually(t, func() bool {
		return len(testServer.getEvents()) == 3
	}, 100*time.Millisecond, time.Millisecond, "Third event should be sent after its batch timeout")

	err = dt.Stop()
	require.NoError(t, err)

	// Verify metrics after all batches are sent
	batchesSent, _ := metrics.Get(dt.metricKeys.counterBatchesSent)
	messagesSent, _ := metrics.Get(dt.metricKeys.counterMessagesSent)

	assert.Equal(t, float64(2), batchesSent)
	assert.Equal(t, float64(3), messagesSent)
}

func TestDirectTransmissionQueueLengthGauge(t *testing.T) {
	testServer := newTestDirectAPIServer(t, 10)
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()

	batchTimeout := 200 * time.Millisecond
	dt := NewDirectTransmission(
		types.TransmitTypeUpstream,
		http.DefaultTransport.(*http.Transport),
		10,
		batchTimeout,
		10*time.Second,
		true,
		nil,
	)
	dt.Config = &config.MockConfig{}
	dt.Logger = &logger.NullLogger{}
	dt.Version = "test-version"
	dt.Metrics = mockMetrics

	err := dt.Start()
	require.NoError(t, err)

	// Send a few events that should queue up
	mockCfg := &config.MockConfig{}
	for i := 0; i < 3; i++ {
		event := &types.Event{
			Context:    context.Background(),
			APIHost:    testServer.server.URL,
			APIKey:     "test-key",
			Dataset:    "test-dataset",
			SampleRate: 1,
			Timestamp:  time.Now(),
			Data:       types.NewPayload(mockCfg, map[string]any{"event_id": i}),
		}
		dt.EnqueueEvent(event)
	}

	// Verify queue length gauge is updated within the first 200ms
	assert.Eventually(t, func() bool {
		v, _ := mockMetrics.Get(dt.metricKeys.gaugeQueueLength)
		return v == 3
	}, 200*time.Millisecond, 10*time.Millisecond)

	// Once the batch timeout has elapsed, the queue length should be back to 0
	assert.Eventually(t, func() bool {
		v, _ := mockMetrics.Get(dt.metricKeys.gaugeQueueLength)
		return v == 0
	}, batchTimeout*2, batchTimeout/10)

	err = dt.Stop()
	require.NoError(t, err)
}

func TestDirectTransmissionRetryLogic(t *testing.T) {
	tests := []struct {
		name           string
		statusCode     int
		retryAfter     string
		expectRetries  int
		expectSuccess  bool
		enableCompress bool
	}{
		{
			name:          "429_with_retry_after_1s",
			statusCode:    http.StatusTooManyRequests,
			retryAfter:    "1",
			expectRetries: 1,
			expectSuccess: true,
		},
		{
			name:          "503_with_retry_after_2s",
			statusCode:    http.StatusServiceUnavailable,
			retryAfter:    "2",
			expectRetries: 1,
			expectSuccess: true,
		},
		{
			name:          "429_with_retry_after_too_long",
			statusCode:    http.StatusTooManyRequests,
			retryAfter:    "61", // Over 60s limit
			expectRetries: 0,
			expectSuccess: false,
		},
		{
			name:          "success_after_retry",
			statusCode:    http.StatusTooManyRequests,
			retryAfter:    "1",
			expectRetries: 1,
			expectSuccess: true,
		},
		{
			name:           "timeout_retries_with_compression",
			statusCode:     0, // Will simulate timeout by hanging
			expectRetries:  1,
			expectSuccess:  false,
			enableCompress: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Track request attempts
			var requestCount atomic.Int32
			var requestBodies [][]byte
			var requestMutex sync.Mutex

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count := requestCount.Add(1)

				// Read and validate the request body to ensure reader works correctly
				require.NotZero(t, r.ContentLength, "Request should have valid Content-Length")
				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, r.ContentLength, int64(len(body)))

				// Store body for later verification that retries use same data
				requestMutex.Lock()
				requestBodies = append(requestBodies, body)
				requestMutex.Unlock()

				// Verify content type and headers
				assert.Equal(t, "application/msgpack", r.Header.Get("Content-Type"))
				if tt.enableCompress {
					assert.Equal(t, "zstd", r.Header.Get("Content-Encoding"))
				}

				if tt.statusCode == 0 {
					// Simulate timeout by hanging the request
					time.Sleep(150 * time.Millisecond) // Longer than client timeout
					return
				}

				// First request gets error status, second succeeds (if expected)
				if count == 1 && tt.statusCode != 0 {
					w.Header().Set("Retry-After", tt.retryAfter)
					w.WriteHeader(tt.statusCode)
					w.Write([]byte(`{"error": "rate limited"}`))
					return
				}

				// Success response
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`[{"status": 202}]`))
			}))
			defer server.Close()

			dt, mockMetrics, _ := setupDirectTransmissionTestWithBatchSize(t, 1, time.Second)
			dt.enableCompression = tt.enableCompress

			// Set shorter timeout for timeout test cases
			if tt.statusCode == 0 {
				dt.httpClient.Timeout = 100 * time.Millisecond
			}

			sendTestEvents(dt, server.URL, 1, "test-key")

			// Wait for processing with timeout for slow retries
			assert.Eventually(t, func() bool {
				return int(requestCount.Load()) >= 1+tt.expectRetries
			}, 5*time.Second, 10*time.Millisecond, "Should receive expected number of requests")

			err := dt.Stop()
			require.NoError(t, err)

			totalRequests := int(requestCount.Load())
			expectedRequests := 1 + tt.expectRetries
			assert.Equal(t, expectedRequests, totalRequests,
				"Expected %d total requests (1 initial + %d retries), got %d",
				expectedRequests, tt.expectRetries, totalRequests)

			// Verify that retry requests use the same data (reader pooling working correctly)
			requestMutex.Lock()
			if len(requestBodies) > 1 {
				for i := 1; i < len(requestBodies); i++ {
					assert.Equal(t, requestBodies[0], requestBodies[i],
						"Retry request %d should have identical body to first request", i)
				}
			}
			requestMutex.Unlock()

			if tt.expectSuccess {
				assert.Contains(t, mockMetrics.CounterIncrements, "libhoney_upstream_response_20x")
			} else {
				assert.Contains(t, mockMetrics.CounterIncrements, "libhoney_upstream_response_errors")
			}
		})
	}
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

	var reader io.Reader = r.Body
	if encoding != "" {
		if encoding != "zstd" {
			s.t.Errorf("unsupported compression: %s", encoding)
			http.Error(w, "unsupported compression", http.StatusBadRequest)
			return
		}
		zr, err := zstd.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			assert.NoError(s.t, err)
			return
		}
		defer zr.Close()
		reader = zr
	}

	// Read the entire request body
	body, err := io.ReadAll(reader)
	if err != nil {
		s.t.Error(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.Body.Close()

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
			Data:              types.NewPayload(&config.MockConfig{}, nil),
		}
		err := events[i].Data.UnmarshalMsgpack(msgpackPayloads[i%len(msgpackPayloads)])
		if err != nil {
			t.Fatalf("failed to unmarshal payload: %v", err)
		}
		events[i].Data.ExtractMetadata()
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
					types.TransmitTypeUpstream,
					httpTransport,
					libhoney.DefaultMaxBatchSize,
					libhoney.DefaultBatchTimeout,
					60*time.Second, // this is the default batch send timeout in libhoney
					false,
					nil,
				)
				dt.Logger = &logger.NullLogger{}
				dt.Version = "benchmark"
				dt.Metrics = mockMetrics

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
				}, 2*time.Second, time.Millisecond) {
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
				}, 2*time.Second, time.Millisecond) {
					receivedCount := server.GetEventCount()
					b.Errorf("Expected %d events, but server received %d", expectedCount, receivedCount)
				}
				b.StopTimer()
			})
		})
	}
}

func TestBuildRequestURL(t *testing.T) {
	const apiHost = "https://test-api"
	const apiHostResult = apiHost + "/1/batch/"
	testcases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "url encoded string",
			input:    "foo%20bar",
			expected: apiHostResult + "foo%2520bar",
		},
		{
			name:     "with a space",
			input:    "foo bar",
			expected: apiHostResult + "foo%20bar",
		},
		{
			name:     "with a slash",
			input:    "foo/bar",
			expected: apiHostResult + "foo%2Fbar",
		},
	}

	for _, tc := range testcases {
		t.Run("all at once/"+tc.name, func(t *testing.T) {
			result, err := buildRequestURL(apiHost, tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestDirectTransmitAdditionalHeaders(t *testing.T) {
	// Create test server that captures headers
	var capturedHeaders http.Header
	var headerMutex sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerMutex.Lock()
		capturedHeaders = r.Header.Clone()
		headerMutex.Unlock()

		// Read and discard body
		io.ReadAll(r.Body)
		r.Body.Close()

		// Send proper response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"status": 202}]`))
	}))
	defer server.Close()

	// Create DirectTransmission with additional headers
	additionalHeaders := map[string]string{
		"FORWARD_TO_URL":  "https://proxy.example.com",
		"X-Custom-Header": "custom-value",
	}

	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()
	mockLogger := &logger.MockLogger{}

	dt := NewDirectTransmission(
		types.TransmitTypeUpstream,
		http.DefaultTransport.(*http.Transport),
		10,
		100*time.Millisecond,
		10*time.Second,
		false, // no compression to simplify test
		additionalHeaders,
	)
	dt.Logger = mockLogger
	dt.Version = "test-version"
	dt.Metrics = mockMetrics

	err := dt.Start()
	require.NoError(t, err)
	defer dt.Stop()

	// Send a test event
	mockCfg := &config.MockConfig{}
	eventData := types.NewPayload(mockCfg, map[string]any{"test": "value"})
	eventData.ExtractMetadata()

	event := &types.Event{
		Context:    context.Background(),
		APIHost:    server.URL,
		APIKey:     "test-api-key",
		Dataset:    "test-dataset",
		SampleRate: 1,
		Timestamp:  time.Now().UTC(),
		Data:       eventData,
	}
	dt.EnqueueEvent(event)

	// Wait for the event to be sent
	require.Eventually(t, func() bool {
		headerMutex.Lock()
		defer headerMutex.Unlock()
		return capturedHeaders != nil
	}, 5*time.Second, 50*time.Millisecond, "headers should be captured")

	// Verify custom headers were sent
	headerMutex.Lock()
	defer headerMutex.Unlock()
	assert.Equal(t, "https://proxy.example.com", capturedHeaders.Get("FORWARD_TO_URL"))
	assert.Equal(t, "custom-value", capturedHeaders.Get("X-Custom-Header"))

	// Verify standard Honeycomb headers are also present
	assert.Equal(t, "test-api-key", capturedHeaders.Get("X-Honeycomb-Team"))
	assert.Equal(t, "application/msgpack", capturedHeaders.Get("Content-Type"))
}

func TestDirectTransmitNoAdditionalHeaders(t *testing.T) {
	// Create test server that captures headers
	var capturedHeaders http.Header
	var headerMutex sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headerMutex.Lock()
		capturedHeaders = r.Header.Clone()
		headerMutex.Unlock()

		io.ReadAll(r.Body)
		r.Body.Close()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"status": 202}]`))
	}))
	defer server.Close()

	// Create DirectTransmission without additional headers (nil)
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()
	mockLogger := &logger.MockLogger{}

	dt := NewDirectTransmission(
		types.TransmitTypeUpstream,
		http.DefaultTransport.(*http.Transport),
		10,
		100*time.Millisecond,
		10*time.Second,
		false,
		nil, // No additional headers
	)
	dt.Logger = mockLogger
	dt.Version = "test-version"
	dt.Metrics = mockMetrics

	err := dt.Start()
	require.NoError(t, err)
	defer dt.Stop()

	// Send a test event
	mockCfg := &config.MockConfig{}
	eventData := types.NewPayload(mockCfg, map[string]any{"test": "value"})
	eventData.ExtractMetadata()

	event := &types.Event{
		Context:    context.Background(),
		APIHost:    server.URL,
		APIKey:     "test-api-key",
		Dataset:    "test-dataset",
		SampleRate: 1,
		Timestamp:  time.Now().UTC(),
		Data:       eventData,
	}
	dt.EnqueueEvent(event)

	// Wait for the event to be sent
	require.Eventually(t, func() bool {
		headerMutex.Lock()
		defer headerMutex.Unlock()
		return capturedHeaders != nil
	}, 5*time.Second, 50*time.Millisecond, "headers should be captured")

	// Verify standard Honeycomb headers are present
	headerMutex.Lock()
	defer headerMutex.Unlock()
	assert.Equal(t, "test-api-key", capturedHeaders.Get("X-Honeycomb-Team"))
	assert.Equal(t, "application/msgpack", capturedHeaders.Get("Content-Type"))

	// Custom headers should not be present
	assert.Empty(t, capturedHeaders.Get("FORWARD_TO_URL"))
}
