package route

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/facebookgo/inject"
	"github.com/honeycombio/refinery/collect"
	"github.com/honeycombio/refinery/config"
	collectortrace "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/collector/trace/v1"
	common "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/common/v1"
	trace "github.com/honeycombio/refinery/internal/opentelemetry-proto-gen/trace/v1"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/transmit"
	"github.com/stretchr/testify/assert"

	"github.com/gorilla/mux"
	"github.com/honeycombio/refinery/sharder"
	"github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v4"
	"google.golang.org/grpc/metadata"
)

func TestDecompression(t *testing.T) {
	payload := "payload"
	pReader := strings.NewReader(payload)

	decoders, err := makeDecoders(numZstdDecoders)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	router := &Router{zstdDecoders: decoders}
	req := &http.Request{
		Body:   ioutil.NopCloser(pReader),
		Header: http.Header{},
	}
	reader, err := router.getMaybeCompressedBody(req)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	if string(b) != payload {
		t.Errorf("%s != %s", string(b), payload)
	}

	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
	_, err = w.Write([]byte(payload))
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	w.Close()

	req.Body = ioutil.NopCloser(buf)
	req.Header.Set("Content-Encoding", "gzip")
	reader, err = router.getMaybeCompressedBody(req)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	b, err = ioutil.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	if string(b) != payload {
		t.Errorf("%s != %s", string(b), payload)
	}

	buf = &bytes.Buffer{}
	zstdW, err := zstd.NewWriter(buf)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	_, err = zstdW.Write([]byte(payload))
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	zstdW.Close()

	req.Body = ioutil.NopCloser(buf)
	req.Header.Set("Content-Encoding", "zstd")
	reader, err = router.getMaybeCompressedBody(req)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}

	b, err = ioutil.ReadAll(reader)
	if err != nil {
		t.Errorf("unexpected err: %s", err.Error())
	}
	if string(b) != payload {
		t.Errorf("%s != %s", string(b), payload)
	}
}

func unmarshalRequest(w *httptest.ResponseRecorder, content string, body io.Reader) {
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data map[string]interface{}
		err := unmarshal(r, r.Body, &data)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}

		var traceID string
		if trID, ok := data["trace.trace_id"]; ok {
			traceID = trID.(string)
		} else if trID, ok := data["traceId"]; ok {
			traceID = trID.(string)
		}

		w.Write([]byte(traceID))
	}).ServeHTTP(w, &http.Request{
		Body: ioutil.NopCloser(body),
		Header: http.Header{
			"Content-Type": []string{content},
		},
	})
}

func unmarshalBatchRequest(w *httptest.ResponseRecorder, content string, body io.Reader) {
	http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var e batchedEvent
		err := unmarshal(r, r.Body, &e)

		if err != nil {
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.Write([]byte(e.getEventTime().Format(time.RFC3339Nano)))
	}).ServeHTTP(w, &http.Request{
		Body: ioutil.NopCloser(body),
		Header: http.Header{
			"Content-Type": []string{content},
		},
	})
}

func TestUnmarshal(t *testing.T) {
	var w *httptest.ResponseRecorder
	var body io.Reader
	now := time.Now().UTC()

	w = httptest.NewRecorder()
	body = bytes.NewBufferString("")
	unmarshalRequest(w, "nope", body)

	if w.Code != http.StatusBadRequest {
		t.Error("Expecting", http.StatusBadRequest, "Received", w.Code)
	}

	w = httptest.NewRecorder()
	body = bytes.NewBufferString(`{"trace.trace_id": "test"}`)
	unmarshalRequest(w, "application/json", body)

	if b := w.Body.String(); b != "test" {
		t.Error("Expecting test")
	}

	w = httptest.NewRecorder()
	body = bytes.NewBufferString(`{"traceId": "test"}`)
	unmarshalRequest(w, "application/json; charset=utf-8", body)

	if b := w.Body.String(); b != "test" {
		t.Error("Expecting test")
	}

	w = httptest.NewRecorder()
	body = bytes.NewBufferString(fmt.Sprintf(`{"time": "%s"}`, now.Format(time.RFC3339Nano)))
	unmarshalBatchRequest(w, "application/json", body)

	if b := w.Body.String(); b != now.Format(time.RFC3339Nano) {
		t.Error("Expecting", now, "Received", b)
	}

	var buf *bytes.Buffer
	var e *msgpack.Encoder
	var in map[string]interface{}
	var err error

	w = httptest.NewRecorder()
	buf = &bytes.Buffer{}
	e = msgpack.NewEncoder(buf)
	in = map[string]interface{}{"trace.trace_id": "test"}
	err = e.Encode(in)

	if err != nil {
		t.Error(err)
	}

	body = buf
	unmarshalRequest(w, "application/msgpack", body)

	if b := w.Body.String(); b != "test" {
		t.Error("Expecting test")
	}

	w = httptest.NewRecorder()
	buf = &bytes.Buffer{}
	e = msgpack.NewEncoder(buf)
	in = map[string]interface{}{"traceId": "test"}
	err = e.Encode(in)

	if err != nil {
		t.Error(err)
	}

	body = buf
	unmarshalRequest(w, "application/msgpack", body)

	if b := w.Body.String(); b != "test" {
		t.Error("Expecting test")
	}

	w = httptest.NewRecorder()
	buf = &bytes.Buffer{}
	e = msgpack.NewEncoder(buf)
	in = map[string]interface{}{"time": now}
	err = e.Encode(in)

	if err != nil {
		t.Error(err)
	}

	body = buf
	unmarshalBatchRequest(w, "application/msgpack", body)

	if b := w.Body.String(); b != now.Format(time.RFC3339Nano) {
		t.Error("Expecting", now, "Received", b)
	}
}

func TestGetAPIKeyAndDatasetFromMetadataCaseInsensitive(t *testing.T) {
	const (
		apiKeyValue  = "test-apikey"
		datasetValue = "test-dataset"
	)

	tests := []struct {
		name          string
		apikeyHeader  string
		datasetHeader string
	}{
		{
			name:          "lowercase",
			apikeyHeader:  "x-honeycomb-team",
			datasetHeader: "x-honeycomb-dataset",
		},
		{
			name:          "uppercase",
			apikeyHeader:  "X-HONEYCOMB-TEAM",
			datasetHeader: "X-HONEYCOMB-DATASET",
		},
		{
			name:          "mixed-case",
			apikeyHeader:  "x-HoNeYcOmB-tEaM",
			datasetHeader: "X-hOnEyCoMb-DaTaSeT",
		},
		{
			name:          "lowercase-short",
			apikeyHeader:  "x-hny-team",
			datasetHeader: "x-honeycomb-dataset",
		},
		{
			name:          "uppercase-short",
			apikeyHeader:  "X-HNY-TEAM",
			datasetHeader: "X-HONEYCOMB-DATASET",
		},
		{
			name:          "mixed-case-short",
			apikeyHeader:  "X-hNy-TeAm",
			datasetHeader: "X-hOnEyCoMb-DaTaSeT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := metadata.MD{}
			md.Set(tt.apikeyHeader, apiKeyValue)
			md.Set(tt.datasetHeader, datasetValue)

			apikey, dataset := getAPIKeyAndDatasetFromMetadata(md)
			if apikey != apiKeyValue {
				t.Errorf("got: %s\n\twant: %v", apikey, apiKeyValue)
			}
			if dataset != datasetValue {
				t.Errorf("got: %s\n\twant: %v", dataset, datasetValue)
			}
		})
	}
}

func TestGetSampleRateFromAttributes(t *testing.T) {
	const (
		defaultSampleRate = 1
	)
	tests := []struct {
		name          string
		attrKey       string
		attrValue     interface{}
		expectedValue int
	}{
		{
			name:          "missing attr gets default value",
			attrKey:       "",
			attrValue:     nil,
			expectedValue: defaultSampleRate,
		},
		{
			name:          "can parse integer value",
			attrKey:       "sampleRate",
			attrValue:     5,
			expectedValue: 5,
		},
		{
			name:          "can parse string value",
			attrKey:       "sampleRate",
			attrValue:     "5",
			expectedValue: 5,
		},
		{
			name:          "can parse int64 value (less than int32 max)",
			attrKey:       "sampleRate",
			attrValue:     int64(100),
			expectedValue: 100,
		},
		{
			name:          "can parse int64 value (greater than int32 max)",
			attrKey:       "sampleRate",
			attrValue:     int64(math.MaxInt32 + 100),
			expectedValue: math.MaxInt32,
		},
		{
			name:          "does not parse float, gets default value",
			attrKey:       "sampleRate",
			attrValue:     0.25,
			expectedValue: defaultSampleRate,
		},
		{
			name:          "does not parse bool, gets default value",
			attrKey:       "sampleRate",
			attrValue:     true,
			expectedValue: defaultSampleRate,
		},
		{
			name:          "does not parse struct, gets default value",
			attrKey:       "sampleRate",
			attrValue:     struct{}{},
			expectedValue: defaultSampleRate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attrs := map[string]interface{}{
				tt.attrKey: tt.attrValue,
			}

			sampleRate, _ := getSampleRateFromAttributes(attrs)
			if sampleRate != tt.expectedValue {
				t.Errorf("got: %d\n\twant: %d", sampleRate, tt.expectedValue)
			}
		})
	}
}

func TestDebugTrace(t *testing.T) {
	req, _ := http.NewRequest("GET", "/debug/trace/123abcdef", nil)
	req = mux.SetURLVars(req, map[string]string{"traceID": "123abcdef"})

	rr := httptest.NewRecorder()
	router := &Router{
		Sharder: &TestSharder{},
	}

	router.debugTrace(rr, req)
	if body := rr.Body.String(); body != `{"traceID":"123abcdef","node":"http://localhost:12345"}` {
		t.Error(body)
	}
}

func TestOTLPHandler(t *testing.T) {
	md := metadata.New(map[string]string{"x-honeycomb-team": "meow", "x-honeycomb-dataset": "ds"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	mockMetrics := metrics.MockMetrics{}
	mockMetrics.Start()
	mockTransmission := &transmit.MockTransmission{}
	mockTransmission.Start()
	router := &Router{
		Config:               &config.MockConfig{},
		Metrics:              &mockMetrics,
		UpstreamTransmission: mockTransmission,
		iopLogger: iopLogger{
			Logger:         &logger.NullLogger{},
			incomingOrPeer: "incoming",
		},
	}

	conf := &config.MockConfig{
		GetSendDelayVal:    0,
		GetTraceTimeoutVal: 60 * time.Second,
		GetSamplerTypeVal:  &config.DeterministicSamplerConfig{SampleRate: 1},
		SendTickerVal:      2 * time.Millisecond,
		GetInMemoryCollectorCacheCapacityVal: config.InMemoryCollectorCacheCapacity{
			CacheCapacity: 100,
			MaxAlloc:      100,
		},
	}

	t.Run("span with status", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: helperOTLPRequestSpansWithStatus(),
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	t.Run("span without status", func(t *testing.T) {
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: helperOTLPRequestSpansWithoutStatus(),
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}
		assert.Equal(t, 2, len(mockTransmission.Events))
		mockTransmission.Flush()
	})

	// TODO: (MG) figuure out how we can test JSON created from OTLP requests
	// Below is example, but requires significant usage of collector, sampler, conf, etc
	t.Run("creates events for span events", func(t *testing.T) {
		t.Skip("need additional work to support inspecting outbound JSON")

		traceID := []byte{0, 0, 0, 0, 1}
		spanID := []byte{1, 0, 0, 0, 0}
		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: []*trace.Span{{
						TraceId: traceID,
						SpanId:  spanID,
						Name:    "span_with_event",
						Events: []*trace.Span_Event{{
							TimeUnixNano: 12345,
							Name:         "span_link",
							Attributes: []*common.KeyValue{{
								Key: "event_attr_key", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "event_attr_val"}},
							}},
						}},
					}},
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}

		time.Sleep(conf.SendTickerVal * 2)

		mockTransmission.Mux.Lock()
		assert.Equal(t, 2, len(mockTransmission.Events))

		spanEvent := mockTransmission.Events[0]
		// assert.Equal(t, time.Unix(0, int64(12345)).UTC(), spanEvent.Timestamp)
		assert.Equal(t, bytesToTraceID(traceID), spanEvent.Data["trace.trace_id"])
		assert.Equal(t, hex.EncodeToString(spanID), spanEvent.Data["trace.span_id"])
		assert.Equal(t, "span_link", spanEvent.Data["span.name"])
		assert.Equal(t, "span_with_event", spanEvent.Data["parent.name"])
		assert.Equal(t, "span_event", spanEvent.Data["meta.annotation_type"])
		assert.Equal(t, "event_attr_key", spanEvent.Data["event_attr_val"])
		mockTransmission.Mux.Unlock()
		mockTransmission.Flush()
	})

	t.Run("creates events for span links", func(t *testing.T) {
		t.Skip("need additional work to support inspecting outbound JSON")

		traceID := []byte{0, 0, 0, 0, 1}
		spanID := []byte{1, 0, 0, 0, 0}
		linkTraceID := []byte{0, 0, 0, 0, 2}
		linkSpanID := []byte{2, 0, 0, 0, 0}

		req := &collectortrace.ExportTraceServiceRequest{
			ResourceSpans: []*trace.ResourceSpans{{
				InstrumentationLibrarySpans: []*trace.InstrumentationLibrarySpans{{
					Spans: []*trace.Span{{
						Name:    "span_with_link",
						TraceId: traceID,
						SpanId:  spanID,
						Links: []*trace.Span_Link{{
							TraceId:    traceID,
							SpanId:     spanID,
							TraceState: "link_trace_state",
							Attributes: []*common.KeyValue{{
								Key: "link_attr_key", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "link_attr_val"}},
							}},
						}},
					}},
				}},
			}},
		}
		_, err := router.Export(ctx, req)
		if err != nil {
			t.Errorf(`Unexpected error: %s`, err)
		}

		time.Sleep(conf.SendTickerVal * 2)
		assert.Equal(t, 2, len(mockTransmission.Events))

		spanLink := mockTransmission.Events[1]
		assert.Equal(t, bytesToTraceID(traceID), spanLink.Data["trace.trace_id"])
		assert.Equal(t, hex.EncodeToString(spanID), spanLink.Data["trace.span_id"])
		assert.Equal(t, bytesToTraceID(linkTraceID), spanLink.Data["trace.link.trace_id"])
		assert.Equal(t, hex.EncodeToString(linkSpanID), spanLink.Data["trace.link.span_id"])
		assert.Equal(t, "link", spanLink.Data["meta.annotation_type"])
		assert.Equal(t, "link_attr_val", spanLink.Data["link_attr_key"])
		mockTransmission.Flush()
	})
}

func TestDependencyInjection(t *testing.T) {
	var g inject.Graph
	err := g.Provide(
		&inject.Object{Value: &Router{}},

		&inject.Object{Value: &config.MockConfig{}},
		&inject.Object{Value: &logger.NullLogger{}},
		&inject.Object{Value: http.DefaultTransport, Name: "upstreamTransport"},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "upstreamTransmission"},
		&inject.Object{Value: &transmit.MockTransmission{}, Name: "peerTransmission"},
		&inject.Object{Value: &TestSharder{}},
		&inject.Object{Value: &collect.InMemCollector{}},
		&inject.Object{Value: &metrics.NullMetrics{}, Name: "metrics"},
	)
	if err != nil {
		t.Error(err)
	}
	if err := g.Populate(); err != nil {
		t.Error(err)
	}
}

func helperOTLPRequestSpansWithoutStatus() []*trace.Span {
	now := time.Now()
	return []*trace.Span{
		{
			StartTimeUnixNano: uint64(now.UnixNano()),
			Events: []*trace.Span_Event{
				{
					TimeUnixNano: uint64(now.UnixNano()),
					Attributes: []*common.KeyValue{
						{
							Key: "attribute_key",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "attribute_value"},
							},
						},
					},
				},
			},
		},
	}
}

func helperOTLPRequestSpansWithStatus() []*trace.Span {
	now := time.Now()
	return []*trace.Span{
		{
			StartTimeUnixNano: uint64(now.UnixNano()),
			Events: []*trace.Span_Event{
				{
					TimeUnixNano: uint64(now.UnixNano()),
					Attributes: []*common.KeyValue{
						{
							Key: "attribute_key",
							Value: &common.AnyValue{
								Value: &common.AnyValue_StringValue{StringValue: "attribute_value"},
							},
						},
					},
				},
			},
			Status: &trace.Status{Code: trace.Status_STATUS_CODE_OK},
		},
	}
}

type TestSharder struct{}

func (s *TestSharder) MyShard() sharder.Shard { return nil }

func (s *TestSharder) WhichShard(string) sharder.Shard {
	return &TestShard{
		addr: "http://localhost:12345",
	}
}

type TestShard struct {
	addr string
}

func (s *TestShard) Equals(other sharder.Shard) bool { return true }
func (s *TestShard) GetAddress() string              { return s.addr }
