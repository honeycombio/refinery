package transmit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/tinylib/msgp/msgp"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

type transmitKey struct {
	apiHost string
	apiKey  string
	dataset string
}

// Transmission to the hny API (peer refinery or honeycomb) via messagepack,
// without involving libhoney. This is designed to be lightweight and sheds
// some of the ergonomics of libhoney in favor of simplicity and performance.
// Sending data directly also gives us the flexibility to use custom
// serialization and potentially send additional metadata to peers.
type DirectTransmission struct {
	Config    config.Config   `inject:""`
	Logger    logger.Logger   `inject:""`
	Metrics   metrics.Metrics // constructed, not injected
	Version   string          `inject:"version"`
	Transport *http.Transport `inject:"upstreamTransport"`

	// Type is peer or upstream, and used only for naming metrics
	Name string

	// Batching configuration
	MaxBatchSize int
	BatchTimeout time.Duration

	transmitQueues map[transmitKey]chan<- *types.Event
	mutex          sync.RWMutex
	shutdownWG     sync.WaitGroup

	httpClient *http.Client
	userAgent  string
}

func NewDirectTransmission(m metrics.Metrics, name string, maxBatchSize int, batchTimeout time.Duration) *DirectTransmission {
	return &DirectTransmission{
		Metrics:        m,
		Name:           name,
		MaxBatchSize:   maxBatchSize,
		BatchTimeout:   batchTimeout,
		transmitQueues: make(map[transmitKey]chan<- *types.Event),
	}
}

func (d *DirectTransmission) Start() error {
	d.Logger.Debug().Logf("Starting DirectTransmission: %s type", d.Name)
	d.userAgent = fmt.Sprintf("refinery/%s %s (%s/%s)", d.Version, strings.Replace(runtime.Version(), "go", "go/", 1), runtime.GOOS, runtime.GOARCH)
	d.httpClient = &http.Client{
		Transport: d.Transport,
		Timeout:   10 * time.Second,
	}
	return nil
}

func (d *DirectTransmission) EnqueueEvent(ev *types.Event) {
	d.Logger.Debug().
		WithField("request_id", ev.Context.Value(types.RequestIDContextKey{})).
		WithString("api_host", ev.APIHost).
		WithString("dataset", ev.Dataset).
		Logf("transmit sending event")

	// Store enqueue time for queue time metrics
	ev.EnqueuedUnixMicro = time.Now().UnixMicro()

	// TODO this may end up on the heap, if so, either construct it inline, or make it
	// a sub-struct of Event (which is already on the heap).
	key := transmitKey{
		apiHost: ev.APIHost,
		apiKey:  ev.APIKey,
		dataset: ev.Dataset,
	}

	d.mutex.RLock()
	q, ok := d.transmitQueues[key]
	d.mutex.RUnlock()

	if !ok {
		d.mutex.Lock()
		q, ok = d.transmitQueues[key]
		if ok {
			d.mutex.Unlock()
		} else {
			// TODO buffer size - use larger buffer to avoid blocking
			ch := make(chan *types.Event, 100)
			d.transmitQueues[key] = ch
			d.mutex.Unlock()

			// TODO one goroutine per dataset/peer will lead to 1M+ goroutines
			// in degenerate cases. This is not a viable production strategy,
			// and needs to be evolved to a more bounded system.
			d.shutdownWG.Add(1)
			go d.batchEvents(ch)
			q = ch
		}
	}

	q <- ev
	d.Metrics.Up(updownQueuedItems)
}

func (d *DirectTransmission) EnqueueSpan(sp *types.Span) {
	// we don't need the trace ID anymore, but it's convenient to accept spans.
	d.EnqueueEvent(&sp.Event)
}

// RegisterMetrics registers the metrics used by the DirectTransmission.
// it should be called after the metrics object has been created.
func (d *DirectTransmission) RegisterMetrics() {
	for _, m := range transmissionMetrics {
		d.Metrics.Register(m)
	}
}

func (d *DirectTransmission) Stop() error {
	for _, q := range d.transmitQueues {
		close(q)
	}
	d.transmitQueues = nil
	d.shutdownWG.Wait()
	return nil
}

// handleBatchFailure handles metrics updates when the entire batch fails
func (d *DirectTransmission) handleBatchFailure(batch []*types.Event) {
	for range batch {
		d.Metrics.Down(updownQueuedItems)
	}
}

// handleEventError logs an error and updates metrics for a single event
func (d *DirectTransmission) handleEventError(ev *types.Event, statusCode int, queueTime int64, errorMsg string, responseBody []byte) {
	log := d.Logger.Error().WithFields(map[string]any{
		"status_code":    statusCode,
		"api_host":       ev.APIHost,
		"dataset":        ev.Dataset,
		"environment":    ev.Environment,
		"roundtrip_usec": queueTime,
	})

	if errorMsg != "" {
		log = log.WithField("error", errorMsg)
	}

	if len(responseBody) > 0 {
		log = log.WithField("response_body", string(responseBody))
	}

	if d.Config != nil {
		for _, k := range d.Config.GetAdditionalErrorFields() {
			if ev.Data.Exists(k) {
				log = log.WithField(k, ev.Data.Get(k))
			}
		}
	}

	log.Logf("error when sending event")
	d.Metrics.Increment(counterResponseErrors)
	d.Metrics.Down(updownQueuedItems)
	d.Metrics.Histogram(histogramQueueTime, float64(queueTime))
}

func (d *DirectTransmission) sendBatch(batch []*types.Event) {
	if len(batch) == 0 {
		return
	}

	// All events in batch should have same destination
	firstEvent := batch[0]
	apiHost := firstEvent.APIHost
	apiKey := firstEvent.APIKey
	dataset := firstEvent.Dataset

	// Create batch of events
	var packed []byte
	packed = msgp.AppendArrayHeader(packed, uint32(len(batch)))

	for _, ev := range batch {
		packEvent := batchedEvent{
			time:       ev.Timestamp,
			sampleRate: int64(ev.SampleRate),
			data:       ev.Data,
		}

		var err error
		packed, err = packEvent.MarshalMsg(packed)
		if err != nil {
			d.Logger.Error().WithField("error", err.Error()).Logf("failed to marshal event")
			d.Metrics.Down(updownQueuedItems)
			continue
		}
	}

	apiURL, err := url.Parse(apiHost)
	if err != nil {
		d.Logger.Error().WithField("error", err.Error()).WithString("api_host", apiHost).Logf("failed to parse API host")
		d.handleBatchFailure(batch)
		return
	}
	apiURL.Path, err = url.JoinPath("/1/batch", url.PathEscape(dataset))
	if err != nil {
		d.Logger.Error().WithField("error", err.Error()).Logf("failed to create request URL")
		d.handleBatchFailure(batch)
		return
	}
	req, err := http.NewRequest("POST", apiURL.String(), bytes.NewReader(packed))
	if err != nil {
		d.Logger.Error().WithField("error", err.Error()).Logf("failed to create request")
		d.handleBatchFailure(batch)
		return
	}

	req.Header.Set("Content-Type", "application/msgpack")
	req.Header.Set("X-Honeycomb-Team", apiKey)
	req.Header.Set("User-Agent", d.userAgent)

	resp, err := d.httpClient.Do(req)
	dequeuedAt := time.Now()

	if err != nil {
		d.Logger.Error().WithField("error", err.Error()).Logf("http POST failed")

		// Network/connection error - affects all events in batch
		for _, ev := range batch {
			queueTime := dequeuedAt.UnixMicro() - ev.EnqueuedUnixMicro
			d.handleEventError(ev, 0, queueTime, err.Error(), nil)
		}
		return
	}

	defer resp.Body.Close()

	// Parse batch response - following libhoney pattern where any status != 200 is an error for all events
	if resp.StatusCode == http.StatusOK {
		// Parse individual responses from batch - handle msgpack or JSON
		var batchResponses []batchResponse
		if resp.Header.Get("Content-Type") == "application/msgpack" {
			err := msgpack.NewDecoder(resp.Body).Decode(&batchResponses)
			if err != nil {
				d.Logger.Error().WithField("error", err.Error()).Logf("failed to decode msgpack batch response")
			}
		} else {
			bodyBytes, err := io.ReadAll(resp.Body)
			if err == nil {
				json.Unmarshal(bodyBytes, &batchResponses)
			}
		}

		// Process each event response
		for i, ev := range batch {
			queueTime := dequeuedAt.UnixMicro() - ev.EnqueuedUnixMicro

			// Check if we have a response for this event
			if i >= len(batchResponses) {
				// Missing response - treat as server error
				d.handleEventError(ev, http.StatusInternalServerError, queueTime, "insufficient responses from server", nil)
				continue
			}

			if batchResponses[i].Status != http.StatusAccepted {
				d.handleEventError(ev, batchResponses[i].Status, queueTime, "", nil)
			} else {
				// Success
				d.Metrics.Increment(counterResponse20x)
				d.Metrics.Down(updownQueuedItems)
				d.Metrics.Histogram(histogramQueueTime, float64(queueTime))
			}
		}
	} else {
		// HTTP error - affects all events in batch
		var bodyBytes []byte

		// Handle msgpack or JSON response body
		if resp.Header.Get("Content-Type") == "application/msgpack" {
			var errorBody interface{}
			decoder := msgpack.NewDecoder(resp.Body)
			err = decoder.Decode(&errorBody)
			if err == nil {
				bodyBytes, _ = json.Marshal(&errorBody)
			}
		} else {
			bodyBytes, _ = io.ReadAll(resp.Body)
		}

		// Special handling for unauthorized responses
		if resp.StatusCode == http.StatusUnauthorized {
			d.Logger.Error().WithString("api_key", apiKey).Logf("APIKey was rejected. Please verify APIKey is correct.")
		}

		for _, ev := range batch {
			queueTime := dequeuedAt.UnixMicro() - ev.EnqueuedUnixMicro
			d.handleEventError(ev, resp.StatusCode, queueTime, "", bodyBytes)
		}
	}
}

// TODO this needs to be modified to not exceed maximum event and batch sizes,
// which is rather a hassle. See libhoney's encodeBatchMsgp method.
// TODO we'll need a benchmark which sends very wide distributions of dataset/host,
// and one which just spams a single one, to identify the unique bottlenecks of
// both cases.
func (d *DirectTransmission) batchEvents(in <-chan *types.Event) {
	defer d.shutdownWG.Done()

	var batch []*types.Event
	for ev := range in {
		// Start a new batch, and wait for the first event.
		batch = batch[:0]
		batch = append(batch, ev)

		timer := time.NewTimer(d.BatchTimeout)

		// Accumulate until the batch is full, or the timer expires
	eventLoop:
		for len(batch) < d.MaxBatchSize {
			select {
			case ev, ok := <-in:
				if !ok {
					break eventLoop
				}

				// Add event to batch (all events on this channel have same destination)
				batch = append(batch, ev)
			case <-timer.C:
				// Timer expired, send batch
				break eventLoop
			}
		}

		// TODO sending batches in-line here is not viable for production.
		d.sendBatch(batch)
	}
}

type batchedEvent struct {
	time       time.Time     `msg:"time"`
	sampleRate int64         `msg:"samplerate"`
	data       types.Payload `msg:"data"`
}

type batchResponse struct {
	Status int `json:"status" msgpack:"status"`
}

// implements msgp.Marshaler, based on generated code.
// Note though that this uses AppendTimeExt (the messagepack standard),
// vs the default AppendTime() which uses a library-specific extension.
// Since we don't use this library universally, AppendTime() will create
// unreadable timestamps.
func (z *batchedEvent) MarshalMsg(b []byte) (o []byte, err error) {
	o = b
	// map header (size 3), followed by string "time"
	o = append(o, 0x83, 0xa4, 0x74, 0x69, 0x6d, 0x65)
	o = msgp.AppendTimeExt(o, z.time)
	// string "samplerate"
	o = append(o, 0xaa, 0x73, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x72, 0x61, 0x74, 0x65)
	o = msgp.AppendInt64(o, z.sampleRate)
	// string "data"
	o = append(o, 0xa4, 0x64, 0x61, 0x74, 0x61)
	o, err = z.data.MarshalMsg(o)
	if err != nil {
		err = msgp.WrapError(err, "data")
		return
	}
	return
}
