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

	"github.com/sourcegraph/conc/pool"
	"github.com/tinylib/msgp/msgp"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

const (
	// Size limit for a serialized request body sent for a batch.
	apiMaxBatchSize int = 5_000_000

	// Size limit for a single serialized event within a batch.
	apiMaxEventSize int = 1_000_000

	// Libhoney uses 80 for this, but in a high-volume environment we may need
	// more. Under ideal conditions this limit is never approached.
	maxConcurrentBatches = 500
)

type transmitKey struct {
	apiHost string
	apiKey  string
	dataset string
}

// eventBatch holds a slice of events and a mutex for thread-safe access
type eventBatch struct {
	mutex  sync.Mutex
	events []*types.Event
}

// Transmission to the hny API (peer refinery or honeycomb) via messagepack,
// without involving libhoney. This is designed to be lightweight and sheds
// some of the ergonomics of libhoney in favor of simplicity and performance.
// Sending data directly also gives us the flexibility to use custom
// serialization and potentially send additional metadata to peers.
type DirectTransmission struct {
	Config  config.Config `inject:""`
	Logger  logger.Logger `inject:""`
	Version string        `inject:"version"`

	// constructed, not injected
	Metrics   metrics.Metrics
	Transport *http.Transport

	// Type is peer or upstream, and used only for naming metrics
	Name string

	// Batching configuration
	MaxBatchSize int
	BatchTimeout time.Duration

	// Slice-based batching
	eventBatches map[transmitKey]*eventBatch
	batchMutex   sync.RWMutex
	batchPool    *pool.Pool
	ticker       *time.Ticker
	done         chan struct{}

	httpClient *http.Client
	userAgent  string
}

func NewDirectTransmission(m metrics.Metrics, name string, maxBatchSize int, batchTimeout time.Duration, transport *http.Transport) *DirectTransmission {
	return &DirectTransmission{
		Metrics:        m,
		Name:           name,
		MaxBatchSize:   maxBatchSize,
		BatchTimeout:   batchTimeout,
		Transport:      transport,
		eventBatches:   make(map[transmitKey]*eventBatch),
		done:           make(chan struct{}),
	}
}

func (d *DirectTransmission) Start() error {
	d.Logger.Debug().Logf("Starting DirectTransmission: %s type", d.Name)
	d.userAgent = fmt.Sprintf("refinery/%s %s (%s/%s)", d.Version, strings.Replace(runtime.Version(), "go", "go/", 1), runtime.GOOS, runtime.GOARCH)
	d.httpClient = &http.Client{
		Transport: d.Transport,
		Timeout:   10 * time.Second,
	}

	// Create a pool for concurrent batch sending
	d.batchPool = pool.New().WithMaxGoroutines(maxConcurrentBatches)

	// Start the ticker for periodic batch dispatch
	d.ticker = time.NewTicker(d.BatchTimeout)
	go d.periodicBatchDispatch()

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

	key := transmitKey{
		apiHost: ev.APIHost,
		apiKey:  ev.APIKey,
		dataset: ev.Dataset,
	}

	// First check if batch exists without write lock
	d.batchMutex.RLock()
	batch, exists := d.eventBatches[key]
	d.batchMutex.RUnlock()

	if !exists {
		// Need to create new batch
		d.batchMutex.Lock()
		// Double-check after acquiring write lock
		batch, exists = d.eventBatches[key]
		if !exists {
			batch = &eventBatch{
				events: make([]*types.Event, 0, d.MaxBatchSize),
			}
			d.eventBatches[key] = batch
		}
		d.batchMutex.Unlock()
	}

	// Add event to batch
	batch.mutex.Lock()
	batch.events = append(batch.events, ev)
	shouldDispatch := len(batch.events) >= d.MaxBatchSize
	if shouldDispatch {
		// Dispatch this batch
		eventsCopy := make([]*types.Event, len(batch.events))
		copy(eventsCopy, batch.events)
		batch.events = batch.events[:0] // Reset slice but keep capacity
		batch.mutex.Unlock()

		d.batchPool.Go(func() {
			d.sendBatch(eventsCopy)
		})
	} else {
		batch.mutex.Unlock()
	}

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
	// Stop the ticker
	if d.ticker != nil {
		d.ticker.Stop()
	}
	
	// Only close done if it was initialized
	if d.done != nil {
		select {
		case <-d.done:
			// Already closed
		default:
			close(d.done)
		}
	}

	// Dispatch all remaining batches
	d.dispatchAllBatches()


	// Wait for all batch sends to complete
	if d.batchPool != nil {
		d.batchPool.Wait()
	}
	d.batchPool = nil

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

// Sends one message or, if the batch is larger than what is allowed, several.
func (d *DirectTransmission) sendBatch(wholeBatch []*types.Event) {
	for len(wholeBatch) > 0 {
		// All events in batch should have same destination
		apiHost := wholeBatch[0].APIHost
		apiKey := wholeBatch[0].APIKey
		dataset := wholeBatch[0].Dataset

		// Msgpack arrays need to be prefixed with the number of elements, but we
		// don't know in advance how many we'll encode, because size estimation is
		// quite expensive. Also, the array header is of variable size based on
		// array length, so we'll need to do some []byte shenanigans at the end of
		// this to properly prepend the header.

		// TODO keep a pool of buffers, being sure to keep original start point
		// Start with a buffer pre-pended with 5 spare bytes, the max size of an
		// array header.
		var packed []byte
		packed = append(packed, 0, 0, 0, 0, 0)
		var subBatch []*types.Event
		var i int
		for i = 0; i < len(wholeBatch); i++ {
			packEvent := batchedEvent{
				time:       wholeBatch[i].Timestamp,
				sampleRate: int64(wholeBatch[i].SampleRate),
				data:       wholeBatch[i].Data,
			}

			var err error
			newPacked, err := packEvent.MarshalMsg(packed)
			if err == nil && len(newPacked)-len(packed) > apiMaxEventSize {
				err = fmt.Errorf("event exceeds max event size of %d bytes, API will not accept this event.", apiMaxEventSize)
			}
			if err != nil {
				// Skip this message and remove it from the list, so we don't
				// try to account for it again.
				d.Logger.Error().WithField("err", err.Error()).Logf("failed to marshal event")
				d.Metrics.Down(updownQueuedItems)
				d.Metrics.Increment(counterResponseErrors)
				continue
			}
			if len(newPacked) > apiMaxBatchSize {
				// Not an error, but we can't send this event in this batch.
				// Dispatch with what we have.
				break
			}
			packed = newPacked
			subBatch = append(subBatch, wholeBatch[i])
		}
		// Any leftover events will be sent in the next iteration.
		wholeBatch = wholeBatch[i:]

		if len(subBatch) == 0 {
			continue
		}

		// Now we know how many events were encoded, so we can do shenanigans
		// to pre-pend the array header, which is variable-width.
		var headerBuf [5]byte
		header := msgp.AppendArrayHeader(headerBuf[:0], uint32(len(subBatch)))
		packed = packed[5-len(header):]
		copy(packed, header)

		apiURL, err := url.Parse(apiHost)
		if err != nil {
			d.Logger.Error().WithField("err", err.Error()).WithString("api_host", apiHost).Logf("failed to parse API host")
			d.handleBatchFailure(subBatch)
			continue
		}
		apiURL.Path, err = url.JoinPath("/1/batch", url.PathEscape(dataset))
		if err != nil {
			d.Logger.Error().WithField("err", err.Error()).Logf("failed to create request URL")
			d.handleBatchFailure(subBatch)
			continue
		}
		req, err := http.NewRequest("POST", apiURL.String(), bytes.NewReader(packed))
		if err != nil {
			d.Logger.Error().WithField("err", err.Error()).Logf("failed to create request")
			d.handleBatchFailure(subBatch)
			continue
		}

		req.Header.Set("Content-Type", "application/msgpack")
		req.Header.Set("X-Honeycomb-Team", apiKey)
		req.Header.Set("User-Agent", d.userAgent)

		resp, err := d.httpClient.Do(req)
		dequeuedAt := time.Now()

		if err != nil {
			d.Logger.Error().WithField("err", err.Error()).Logf("http POST failed")

			// Network/connection error - affects all events in batch
			for _, ev := range subBatch {
				queueTime := dequeuedAt.UnixMicro() - ev.EnqueuedUnixMicro
				d.handleEventError(ev, 0, queueTime, err.Error(), nil)
			}
			continue
		}

		// Parse batch response - following libhoney pattern where any status != 200 is an error for all events
		if resp.StatusCode == http.StatusOK {
			// Parse individual responses from batch - handle msgpack or JSON
			var batchResponses []batchResponse
			if resp.Header.Get("Content-Type") == "application/msgpack" {
				err := msgpack.NewDecoder(resp.Body).Decode(&batchResponses)
				if err != nil {
					d.Logger.Error().WithField("err", err.Error()).Logf("failed to decode msgpack batch response")
				}
			} else {
				bodyBytes, err := io.ReadAll(resp.Body)
				if err == nil {
					json.Unmarshal(bodyBytes, &batchResponses)
				}
			}
			resp.Body.Close()

			// Process each event response
			for i, ev := range subBatch {
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
			resp.Body.Close()

			// Special handling for unauthorized responses
			if resp.StatusCode == http.StatusUnauthorized {
				d.Logger.Error().WithString("api_key", apiKey).Logf("APIKey was rejected. Please verify APIKey is correct.")
			}

			for _, ev := range subBatch {
				queueTime := dequeuedAt.UnixMicro() - ev.EnqueuedUnixMicro
				d.handleEventError(ev, resp.StatusCode, queueTime, "", bodyBytes)
			}
		}
	}
}

// periodicBatchDispatch runs on a ticker and dispatches all pending batches
func (d *DirectTransmission) periodicBatchDispatch() {
	for {
		select {
		case <-d.ticker.C:
			d.dispatchAllBatches()
		case <-d.done:
			return
		}
	}
}

// dispatchAllBatches sends all pending batches
func (d *DirectTransmission) dispatchAllBatches() {
	// Get snapshot of all keys
	d.batchMutex.RLock()
	keys := make([]transmitKey, 0, len(d.eventBatches))
	for k := range d.eventBatches {
		keys = append(keys, k)
	}
	d.batchMutex.RUnlock()

	// Dispatch each batch
	for _, key := range keys {
		d.batchMutex.RLock()
		batch, exists := d.eventBatches[key]
		d.batchMutex.RUnlock()

		if !exists {
			continue
		}

		batch.mutex.Lock()
		if len(batch.events) > 0 {
			// Copy events and reset batch
			eventsCopy := make([]*types.Event, len(batch.events))
			copy(eventsCopy, batch.events)
			batch.events = batch.events[:0]
			batch.mutex.Unlock()

			d.batchPool.Go(func() {
				d.sendBatch(eventsCopy)
			})
		} else {
			batch.mutex.Unlock()
		}
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
