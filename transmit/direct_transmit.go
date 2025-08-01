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

	"github.com/jonboulle/clockwork"
	"github.com/klauspost/compress/zstd"
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

const (
	counterEnqueueErrors        = "_enqueue_errors"
	counterResponse20x          = "_response_20x"
	counterResponseErrors       = "_response_errors"
	updownQueuedItems           = "_queued_items"
	histogramQueueTime          = "_queue_time"
	gaugeQueueLength            = "_queue_length"
	counterSendErrors           = "_send_errors"
	counterSendRetries          = "_send_retries"
	counterBatchesSent          = "_batches_sent"
	counterMessagesSent         = "_messages_sent"
	counterResponseDecodeErrors = "_response_decode_errors"
)

// Instantiating a new encoder is expensive, so use a global one.
// EncodeAll() is concurrency-safe.
var zstdEncoder *zstd.Encoder

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(
		nil,
		// Compression level 2 gives a good balance of speed and compression.
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(2)),
		// zstd allocates 2 * GOMAXPROCS * window size, so use a small window.
		// Most honeycomb messages are smaller than this.
		zstd.WithWindowSize(1<<16),
	)
	if err != nil {
		panic(err)
	}
}

var transmissionMetrics = []metrics.Metadata{
	{Name: counterEnqueueErrors, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "The number of errors encountered when enqueueing events"},
	{Name: counterResponse20x, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "The number of successful responses from Honeycomb"},
	{Name: counterResponseErrors, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "The number of errors encountered when sending events to Honeycomb"},
	{Name: updownQueuedItems, Type: metrics.UpDown, Unit: metrics.Dimensionless, Description: "The number of events queued for transmission to Honeycomb"},
	{Name: histogramQueueTime, Type: metrics.Histogram, Unit: metrics.Microseconds, Description: "The time spent in the queue before being sent to Honeycomb"},
	{Name: gaugeQueueLength, Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of events waiting to be sent to destination"},
	{Name: counterSendErrors, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of errors encountered while sending a batch of events to destination"},
	{Name: counterSendRetries, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of times a batch of events was retried"},
	{Name: counterBatchesSent, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of batches of events sent to destination"},
	{Name: counterMessagesSent, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of messages sent to destination"},
	{Name: counterResponseDecodeErrors, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of errors encountered while decoding responses from destination"},
}

type metricKeys struct {
	updownQueuedItems     string
	histogramQueueTime    string
	counterEnqueueErrors  string
	counterResponse20x    string
	counterResponseErrors string

	gaugeQueueLength            string
	counterSendErrors           string
	counterSendRetries          string
	counterBatchesSent          string
	counterMessagesSent         string
	counterResponseDecodeErrors string
}

type transmitKey struct {
	apiHost string
	apiKey  string
	dataset string
}

// eventBatch holds a slice of events and a mutex for thread-safe access
type eventBatch struct {
	mutex     sync.Mutex
	events    []*types.Event
	startTime time.Time // Time when the batch was created
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
	Clock     clockwork.Clock

	// Type is peer or upstream, and used only for naming metrics
	transmitType types.TransmitType

	enableCompression bool

	// Batching configuration
	maxBatchSize int
	batchTimeout time.Duration

	eventBatches map[transmitKey]*eventBatch
	batchMutex   sync.RWMutex
	dispatchPool *pool.Pool
	stop         chan struct{}
	stopWG       sync.WaitGroup

	httpClient *http.Client
	userAgent  string
	metricKeys metricKeys
}

func NewDirectTransmission(
	m metrics.Metrics,
	transmitType types.TransmitType,
	transport *http.Transport,
	maxBatchSize int,
	batchTimeout time.Duration,
	enableCompression bool,
) *DirectTransmission {
	return &DirectTransmission{
		Metrics:           m,
		Transport:         transport,
		Clock:             clockwork.NewRealClock(),
		transmitType:      transmitType,
		enableCompression: enableCompression,
		maxBatchSize:      maxBatchSize,
		batchTimeout:      batchTimeout,
		eventBatches:      make(map[transmitKey]*eventBatch),
		stop:              make(chan struct{}),
	}
}

func (d *DirectTransmission) Start() error {
	d.Logger.Debug().Logf("Starting DirectTransmission: %s type", d.transmitType.String())
	d.userAgent = fmt.Sprintf("refinery/%s %s (%s/%s)", d.Version, strings.Replace(runtime.Version(), "go", "go/", 1), runtime.GOOS, runtime.GOARCH)
	d.httpClient = &http.Client{
		Transport: d.Transport,
		Timeout:   10 * time.Second,
	}

	// Create a pool for concurrent batch sending
	d.dispatchPool = pool.New().WithMaxGoroutines(maxConcurrentBatches)

	d.stopWG.Add(1)
	go d.dispatchStaleBatches()

	return nil
}

func (d *DirectTransmission) EnqueueEvent(ev *types.Event) {
	d.Logger.Debug().
		WithField("request_id", ev.Context.Value(types.RequestIDContextKey{})).
		WithString("api_host", ev.APIHost).
		WithString("dataset", ev.Dataset).
		Logf("transmit sending event")

	// Store enqueue time for queue time metrics
	ev.EnqueuedUnixMicro = d.Clock.Now().UnixMicro()

	key := transmitKey{
		apiHost: ev.APIHost,
		apiKey:  ev.APIKey,
		dataset: ev.Dataset,
	}

	d.batchMutex.RLock()
	batch, exists := d.eventBatches[key]
	d.batchMutex.RUnlock()

	if !exists {
		d.batchMutex.Lock()
		batch, exists = d.eventBatches[key]
		if !exists {
			// Need to create new batch
			batch = &eventBatch{}
			d.eventBatches[key] = batch
		}
		d.batchMutex.Unlock()
	}

	// Add event to batch
	batch.mutex.Lock()
	if batch.events == nil {
		batch.events = make([]*types.Event, 0, d.maxBatchSize)
		batch.startTime = d.Clock.Now()
	}
	batch.events = append(batch.events, ev)
	shouldDispatch := len(batch.events) >= d.maxBatchSize
	if shouldDispatch {
		events := batch.events
		batch.events = nil
		batch.mutex.Unlock()

		d.dispatchPool.Go(func() {
			d.sendBatch(events)
		})
	} else {
		batch.mutex.Unlock()
	}

	d.Metrics.Up(d.metricKeys.updownQueuedItems)
}

func (d *DirectTransmission) EnqueueSpan(sp *types.Span) {
	// we don't need the trace ID anymore, but it's convenient to accept spans.
	d.EnqueueEvent(&sp.Event)
}

// RegisterMetrics registers the metrics used by the DirectTransmission.
// it should be called after the metrics object has been created.
func (d *DirectTransmission) RegisterMetrics() {
	for _, m := range transmissionMetrics {
		fullName := d.transmitType.String() + m.Name
		switch m.Name {
		case updownQueuedItems:
			d.metricKeys.updownQueuedItems = fullName
		case histogramQueueTime:
			d.metricKeys.histogramQueueTime = fullName
		case counterEnqueueErrors:
			d.metricKeys.counterEnqueueErrors = fullName
		case counterResponse20x:
			d.metricKeys.counterResponse20x = fullName
		case counterResponseErrors:
			d.metricKeys.counterResponseErrors = fullName
		// Below are metrics previously associated with the libhoney transmission used to send data upstream or to peers.
		// Even though libhoney isn't used, include the prefix in these metric names to avoid breaking existing Refinery operations boards & queries.
		case gaugeQueueLength:
			fullName = "libhoney_" + fullName
			d.metricKeys.gaugeQueueLength = fullName
		case counterSendErrors:
			fullName = "libhoney_" + fullName
			d.metricKeys.counterSendErrors = fullName
		case counterSendRetries:
			fullName = "libhoney_" + fullName
			d.metricKeys.counterSendRetries = fullName
		case counterBatchesSent:
			fullName = "libhoney_" + fullName
			d.metricKeys.counterBatchesSent = fullName
		case counterMessagesSent:
			fullName = "libhoney_" + fullName
			d.metricKeys.counterMessagesSent = fullName
		case counterResponseDecodeErrors:
			fullName = "libhoney_" + fullName
			d.metricKeys.counterResponseDecodeErrors = fullName
		}
		m.Name = fullName // Update the metric name to include the transmit type
		d.Metrics.Register(m)
	}
}

func (d *DirectTransmission) Stop() error {
	if d.stop != nil {
		close(d.stop)
	}
	d.stopWG.Wait()

	// Dispatch all remaining batches; we don't need locks here since no
	// further enqueues are possible.
	eventBatches := d.eventBatches
	d.eventBatches = nil
	for _, batch := range eventBatches {
		if len(batch.events) > 0 {
			d.dispatchPool.Go(func() {
				d.sendBatch(batch.events)
			})
		}
	}

	// Wait for all batch sends to complete
	if d.dispatchPool != nil {
		d.dispatchPool.Wait()
	}
	d.dispatchPool = nil
	d.stop = nil

	return nil
}

// handleBatchFailure handles metrics updates when the entire batch fails
func (d *DirectTransmission) handleBatchFailure(batch []*types.Event) {
	d.Metrics.Increment(d.metricKeys.counterSendErrors)
	for range batch {
		d.Metrics.Down(d.metricKeys.updownQueuedItems)
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
	d.Metrics.Increment(d.metricKeys.counterResponseErrors)
	d.Metrics.Down(d.metricKeys.updownQueuedItems)
	d.Metrics.Histogram(d.metricKeys.histogramQueueTime, float64(queueTime))
}

// Stores *[]byte instead of []byte to avoid having the slice headers themselves
// moved onto the heap on every Put().
var batchBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 16*1024)
		return &buf
	},
}

// Pool for bytes.Reader objects to avoid allocation on retries
var readerPool = sync.Pool{
	New: func() any {
		return &bytes.Reader{}
	},
}

// Sends one message or, if the batch is larger than what is allowed, several.
func (d *DirectTransmission) sendBatch(wholeBatch []*types.Event) {
	subBatch := make([]*types.Event, 0, len(wholeBatch))

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

		bufPtr := batchBufferPool.Get().(*[]byte)
		packed := append(*bufPtr, 0, 0, 0, 0, 0)
		subBatch = subBatch[:0]
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
				d.Metrics.Down(d.metricKeys.updownQueuedItems)
				d.Metrics.Increment(d.metricKeys.counterResponseErrors)
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

		// packed is now at its full size, we'll put that back in the pool.
		*bufPtr = packed[:0]
		defer batchBufferPool.Put(bufPtr)

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

		var resp *http.Response
		var compressedData []byte
		var zBufPtr *[]byte

		if d.enableCompression {
			zBufPtr = batchBufferPool.Get().(*[]byte)
			compressedData = zstdEncoder.EncodeAll(packed, *zBufPtr)
		}

		var req *http.Request
		readerPtr := readerPool.Get().(*bytes.Reader)
		defer readerPool.Put(readerPtr)

		for try := 0; try < 2; try++ {
			if try > 0 {
				d.Metrics.Increment(d.metricKeys.counterSendRetries)
			}

			if d.enableCompression {
				readerPtr.Reset(compressedData)
			} else {
				readerPtr.Reset(packed)
			}

			req, err = http.NewRequest("POST", apiURL.String(), readerPtr)
			if err != nil {
				d.Logger.Error().WithField("err", err.Error()).Logf("failed to create request")
				d.handleBatchFailure(subBatch)
				break
			}

			req.Header.Set("Content-Type", "application/msgpack")
			req.Header.Set("X-Honeycomb-Team", apiKey)
			req.Header.Set("User-Agent", d.userAgent)
			if d.enableCompression {
				req.Header.Set("Content-Encoding", "zstd")
			}

			resp, err = d.httpClient.Do(req)
			// Handle 429 or 503 with Retry-After
			if resp != nil && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable) {
				retryAfter := resp.Header.Get("Retry-After")
				sleepDur := time.Second // default 1s
				if retryAfter != "" {
					if secs, err := time.ParseDuration(retryAfter + "s"); err == nil {
						sleepDur = secs
					} else if t, err := http.ParseTime(retryAfter); err == nil {
						sleepDur = d.Clock.Until(t)
					}
				}
				if sleepDur > 0 && sleepDur < 60*time.Second {
					resp.Body.Close()
					d.Clock.Sleep(sleepDur)
					continue // retry in the loop
				}
			}

			if httpErr, ok := err.(httpError); ok && httpErr.Timeout() {
				continue
			}

			break
		}

		// Clean up compression buffer if used
		if d.enableCompression && zBufPtr != nil {
			*zBufPtr = compressedData[:0]
			batchBufferPool.Put(zBufPtr)
		}

		dequeuedAt := d.Clock.Now()

		if err != nil {
			d.Logger.Error().WithField("err", err.Error()).Logf("http POST failed")

			// Network/connection error - affects all events in batch
			for _, ev := range subBatch {
				queueTime := dequeuedAt.UnixMicro() - ev.EnqueuedUnixMicro
				d.handleEventError(ev, 0, queueTime, err.Error(), nil)
			}
			continue
		}

		d.Metrics.Increment(d.metricKeys.counterBatchesSent)
		d.Metrics.Count(d.metricKeys.counterMessagesSent, int64(len(subBatch)))

		// Parse batch response - following libhoney pattern where any status != 200 is an error for all events
		if resp.StatusCode == http.StatusOK {
			// Parse individual responses from batch - handle msgpack or JSON
			var batchResponses []batchResponse
			var err error
			if resp.Header.Get("Content-Type") == "application/msgpack" {
				err = msgpack.NewDecoder(resp.Body).Decode(&batchResponses)
				if err != nil {
					d.Logger.Error().WithField("err", err.Error()).Logf("failed to decode msgpack batch response")
				}
			} else {
				bodyBytes, err := io.ReadAll(resp.Body)
				if err != nil {
					d.Logger.Error().WithField("err", err.Error()).Logf("failed to read response body")
				} else {
					err = json.Unmarshal(bodyBytes, &batchResponses)
					if err != nil {
						d.Logger.Error().WithField("err", err.Error()).Logf("failed to decode JSON batch response")
					}
				}
			}
			resp.Body.Close()
			if err != nil {
				d.Metrics.Increment(d.metricKeys.counterResponseDecodeErrors)
			}

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
					d.Metrics.Increment(d.metricKeys.counterResponse20x)
					d.Metrics.Down(d.metricKeys.updownQueuedItems)
					d.Metrics.Histogram(d.metricKeys.histogramQueueTime, float64(queueTime))
				}
			}
		} else {
			// HTTP error - affects all events in batch
			var bodyBytes []byte
			d.Metrics.Increment(d.metricKeys.counterSendErrors)

			// Handle msgpack or JSON response body
			if resp.Header.Get("Content-Type") == "application/msgpack" {
				var errorBody interface{}
				decoder := msgpack.NewDecoder(resp.Body)
				err = decoder.Decode(&errorBody)
				if err == nil {
					bodyBytes, err = json.Marshal(&errorBody)
				}
			} else {
				bodyBytes, err = io.ReadAll(resp.Body)
			}
			resp.Body.Close()
			if err != nil {
				d.Metrics.Increment(d.metricKeys.counterResponseDecodeErrors)
			}

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

// dispatchStaleBatches runs on a ticker and dispatches all pending batches
func (d *DirectTransmission) dispatchStaleBatches() {
	defer d.stopWG.Done()

	batchTicker := d.Clock.NewTicker(d.batchTimeout / 4)
	defer batchTicker.Stop()

	metricsTicker := d.Clock.NewTicker(100 * time.Millisecond) // Static 100ms interval for metrics
	defer metricsTicker.Stop()

	var keys []transmitKey

	for {
		select {
		case <-batchTicker.Chan():
			now := d.Clock.Now()

			// Get a snapshot of all keys
			keys = keys[:0]
			d.batchMutex.RLock()
			for k := range d.eventBatches {
				keys = append(keys, k)
			}
			d.batchMutex.RUnlock()

			// Dispatch batches that are old enough
			for _, key := range keys {
				d.batchMutex.RLock()
				batch, exists := d.eventBatches[key]
				d.batchMutex.RUnlock()

				if !exists {
					continue
				}

				batch.mutex.Lock()
				batchCount := len(batch.events)
				if batchCount > 0 && now.Sub(batch.startTime) >= d.batchTimeout {
					events := batch.events
					batch.events = nil
					batch.mutex.Unlock()

					d.dispatchPool.Go(func() {
						d.sendBatch(events)
					})
				} else {
					batch.mutex.Unlock()
				}
			}

		case <-metricsTicker.Chan():
			// Calculate current pending count for metrics
			var pendingEventCount int64
			keys = keys[:0]
			d.batchMutex.RLock()
			for k := range d.eventBatches {
				keys = append(keys, k)
			}
			d.batchMutex.RUnlock()

			for _, key := range keys {
				d.batchMutex.RLock()
				batch, exists := d.eventBatches[key]
				d.batchMutex.RUnlock()

				if !exists {
					continue
				}

				batch.mutex.Lock()
				pendingEventCount += int64(len(batch.events))
				batch.mutex.Unlock()
			}
			d.Metrics.Gauge(d.metricKeys.gaugeQueueLength, float64(pendingEventCount))

		case <-d.stop:
			return
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

type httpError interface {
	Timeout() bool
}
