package metrics

import (
	"context"
	"math"
	"net/http"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
)

type LegacyMetrics struct {
	Config            config.Config   `inject:""`
	Logger            logger.Logger   `inject:""`
	UpstreamTransport *http.Transport `inject:"upstreamTransport"`
	Version           string          `inject:"version"`

	lock       sync.RWMutex
	counters   map[string]*counter
	gauges     map[string]*gauge
	histograms map[string]*histogram
	updowns    map[string]*updown
	constants  map[string]*gauge

	libhClient *libhoney.Client

	latestMemStatsLock sync.RWMutex
	latestMemStats     runtime.MemStats

	//reportingFreq is the interval with which to report statistics
	reportingFreq       time.Duration
	reportingCancelFunc func()
}

type counter struct {
	lock sync.Mutex
	name string
	val  int
}

type gauge struct {
	lock sync.Mutex
	name string
	val  float64
}

type histogram struct {
	lock sync.Mutex
	name string
	vals []float64
}

type updown struct {
	lock sync.Mutex
	name string
	val  int
}

func (h *LegacyMetrics) Start() error {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.Logger.Debug().Logf("Starting LegacyMetrics")
	defer func() { h.Logger.Debug().Logf("Finished starting LegacyMetrics") }()
	mc := h.Config.GetLegacyMetricsConfig()
	if mc.ReportingInterval < config.Duration(1*time.Second) {
		mc.ReportingInterval = config.Duration(1 * time.Second)
	}
	h.reportingFreq = time.Duration(mc.ReportingInterval)

	if err := h.initLibhoney(mc); err != nil {
		return err
	}

	h.counters = make(map[string]*counter)
	h.gauges = make(map[string]*gauge)
	h.histograms = make(map[string]*histogram)
	h.updowns = make(map[string]*updown)
	h.constants = make(map[string]*gauge)

	// listen for config reloads
	h.Config.RegisterReloadCallback(h.reloadBuilder)

	return nil
}

func (h *LegacyMetrics) reloadBuilder(cfgHash, ruleHash string) {
	h.Logger.Debug().Logf("reloading config for honeycomb metrics reporter")
	mc := h.Config.GetLegacyMetricsConfig()
	h.libhClient.Close()
	// cancel the two reporting goroutines and restart them
	h.reportingCancelFunc()
	h.initLibhoney(mc)
}

func (h *LegacyMetrics) initLibhoney(mc config.LegacyMetricsConfig) error {
	metricsTx := &transmission.Honeycomb{
		// metrics are always sent as a single event, so don't wait for the timeout
		MaxBatchSize:      1,
		BlockOnSend:       true,
		UserAgentAddition: "refinery/" + h.Version + " (metrics)",
		Transport:         h.UpstreamTransport,
	}
	libhClientConfig := libhoney.ClientConfig{
		APIHost:      mc.APIHost,
		APIKey:       mc.APIKey,
		Dataset:      mc.Dataset,
		Transmission: metricsTx,
	}
	libhClient, err := libhoney.NewClient(libhClientConfig)
	if err != nil {
		return err
	}
	h.libhClient = libhClient

	h.libhClient.AddField("refinery_version", h.Version)
	// add some general go metrics to every report
	// goroutines
	if hostname, err := os.Hostname(); err == nil {
		h.libhClient.AddField("hostname", hostname)
	}
	h.libhClient.AddDynamicField("num_goroutines",
		func() interface{} { return runtime.NumGoroutine() })
	ctx, cancel := context.WithCancel(context.Background())
	h.reportingCancelFunc = cancel
	go h.refreshMemStats(ctx)
	go h.readResponses(ctx)
	getAlloc := func() interface{} {
		var mem runtime.MemStats
		h.readMemStats(&mem)
		return mem.Alloc
	}
	h.libhClient.AddDynamicField("memory_inuse", getAlloc)
	startTime := time.Now()
	h.libhClient.AddDynamicField("process_uptime_seconds", func() interface{} {
		return time.Since(startTime) / time.Second
	})
	go h.reportToHoneycomb(ctx)
	return nil
}

// refreshMemStats caches memory statistics to avoid blocking sending honeycomb
// metrics on gc pauses
func (h *LegacyMetrics) refreshMemStats(ctx context.Context) {
	// get memory metrics 5 times more frequently than we send metrics to make sure
	// we have relatively up to date mem statistics but not go wild and get them
	// all the time.
	ticker := time.NewTicker(h.reportingFreq / 5)
	for {
		select {
		case <-ticker.C:
			// Blocks if GC is running, maybe for a *looong* time.
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)

			h.latestMemStatsLock.Lock()
			h.latestMemStats = mem
			h.latestMemStatsLock.Unlock()
		case <-ctx.Done():
			// context canceled? we're being asked to stop this so it can be restarted.
			h.Logger.Debug().Logf("restarting honeycomb metrics refreshMemStats goroutine")
			return
		}
	}
}

// readResponses reads the responses from the libhoney responses queue and logs
// any errors that come down it
func (h *LegacyMetrics) readResponses(ctx context.Context) {
	resps := h.libhClient.TxResponses()
	for {
		select {
		case resp := <-resps:
			// read response, log if there's an error
			var msg string
			var log logger.Entry
			switch {
			case resp.Err != nil:
				msg = "Metrics reporter got an error back from Honeycomb"
				log = h.Logger.Error().WithField("error", resp.Err.Error())
			case resp.StatusCode > 202:
				msg = "Metrics reporter got an unexpected status code back from Honeycomb"
				log = h.Logger.Error()
			}
			if log != nil {
				log.WithFields(map[string]interface{}{
					"status_code": resp.StatusCode,
					"body":        string(resp.Body),
					"duration":    resp.Duration,
				}).Logf(msg)
			}
		case <-ctx.Done():
			// bail out; we're refreshing the config and will launch a new
			// response reader.
			h.Logger.Debug().Logf("restarting honeycomb metrics read libhoney responses goroutine")
			return
		}
	}
}

// readMemStats is a drop-in replacement for runtime.ReadMemStats which won't
// block waiting for a GC to finish.
func (h *LegacyMetrics) readMemStats(mem *runtime.MemStats) {
	h.latestMemStatsLock.RLock()
	defer h.latestMemStatsLock.RUnlock()

	*mem = h.latestMemStats
}

func (h *LegacyMetrics) reportToHoneycomb(ctx context.Context) {
	tick := time.NewTicker(h.reportingFreq)
	for {
		select {
		case <-ctx.Done():
			// context canceled? we're being asked to stop this so it can be restarted.
			return
		case <-tick.C:
			ev := h.libhClient.NewEvent()
			ev.Metadata = map[string]any{
				"api_host": ev.APIHost,
				"dataset":  ev.Dataset,
			}

			h.lock.RLock()
			for _, count := range h.counters {
				count.lock.Lock()
				ev.AddField(count.name, count.val)
				count.val = 0
				count.lock.Unlock()
			}

			for _, updown := range h.updowns {
				updown.lock.Lock()
				ev.AddField(updown.name, updown.val)
				// updown.val = 0   // updowns are never reset to 0
				updown.lock.Unlock()
			}

			for _, gauge := range h.gauges {
				gauge.lock.Lock()
				ev.AddField(gauge.name, gauge.val)
				// gauges should remain where they are until changed
				// gauge.val = 0
				gauge.lock.Unlock()
			}

			for _, histogram := range h.histograms {
				histogram.lock.Lock()
				if len(histogram.vals) != 0 {
					sort.Float64s(histogram.vals)
					p50Index := int(math.Floor(float64(len(histogram.vals)) * 0.5))
					p95Index := int(math.Floor(float64(len(histogram.vals)) * 0.95))
					p99Index := int(math.Floor(float64(len(histogram.vals)) * 0.99))
					ev.AddField(histogram.name+"_p50", histogram.vals[p50Index])
					ev.AddField(histogram.name+"_p95", histogram.vals[p95Index])
					ev.AddField(histogram.name+"_p99", histogram.vals[p99Index])
					ev.AddField(histogram.name+"_min", histogram.vals[0])
					ev.AddField(histogram.name+"_max", histogram.vals[len(histogram.vals)-1])
					ev.AddField(histogram.name+"_avg", average(histogram.vals))
					histogram.vals = histogram.vals[:0]
				}
				histogram.lock.Unlock()
			}
			h.lock.RUnlock()

			ev.Send()
		}
	}
}

// Retrieves the current value of a gauge, constant, counter, or updown as a float64
// (even if it's an integer value). Returns 0 if the name isn't found.
func (h *LegacyMetrics) Get(name string) (float64, bool) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	if m, ok := h.counters[name]; ok {
		return float64(m.val), true
	}
	if m, ok := h.updowns[name]; ok {
		return float64(m.val), true
	}
	if m, ok := h.gauges[name]; ok {
		return m.val, true
	}
	if m, ok := h.constants[name]; ok {
		return m.val, true
	}
	return 0, false
}

func average(vals []float64) float64 {
	var total float64
	for _, val := range vals {
		total += val
	}
	return total / float64(len(vals))
}

func (h *LegacyMetrics) Register(name string, metricType string) {
	h.Logger.Debug().Logf("metrics registering %s with name %s", metricType, name)
	switch metricType {
	case "counter":
		getOrAdd(&h.lock, name, h.counters, createCounter)
	case "gauge":
		getOrAdd(&h.lock, name, h.gauges, createGauge)
	case "histogram":
		getOrAdd(&h.lock, name, h.histograms, createHistogram)
	case "updown":
		getOrAdd(&h.lock, name, h.updowns, createUpdown)
	default:
		h.Logger.Debug().Logf("unsupported metric type %s", metricType)
	}
}

// getOrAdd attempts to retrieve a (generic) metric from the provided map by name, wrapping the read operation
// with a read lock (RLock). If the metric is not present in the map, it acquires a write lock and executes
// a create function to add it to the map.
func getOrAdd[T *counter | *gauge | *histogram | *updown](lock *sync.RWMutex, name string, metrics map[string]T, createMetric func(name string) T) T {
	// attempt to get metric by name using read lock
	lock.RLock()
	metric, ok := metrics[name]
	lock.RUnlock()

	// if found, return existing metric
	if ok {
		return metric
	}

	// acquire write lock
	lock.Lock()
	// check again to see if it's been added while waiting for write lock
	metric, ok = metrics[name]
	if !ok {
		// create new metric using create function and add to map
		metric = createMetric(name)
		metrics[name] = metric
	}
	lock.Unlock()
	return metric
}

func createCounter(name string) *counter {
	return &counter{
		name: name,
	}
}

func createGauge(name string) *gauge {
	return &gauge{
		name: name,
	}
}

func createHistogram(name string) *histogram {
	return &histogram{
		name: name,
		vals: make([]float64, 0),
	}
}

func createUpdown(name string) *updown {
	return &updown{
		name: name,
	}
}

func (h *LegacyMetrics) Count(name string, n interface{}) {
	counter := getOrAdd(&h.lock, name, h.counters, createCounter)

	// update value, using counter's lock
	counter.lock.Lock()
	counter.val = counter.val + int(ConvertNumeric(n))
	counter.lock.Unlock()
}

func (h *LegacyMetrics) Increment(name string) {
	h.Count(name, 1)
}

func (h *LegacyMetrics) Gauge(name string, val interface{}) {
	gauge := getOrAdd(&h.lock, name, h.gauges, createGauge)

	// update value, using gauge's lock
	gauge.lock.Lock()
	gauge.val = ConvertNumeric(val)
	gauge.lock.Unlock()
}

func (h *LegacyMetrics) Histogram(name string, obs interface{}) {
	histogram := getOrAdd(&h.lock, name, h.histograms, createHistogram)

	// update value, using histogram's lock
	histogram.lock.Lock()
	histogram.vals = append(histogram.vals, ConvertNumeric(obs))
	histogram.lock.Unlock()
}

func (h *LegacyMetrics) Up(name string) {
	counter := getOrAdd(&h.lock, name, h.updowns, createUpdown)

	// update value, using counter's lock
	counter.lock.Lock()
	counter.val++
	counter.lock.Unlock()
}

func (h *LegacyMetrics) Down(name string) {
	counter := getOrAdd(&h.lock, name, h.updowns, createUpdown)

	// update value, using counter's lock
	counter.lock.Lock()
	counter.val--
	counter.lock.Unlock()
}

func (h *LegacyMetrics) Store(name string, val float64) {
	constant := getOrAdd(&h.lock, name, h.constants, createGauge)

	// update value, using constant's lock
	constant.lock.Lock()
	constant.val = ConvertNumeric(val)
	constant.lock.Unlock()
}
