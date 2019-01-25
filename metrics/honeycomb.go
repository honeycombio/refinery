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

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
)

type HoneycombMetrics struct {
	Config     config.Config `inject:""`
	Logger     logger.Logger `inject:""`
	HTTPClient *http.Client  `inject:"upstreamClient"`
	Version    string        `inject:"version"`

	countersLock   sync.Mutex
	counters       map[string]*counter
	gaugesLock     sync.Mutex
	gauges         map[string]*gauge
	histogramsLock sync.Mutex
	histograms     map[string]*histogram

	builder *libhoney.Builder

	latestMemStatsLock sync.RWMutex
	latestMemStats     runtime.MemStats

	//reportingFreq is the interval with which to report statistics
	reportingFreq       int64
	reportingCancelFunc func()
}

type MetricsConfig struct {
	MetricsHoneycombAPI      string
	MetricsAPIKey            string
	MetricsDataset           string
	MetricsReportingInterval int64
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

func (h *HoneycombMetrics) Start() error {
	mc := MetricsConfig{}
	err := h.Config.GetOtherConfig("HoneycombMetrics", &mc)
	if err != nil {
		return err
	}
	if mc.MetricsReportingInterval < 1 {
		mc.MetricsReportingInterval = 1
	}
	h.reportingFreq = mc.MetricsReportingInterval

	libhConfig := libhoney.Config{
		APIHost:   mc.MetricsHoneycombAPI,
		Transport: h.HTTPClient.Transport,
		// Logger:    &libhoney.DefaultLogger{},
	}
	libhoney.Init(libhConfig)
	libhoney.UserAgentAddition = "samproxy/" + h.Version
	h.builder = libhoney.NewBuilder()
	h.builder.APIHost = mc.MetricsHoneycombAPI
	h.builder.WriteKey = mc.MetricsAPIKey
	h.builder.Dataset = mc.MetricsDataset

	// add some general go metrics to every report
	// goroutines
	if hostname, err := os.Hostname(); err == nil {
		h.builder.AddField("hostname", hostname)
	}
	h.builder.AddDynamicField("num_goroutines",
		func() interface{} { return runtime.NumGoroutine() })
	ctx, cancel := context.WithCancel(context.Background())
	h.reportingCancelFunc = cancel
	go h.refreshMemStats(ctx)
	getAlloc := func() interface{} {
		var mem runtime.MemStats
		h.readMemStats(&mem)
		return mem.Alloc
	}
	h.builder.AddDynamicField("memory_inuse", getAlloc)
	startTime := time.Now()
	h.builder.AddDynamicField("process_uptime_seconds", func() interface{} {
		return time.Now().Sub(startTime) / time.Second
	})

	h.counters = make(map[string]*counter)
	h.gauges = make(map[string]*gauge)
	h.histograms = make(map[string]*histogram)

	go h.reportToHoneycommb(ctx)

	// listen for config reloads
	h.Config.RegisterReloadCallback(h.reloadBuilder)

	return nil
}

func (h *HoneycombMetrics) reloadBuilder() {
	mc := MetricsConfig{}
	err := h.Config.GetOtherConfig("HoneycombMetrics", &mc)
	if err != nil {
		// complain about this both to STDOUT and to the previously configured
		// honeycomb logger
		h.Logger.Errorf("failed to reload configs for Honeycomb metrics: %+v\n", err)
		return
	}
	h.builder.APIHost = mc.MetricsHoneycombAPI
	h.builder.WriteKey = mc.MetricsAPIKey
	h.builder.Dataset = mc.MetricsDataset
	if h.reportingFreq != mc.MetricsReportingInterval {
		// changed reporting interval
		h.reportingFreq = mc.MetricsReportingInterval
		// cancel the two reporting goroutines and restart them
		h.reportingCancelFunc()
		ctx, cancel := context.WithCancel(context.Background())
		h.reportingCancelFunc = cancel
		go h.refreshMemStats(ctx)
		go h.reportToHoneycommb(ctx)
	}
}

// refreshMemStats caches memory statistics to avoid blocking sending honeycomb
// metrics on gc pauses
func (h *HoneycombMetrics) refreshMemStats(ctx context.Context) {
	// get memory metrics 5 times more frequently than we send metrics to make sure
	// we have relatively up to date mem statistics but not go wild and get them
	// all the time.
	// for _ = range  {
	ticker := time.NewTicker(time.Duration(h.reportingFreq*1000/5) * time.Millisecond)
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
			h.Logger.Debugf("restarting refreshMemStats goroutine")
			return
		}
	}
}

// readMemStats is a drop-in replacement for runtime.ReadMemStats which won't
// block waiting for a GC to finish.
func (h *HoneycombMetrics) readMemStats(mem *runtime.MemStats) {
	h.latestMemStatsLock.RLock()
	defer h.latestMemStatsLock.RUnlock()

	*mem = h.latestMemStats
}

func (h *HoneycombMetrics) reportToHoneycommb(ctx context.Context) {
	tick := time.NewTicker(time.Duration(h.reportingFreq) * time.Second)
	for {
		select {
		case <-ctx.Done():
			// context canceled? we're being asked to stop this so it can be restarted.
			return
		case <-tick.C:
			ev := h.builder.NewEvent()
			h.countersLock.Lock()
			for _, count := range h.counters {
				count.lock.Lock()
				ev.AddField(count.name, count.val)
				count.val = 0
				count.lock.Unlock()
			}
			h.countersLock.Unlock()

			h.gaugesLock.Lock()
			for _, gauge := range h.gauges {
				gauge.lock.Lock()
				ev.AddField(gauge.name, gauge.val)
				// gauges should remain where they are until changed
				// gauge.val = 0
				gauge.lock.Unlock()
			}
			h.gaugesLock.Unlock()

			h.histogramsLock.Lock()
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
			h.histogramsLock.Unlock()

			ev.Send()
		}
	}
}

func average(vals []float64) float64 {
	var total float64
	for _, val := range vals {
		total += val
	}
	return total / float64(len(vals))
}

func (h *HoneycombMetrics) Register(name string, metricType string) {
	switch metricType {
	case "counter":
		h.countersLock.Lock()
		defer h.countersLock.Unlock()
		// inside the lock, let's not race to create the counter
		_, ok := h.counters[name]
		if !ok {
			newCounter := &counter{
				name: name,
			}
			h.counters[name] = newCounter
		}
	case "gauge":
		h.gaugesLock.Lock()
		defer h.gaugesLock.Unlock()
		_, ok := h.gauges[name]
		if !ok {
			newGauge := &gauge{
				name: name,
			}
			h.gauges[name] = newGauge
		}
	case "histogram":
		h.histogramsLock.Lock()
		defer h.histogramsLock.Unlock()
		_, ok := h.histograms[name]
		if !ok {
			newGauge := &histogram{
				name: name,
				vals: make([]float64, 0),
			}
			h.histograms[name] = newGauge
		}
	default:
		h.Logger.Debugf("unspported metric type %s", metricType)
	}
}

func (h *HoneycombMetrics) IncrementCounter(name string) {
	count, ok := h.counters[name]
	if !ok {
		h.Register(name, "counter")
		count = h.counters[name]
	}
	count.lock.Lock()
	defer count.lock.Unlock()
	count.val++
}

func (h *HoneycombMetrics) Gauge(name string, val float64) {
	gauge, ok := h.gauges[name]
	if !ok {
		h.Register(name, "gauge")
		gauge = h.gauges[name]
	}
	gauge.lock.Lock()
	defer gauge.lock.Unlock()
	gauge.val = val
}

func (h *HoneycombMetrics) Histogram(name string, obs float64) {
	histogram, ok := h.histograms[name]
	if !ok {
		h.Register(name, "histogram")
		histogram = h.histograms[name]
	}
	histogram.lock.Lock()
	defer histogram.lock.Unlock()
	histogram.vals = append(histogram.vals, obs)
}
