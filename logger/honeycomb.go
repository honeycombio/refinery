package logger

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/honeycombio/dynsampler-go"
	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"

	"github.com/honeycombio/refinery/config"
)

// HoneycombLogger is a Logger implementation that sends all logs to a Honeycomb
// dataset.
type HoneycombLogger struct {
	Config            config.Config   `inject:""`
	UpstreamTransport *http.Transport `inject:"upstreamTransport"`
	Version           string          `inject:"version"`
	level             config.Level
	loggerConfig      config.HoneycombLoggerConfig
	libhClient        *libhoney.Client
	builder           *libhoney.Builder
	sampler           dynsampler.Sampler
}

var _ = Logger((*HoneycombLogger)(nil))

type HoneycombEntry struct {
	loggerConfig config.HoneycombLoggerConfig
	builder      *libhoney.Builder
	sampler      dynsampler.Sampler
}

func (h *HoneycombLogger) Start() error {
	// logLevel is defined outside the HoneycombLogger section
	// and is set independently, before Start() is called, so we need to
	// preserve it.
	// TODO: make LogLevel part of the HoneycombLogger/LogrusLogger sections?
	h.level = h.Config.GetLoggerLevel()
	loggerConfig := h.Config.GetHoneycombLoggerConfig()
	h.loggerConfig = loggerConfig
	var loggerTx transmission.Sender
	if h.loggerConfig.APIKey == "" {
		loggerTx = &transmission.DiscardSender{}
	} else {
		loggerTx = &transmission.Honeycomb{
			// logs are often sent in flurries; flush every half second
			MaxBatchSize:        100,
			BatchTimeout:        500 * time.Millisecond,
			UserAgentAddition:   "refinery/" + h.Version + " (metrics)",
			Transport:           h.UpstreamTransport,
			PendingWorkCapacity: libhoney.DefaultPendingWorkCapacity,
		}
	}

	if loggerConfig.GetSamplerEnabled() {
		h.sampler = &dynsampler.PerKeyThroughput{
			ClearFrequencyDuration: 10 * time.Second,
			PerKeyThroughputPerSec: loggerConfig.SamplerThroughput,
			MaxKeys:                1000,
		}
		err := h.sampler.Start()
		if err != nil {
			return err
		}
	}

	libhClientConfig := libhoney.ClientConfig{
		APIHost:      h.loggerConfig.APIHost,
		APIKey:       h.loggerConfig.APIKey,
		Dataset:      h.loggerConfig.Dataset,
		Transmission: loggerTx,
	}
	libhClient, err := libhoney.NewClient(libhClientConfig)
	if err != nil {
		return err
	}
	h.libhClient = libhClient

	h.libhClient.AddField("refinery_version", h.Version)
	if hostname, err := os.Hostname(); err == nil {
		h.libhClient.AddField("hostname", hostname)
	}
	startTime := time.Now()
	h.libhClient.AddDynamicField("process_uptime_seconds", func() interface{} {
		return time.Since(startTime) / time.Second
	})

	h.builder = h.libhClient.NewBuilder()

	// listen for responses from honeycomb, log to STDOUT if something unusual
	// comes back
	go h.readResponses()

	// listen for config reloads
	h.Config.RegisterReloadCallback(h.reloadBuilder)

	fmt.Printf("Starting Honeycomb Logger - see Honeycomb %s dataset for service logs\n", h.loggerConfig.Dataset)

	return nil
}

func (h *HoneycombLogger) readResponses() {
	resps := h.libhClient.TxResponses()
	for resp := range resps {
		respString := fmt.Sprintf("Response: status: %d, duration: %s", resp.StatusCode, resp.Duration)
		// read response, log if there's an error
		switch {
		case resp.StatusCode == 0: // log message dropped due to sampling
			continue
		case resp.Err != nil:
			fmt.Fprintf(os.Stderr, "Honeycomb Logger got an error back from Honeycomb while trying to send a log line: %s, error: %s, body: %s\n", respString, resp.Err.Error(), string(resp.Body))
		case resp.StatusCode > 202:
			fmt.Fprintf(os.Stderr, "Honeycomb Logger got an unexpected status code back from Honeycomb while trying to send a log line: %s, %s\n", respString, string(resp.Body))
		}
	}
}

func (h *HoneycombLogger) reloadBuilder(cfgHash, ruleHash string) {
	h.Debug().Logf("reloading config for Honeycomb logger")
	// preserve log level
	h.level = h.Config.GetLoggerLevel()
	loggerConfig := h.Config.GetHoneycombLoggerConfig()
	h.loggerConfig = loggerConfig
	h.builder.APIHost = h.loggerConfig.APIHost
	h.builder.WriteKey = h.loggerConfig.APIKey
	h.builder.Dataset = h.loggerConfig.Dataset
}

func (h *HoneycombLogger) Stop() error {
	fmt.Printf("stopping honey logger\n")
	libhoney.Flush()
	return nil
}

func (h *HoneycombLogger) Debug() Entry {
	if h.level > config.DebugLevel {
		return nullEntry
	}

	ev := &HoneycombEntry{
		loggerConfig: h.loggerConfig,
		builder:      h.builder.Clone(),
		sampler:      h.sampler,
	}
	ev.builder.AddField("level", "debug")

	return ev
}

func (h *HoneycombLogger) Info() Entry {
	if h.level > config.InfoLevel {
		return nullEntry
	}

	ev := &HoneycombEntry{
		loggerConfig: h.loggerConfig,
		builder:      h.builder.Clone(),
		sampler:      h.sampler,
	}
	ev.builder.AddField("level", "info")

	return ev
}

func (h *HoneycombLogger) Warn() Entry {
	if h.level > config.WarnLevel {
		return nullEntry
	}

	ev := &HoneycombEntry{
		loggerConfig: h.loggerConfig,
		builder:      h.builder.Clone(),
		sampler:      h.sampler,
	}
	ev.builder.AddField("level", "warn")

	return ev
}

func (h *HoneycombLogger) Error() Entry {
	if h.level > config.ErrorLevel {
		return nullEntry
	}

	ev := &HoneycombEntry{
		loggerConfig: h.loggerConfig,
		builder:      h.builder.Clone(),
		sampler:      h.sampler,
	}
	ev.builder.AddField("level", "error")

	return ev
}

func (h *HoneycombLogger) SetLevel(lvl string) error {
	h.level = config.ParseLevel(lvl)
	return nil
}

func (h *HoneycombEntry) WithField(key string, value interface{}) Entry {
	h.builder.AddField(key, value)
	return h
}

func (h *HoneycombEntry) WithString(key string, value string) Entry {
	return h.WithField(key, value)
}

func (h *HoneycombEntry) WithFields(fields map[string]interface{}) Entry {
	h.builder.Add(fields)
	return h
}

func (h *HoneycombEntry) Logf(f string, args ...interface{}) {
	ev := h.builder.NewEvent()
	msg := fmt.Sprintf(f, args...)
	ev.AddField("msg", msg)
	ev.Metadata = map[string]any{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}
	level, ok := ev.Fields()["level"].(string)
	if !ok {
		level = "unknown"
	}
	if h.sampler != nil {
		// use the level and the format string as the key for the sampler
		// this allows us to avoid sampling on high-cardinality fields in the message
		rate := h.sampler.GetSampleRate(fmt.Sprintf(`%s:%s`, level, f))
		ev.SampleRate = uint(rate)
	}
	ev.Send()
}
