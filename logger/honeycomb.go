package logger

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/honeycombio/dynsampler-go"
	libhoney "github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/libhoney-go/transmission"

	"github.com/honeycombio/refinery/config"
)

// HoneycombLogger is a Logger implementation that sends all logs to a Honeycomb
// dataset. It requires a HoneycombLogger section of the config to exist with
// three keys, LoggerHoneycombAPI, LoggerAPIKey, and LoggerDataset.
type HoneycombLogger struct {
	Config            config.Config   `inject:""`
	UpstreamTransport *http.Transport `inject:"upstreamTransport"`
	Version           string          `inject:"version"`
	loggerConfig      config.HoneycombLoggerConfig
	libhClient        *libhoney.Client
	builder           *libhoney.Builder
	sampler           dynsampler.Sampler
}

type HoneycombEntry struct {
	loggerConfig config.HoneycombLoggerConfig
	builder      *libhoney.Builder
	sampler      dynsampler.Sampler
}

const (
	UnknownLevel config.HoneycombLevel = iota
	DebugLevel
	InfoLevel
	WarnLevel
	ErrorLevel
	PanicLevel
)

func (h *HoneycombLogger) Start() error {
	// logLevel is defined outside the HoneycombLogger section
	// and is set independently, before Start() is called, so we need to
	// preserve it.
	// TODO: make LogLevel part of the HoneycombLogger/LogrusLogger sections?
	logLevel := h.loggerConfig.Level
	loggerConfig, err := h.Config.GetHoneycombLoggerConfig()
	if err != nil {
		return err
	}
	loggerConfig.Level = logLevel
	h.loggerConfig = loggerConfig
	var loggerTx transmission.Sender
	if h.loggerConfig.LoggerAPIKey == "" {
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

	if loggerConfig.LoggerSamplerEnabled {
		h.sampler = &dynsampler.PerKeyThroughput{
			ClearFrequencySec:      10,
			PerKeyThroughputPerSec: loggerConfig.LoggerSamplerThroughput,
			MaxKeys:                1000,
		}
		err := h.sampler.Start()
		if err != nil {
			return err
		}
	}

	libhClientConfig := libhoney.ClientConfig{
		APIHost:      h.loggerConfig.LoggerHoneycombAPI,
		APIKey:       h.loggerConfig.LoggerAPIKey,
		Dataset:      h.loggerConfig.LoggerDataset,
		Transmission: loggerTx,
	}
	libhClient, err := libhoney.NewClient(libhClientConfig)
	if err != nil {
		return err
	}
	h.libhClient = libhClient

	if hostname, err := os.Hostname(); err == nil {
		h.libhClient.AddField("hostname", hostname)
	}
	startTime := time.Now()
	h.libhClient.AddDynamicField("process_uptime_seconds", func() interface{} {
		return time.Now().Sub(startTime) / time.Second
	})

	h.builder = h.libhClient.NewBuilder()

	// listen for responses from honeycomb, log to STDOUT if something unusual
	// comes back
	go h.readResponses()

	// listen for config reloads
	h.Config.RegisterReloadCallback(h.reloadBuilder)

	fmt.Printf("Starting Honeycomb Logger - see Honeycomb %s dataset for service logs\n", h.loggerConfig.LoggerDataset)

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

func (h *HoneycombLogger) reloadBuilder() {
	h.Debug().Logf("reloading config for Honeycomb logger")
	// preseve log level
	logLevel := h.loggerConfig.Level
	loggerConfig, err := h.Config.GetHoneycombLoggerConfig()
	if err != nil {
		// complain about this both to STDOUT and to the previously configured
		// honeycomb logger
		fmt.Printf("failed to reload configs for Honeycomb logger: %+v\n", err)
		h.Error().Logf("failed to reload configs for Honeycomb logger: %+v", err)
		return
	}
	loggerConfig.Level = logLevel
	h.loggerConfig = loggerConfig
	h.builder.APIHost = h.loggerConfig.LoggerHoneycombAPI
	h.builder.WriteKey = h.loggerConfig.LoggerAPIKey
	h.builder.Dataset = h.loggerConfig.LoggerDataset
}

func (h *HoneycombLogger) Stop() error {
	fmt.Printf("stopping honey logger\n")
	libhoney.Flush()
	return nil
}

func (h *HoneycombLogger) Debug() Entry {
	if h.loggerConfig.Level > DebugLevel {
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
	if h.loggerConfig.Level > InfoLevel {
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

func (h *HoneycombLogger) Error() Entry {
	if h.loggerConfig.Level > ErrorLevel {
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

func (h *HoneycombLogger) SetLevel(level string) error {
	sanitizedLevel := strings.TrimSpace(strings.ToLower(level))
	var lvl config.HoneycombLevel
	switch sanitizedLevel {
	case "debug":
		lvl = DebugLevel
	case "info":
		lvl = InfoLevel
	case "warn", "warning":
		lvl = WarnLevel
	case "error":
		lvl = ErrorLevel
	case "panic":
		lvl = PanicLevel
	default:
		return errors.New(fmt.Sprintf("unrecognized logging level: %s", level))
	}
	h.loggerConfig.Level = lvl
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
	ev.Metadata = map[string]string{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}
	level, ok := ev.Fields()["level"].(string)
	if !ok {
		level = "unknown"
	}
	if h.sampler != nil {
		rate := h.sampler.GetSampleRate(fmt.Sprintf(`%s:%s`, level, msg))
		ev.SampleRate = uint(rate)
	}
	ev.Send()
}
