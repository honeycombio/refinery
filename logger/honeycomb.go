package logger

import (
	"errors"
	"fmt"
	"os"
	"strings"

	libhoney "github.com/honeycombio/libhoney-go"

	"github.com/honeycombio/samproxy/config"
)

// HoneycombLogger is a Logger implementation that sends all logs to a Honeycomb
// dataset. It requires a HoneycombLogger section of the config to exist with
// three keys, LoggerHoneycombAPI, LoggerAPIKey, and LoggerDataset.
type HoneycombLogger struct {
	Config       config.Config `inject:""`
	loggerConfig HoneycombLoggerConfig
	builder      *libhoney.Builder
}

type HoneycombLoggerConfig struct {
	LoggerHoneycombAPI string
	LoggerAPIKey       string
	LoggerDataset      string

	level HoneycombLevel
}

type HoneycombEntry struct {
	loggerConfig HoneycombLoggerConfig
	builder      *libhoney.Builder
}

type HoneycombLevel int

const (
	UnknownLevel HoneycombLevel = iota
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
	logLevel := h.loggerConfig.level
	loggerConfig := HoneycombLoggerConfig{}
	err := h.Config.GetOtherConfig("HoneycombLogger", &loggerConfig)
	if err != nil {
		return err
	}
	loggerConfig.level = logLevel
	h.loggerConfig = loggerConfig
	if h.loggerConfig.LoggerAPIKey != "" {
		libhConf := libhoney.Config{
			APIHost:  h.loggerConfig.LoggerHoneycombAPI,
			WriteKey: h.loggerConfig.LoggerAPIKey,
			// Output:   &libhoney.WriterOutput{},
			// Logger: &libhoney.DefaultLogger{},
			BlockOnSend:     true,
			BlockOnResponse: true,
		}
		libhoney.Init(libhConf)
	}
	h.builder = libhoney.NewBuilder()
	h.builder.Dataset = h.loggerConfig.LoggerDataset

	if hostname, err := os.Hostname(); err == nil {
		h.builder.AddField("hostname", hostname)
	}

	// listen for config reloads
	h.Config.RegisterReloadCallback(h.reloadBuilder)

	return nil
}

func (h *HoneycombLogger) reloadBuilder() {
	// preseve log level
	logLevel := h.loggerConfig.level
	loggerConfig := HoneycombLoggerConfig{}
	err := h.Config.GetOtherConfig("HoneycombLogger", &loggerConfig)
	if err != nil {
		// complain about this both to STDOUT and to the previously configured
		// honeycomb logger
		fmt.Printf("failed to reload configs for Honeycomb logger: %+v\n", err)
		h.Errorf("failed to reload configs for Honeycomb logger: %+v", err)
		return
	}
	loggerConfig.level = logLevel
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

func (h *HoneycombLogger) WithField(key string, value interface{}) Entry {
	entry := &HoneycombEntry{
		loggerConfig: h.loggerConfig,
		builder:      h.builder.Clone(),
	}
	entry.builder.AddField(key, value)
	return entry
}

func (h *HoneycombLogger) WithFields(fields map[string]interface{}) Entry {
	entry := &HoneycombEntry{
		loggerConfig: h.loggerConfig,
		builder:      h.builder.Clone(),
	}
	entry.builder.Add(fields)
	return entry
}

func (h *HoneycombLogger) Debugf(f string, args ...interface{}) {
	if h.loggerConfig.level > DebugLevel {
		return
	}
	ev := h.builder.NewEvent()
	ev.AddField("level", "debug")
	ev.AddField("msg", fmt.Sprintf(f, args...))
	ev.Metadata = map[string]string{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}
	ev.Send()
}

func (h *HoneycombLogger) Infof(f string, args ...interface{}) {
	if h.loggerConfig.level > InfoLevel {
		return
	}
	ev := h.builder.NewEvent()
	ev.AddField("level", "info")
	ev.AddField("msg", fmt.Sprintf(f, args...))
	ev.Metadata = map[string]string{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}
	ev.Send()
}

func (h *HoneycombLogger) Errorf(f string, args ...interface{}) {
	if h.loggerConfig.level > ErrorLevel {
		return
	}
	ev := h.builder.NewEvent()
	ev.AddField("level", "error")
	ev.AddField("msg", fmt.Sprintf(f, args...))
	ev.Metadata = map[string]string{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}
	ev.Send()
}

func (h *HoneycombLogger) SetLevel(level string) error {
	sanitizedLevel := strings.TrimSpace(strings.ToLower(level))
	var lvl HoneycombLevel
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
	h.loggerConfig.level = lvl
	return nil
}

func (h *HoneycombEntry) WithField(key string, value interface{}) Entry {
	h.builder.AddField(key, value)
	return h
}

func (h *HoneycombEntry) WithFields(fields map[string]interface{}) Entry {
	h.builder.Add(fields)
	return h
}

func (h *HoneycombEntry) Debugf(f string, args ...interface{}) {
	if h.loggerConfig.level > DebugLevel {
		return
	}
	ev := h.builder.NewEvent()
	ev.AddField("level", "debug")
	ev.AddField("msg", fmt.Sprintf(f, args...))
	ev.Metadata = map[string]string{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}
	ev.Send()
}

func (h *HoneycombEntry) Infof(f string, args ...interface{}) {
	if h.loggerConfig.level > InfoLevel {
		return
	}
	ev := h.builder.NewEvent()
	ev.AddField("level", "info")
	ev.AddField("msg", fmt.Sprintf(f, args...))
	ev.Metadata = map[string]string{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}
	ev.Send()
}

func (h *HoneycombEntry) Errorf(f string, args ...interface{}) {
	if h.loggerConfig.level > ErrorLevel {
		return
	}
	ev := h.builder.NewEvent()
	ev.AddField("level", "error")
	ev.AddField("msg", fmt.Sprintf(f, args...))
	ev.Metadata = map[string]string{
		"api_host": ev.APIHost,
		"dataset":  ev.Dataset,
	}
	ev.Send()
}
