package logger

import (
	"errors"
	"fmt"
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
	loggerConfig := HoneycombLoggerConfig{}
	err := h.Config.GetOtherConfig("HoneycombLogger", &loggerConfig)
	if err != nil {
		return err
	}
	h.loggerConfig = loggerConfig
	if h.loggerConfig.LoggerAPIKey != "" {
		libhConf := libhoney.Config{
			APIHost:  h.loggerConfig.LoggerHoneycombAPI,
			WriteKey: h.loggerConfig.LoggerAPIKey,
			// Output:   &libhoney.WriterOutput{},
			// Logger: &libhoney.DefaultLogger{},
		}
		libhoney.Init(libhConf)
	}
	h.builder = libhoney.NewBuilder()
	h.builder.Dataset = h.loggerConfig.LoggerDataset

	// listen for config reloads
	h.Config.RegisterReloadCallback(h.reloadBuilder)

	return nil
}

func (h *HoneycombLogger) reloadBuilder() {
	loggerConfig := HoneycombLoggerConfig{}
	err := h.Config.GetOtherConfig("HoneycombLogger", &loggerConfig)
	if err != nil {
		// complain about this both to STDOUT and to the previously configured
		// honeycomb logger
		fmt.Printf("failed to reload configs for Honeycomb logger: %+v\n", err)
		h.Errorf("failed to reload configs for Honeycomb logger: %+v", err)
		return
	}
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

func (h *HoneycombLogger) Debugf(f string, args ...interface{}) {
	if h.loggerConfig.level > DebugLevel {
		return
	}
	ev := h.builder.NewEvent()
	ev.AddField("level", "debug")
	ev.AddField("msg", fmt.Sprintf(f, args...))
	ev.Send()
}

func (h *HoneycombLogger) Infof(f string, args ...interface{}) {
	if h.loggerConfig.level > InfoLevel {
		return
	}
	ev := h.builder.NewEvent()
	ev.AddField("level", "info")
	ev.AddField("msg", fmt.Sprintf(f, args...))
	ev.Send()
}

func (h *HoneycombLogger) Errorf(f string, args ...interface{}) {
	if h.loggerConfig.level > ErrorLevel {
		return
	}
	ev := h.builder.NewEvent()
	ev.AddField("level", "error")
	ev.AddField("msg", fmt.Sprintf(f, args...))
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
