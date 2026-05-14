package logger

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/honeycombio/refinery/config"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// OTelLogger implements the Refinery Logger interface using the OpenTelemetry Logs API.
type OTelLogger struct {
	Config       config.Config `inject:""`
	Version      string        `inject:"version"`
	level        config.Level
	logEmitter   otellog.Logger
	shutdownFunc func(context.Context) error
}

func (l *OTelLogger) Start() error {
	l.level = l.Config.GetLoggerLevel()
	cfg := l.Config.GetOTelLoggerConfig()

	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String(cfg.Dataset),
		semconv.ServiceVersionKey.String(l.Version),
	)

	apiHost, err := url.Parse(cfg.APIHost)
	if err != nil {
		return fmt.Errorf("otel logger: failed to parse APIHost: %w", err)
	}

	hdrs := map[string]string{}
	if cfg.APIKey != "" {
		hdrs["x-honeycomb-team"] = cfg.APIKey
		hdrs["x-honeycomb-dataset"] = cfg.Dataset
	}

	opts := []otlploghttp.Option{
		otlploghttp.WithEndpoint(apiHost.Host),
	}
	if len(hdrs) > 0 {
		opts = append(opts, otlploghttp.WithHeaders(hdrs))
	}
	if apiHost.Scheme == "http" {
		opts = append(opts, otlploghttp.WithInsecure())
	}
	compression := otlploghttp.GzipCompression
	if cfg.Compression == "none" {
		compression = otlploghttp.NoCompression
	}
	opts = append(opts, otlploghttp.WithCompression(compression))

	exporter, err := otlploghttp.New(context.Background(), opts...)
	if err != nil {
		return fmt.Errorf("otel logger: failed to create exporter: %w", err)
	}

	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)
	l.logEmitter = provider.Logger("github.com/honeycombio/refinery")
	l.shutdownFunc = provider.Shutdown
	return nil
}

func (l *OTelLogger) Stop() error {
	if l.shutdownFunc != nil {
		return l.shutdownFunc(context.Background())
	}
	return nil
}

func (l *OTelLogger) SetLevel(lvl string) error {
	l.level = config.ParseLevel(lvl)
	return nil
}

func (l *OTelLogger) Debug() Entry {
	if l.level > config.DebugLevel {
		return nullEntry
	}
	return &otelEntry{logger: l, level: config.DebugLevel}
}

func (l *OTelLogger) Info() Entry {
	if l.level > config.InfoLevel {
		return nullEntry
	}
	return &otelEntry{logger: l, level: config.InfoLevel}
}

func (l *OTelLogger) Warn() Entry {
	if l.level > config.WarnLevel {
		return nullEntry
	}
	return &otelEntry{logger: l, level: config.WarnLevel}
}

func (l *OTelLogger) Error() Entry {
	if l.level > config.ErrorLevel {
		return nullEntry
	}
	return &otelEntry{logger: l, level: config.ErrorLevel}
}

type otelEntry struct {
	logger *OTelLogger
	level  config.Level
	attrs  map[string]string
}

func (e *otelEntry) withAttr(key, value string) *otelEntry {
	merged := make(map[string]string, len(e.attrs)+1)
	for k, v := range e.attrs {
		merged[k] = v
	}
	merged[key] = value
	return &otelEntry{logger: e.logger, level: e.level, attrs: merged}
}

func (e *otelEntry) WithField(key string, value interface{}) Entry {
	return e.withAttr(key, fmt.Sprintf("%v", value))
}

func (e *otelEntry) WithString(key string, value string) Entry {
	return e.withAttr(key, value)
}

func (e *otelEntry) WithFields(fields map[string]interface{}) Entry {
	merged := make(map[string]string, len(e.attrs)+len(fields))
	for k, v := range e.attrs {
		merged[k] = v
	}
	for k, v := range fields {
		merged[k] = fmt.Sprintf("%v", v)
	}
	return &otelEntry{logger: e.logger, level: e.level, attrs: merged}
}

func (e *otelEntry) Logf(format string, args ...interface{}) {
	if e.logger.logEmitter == nil {
		return
	}
	var rec otellog.Record
	rec.SetTimestamp(time.Now())
	rec.SetSeverity(severityFromLevel(e.level))
	rec.SetSeverityText(e.level.String())
	rec.SetBody(otellog.StringValue(fmt.Sprintf(format, args...)))
	kvs := make([]otellog.KeyValue, 0, len(e.attrs))
	for k, v := range e.attrs {
		kvs = append(kvs, otellog.String(k, v))
	}
	rec.AddAttributes(kvs...)
	e.logger.logEmitter.Emit(context.Background(), rec)
}

func severityFromLevel(level config.Level) otellog.Severity {
	switch level {
	case config.DebugLevel:
		return otellog.SeverityDebug
	case config.InfoLevel:
		return otellog.SeverityInfo
	case config.WarnLevel:
		return otellog.SeverityWarn
	case config.ErrorLevel:
		return otellog.SeverityError
	default:
		return otellog.SeverityInfo
	}
}
