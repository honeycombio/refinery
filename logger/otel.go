package logger

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/honeycombio/refinery/config"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// OTelLogger implements the Refinery Logger interface and exports logs via OpenTelemetry
type OTelLogger struct {
	Config       config.Config `inject:""`
	logEmitter   otellog.Logger
	Version      string `inject:"version"`
	level        config.Level
	loggerConfig config.OTelLoggerConfig
	resource     *sdkresource.Resource
	shutdownFunc func(context.Context) error
}

// NewOTELLogger creates a new OpenTelemetry logger
func (ol *OTelLogger) Start() error {
	ol.level = ol.Config.GetLoggerLevel()
	loggerConfig := ol.Config.GetOTelLoggerConfig()
	ol.loggerConfig = loggerConfig

	// Create resource with service information
	res, err := sdkresource.New(context.Background(),
		sdkresource.WithAttributes(
			semconv.ServiceName(ol.loggerConfig.Dataset),
			semconv.ServiceVersion(ol.Version),
		),
		sdkresource.WithHost(),
		sdkresource.WithOS(),
	)
	if err != nil {
		return fmt.Errorf("failed to create resource: %w", err)
	}

	// otel can't handle the default endpoint so we have to parse it
	options := []otlploghttp.Option{}
	host, err := url.Parse(ol.loggerConfig.APIHost)
	if err != nil {
		return err
	}
	options = append(options, otlploghttp.WithEndpoint(host.Host))
	hdrs := map[string]string{}
	if ol.loggerConfig.APIKey != "" {
		hdrs["x-honeycomb-team"] = ol.loggerConfig.APIKey
	}
	if ol.loggerConfig.Dataset != "" {
		hdrs["x-honeycomb-dataset"] = ol.loggerConfig.Dataset
	}

	if len(hdrs) > 0 {
		options = append(options, otlploghttp.WithHeaders(hdrs))
	}

	if host.Scheme == "http" {
		options = append(options, otlploghttp.WithInsecure())
	}
	compression := otlploghttp.GzipCompression
	if ol.loggerConfig.Compression == "none" {
		compression = otlploghttp.NoCompression
	}

	options = append(options, otlploghttp.WithCompression(compression))

	// Configure OTLP exporter
	ctx := context.Background()
	// Use HTTP transport
	exporter, err := otlploghttp.New(ctx,
		options...,
	)
	if err != nil {
		return fmt.Errorf("failed to create OTLP log client: %w", err)
	}

	processor := sdklog.NewBatchProcessor(exporter)

	// Create the log provider
	loggerProvider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(processor),
	)

	// Create our logger instance
	instrumentationScope := instrumentation.Scope{
		Name:    "github.com/honeycombio/refinery",
		Version: ol.Version,
	}
	ol.logEmitter = loggerProvider.Logger(instrumentationScope.Name)
	ol.shutdownFunc = loggerProvider.Shutdown

	return nil
}
func (l *OTelLogger) SetLevel(lvl string) error {
	l.level = config.ParseLevel(lvl)
	return nil
}

// Helper method to create attributes from a map
func mapToAttributes(attrs map[string]string) []otellog.KeyValue {
	attributes := make([]otellog.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		attributes = append(attributes, otellog.String(k, v))
	}
	return attributes
}

// Debug logs at DEBUG level
func (l *OTelLogger) Debug() Entry {
	if l.level > config.DebugLevel {
		return nullEntry
	}
	return &OTELLoggerEntry{
		logger: l,
		level:  config.DebugLevel,
	}
}

// Info logs at INFO level
func (l *OTelLogger) Info() Entry {
	if l.level > config.InfoLevel {
		return nullEntry
	}
	return &OTELLoggerEntry{
		logger: l,
		level:  config.InfoLevel,
	}
}

// Warn logs at WARN level
func (l *OTelLogger) Warn() Entry {
	if l.level > config.WarnLevel {
		return nullEntry
	}
	return &OTELLoggerEntry{
		logger: l,
		level:  config.WarnLevel,
	}
}

// Error logs at ERROR level
func (l *OTelLogger) Error() Entry {
	if l.level > config.ErrorLevel {
		return nullEntry
	}
	return &OTELLoggerEntry{
		logger: l,
		level:  config.ErrorLevel,
	}
}

// OTELLoggerEntry represents a log event with the OpenTelemetry logger
type OTELLoggerEntry struct {
	logger *OTelLogger
	level  config.Level
	attrs  map[string]string
}

func (e *OTELLoggerEntry) WithField(key string, value interface{}) Entry {
	if e.logger.logEmitter == nil {
		return nullEntry
	}
	newLogger := &OTELLoggerEntry{
		logger: e.logger,
		level:  e.level,
		attrs:  make(map[string]string),
	}
	newLogger.attrs[key] = fmt.Sprintf("%v", value)
	return newLogger
}

func (e *OTELLoggerEntry) WithString(key string, value string) Entry {
	if e.logger.logEmitter == nil {
		return nullEntry
	}
	newLogger := &OTELLoggerEntry{
		logger: e.logger,
		level:  e.level,
		attrs:  make(map[string]string),
	}
	newLogger.attrs[key] = value
	return newLogger
}

func (e *OTELLoggerEntry) WithFields(fields map[string]interface{}) Entry {
	if e.logger.logEmitter == nil {
		return nullEntry
	}
	newLogger := &OTELLoggerEntry{
		logger: e.logger,
		level:  e.level,
		attrs:  make(map[string]string),
	}
	for k, v := range fields {
		newLogger.attrs[k] = fmt.Sprintf("%v", v)
	}
	return newLogger
}

// Logf formats and logs a message
func (e *OTELLoggerEntry) Logf(format string, args ...interface{}) {
	if e.logger.logEmitter == nil {
		return
	}

	message := fmt.Sprintf(format, args...)

	// Create the log record
	record := otellog.Record{}
	record.SetTimestamp(time.Now())
	record.SetSeverityText(e.level.String())
	record.SetSeverity(severityFromLevel(e.level))
	record.AddAttributes(mapToAttributes(e.attrs)...)
	record.SetBody(otellog.StringValue(message))

	// Emit the log
	e.logger.logEmitter.Emit(context.Background(), record)
}

// severityFromLevel converts internal log levels to OpenTelemetry severity
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

// Shutdown gracefully shuts down the logger
func (l *OTelLogger) Stop() error {
	// If we can access the LoggerProvider through the Logger, shut it down
	if l.shutdownFunc != nil {
		return l.shutdownFunc(context.Background())
	}
	return nil
}
