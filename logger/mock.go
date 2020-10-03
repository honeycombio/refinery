package logger

import (
	"fmt"

	"github.com/honeycombio/refinery/config"
)

type MockLogger struct {
	Events []*MockLoggerEvent
}

type MockLoggerEvent struct {
	l      *MockLogger
	level  config.HoneycombLevel
	Fields map[string]interface{}
}

func (l *MockLogger) Debug() Entry {
	return &MockLoggerEvent{
		l:      l,
		level:  DebugLevel,
		Fields: make(map[string]interface{}),
	}
}

func (l *MockLogger) Info() Entry {
	return &MockLoggerEvent{
		l:      l,
		level:  InfoLevel,
		Fields: make(map[string]interface{}),
	}
}

func (l *MockLogger) Error() Entry {
	return &MockLoggerEvent{
		l:      l,
		level:  ErrorLevel,
		Fields: make(map[string]interface{}),
	}
}

func (l *MockLogger) SetLevel(level string) error {
	return nil
}

func (e *MockLoggerEvent) WithField(key string, value interface{}) Entry {
	e.Fields[key] = value

	return e
}

func (e *MockLoggerEvent) WithString(key string, value string) Entry {
	return e.WithField(key, value)
}

func (e *MockLoggerEvent) WithFields(fields map[string]interface{}) Entry {
	for k, v := range fields {
		e.Fields[k] = v
	}

	return e
}

func (e *MockLoggerEvent) Logf(f string, args ...interface{}) {
	msg := fmt.Sprintf(f, args...)
	switch e.level {
	case DebugLevel:
		e.WithField("debug", msg)
	case InfoLevel:
		e.WithField("info", msg)
	case ErrorLevel:
		e.WithField("error", msg)
	default:
		panic("unexpected log level")
	}
	e.l.Events = append(e.l.Events, e)
}
