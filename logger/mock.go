package logger

import "fmt"

type MockLogger struct {
	Events []*MockLoggerEvent
}

type MockLoggerEvent struct {
	l      *MockLogger
	Fields map[string]interface{}
}

func (l *MockLogger) WithField(key string, value interface{}) Entry {
	e := &MockLoggerEvent{
		l:      l,
		Fields: make(map[string]interface{}),
	}
	return e.WithField(key, value)
}

func (l *MockLogger) WithFields(fields map[string]interface{}) Entry {
	e := &MockLoggerEvent{
		l:      l,
		Fields: make(map[string]interface{}),
	}
	return e.WithFields(fields)
}

func (l *MockLogger) Debugf(f string, args ...interface{}) {
	e := &MockLoggerEvent{
		l:      l,
		Fields: make(map[string]interface{}),
	}
	e.Debugf(f, args...)
}

func (l *MockLogger) Infof(f string, args ...interface{}) {
	e := &MockLoggerEvent{
		l:      l,
		Fields: make(map[string]interface{}),
	}
	e.Infof(f, args...)
}

func (l *MockLogger) Errorf(f string, args ...interface{}) {
	e := &MockLoggerEvent{
		l:      l,
		Fields: make(map[string]interface{}),
	}
	e.Errorf(f, args...)
}

func (l *MockLogger) SetLevel(level string) error {
	return nil
}

func (e *MockLoggerEvent) WithField(key string, value interface{}) Entry {
	e.Fields[key] = value

	return e
}

func (e *MockLoggerEvent) WithFields(fields map[string]interface{}) Entry {
	for k, v := range fields {
		e.Fields[k] = v
	}

	return e
}

func (e *MockLoggerEvent) Debugf(f string, args ...interface{}) {
	e.WithField("debug", fmt.Sprintf(f, args...))
	e.l.Events = append(e.l.Events, e)
}

func (e *MockLoggerEvent) Infof(f string, args ...interface{}) {
	e.WithField("info", fmt.Sprintf(f, args...))
	e.l.Events = append(e.l.Events, e)
}

func (e *MockLoggerEvent) Errorf(f string, args ...interface{}) {
	e.WithField("error", fmt.Sprintf(f, args...))
	e.l.Events = append(e.l.Events, e)
}
