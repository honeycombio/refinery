package logger

import (
	"github.com/sirupsen/logrus"

	"github.com/honeycombio/samproxy/config"
)

// LogrusLogger is a Logger implementation that sends all logs to stdout using
// the Logrus package to get nice formatting
type LogrusLogger struct {
	Config config.Config `inject:""`

	logger *logrus.Logger
	level  *logrus.Level
}

type LogrusEntry struct {
	entry *logrus.Entry
}

func (l *LogrusLogger) Start() error {
	l.logger = logrus.New()
	if l.level != nil {
		l.logger.SetLevel(*l.level)
	}
	return nil
}
func (l *LogrusLogger) WithField(key string, value interface{}) Entry {
	return &LogrusEntry{
		entry: l.logger.WithField(key, value),
	}
}
func (l *LogrusLogger) WithFields(fields map[string]interface{}) Entry {
	return &LogrusEntry{
		entry: l.logger.WithFields(fields),
	}
}
func (l *LogrusLogger) Debugf(f string, args ...interface{}) {
	l.logger.Debugf(f, args...)
}
func (l *LogrusLogger) Infof(f string, args ...interface{}) {
	l.logger.Infof(f, args...)
}
func (l *LogrusLogger) Errorf(f string, args ...interface{}) {
	l.logger.Errorf(f, args...)
}
func (l *LogrusLogger) SetLevel(level string) error {
	logrusLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	// record the choice and set it if we're already initialized
	l.level = &logrusLevel
	if l.logger != nil {
		l.logger.SetLevel(logrusLevel)
	}
	return nil
}

func (l *LogrusEntry) WithField(key string, value interface{}) Entry {
	return &LogrusEntry{
		entry: l.entry.WithField(key, value),
	}
}
func (l *LogrusEntry) WithFields(fields map[string]interface{}) Entry {
	return &LogrusEntry{
		entry: l.entry.WithFields(fields),
	}
}
func (l *LogrusEntry) Debugf(f string, args ...interface{}) {
	l.entry.Debugf(f, args...)
}
func (l *LogrusEntry) Infof(f string, args ...interface{}) {
	l.entry.Infof(f, args...)
}
func (l *LogrusEntry) Errorf(f string, args ...interface{}) {
	l.entry.Errorf(f, args...)
}
