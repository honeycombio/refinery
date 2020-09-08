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
	level  logrus.Level
}

type LogrusEntry struct {
	entry *logrus.Entry
	level logrus.Level
}

func (l *LogrusLogger) Start() error {
	l.logger = logrus.New()
	l.logger.SetLevel(l.level)
	return nil
}

func (l *LogrusLogger) Debug() Entry {
	if l.level > logrus.DebugLevel {
		return nullEntry
	}

	return &LogrusEntry{
		entry: logrus.NewEntry(l.logger),
		level: logrus.DebugLevel,
	}
}

func (l *LogrusLogger) Info() Entry {
	if l.level > logrus.InfoLevel {
		return nullEntry
	}

	return &LogrusEntry{
		entry: logrus.NewEntry(l.logger),
		level: logrus.InfoLevel,
	}
}

func (l *LogrusLogger) Error() Entry {
	if l.level > logrus.ErrorLevel {
		return nullEntry
	}

	return &LogrusEntry{
		entry: logrus.NewEntry(l.logger),
		level: logrus.ErrorLevel,
	}
}

func (l *LogrusLogger) SetLevel(level string) error {
	logrusLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	// record the choice and set it if we're already initialized
	l.level = logrusLevel
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

func (l *LogrusEntry) Logf(f string, args ...interface{}) {
	switch l.level {
	case logrus.DebugLevel:
		l.entry.Debugf(f, args...)
	case logrus.InfoLevel:
		l.entry.Infof(f, args...)
	default:
		l.entry.Errorf(f, args...)
	}
}
