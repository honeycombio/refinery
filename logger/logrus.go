package logger

import (
	"github.com/sirupsen/logrus"

	"github.com/honeycombio/refinery/config"
)

// StdoutLogger is a Logger implementation that sends all logs to stdout using
// the Logrus package to get nice formatting
type StdoutLogger struct {
	Config config.Config `inject:""`

	logger *logrus.Logger
	level  logrus.Level
}

var _ = Logger((*StdoutLogger)(nil))

type LogrusEntry struct {
	entry *logrus.Entry
	level logrus.Level
}

func (l *StdoutLogger) Start() error {
	l.logger = logrus.New()
	l.logger.SetLevel(l.level)
	cfg, err := l.Config.GetStdoutLoggerConfig()
	if err != nil {
		return err
	}

	if cfg.Structured {
		l.logger.SetFormatter(&logrus.JSONFormatter{})
	}

	return nil
}

func (l *StdoutLogger) Debug() Entry {
	if !l.logger.IsLevelEnabled(logrus.DebugLevel) {
		return nullEntry
	}

	return &LogrusEntry{
		entry: logrus.NewEntry(l.logger),
		level: logrus.DebugLevel,
	}
}

func (l *StdoutLogger) Info() Entry {
	if !l.logger.IsLevelEnabled(logrus.InfoLevel) {
		return nullEntry
	}

	return &LogrusEntry{
		entry: logrus.NewEntry(l.logger),
		level: logrus.InfoLevel,
	}
}

func (l *StdoutLogger) Warn() Entry {
	if !l.logger.IsLevelEnabled(logrus.WarnLevel) {
		return nullEntry
	}

	return &LogrusEntry{
		entry: logrus.NewEntry(l.logger),
		level: logrus.WarnLevel,
	}
}

func (l *StdoutLogger) Error() Entry {
	if !l.logger.IsLevelEnabled(logrus.ErrorLevel) {
		return nullEntry
	}

	return &LogrusEntry{
		entry: logrus.NewEntry(l.logger),
		level: logrus.ErrorLevel,
	}
}

func (l *StdoutLogger) SetLevel(level string) error {
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
		level: l.level,
	}
}

func (l *LogrusEntry) WithString(key string, value string) Entry {
	return &LogrusEntry{
		entry: l.entry.WithField(key, value),
		level: l.level,
	}
}

func (l *LogrusEntry) WithFields(fields map[string]interface{}) Entry {
	return &LogrusEntry{
		entry: l.entry.WithFields(fields),
		level: l.level,
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
