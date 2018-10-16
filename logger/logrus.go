package logger

import (
	"github.com/sirupsen/logrus"

	"github.com/honeycombio/malinois/config"
)

// LogrusLogger is a Logger implementation that sends all logs to stdout using
// the Logrus package to get nice formatting
type LogrusLogger struct {
	Config config.Config `inject:""`

	logger *logrus.Logger
	level  *logrus.Level
}

func (l *LogrusLogger) Start() error {
	l.logger = logrus.New()
	if l.level != nil {
		l.logger.SetLevel(*l.level)
	}
	return nil
}
func (l *LogrusLogger) Debugf(f string, args ...interface{}) {
	l.logger.Debugf(f, args...)
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
