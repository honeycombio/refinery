package logger

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/honeycombio/dynsampler-go"
	"github.com/honeycombio/refinery/config"
)

// StdoutLogger is a Logger implementation that sends all logs to stdout using
// the Logrus package to get nice formatting
type StdoutLogger struct {
	Config config.Config `inject:""`

	logger *logrus.Logger
	level  logrus.Level

	sampler dynsampler.Sampler
}

var _ = Logger((*StdoutLogger)(nil))

type LogrusEntry struct {
	entry   *logrus.Entry
	level   logrus.Level
	sampler dynsampler.Sampler
}

func (l *StdoutLogger) Start() error {
	l.logger = logrus.New()
	l.logger.SetLevel(l.level)
	cfg := l.Config.GetStdoutLoggerConfig()

	if cfg.Structured {
		l.logger.SetFormatter(&logrus.JSONFormatter{})
	}

	if cfg.SamplerEnabled {
		l.sampler = &dynsampler.PerKeyThroughput{
			ClearFrequencyDuration: 10 * time.Second,
			PerKeyThroughputPerSec: cfg.SamplerThroughput,
			MaxKeys:                1000,
		}
		err := l.sampler.Start()
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *StdoutLogger) Debug() Entry {
	if !l.logger.IsLevelEnabled(logrus.DebugLevel) {
		return nullEntry
	}

	return &LogrusEntry{
		entry:   logrus.NewEntry(l.logger),
		level:   logrus.DebugLevel,
		sampler: l.sampler,
	}
}

func (l *StdoutLogger) Info() Entry {
	if !l.logger.IsLevelEnabled(logrus.InfoLevel) {
		return nullEntry
	}

	return &LogrusEntry{
		entry:   logrus.NewEntry(l.logger),
		level:   logrus.InfoLevel,
		sampler: l.sampler,
	}
}

func (l *StdoutLogger) Warn() Entry {
	if !l.logger.IsLevelEnabled(logrus.WarnLevel) {
		return nullEntry
	}

	return &LogrusEntry{
		entry:   logrus.NewEntry(l.logger),
		level:   logrus.WarnLevel,
		sampler: l.sampler,
	}
}

func (l *StdoutLogger) Error() Entry {
	if !l.logger.IsLevelEnabled(logrus.ErrorLevel) {
		return nullEntry
	}

	return &LogrusEntry{
		entry:   logrus.NewEntry(l.logger),
		level:   logrus.ErrorLevel,
		sampler: l.sampler,
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
		entry:   l.entry.WithField(key, value),
		level:   l.level,
		sampler: l.sampler,
	}
}

func (l *LogrusEntry) WithString(key string, value string) Entry {
	return &LogrusEntry{
		entry:   l.entry.WithField(key, value),
		level:   l.level,
		sampler: l.sampler,
	}
}

func (l *LogrusEntry) WithFields(fields map[string]interface{}) Entry {
	return &LogrusEntry{
		entry:   l.entry.WithFields(fields),
		level:   l.level,
		sampler: l.sampler,
	}
}

func (l *LogrusEntry) Logf(f string, args ...interface{}) {
	if l.sampler != nil {
		// use the level and format string as the key to sample on
		// this will give us a different sample rate for each level and format string
		// and avoid high cardinality args making the throughput sampler less effective
		rate := l.sampler.GetSampleRate(fmt.Sprintf("%s:%s", l.level, f))
		if shouldDrop(uint(rate)) {
			return
		}
		l.entry.WithField("SampleRate", rate)
	}

	switch l.level {
	case logrus.DebugLevel:
		l.entry.Debugf(f, args...)
	case logrus.InfoLevel:
		l.entry.Infof(f, args...)
	case logrus.WarnLevel:
		l.entry.Warnf(f, args...)
	default:
		l.entry.Errorf(f, args...)
	}
}

func shouldDrop(rate uint) bool {
	if rate <= 1 {
		return false
	}

	return rand.Intn(int(rate)) != 0
}
