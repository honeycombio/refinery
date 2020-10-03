package logger

import (
	"fmt"
	"os"

	"github.com/honeycombio/refinery/config"
)

type Logger interface {
	Debug() Entry
	Info() Entry
	Error() Entry
	// SetLevel sets the logging level (debug, info, warn, error)
	SetLevel(level string) error
}

type Entry interface {
	WithField(key string, value interface{}) Entry

	// WithString does the same thing as WithField, but is more efficient for
	// disabled log levels. (Because the value parameter doesn't escape.)
	WithString(key string, value string) Entry

	WithFields(fields map[string]interface{}) Entry
	Logf(f string, args ...interface{})
}

func GetLoggerImplementation(c config.Config) Logger {
	var logger Logger
	loggerType, err := c.GetLoggerType()
	if err != nil {
		fmt.Printf("unable to get logger type from config: %v\n", err)
		os.Exit(1)
	}
	switch loggerType {
	case "honeycomb":
		logger = &HoneycombLogger{}
	case "logrus":
		logger = &LogrusLogger{}
	default:
		fmt.Printf("unknown logger type %s. Exiting.\n", loggerType)
		os.Exit(1)
	}
	return logger
}
