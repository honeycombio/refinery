package logger

import (
	"fmt"
	"os"

	"github.com/honeycombio/samproxy/config"
)

type Logger interface {
	Debugf(f string, args ...interface{})
	Errorf(f string, args ...interface{})
	// SetLevel sets the logging level (debug, info, warn, error)
	SetLevel(level string) error
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
