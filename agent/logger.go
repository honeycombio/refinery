package agent

import (
	"context"

	"github.com/honeycombio/refinery/logger"
	"github.com/open-telemetry/opamp-go/client/types"
)

var _ types.Logger = &Logger{}

type Logger struct {
	Logger logger.Logger
}

func (l *Logger) Debugf(_ context.Context, format string, v ...interface{}) {
	l.Logger.Debug().Logf(format, v...)
}

func (l *Logger) Errorf(_ context.Context, format string, v ...interface{}) {
	l.Logger.Error().Logf(format, v...)
}
