package config

import (
	"strings"
)

type Level int

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	PanicLevel
	UnknownLevel // unknown should be higher than everything
)

func ParseLevel(s string) Level {
	sanitizedLevel := strings.TrimSpace(strings.ToLower(s))
	switch sanitizedLevel {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn", "warning":
		return WarnLevel
	case "error":
		return ErrorLevel
	case "panic":
		return PanicLevel
	default:
		return UnknownLevel
	}
}

func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case PanicLevel:
		return "panic"
	default:
		return "unknown"
	}
}
