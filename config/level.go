package config

import (
	"errors"
	"strings"
)

type Level int

const (
	UnknownLevel Level = iota
	DebugLevel
	InfoLevel
	WarnLevel
	ErrorLevel
	PanicLevel
)

func ParseLevel(s string) Level {
	sanitizedLevel := strings.TrimSpace(strings.ToLower(s))
	switch sanitizedLevel {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn":
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

func (l Level) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *Level) UnmarshalText(text []byte) error {
	*l = ParseLevel(string(text))
	if *l == UnknownLevel {
		return errors.New("unknown logging level '" + string(text) + "'")
	}
	return nil
}
