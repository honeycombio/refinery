package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/honeycombio/refinery/internal/tlsconfig"
	"github.com/honeycombio/refinery/logger"
)

type spyEntry struct {
	formats *[]string
	fields  map[string]interface{}
	msgs    *[]string
}

func (s spyEntry) WithField(k string, v interface{}) logger.Entry { s.fields[k] = v; return s }
func (s spyEntry) WithString(k, v string) logger.Entry            { s.fields[k] = v; return s }
func (s spyEntry) WithFields(f map[string]interface{}) logger.Entry {
	for k, v := range f {
		s.fields[k] = v
	}
	return s
}
func (s spyEntry) Logf(f string, args ...interface{}) {
	*s.formats = append(*s.formats, f)
	*s.msgs = append(*s.msgs, fmt.Sprintf(f, args...))
}

func TestLogTLSWarningUsesDistinctFormats(t *testing.T) {
	msgs := []string{tlsconfig.MsgCertExpired, tlsconfig.MsgCertExpiresSoon, tlsconfig.MsgCertReloadFailed, "something else"}
	seen := map[string]bool{"%s": true} // the format shared by other warnings
	for _, m := range msgs {
		var formats, out []string
		logTLSWarning(spyEntry{&formats, map[string]interface{}{}, &out}, m)
		assert.Len(t, formats, 1)
		assert.False(t, seen[formats[0]], "format %q for %q collides with another warning", formats[0], m)
		seen[formats[0]] = true
		assert.Contains(t, out[0], m)
	}
}
