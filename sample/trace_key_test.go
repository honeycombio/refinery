// +build all race

package sample

import (
	"testing"

	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
)

func TestKeyGeneration(t *testing.T) {
	fields := []string{"http.status_code", "request.path", "app.team.id", "important_field"}
	addKey := true
	key := "meta.key"
	useTraceLength := true

	generator := newTraceKey(fields, useTraceLength, addKey, key)

	trace := &types.Trace{}

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 200,
				"request.path":     "/{slug}/home",
				"app.team.id":      float64(2),
				"important_field":  true,
			},
		},
	})

	expected := "2•,200•,true•,/{slug}/home•,1"

	assert.Equal(t, expected, generator.build(trace))

	fields = []string{"http.status_code", "request.path", "app.team.id", "important_field"}
	addKey = true
	key = "meta.key"
	useTraceLength = true

	generator = newTraceKey(fields, useTraceLength, addKey, key)

	trace = &types.Trace{}

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 200,
			},
		},
	})

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"request.path": "/{slug}/home",
			},
		},
	})

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"app.team.id": float64(2),
			},
		},
	})

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"important_field": true,
			},
		},
	})

	expected = "2•,200•,true•,/{slug}/home•,4"

	assert.Equal(t, expected, generator.build(trace))

	// now test that multiple values across spans are condensed correctly
	fields = []string{"http.status_code"}
	addKey = true
	key = "meta.key"
	useTraceLength = true

	generator = newTraceKey(fields, useTraceLength, addKey, key)

	trace = &types.Trace{}

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 200,
			},
		},
	})

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 200,
			},
		},
	})

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 404,
			},
		},
	})

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 404,
			},
		},
	})

	expected = "200•404•,4"

	assert.Equal(t, expected, generator.build(trace))

	// now test that multiple values across spans in a different order are condensed the same
	fields = []string{"http.status_code"}
	addKey = true
	key = "meta.key"
	useTraceLength = true

	generator = newTraceKey(fields, useTraceLength, addKey, key)

	trace = &types.Trace{}

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 404,
			},
		},
	})

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 404,
			},
		},
	})

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 200,
			},
		},
	})

	trace.AddSpan(&types.Span{
		Event: types.Event{
			Data: map[string]interface{}{
				"http.status_code": 200,
			},
		},
	})

	expected = "200•404•,4"

	assert.Equal(t, expected, generator.build(trace))
}
