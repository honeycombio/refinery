package sample

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/honeycombio/refinery/types"
)

type traceKey struct {
	fields            []string
	useTraceLength    bool
	addDynsampleKey   bool
	addDynsampleField string
}

func newTraceKey(fields []string, useTraceLength, addDynsampleKey bool, addDynsampleField string) *traceKey {
	// always put the field list in sorted order for easier comparison
	sort.Strings(fields)
	return &traceKey{
		fields:            fields,
		useTraceLength:    useTraceLength,
		addDynsampleKey:   addDynsampleKey,
		addDynsampleField: addDynsampleField,
	}
}

// buildAndAdd, builds the trace key and adds it to the trace if configured to
// do so
func (d *traceKey) buildAndAdd(trace *types.Trace) string {
	key := d.build(trace)

	if d.addDynsampleKey {
		for _, span := range trace.GetSpans() {
			span.Data[d.addDynsampleField] = key
		}
	}

	return key
}

// build, builds the trace key based on the configuration of the traceKeyGenerator
func (d *traceKey) build(trace *types.Trace) string {
	// fieldCollector gets all values from the fields listed in the config, even
	// if they happen multiple times.
	fieldCollector := map[string][]string{}

	// for each field, for each span, get the value of that field
	spans := trace.GetSpans()
	for _, field := range d.fields {
		for _, span := range spans {
			if val, ok := span.Data[field]; ok {
				fieldCollector[field] = append(fieldCollector[field], fmt.Sprintf("%v", val))
			}
		}
	}
	// ok, now we have a map of fields to a list of all values for that field.

	var key string
	for _, field := range d.fields {
		// sort and collapse list
		sort.Strings(fieldCollector[field])
		var prevStr string
		for _, str := range fieldCollector[field] {
			if str != prevStr {
				key += str + "•"
			}
			prevStr = str
		}
		// get ready for the next element
		key += ","
	}

	if d.useTraceLength {
		key += strconv.FormatInt(int64(len(spans)), 10)
	}

	return key
}
