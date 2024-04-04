package sample

import (
	"fmt"
	"sort"
	"strconv"
)

type traceKey struct {
	fields         []string
	useTraceLength bool
}

func newTraceKey(fields []string, useTraceLength bool) *traceKey {
	// always put the field list in sorted order for easier comparison
	sort.Strings(fields)
	return &traceKey{
		fields:         fields,
		useTraceLength: useTraceLength,
	}
}

// build, builds the trace key based on the configuration of the traceKeyGenerator
func (d *traceKey) build(trace KeyInfoExtractor) string {
	// fieldCollector gets all values from the fields listed in the config, even
	// if they happen multiple times.
	fieldCollector := map[string][]string{}

	// for each field, for each span, get the value of that field
	spans := trace.AllKeyFields()
	for _, field := range d.fields {
		for _, span := range spans {
			if val, ok := span.Fields()[field]; ok {
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
