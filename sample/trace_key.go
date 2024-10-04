package sample

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/honeycombio/refinery/generics"
	"github.com/honeycombio/refinery/types"
)

// once a key gets this many unique values, it's off the charts in terms of uniqueness
// so we just stop looking for more.
// This is a safety valve to prevent us from someone sending a high-cardinality field
// in a giant trace.
const maxKeyLength = 100

type traceKey struct {
	fields         []string
	rootOnlyFields []string
	useTraceLength bool
}

func newTraceKey(fields []string, useTraceLength bool) *traceKey {
	// always put the field list in sorted order for easier comparison
	sort.Strings(fields)
	rootOnlyFields := make([]string, 0, len(fields)/2)
	nonRootFields := make([]string, 0, len(fields)/2)
	for _, field := range fields {
		if strings.HasPrefix(field, RootPrefix) {
			rootOnlyFields = append(rootOnlyFields, field[len(RootPrefix):])
			continue
		}

		nonRootFields = append(nonRootFields, field)
	}

	return &traceKey{
		fields:         nonRootFields,
		rootOnlyFields: rootOnlyFields,
		useTraceLength: useTraceLength,
	}
}

// build, builds the trace key based on the configuration of the traceKeyGenerator
// returns the number of values used to build the key
func (d *traceKey) build(trace *types.Trace) (string, int) {
	fieldCount := 0
	// fieldCollector gets all values from the fields listed in the config, even
	// if they happen multiple times.
	fieldCollector := make(map[string][]string)

	// for each field, for each span, get the value of that field
	spans := trace.GetSpans()
	uniques := generics.NewSetWithCapacity[string](maxKeyLength)
outer:
	for _, field := range d.fields {
		for _, span := range spans {
			if val, ok := span.Data[field]; ok {
				u := fmt.Sprintf("%s/%v", field, val)
				// don't bother to add it if we've already seen it
				if uniques.Contains(u) {
					continue
				}
				uniques.Add(u)
				if len(uniques) >= maxKeyLength {
					break outer
				}
				fieldCollector[field] = append(fieldCollector[field], fmt.Sprintf("%v", val))
			}
		}
	}
	// ok, now we have a map of fields to a list of all unique values for that field.
	// (unless it was huge, in which case we have a bunch of them)

	var key strings.Builder
	for _, field := range d.fields {
		// sort and collapse list
		sort.Strings(fieldCollector[field])
		var prevStr string
		for _, str := range fieldCollector[field] {
			if str != prevStr {
				key.WriteString(str)
				key.WriteRune('•')
				fieldCount += 1
			}
			prevStr = str
		}
		// get ready for the next element
		key.WriteRune(',')
	}

	if trace.RootSpan != nil {
		for _, field := range d.rootOnlyFields {
			if val, ok := trace.RootSpan.Data[field]; ok {
				key.WriteString(fmt.Sprintf("%v,", val))
				fieldCount += 1
			}
		}
	}

	if d.useTraceLength {
		key.WriteString(strconv.FormatInt(int64(len(spans)), 10))
		fieldCount += 1
	}

	return key.String(), fieldCount
}
