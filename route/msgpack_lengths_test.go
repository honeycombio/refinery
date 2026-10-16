package route

import (
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
)

// Headers written with whatever length we claim, regardless of what follows.
func arr32(n uint32) []byte { return append([]byte{0xdd}, be32(n)...) }
func map32(n uint32) []byte { return append([]byte{0xdf}, be32(n)...) }
func bin32(n uint32) []byte { return append([]byte{0xc6}, be32(n)...) }
func str32(n uint32) []byte { return append([]byte{0xdb}, be32(n)...) }

func be32(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}

func fixstr(s string) []byte { return append([]byte{0xa0 | byte(len(s))}, []byte(s)...) }

func join(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// nilElems returns n encoded nils, one byte each, the densest valid batch.
func nilElems(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 0xc0
	}
	return b
}

// chained nests array headers that each pass alone but together declare many
// times the body, ended by a byte no decoder can read.
func chained(bodyLen, levels int) []byte {
	out := join(map32(1), fixstr("a"))
	for i := range levels {
		remaining := max(bodyLen-len(out)-5-(levels-i-1)*5-1, 0)
		out = append(out, arr32(uint32(remaining))...)
	}
	out = append(out, 0xc1)
	return append(out, nilElems(max(bodyLen-len(out), 0))...)
}

const (
	// Larger than the count cap and far larger than the body carrying it.
	declaredHuge = 10_000_000
	// Under the cap, so only the body-length check can reject it.
	declaredUnderCap = 400_000
)

func TestCheckMsgpackDeclaredLengths(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		want error
	}{
		{"array header longer than the body", arr32(declaredUnderCap), ErrMsgpackLengthMismatch},
		{"array header far over the body", arr32(declaredHuge), ErrMsgpackLengthMismatch},
		{"bin header nested in an array", join(arr32(1), bin32(declaredHuge)), ErrMsgpackLengthMismatch},
		{"map header nested two deep", join(arr32(1), map32(1), fixstr("data"), map32(declaredHuge)), ErrMsgpackLengthMismatch},
		{"top-level map header longer than the body", map32(declaredHuge), ErrMsgpackLengthMismatch},
		{"string field value longer than the body", join(map32(1), fixstr("f"), str32(declaredHuge)), ErrMsgpackLengthMismatch},
		{"array field value longer than the body", join(map32(1), fixstr("f"), arr32(declaredHuge)), ErrMsgpackLengthMismatch},
		// A guard keyed off msgp.Skip's error lets these through.
		{"map header masked by a trailing invalid byte", append(map32(declaredHuge), 0xc1), ErrMsgpackLengthMismatch},
		{"array header masked by a trailing invalid byte", append(arr32(declaredUnderCap), 0xc1), ErrMsgpackLengthMismatch},
		{"chained array headers masked by a trailing invalid byte", chained(4096, 10), ErrMsgpackLengthMismatch},
		{"deeply chained array headers masked", chained(4096, 500), ErrMsgpackLengthMismatch},
		// Not a lie: a dense body can hold that many one-byte elements.
		{"single array claiming every remaining byte", join(arr32(16), nilElems(16)), nil},
		{"well-formed batch", join(arr32(2), map32(1), fixstr("a"), fixstr("b"), nilElems(1)), nil},
		// Anything but an overrun has to reach the decoder.
		{"invalid prefix is left for the decoder", []byte{0xc1}, nil},
		{"invalid prefix before a lying header", append([]byte{0xc1}, map32(declaredHuge)...), nil},
		{"empty body is left for the decoder", []byte{}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkMsgpackDeclaredLengths(tc.body)
			if tc.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestCheckMsgpackDeclaredLengthsNesting(t *testing.T) {
	// 0x91 is a one-element fixarray, so each byte opens another level.
	deep := make([]byte, maxMsgpackDepth+2)
	for i := range deep {
		deep[i] = 0x91
	}
	require.ErrorIs(t, checkMsgpackDeclaredLengths(deep), ErrMsgpackTooDeeplyNested)
}

// TestBatchedEventsRejectsOversizedCount covers the O(1) check at the one site
// on the batch path that sizes an allocation from a header.
func TestBatchedEventsRejectsOversizedCount(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		want error
	}{
		{"count past the bytes remaining", arr32(declaredUnderCap), ErrMsgpackLengthMismatch},
		{"count far past the bytes remaining", arr32(declaredHuge), ErrMsgpackLengthMismatch},
		// Nil elements fail later on type. What matters is the count check passes.
		{"at the event count limit", join(arr32(maxBatchEventCount), nilElems(maxBatchEventCount)), nil},
		{"one over the event count limit", join(arr32(maxBatchEventCount+1), nilElems(maxBatchEventCount+1)), ErrBatchTooManyEvents},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newTestBatchedEvents().UnmarshalMsg(tc.body)
			if tc.want == nil {
				assert.NotErrorIs(t, err, ErrBatchTooManyEvents)
				assert.NotErrorIs(t, err, ErrMsgpackLengthMismatch)
				return
			}
			require.ErrorIs(t, err, tc.want)
		})
	}
}

// TestBatchPathDoesNotPreSizeNestedHeaders pins what lets the batch path get by
// with a count check instead of a walk. If a decoder here ever starts pre-sizing
// from a header, this fails and the walk has to cover this path too.
func TestBatchPathDoesNotPreSizeNestedHeaders(t *testing.T) {
	const ceiling = 1 << 20
	req := msgpackRequest()

	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"bin32 inside an element", join(arr32(1), bin32(declaredHuge))},
		{"map32 as an event data field", join(arr32(1), map32(1), fixstr("data"), map32(declaredHuge))},
		{"str32 as a data value", join(arr32(1), map32(1), fixstr("data"), map32(1), fixstr("k"), str32(declaredHuge))},
		{"array32 as a data value", join(arr32(1), map32(1), fixstr("data"), map32(1), fixstr("k"), arr32(declaredHuge))},
		{"map32 element header", join(arr32(1), map32(declaredHuge))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			allocated := allocatedBy(func() { err = unmarshal(req, tc.body, newTestBatchedEvents()) })
			require.Error(t, err, "a lying nested header must still be rejected")
			assert.Less(t, allocated, uint64(ceiling),
				"allocated %d bytes for a %d byte body: a batch decoder now pre-sizes from a header, so the batch path needs the walk",
				allocated, len(tc.body))
		})
	}
}

func msgpackRequest() *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/1/batch/dataset", nil)
	req.Header.Set("Content-Type", "application/x-msgpack")
	return req
}

func newTestBatchedEvents() *batchedEvents {
	return newBatchedEvents(types.CoreFieldsUnmarshalerOptions{
		Config:  &config.MockConfig{},
		APIKey:  "api-key",
		Env:     "env",
		Dataset: "dataset",
	})
}

// allocatedBy reports bytes allocated while running f.
func allocatedBy(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestUnmarshalBoundsAllocations checks a tiny body with a vast declared length
// is rejected without allocating on the way, on both decode paths.
func TestUnmarshalBoundsAllocations(t *testing.T) {
	const ceiling = 1 << 20

	cases := []struct {
		name   string
		body   []byte
		target func() any
	}{
		{"batch array header", arr32(declaredUnderCap), func() any { return newTestBatchedEvents() }},
		{"batch element bin header", join(arr32(1), bin32(declaredHuge)), func() any { return newTestBatchedEvents() }},
		{"batch array masked by a trailing invalid byte", append(arr32(declaredUnderCap), 0xc1), func() any { return newTestBatchedEvents() }},
		{"single event map header", map32(declaredHuge), func() any { return &map[string]interface{}{} }},
		{"single event nested array value", join(map32(1), fixstr("f"), arr32(declaredHuge)), func() any { return &map[string]interface{}{} }},
		{"single event masked by a trailing invalid byte", append(map32(declaredHuge), 0xc1), func() any { return &map[string]interface{}{} }},
		{"single event chained headers", chained(64<<10, 10), func() any { return &map[string]interface{}{} }},
	}

	req := msgpackRequest()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			allocated := allocatedBy(func() { err = unmarshal(req, tc.body, tc.target()) })

			require.Error(t, err, "a declared length past the end of the body must be rejected")
			assert.Less(t, allocated, uint64(ceiling),
				"allocated %d bytes for a %d byte body: the declared length reached an allocation",
				allocated, len(tc.body))
		})
	}
}

// TestUnmarshalAcceptsValidMsgpack guards against rejecting traffic that works.
func TestUnmarshalAcceptsValidMsgpack(t *testing.T) {
	req := msgpackRequest()

	t.Run("batch", func(t *testing.T) {
		body := realisticBatch(2, 10)
		batch := newTestBatchedEvents()
		require.NoError(t, unmarshal(req, body, batch))
		assert.Len(t, batch.events, 2)
		assert.Equal(t, "value.0", batch.events[0].Data.Get("field.0"))
	})

	t.Run("single event", func(t *testing.T) {
		body := join(map32(2), fixstr("field"), fixstr("value"), fixstr("n"), []byte{0x2a})
		data := map[string]interface{}{}
		require.NoError(t, unmarshal(req, body, &data))
		assert.Equal(t, "value", data["field"])
	})
}

// realisticEvent builds one event's worth of fields, the shape the single event
// path decodes.
func realisticEvent(fields int) []byte {
	body := msgp.AppendMapHeader(nil, uint32(fields))
	for f := range fields {
		body = msgp.AppendString(body, "field."+itoa(f))
		switch f % 4 {
		case 0:
			body = msgp.AppendString(body, "value."+itoa(f))
		case 1:
			body = msgp.AppendInt64(body, int64(f)*1000)
		case 2:
			body = msgp.AppendFloat64(body, float64(f)*1.5)
		case 3:
			body = msgp.AppendBool(body, f%8 == 3)
		}
	}
	return body
}

// realisticBatch builds a libhoney-shaped batch, to price against real traffic.
func realisticBatch(events, fields int) []byte {
	body := arr32(uint32(events))
	for range events {
		body = msgp.AppendMapHeader(body, 2)
		body = msgp.AppendString(body, "samplerate")
		body = msgp.AppendInt64(body, 1)
		body = msgp.AppendString(body, "data")
		body = msgp.AppendMapHeader(body, uint32(fields))
		for f := range fields {
			body = msgp.AppendString(body, "field."+itoa(f))
			switch f % 4 {
			case 0:
				body = msgp.AppendString(body, "value."+itoa(f))
			case 1:
				body = msgp.AppendInt64(body, int64(f)*1000)
			case 2:
				body = msgp.AppendFloat64(body, float64(f)*1.5)
			case 3:
				body = msgp.AppendBool(body, f%8 == 3)
			}
		}
	}
	return body
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

// BenchmarkCheckMsgpackDeclaredLengths prices the walk on the shape it actually
// sees. Only the single event path runs it, so a batch-shaped body here would
// measure a case that cannot occur.
func BenchmarkCheckMsgpackDeclaredLengths(b *testing.B) {
	for _, fields := range []int{10, 50, 200} {
		body := realisticEvent(fields)
		b.Run("fields="+itoa(fields), func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				if err := checkMsgpackDeclaredLengths(body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUnmarshalMsgpackEvent is the single event path end to end, walk
// included. Stub out checkMsgpackDeclaredLengths to measure the delta.
func BenchmarkUnmarshalMsgpackEvent(b *testing.B) {
	req := msgpackRequest()
	for _, fields := range []int{10, 50, 200} {
		body := realisticEvent(fields)
		b.Run("fields="+itoa(fields), func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				data := map[string]interface{}{}
				if err := unmarshal(req, body, &data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUnmarshalMsgpackBatch is a full batch decode. The walk does not run
// on this path, so the bound being measured is the count check in
// batchedEvents.UnmarshalMsg.
func BenchmarkUnmarshalMsgpackBatch(b *testing.B) {
	req := msgpackRequest()
	for _, shape := range []struct {
		name           string
		events, fields int
	}{{"typical/2events", 2, 20}, {"p999/500events", 500, 20}, {"large/10000events", 10_000, 20}} {
		body := realisticBatch(shape.events, shape.fields)
		b.Run(shape.name, func(b *testing.B) {
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				if err := unmarshal(req, body, newTestBatchedEvents()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
