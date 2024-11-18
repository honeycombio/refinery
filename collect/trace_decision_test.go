package collect

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDropDecisionRoundTrip(t *testing.T) {
	// Test data for dropped decisions covering all fields
	tds := []TraceDecision{
		{
			TraceID: "trace1",
		},
		{
			TraceID: "trace2",
		},
	}

	// Step 1: Create a dropped decision message
	msg, err := newDroppedDecisionMessage(tds, "sender1")
	assert.NoError(t, err, "expected no error for valid dropped decision message")
	assert.NotEmpty(t, msg, "expected non-empty message")

	// Step 2: Decompress the message back to TraceDecision using newDroppedTraceDecision
	decompressedTds, err := newDroppedTraceDecision(msg, "sender2")
	assert.NoError(t, err, "expected no error during decompression of the dropped decision message")
	assert.Len(t, decompressedTds, len(tds), "expected decompressed TraceDecision length to match original")

	// Step 3: Verify that the decompressed data matches the original TraceDecision data
	for i, td := range decompressedTds {
		assert.Equal(t, td.TraceID, tds[i].TraceID, "expected TraceID to match")
	}

	// Make sure we only ignore messages that are produced from the same node
	msg, err = newDroppedDecisionMessage(tds, "sender1")
	assert.NoError(t, err, "expected no error for valid dropped decision message")
	assert.NotEmpty(t, msg, "expected non-empty message")

	decompressedTds, err = newDroppedTraceDecision(msg, "sender1")
	assert.NoError(t, err, "expected no error during decompression of the dropped decision message")
	assert.Empty(t, decompressedTds)
}

func TestKeptDecisionRoundTrip(t *testing.T) {
	// Test data for kept decisions covering all fields
	tds := []TraceDecision{
		{
			TraceID:         "trace1",
			Kept:            true,
			Rate:            1,
			SamplerKey:      "sampler1",
			SamplerSelector: "selector1",
			SendReason:      "reason1",
			HasRoot:         true,
			Reason:          "valid reason 1",
			Count:           5,
			EventCount:      10,
			LinkCount:       15,
		},
		{
			TraceID:         "trace2",
			Kept:            true,
			Rate:            2,
			SamplerKey:      "sampler2",
			SamplerSelector: "selector2",
			SendReason:      "reason2",
			HasRoot:         false,
			Reason:          "valid reason 2",
			Count:           3,
			EventCount:      6,
			LinkCount:       9,
		},
	}

	// Step 1: Create a kept decision message
	msg, err := newKeptDecisionMessage(tds, "sender1")
	assert.NoError(t, err, "expected no error for valid kept decision message")
	assert.NotEmpty(t, msg, "expected non-empty message")

	// Step 2: Decompress the message back to TraceDecision using newKeptTraceDecision
	decompressedTds, err := newKeptTraceDecision(msg, "sender2")
	assert.NoError(t, err, "expected no error during decompression of the kept decision message")
	assert.Len(t, decompressedTds, len(tds), "expected decompressed TraceDecision length to match original")

	// Step 3: Verify that the decompressed data matches the original TraceDecision data
	for i, td := range decompressedTds {
		assert.Equal(t, td.TraceID, tds[i].TraceID, "expected TraceID to match")
		assert.Equal(t, td.Kept, tds[i].Kept, "expected Kept status to match")
		assert.Equal(t, td.Rate, tds[i].Rate, "expected Rate to match")
		assert.Equal(t, td.SamplerKey, tds[i].SamplerKey, "expected SamplerKey to match")
		assert.Equal(t, td.SamplerSelector, tds[i].SamplerSelector, "expected SamplerSelector to match")
		assert.Equal(t, td.SendReason, tds[i].SendReason, "expected SendReason to match")
		assert.Equal(t, td.HasRoot, tds[i].HasRoot, "expected HasRoot to match")
		assert.Equal(t, td.Reason, tds[i].Reason, "expected Reason to match")
		assert.Equal(t, td.Count, tds[i].Count, "expected Count to match")
		assert.Equal(t, td.EventCount, tds[i].EventCount, "expected EventCount to match")
		assert.Equal(t, td.LinkCount, tds[i].LinkCount, "expected LinkCount to match")
	}

	// Make sure we only ignore messages that are produced from the same node
	msg, err = newKeptDecisionMessage(tds, "sender1")
	assert.NoError(t, err, "expected no error for valid kept decision message")
	assert.NotEmpty(t, msg, "expected non-empty message")

	// Step 2: Decompress the message back to TraceDecision using newKeptTraceDecision
	decompressedTds, err = newKeptTraceDecision(msg, "sender1")
	assert.NoError(t, err, "expected no error during decompression of the kept decision message")
	assert.Empty(t, decompressedTds)

}

// used in test only
func newKeptDecisionMessageJSON(tds []TraceDecision) (string, error) {
	if len(tds) == 0 {
		return "", fmt.Errorf("no kept trace decisions provided")
	}
	data, err := json.Marshal(tds)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func BenchmarkDynamicJSONEncoding(b *testing.B) {
	decisions := generateRandomDecisions(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := newKeptDecisionMessageJSON(decisions)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDynamicCompressedEncoding(b *testing.B) {
	decisions := generateRandomDecisions(1000)

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := newKeptDecisionMessage(decisions, "sender1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDynamicJSONDecoding(b *testing.B) {
	decisions := generateRandomDecisions(1000)
	jsonData, err := newKeptDecisionMessage(decisions, "sender1")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := newKeptTraceDecision(jsonData, "sender1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDynamicCompressedDecoding(b *testing.B) {
	decisions := generateRandomDecisions(1000)
	compressedData, err := newKeptDecisionMessage(decisions, "sender1")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := newKeptTraceDecision(compressedData, "sender1")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// / Benchmark to compare the size of JSON vs Snappy compression
func BenchmarkCompressionSizes(b *testing.B) {
	tds := generateRandomDecisions(1000)

	var jsonTotalSize int
	var snappyTotalSize int

	// Benchmark JSON Encoding size
	b.Run("JSON Encoding", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			size, err := jsonEncodeSize(tds)
			if err != nil {
				b.Fatalf("Error encoding JSON: %v", err)
			}
			jsonTotalSize += size
		}
		avgJSONSize := float64(jsonTotalSize) / float64(b.N)
		b.Logf("JSON Encoding: Total Batch: %d, Total Size: %d bytes, Average Size: %.2f bytes", b.N, jsonTotalSize, avgJSONSize)
	})

	// Benchmark Snappy Compression size
	b.Run("Snappy Compression", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			size, err := snappyCompressSize(tds)
			if err != nil {
				b.Fatalf("Error compressing with Snappy: %v", err)
			}
			snappyTotalSize += size
		}
		avgSnappySize := float64(snappyTotalSize) / float64(b.N)
		b.Logf("Snappy Compression: Total Batch: %d, Total Size: %d bytes, Average Size: %.2f bytes", b.N, snappyTotalSize, avgSnappySize)
	})
}

func generateRandomTraceDecision() TraceDecision {
	traceID := fmt.Sprintf("trace-%d", rand.Int63())

	rate := uint(rand.Intn(100))
	samplerKey := fmt.Sprintf("key-%d", rand.Intn(1000))

	sendReasons := []string{TraceSendExpired, TraceSendGotRoot, TraceSendLateSpan, TraceSendSpanLimit, TraceSendEjectedFull, TraceSendEjectedMemsize}
	reasons := []string{"deterministic", "dynamicsampler", "rule/deterministic", "emathroughput", "emadynamic"}

	sendReason := sendReasons[rand.Intn(len(sendReasons))]
	reason := reasons[rand.Intn(len(reasons))]

	return TraceDecision{
		TraceID:         traceID,
		Kept:            true,
		Rate:            rate,
		SamplerKey:      samplerKey,
		SamplerSelector: fmt.Sprintf("selector-%d", rand.Intn(5)),
		SendReason:      sendReason,
		HasRoot:         rand.Intn(2) == 0,
		Reason:          reason,
		Count:           uint32(rand.Intn(100)),
		EventCount:      uint32(rand.Intn(50)),
		LinkCount:       uint32(rand.Intn(20)),
	}
}

// GenerateRandomDecisions generates multiple TraceDecision
func generateRandomDecisions(num int) []TraceDecision {
	decisions := make([]TraceDecision, num)
	for i := 0; i < num; i++ {
		decisions[i] = generateRandomTraceDecision()
	}
	return decisions
}

// calculate the size of the original JSON encoding
func jsonEncodeSize(tds []TraceDecision) (int, error) {
	data, err := json.Marshal(tds)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// calculate the size of the Snappy compressed and gob encoding
func snappyCompressSize(tds []TraceDecision) (int, error) {
	compressed, err := compress(tds)
	if err != nil {
		return 0, err
	}
	return len(compressed), nil
}
