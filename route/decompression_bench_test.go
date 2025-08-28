package route

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Generate more realistic, less compressible data
func generateRealisticData(size int) []byte {
	// Create a mix of structured data (like JSON) with some randomness
	data := make([]byte, 0, size)

	for len(data) < size {
		// Create a realistic JSON-like event
		event := map[string]interface{}{
			"timestamp":   time.Now().UnixNano() + int64(len(data)), // Unique timestamps
			"level":       []string{"INFO", "WARN", "ERROR", "DEBUG"}[len(data)%4],
			"message":     fmt.Sprintf("Processing request %d with unique identifier %x", len(data), len(data)),
			"user_id":     fmt.Sprintf("user_%d_%x", len(data)%1000, len(data)),
			"request_id":  fmt.Sprintf("%x-%x-%x", len(data), time.Now().UnixNano(), len(data)*3),
			"duration_ms": float64(len(data)%500) + 0.123,
			"status_code": []int{200, 201, 400, 404, 500}[len(data)%5],
			"ip_address":  fmt.Sprintf("192.168.%d.%d", len(data)%256, (len(data)/256)%256),
		}

		jsonData, _ := json.Marshal(event)
		data = append(data, jsonData...)
		data = append(data, '\n')

		// Add some truly random bytes to make it less compressible
		if len(data) < size-32 {
			randomBytes := make([]byte, 8)
			rand.Read(randomBytes)
			data = append(data, randomBytes...)
		}
	}

	return data[:size]
}

// Benchmark data - various sizes with realistic content
var benchmarkData = []struct {
	originalSize int
	data         []byte
}{
	{100, generateRealisticData(100)},
	{1024, generateRealisticData(1024)},
	{10 * 1024, generateRealisticData(10 * 1024)},
	{100 * 1024, generateRealisticData(100 * 1024)},
}

// Helper function to compress data
func compressData(data []byte) []byte {
	encoder, _ := zstd.NewWriter(nil)
	compressed := encoder.EncodeAll(data, nil)
	encoder.Close()
	return compressed
}

// Old approach: Pool of decoders (for comparison)
func makeDecodersForBench(num int) (chan *zstd.Decoder, error) {
	zstdDecoders := make(chan *zstd.Decoder, num)
	for i := 0; i < num; i++ {
		zReader, err := zstd.NewReader(
			nil,
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(8*1024*1024),
		)
		if err != nil {
			return nil, err
		}
		zstdDecoders <- zReader
	}
	return zstdDecoders, nil
}

// Separate buffer pools for fair benchmarking
var oldBenchmarkBufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

func recycleOldBuffer(b *bytes.Buffer) {
	if b != nil {
		b.Reset()
		oldBenchmarkBufferPool.Put(b)
	}
}

// Old approach decompression logic
func (r *Router) readAndCloseCompressedBodyOld(req *http.Request, decoders chan *zstd.Decoder) (*bytes.Buffer, error) {
	defer req.Body.Close()

	var reader io.Reader
	switch req.Header.Get("Content-Encoding") {
	case "zstd":
		zReader := <-decoders
		defer func(zReader *zstd.Decoder) {
			zReader.Reset(nil)
			decoders <- zReader
		}(zReader)

		err := zReader.Reset(req.Body)
		if err != nil {
			return nil, err
		}
		reader = zReader

	default:
		reader = req.Body
	}

	buf := oldBenchmarkBufferPool.Get().(*bytes.Buffer)
	if _, err := io.Copy(buf, io.LimitReader(reader, HTTPMessageSizeMax)); err != nil {
		recycleOldBuffer(buf)
		return nil, err
	}
	return buf, nil
}

// Benchmark old approach (pool of decoders)
func BenchmarkDecoderOld(b *testing.B) {
	decoders, err := makeDecodersForBench(4)
	if err != nil {
		b.Fatal(err)
	}

	router := &Router{}

	for _, bd := range benchmarkData {
		compressed := compressData(bd.data)
		testName := fmt.Sprintf("orig_%dB_comp_%dB", bd.originalSize, len(compressed))
		b.Run(testName, func(b *testing.B) {

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					req := &http.Request{
						Body:   io.NopCloser(bytes.NewReader(compressed)),
						Header: http.Header{},
					}
					req.Header.Set("Content-Encoding", "zstd")

					buf, err := router.readAndCloseCompressedBodyOld(req, decoders)
					if err != nil {
						b.Fatal(err)
					}

					if buf.Len() != len(bd.data) {
						b.Fatalf("decompressed size mismatch: got %d, want %d", buf.Len(), len(bd.data))
					}

					recycleOldBuffer(buf)
				}
			})
		})
	}
}

// Benchmark new approach (single decoder with DecodeAll)
func BenchmarkDecoderNew(b *testing.B) {
	d, err := makeDecoders(4)
	if err != nil {
		b.Fatal(err)
	}

	router := &Router{zstdDecoder: d}

	for _, bd := range benchmarkData {
		compressed := compressData(bd.data)
		testName := fmt.Sprintf("orig_%dB_comp_%dB", bd.originalSize, len(compressed))
		b.Run(testName, func(b *testing.B) {

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					req := &http.Request{
						Body:   io.NopCloser(bytes.NewReader(compressed)),
						Header: http.Header{},
					}
					req.Header.Set("Content-Encoding", "zstd")

					buf, err := router.readAndCloseMaybeCompressedBody(req)
					if err != nil {
						b.Fatal(err)
					}

					if buf.Len() != len(bd.data) {
						b.Fatalf("decompressed size mismatch: got %d, want %d", buf.Len(), len(bd.data))
					}

					recycleHTTPBodyBuffer(buf)
				}
			})
		})
	}
}
