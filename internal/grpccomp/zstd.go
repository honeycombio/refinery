// Package grpccomp registers a zstd compressor for gRPC.
// Import with a blank identifier to enable zstd compression:
//
//	import _ "github.com/honeycombio/refinery/internal/grpccomp"
package grpccomp

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

func init() {
	encoding.RegisterCompressor(&zstdCompressor{})
}

type zstdCompressor struct{}

func (c *zstdCompressor) Name() string { return "zstd" }

func (c *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	r := encoderPool.Get().(*encoderResult)
	if r.err != nil {
		return nil, r.err
	}
	r.enc.Reset(w)
	return &zstdWriteCloser{enc: r.enc}, nil
}

func (c *zstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	dr := decoderPool.Get().(*decoderResult)
	if dr.err != nil {
		return nil, dr.err
	}
	if err := dr.dec.Reset(r); err != nil {
		decoderPool.Put(dr)
		return nil, err
	}
	return &zstdReader{dec: dr.dec}, nil
}

type encoderResult struct {
	enc *zstd.Encoder
	err error
}

type decoderResult struct {
	dec *zstd.Decoder
	err error
}

var encoderPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		return &encoderResult{enc: enc, err: err}
	},
}

// maxDecodedMessage bounds the memory a zstd decoder may allocate while
// decompressing one message. gRPC caps the decompressed output at the server's
// MaxRecvMsgSize (15MB by default in refinery's config) and a frame's window
// never needs to exceed the message it carries, so this covers that limit and
// leaves room for doubling it. Without a bound the decoder honors whatever
// window a frame declares, up to the library default of 512MiB, and a message a
// few bytes long can make refinery allocate that much.
const maxDecodedMessage = 32 << 20

var decoderPool = sync.Pool{
	New: func() any {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(maxDecodedMessage))
		return &decoderResult{dec: dec, err: err}
	},
}

type zstdWriteCloser struct {
	enc *zstd.Encoder
}

func (w *zstdWriteCloser) Write(p []byte) (int, error) {
	return w.enc.Write(p)
}

func (w *zstdWriteCloser) Close() error {
	err := w.enc.Close()
	if err == nil {
		encoderPool.Put(&encoderResult{enc: w.enc})
	}
	return err
}

type zstdReader struct {
	mu       sync.Mutex
	dec      *zstd.Decoder
	released bool
}

// Read and Close take the lock because a decoder decodes one stream at a time,
// and a released decoder is already back in the pool for another message.
func (r *zstdReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.released {
		return 0, io.EOF
	}
	n, err := r.dec.Read(p)
	if err != nil {
		r.releaseLocked()
	}
	return n, err
}

// Close returns the decoder to the pool. gRPC stops reading as soon as the
// decompressed message exceeds its size limit and closes the reader, so a
// decoder abandoned mid-stream would otherwise keep its window buffer forever.
func (r *zstdReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.releaseLocked()
	return nil
}

func (r *zstdReader) releaseLocked() {
	if r.released {
		return
	}
	r.released = true
	// Reset(nil) drains the stream and drops the frame, so the decoder is safe
	// to hand to the next stream. If it fails, the decoder's state is unknown,
	// so drop it instead of pooling it.
	if err := r.dec.Reset(nil); err != nil {
		return
	}
	decoderPool.Put(&decoderResult{dec: r.dec})
}
