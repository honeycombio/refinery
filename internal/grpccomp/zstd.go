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

// maxDecodedMessage bounds the window a client-supplied frame may declare. gRPC
// caps the decompressed message at the server's MaxRecvMsgSize, but the decoder
// allocates the frame's window before any output reaches that limit, and the
// library default allows 512MiB. Every encoder level of this library emits at
// most an 8MiB window unless the client sets one explicitly, and a message
// bounded by MaxRecvMsgSize has no use for a longer one. The cap does not depend
// on MaxRecvMsgSize, which can be configured much higher: larger messages still
// decode, they just do not need a larger window. This matches the HTTP path in
// route.makeDecoders.
const maxDecodedMessage = 8 << 20

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
