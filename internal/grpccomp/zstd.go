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

var decoderPool = sync.Pool{
	New: func() any {
		dec, err := zstd.NewReader(nil)
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
	dec *zstd.Decoder
}

func (r *zstdReader) Read(p []byte) (int, error) {
	n, err := r.dec.Read(p)
	if err != nil {
		decoderPool.Put(&decoderResult{dec: r.dec})
	}
	return n, err
}
