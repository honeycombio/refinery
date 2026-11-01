package grpccomp

import (
	"bytes"
	"encoding/binary"
	"io"
	"runtime"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

// zstdFrameWithWindow builds the smallest possible zstd frame whose header
// declares a window of 1<<windowLog bytes. The frame holds a single empty
// block, so it decodes to nothing; only the declared window matters.
func zstdFrameWithWindow(windowLog uint8) []byte {
	return []byte{
		0x28, 0xb5, 0x2f, 0xfd, // frame magic number
		0x00,                  // frame header descriptor: no checksum, no dict, no content size
		(windowLog - 10) << 3, // window descriptor: exponent, mantissa 0
		0x01, 0x00, 0x00,      // last block, raw, zero length
	}
}

// zstdFrameWithContentSize builds the smallest possible zstd frame whose header
// declares the given decompressed content size in a single-segment frame.
func zstdFrameWithContentSize(size uint64) []byte {
	frame := []byte{
		0x28, 0xb5, 0x2f, 0xfd, // frame magic number
		0xe0, // frame header descriptor: content size field of 8 bytes, single segment
	}
	frame = binary.LittleEndian.AppendUint64(frame, size)
	return append(frame, 0x01, 0x00, 0x00) // last block, raw, zero length
}

// TestDecompressRejectsOversizedFrame covers frames that declare far more
// memory than they carry: a huge window, and a huge single-segment content
// size. Both must be refused from the frame header, before the decoder
// allocates anything proportional to the declared size.
func TestDecompressRejectsOversizedFrame(t *testing.T) {
	tests := []struct {
		name    string
		frame   []byte
		wantErr error
	}{
		{
			name:    "window",
			frame:   zstdFrameWithWindow(29), // 512 MiB, the library's default maximum
			wantErr: zstd.ErrWindowSizeExceeded,
		},
		{
			name:    "content size",
			frame:   zstdFrameWithContentSize(256 << 20), // 256 MiB, under the library's default 512 MiB window limit
			wantErr: zstd.ErrDecoderSizeExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressor := &zstdCompressor{}
			reader, err := compressor.Decompress(bytes.NewReader(tt.frame))
			require.NoError(t, err)

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err = io.Copy(io.Discard, reader)
			runtime.ReadMemStats(&after)

			require.ErrorIs(t, err, tt.wantErr)
			allocated := after.TotalAlloc - before.TotalAlloc
			require.Less(t, allocated, uint64(64<<20),
				"decoding a %d byte frame allocated %d bytes", len(tt.frame), allocated)
		})
	}
}

func TestDecompressRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("refinery zstd payload "), 100_000) // ~2.2 MiB
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer encoder.Close()
	compressed := encoder.EncodeAll(payload, nil)

	compressor := &zstdCompressor{}
	reader, err := compressor.Decompress(bytes.NewReader(compressed))
	require.NoError(t, err)
	defer closeReader(t, reader)

	out, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, payload, out)
}

// TestDecompressReleasesDecoder checks that a decoder is returned to the pool
// when the caller stops reading early, as gRPC does once the decompressed
// message exceeds its size limit, and that reuse afterwards still works.
func TestDecompressReleasesDecoder(t *testing.T) {
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer encoder.Close()
	compressed := encoder.EncodeAll(bytes.Repeat([]byte("x"), 1<<20), nil)

	compressor := &zstdCompressor{}
	for range 4 {
		reader, err := compressor.Decompress(bytes.NewReader(compressed))
		require.NoError(t, err)

		// Read less than the whole message, then close, like gRPC's
		// io.LimitReader does when a message is too large.
		_, err = io.CopyN(io.Discard, reader, 1024)
		require.NoError(t, err)
		closeReader(t, reader)

		full, err := compressor.Decompress(bytes.NewReader(compressed))
		require.NoError(t, err)
		out, err := io.ReadAll(full)
		require.NoError(t, err)
		require.Len(t, out, 1<<20)
		closeReader(t, full)
	}
}

func closeReader(t *testing.T, r io.Reader) {
	t.Helper()
	closer, ok := r.(io.Closer)
	require.True(t, ok, "decompressed reader must be closeable so its decoder returns to the pool")
	require.NoError(t, closer.Close())
}
