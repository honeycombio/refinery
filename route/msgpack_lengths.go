package route

import (
	"errors"
	"fmt"

	"github.com/tinylib/msgp/msgp"
)

const (
	// maxBatchEventCount is the most events we'll accept in a single batch, as
	// a ceiling on well-formed batches rather than a size limit. It matches the
	// Datadog trace decoder's array limit.
	maxBatchEventCount = 500_000

	// maxMsgpackDepth bounds the walk's recursion. Real events nest a few
	// levels, but it does reject deeper bodies that the msgpack decoder would
	// otherwise accept.
	maxMsgpackDepth = 1000
)

var (
	// ErrMsgpackLengthMismatch is returned when a length header describes more
	// data than the body carries.
	ErrMsgpackLengthMismatch = errors.New("request body declares more data than it contains")
	// ErrMsgpackTooDeeplyNested is returned when a body nests past
	// maxMsgpackDepth.
	ErrMsgpackTooDeeplyNested = errors.New("request body is nested too deeply to be read")
	// ErrBatchTooManyEvents is returned when a well-formed batch exceeds
	// maxBatchEventCount.
	ErrBatchTooManyEvents = fmt.Errorf("batch declares more than %d events", maxBatchEventCount)

	// errStopWalk means the walk hit a byte it cannot read, so the body passes
	// through unjudged. The decoder fails on that byte too, so this only changes
	// which error the client gets, never whether the request succeeds.
	// TestGuardAddsNoRejections pins that.
	errStopWalk = errors.New("cannot interpret further")
)

// checkMsgpackDeclaredLengths rejects a body whose array and map headers declare
// more objects than it contains. It guards the msgpack decoder, which sizes
// slices and maps from those headers and has no limit of its own.
//
// It counts every object declared but not yet visited across all open containers
// and requires the total to fit in the bytes remaining, since each object needs
// at least one byte. Checking at the header keeps the count out of reach of a
// trailing unreadable byte, and counting across containers stops nested headers
// from each claiming the whole body.
func checkMsgpackDeclaredLengths(b []byte) error {
	// An empty body isn't an overrun. Leave it to the decoder to report.
	if len(b) == 0 {
		return nil
	}

	pending := uint64(1) // the top-level object
	if _, err := walkMsgpackLengths(b, 0, &pending); err != nil && !errors.Is(err, errStopWalk) {
		return err
	}
	return nil
}

// walkMsgpackLengths checks one object and returns the bytes after it. The
// object it is about to read is already counted in pending.
func walkMsgpackLengths(b []byte, depth int, pending *uint64) ([]byte, error) {
	if depth > maxMsgpackDepth {
		return nil, ErrMsgpackTooDeeplyNested
	}
	*pending--

	switch msgp.NextType(b) {
	case msgp.ArrayType:
		n, rest, err := msgp.ReadArrayHeaderBytes(b)
		if err != nil {
			return nil, errStopWalk
		}
		if err := addPending(pending, uint64(n), uint64(len(rest))); err != nil {
			return nil, err
		}
		for range n {
			if rest, err = walkMsgpackLengths(rest, depth+1, pending); err != nil {
				return nil, err
			}
		}
		return rest, nil

	case msgp.MapType:
		n, rest, err := msgp.ReadMapHeaderBytes(b)
		if err != nil {
			return nil, errStopWalk
		}
		// A key and its value are one object each.
		if err := addPending(pending, uint64(n)*2, uint64(len(rest))); err != nil {
			return nil, err
		}
		for range uint64(n) * 2 {
			if rest, err = walkMsgpackLengths(rest, depth+1, pending); err != nil {
				return nil, err
			}
		}
		return rest, nil

	default:
		// Scalars and blobs, whose lengths msgp.Skip already checks.
		rest, err := msgp.Skip(b)
		switch {
		case err == nil:
			return rest, nil
		case errors.Is(err, msgp.ErrShortBytes):
			return nil, ErrMsgpackLengthMismatch
		default:
			return nil, errStopWalk
		}
	}
}

// addPending adds a container's declared objects to the outstanding count and
// checks the whole count still fits in budget, the bytes following its header.
func addPending(pending *uint64, declared, budget uint64) error {
	*pending += declared
	if *pending > budget {
		return ErrMsgpackLengthMismatch
	}
	return nil
}
