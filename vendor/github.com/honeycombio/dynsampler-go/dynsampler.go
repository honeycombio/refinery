package dynsampler

// Sampler is the interface to samplers using different methods to determine
// sample rate. You should instantiate one of the actual samplers in this
// package, depending on the sample method you'd like to use. Each sampling
// method has its own set of struct variables you should set before Start()ing
// the sampler.
type Sampler interface {
	// Start initializes the sampler. You should call Start() before using the
	// sampler.
	Start() error

	// GetSampleRate will return the sample rate to use for the string given. You
	// should call it with whatever key you choose to use to partition traffic
	// into different sample rates.
	GetSampleRate(string) int

	// SaveState returns a byte array containing the state of the Sampler implementation.
	// It can be used to persist state between process restarts.
	SaveState() ([]byte, error)

	// LoadState accepts a byte array containing the serialized, previous state of the sampler
	// implementation. It should be called before `Start`.
	LoadState([]byte) error
}
