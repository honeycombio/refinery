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
}
