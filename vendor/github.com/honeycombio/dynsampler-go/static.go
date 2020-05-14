package dynsampler

// Static implements Sampler with a static mapping for sample rates. This is
// useful if you have a known set of keys that you want to sample at specific
// rates and apply a default to everything else.
type Static struct {
	// Rates is the set of sample rates to use
	Rates map[string]int
	// Default is the value to use if the key is not whitelisted in Rates
	Default int
}

// Start initializes the static dynsampler
func (s *Static) Start() error {
	if s.Default == 0 {
		s.Default = 1
	}
	return nil
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key
func (s *Static) GetSampleRate(key string) int {
	if rate, found := s.Rates[key]; found {
		return rate
	}
	return s.Default
}

// SaveState is not implemented
func (s *Static) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (s *Static) LoadState(state []byte) error {
	return nil
}
