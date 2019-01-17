package sample

import (
	"crypto/sha1"
	"fmt"
	"math"

	"github.com/honeycombio/samproxy/config"
	"github.com/honeycombio/samproxy/logger"
	"github.com/honeycombio/samproxy/types"
)

type DeterministicSampler struct {
	Config config.Config `inject:""`
	Logger logger.Logger `inject:""`

	sampleRate int
	upperBound uint32
	configName string
}

type DetSamplerConfig struct {
	SampleRate int
}

func (d *DeterministicSampler) Start() error {
	if err := d.loadConfigs(); err != nil {
		return err
	}

	// listen for config reloads with an errorless version of the reload
	d.Config.RegisterReloadCallback(func() {
		if err := d.loadConfigs(); err != nil {
			d.Logger.Errorf("failed to reload deterministic sampler configs: %+v", err)
		}
	})

	return nil
}

func (d *DeterministicSampler) loadConfigs() error {
	dsConfig := DetSamplerConfig{}
	configKey := fmt.Sprintf("SamplerConfig.%s", d.configName)
	err := d.Config.GetOtherConfig(configKey, &dsConfig)
	if err != nil {
		return err
	}
	if dsConfig.SampleRate < 1 {
		d.Logger.Debugf("configured sample rate for deterministic sampler was %d; forcing to 1", dsConfig.SampleRate)
		dsConfig.SampleRate = 1
	}
	d.sampleRate = dsConfig.SampleRate

	// Get the actual upper bound - the largest possible value divided by
	// the sample rate. In the case where the sample rate is 1, this should
	// sample every value.
	d.upperBound = math.MaxUint32 / uint32(d.sampleRate)
	return nil
}

func (d *DeterministicSampler) GetSampleRate(trace *types.Trace) (rate uint, keep bool) {
	if d.sampleRate <= 1 {
		return 1, true
	}
	sum := sha1.Sum([]byte(trace.TraceID))
	v := bytesToUint32be(sum[:4])
	return uint(d.sampleRate), v <= d.upperBound
}

// bytesToUint32 takes a slice of 4 bytes representing a big endian 32 bit
// unsigned value and returns the equivalent uint32.
func bytesToUint32be(b []byte) uint32 {
	return uint32(b[3]) | (uint32(b[2]) << 8) | (uint32(b[1]) << 16) | (uint32(b[0]) << 24)
}
