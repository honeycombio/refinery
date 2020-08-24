package config

import (
	"fmt"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	libhoney "github.com/honeycombio/libhoney-go"
	viper "github.com/spf13/viper"
)

type fileConfig struct {
	config    *viper.Viper
	rules     *viper.Viper
	conf      *configContents
	callbacks []func()
	mux       sync.Mutex
}

type configContents struct {
	ListenAddr         string
	PeerListenAddr     string
	APIKeys            []string
	HoneycombAPI       string
	Logger             string
	LoggingLevel       string
	Collector          string
	Sampler            string
	Metrics            string
	SendDelay          time.Duration
	TraceTimeout       time.Duration
	SendTicker         time.Duration
	UpstreamBufferSize int
	PeerBufferSize     int
	DebugServicePort   string
}

// Used to marshall in the sampler type in SamplerConfig definitions
// other fields are ignored
type samplerConfigType struct {
	Sampler string
}

// NewConfig creates a new config struct
func NewConfig(config, rules string) (Config, error) {
	c := viper.New()

	c.BindEnv("redishost", "SAMPROXY_REDIS_HOST")
	c.SetDefault("ListenAddr", "0.0.0.0:8080")
	c.SetDefault("PeerListenAddr", "0.0.0.0:8081")
	c.SetDefault("APIKeys", []string{"*"})
	c.SetDefault("PeerManagement.Peers", []string{"http://127.0.0.1:8081"})
	c.SetDefault("PeerManagement.Type", "file")
	c.SetDefault("PeerManagement.UseIPV6Identifier", false)
	c.SetDefault("HoneycombAPI", "https://api.honeycomb.io")
	c.SetDefault("Logger", "logrus")
	c.SetDefault("LoggingLevel", "debug")
	c.SetDefault("Collector", "InMemCollector")
	c.SetDefault("Sampler", "DynamicSampler")
	c.SetDefault("Metrics", "honeycomb")
	c.SetDefault("SendDelay", 2*time.Second)
	c.SetDefault("TraceTimeout", 60*time.Second)
	c.SetDefault("SendTicker", 100*time.Millisecond)
	c.SetDefault("UpstreamBufferSize", libhoney.DefaultPendingWorkCapacity)
	c.SetDefault("PeerBufferSize", libhoney.DefaultPendingWorkCapacity)

	c.SetConfigFile(config)
	err := c.ReadInConfig()

	if err != nil {
		return nil, err
	}

	r := viper.New()
	r.SetConfigFile(rules)
	err = r.ReadInConfig()

	if err != nil {
		return nil, err
	}

	fc := &fileConfig{
		config:    c,
		rules:     r,
		conf:      &configContents{},
		callbacks: make([]func(), 0),
	}

	err = fc.unmarshal()

	if err != nil {
		return nil, err
	}

	c.WatchConfig()
	c.OnConfigChange(fc.onChange)

	r.WatchConfig()
	r.OnConfigChange(fc.onChange)

	return fc, nil
}

func (f *fileConfig) onChange(in fsnotify.Event) {
	f.unmarshal()

	for _, c := range f.callbacks {
		c()
	}
}

func (f *fileConfig) unmarshal() error {
	f.mux.Lock()
	defer f.mux.Unlock()
	err := f.config.Unmarshal(f.conf)

	if err != nil {
		return err
	}

	err = f.rules.Unmarshal(f.conf)

	if err != nil {
		return err
	}

	return nil
}

func (f *fileConfig) RegisterReloadCallback(cb func()) {
	f.callbacks = append(f.callbacks, cb)
}

func (f *fileConfig) GetListenAddr() (string, error) {
	return f.conf.ListenAddr, nil
}

func (f *fileConfig) GetPeerListenAddr() (string, error) {
	return f.conf.PeerListenAddr, nil
}

func (f *fileConfig) GetAPIKeys() ([]string, error) {
	return f.conf.APIKeys, nil
}

func (f *fileConfig) GetPeerManagementType() (string, error) {
	return f.config.GetString("PeerManagement.Type"), nil
}

func (f *fileConfig) GetPeers() ([]string, error) {
	return f.config.GetStringSlice("PeerManagement.Peers"), nil
}

func (f *fileConfig) GetRedisHost() (string, error) {
	return f.config.GetString("PeerManagement.RedisHost"), nil
}

func (f *fileConfig) GetIdentifierInterfaceName() (string, error) {
	return f.config.GetString("PeerManagement.IdentifierInterfaceName"), nil
}

func (f *fileConfig) GetUseIPV6Identifier() (bool, error) {
	return f.config.GetBool("PeerManagement.UseIPV6Identifier"), nil
}

func (f *fileConfig) GetRedisIdentifier() (string, error) {
	return f.config.GetString("PeerManagement.RedisIdentifier"), nil
}

func (f *fileConfig) GetHoneycombAPI() (string, error) {
	return f.conf.HoneycombAPI, nil
}

func (f *fileConfig) GetLoggingLevel() (string, error) {
	return f.conf.LoggingLevel, nil
}

func (f *fileConfig) GetLoggerType() (string, error) {
	return f.conf.Logger, nil
}

func (f *fileConfig) GetCollectorType() (string, error) {
	return f.conf.Collector, nil
}

func (f *fileConfig) GetDefaultSamplerType() (string, error) {
	t := samplerConfigType{}
	err := f.GetOtherConfig("SamplerConfig._default", &t)
	if err != nil {
		return "", err
	}
	return t.Sampler, nil
}

func (f *fileConfig) GetSamplerTypeForDataset(dataset string) (string, error) {
	t := samplerConfigType{}
	err := f.GetOtherConfig("SamplerConfig."+dataset, &t)
	if err != nil {
		return "", err
	}
	return t.Sampler, nil
}

func (f *fileConfig) GetMetricsType() (string, error) {
	return f.conf.Metrics, nil
}

func (f *fileConfig) GetSendDelay() (time.Duration, error) {
	return f.conf.SendDelay, nil
}

func (f *fileConfig) GetTraceTimeout() (time.Duration, error) {
	return f.conf.TraceTimeout, nil
}

func (f *fileConfig) GetOtherConfig(name string, iface interface{}) error {
	if sub := f.config.Sub(name); sub != nil {
		return sub.Unmarshal(iface)
	}

	if sub := f.rules.Sub(name); sub != nil {
		return sub.Unmarshal(iface)
	}

	return fmt.Errorf("failed to find config tree for %s", name)
}

func (f *fileConfig) GetUpstreamBufferSize() int {
	return f.conf.UpstreamBufferSize
}

func (f *fileConfig) GetPeerBufferSize() int {
	return f.conf.PeerBufferSize
}

func (f *fileConfig) GetSendTickerValue() time.Duration {
	return f.conf.SendTicker
}

func (f *fileConfig) GetDebugServicePort() string {
	return f.conf.DebugServicePort
}
