package config

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	"github.com/go-playground/validator"
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
	ListenAddr         string        `validate:"required"`
	PeerListenAddr     string        `validate:"required"`
	APIKeys            []string      `validate:"required"`
	HoneycombAPI       string        `validate:"required,url"`
	Logger             string        `validate:"required,oneof= logrus honeycomb"`
	LoggingLevel       string        `validate:"required"`
	Collector          string        `validate:"required,oneof= InMemCollector"`
	Sampler            string        `validate:"required,oneof= DeterministicSampler DynamicSampler"`
	Metrics            string        `validate:"required,oneof= prometheus honeycomb"`
	SendDelay          time.Duration `validate:"required"`
	TraceTimeout       time.Duration `validate:"required"`
	SendTicker         time.Duration `validate:"required"`
	UpstreamBufferSize int           `validate:"required"`
	PeerBufferSize     int           `validate:"required"`
	DebugServiceAddr   string
	DryRun             bool
	PeerManagement     PeerManagementConfig           `validate:"required"`
	InMemCollector     InMemoryCollectorCacheCapacity `validate:"required"`
}

// Used to marshall in the sampler type in SamplerConfig definitions
// other fields are ignored
type samplerConfigType struct {
	Sampler string
}

type InMemoryCollectorCacheCapacity struct {
	// CacheCapacity must be less than math.MaxInt32
	CacheCapacity int `validate:"required,lt=2147483647"`
}

type HoneycombLevel int

type HoneycombLoggerConfig struct {
	LoggerHoneycombAPI string         `validate:"required,url"`
	LoggerAPIKey       string         `validate:"required"`
	LoggerDataset      string         `validate:"required"`
	Level              HoneycombLevel `validate:"required"`
}

type PrometheusMetricsConfig struct {
	MetricsListenAddr string `validate:"required"`
}

type HoneycombMetricsConfig struct {
	MetricsHoneycombAPI      string `validate:"required,url"`
	MetricsAPIKey            string `validate:"required"`
	MetricsDataset           string `validate:"required"`
	MetricsReportingInterval int64  `validate:"required"`
}

type PeerManagementConfig struct {
	Type                    string   `validate:"required,oneof= file redis"`
	Peers                   []string `validate:"required,dive,url"`
	RedisHost               string
	IdentifierInterfaceName string
	UseIPV6Identifier       bool
	RedisIdentifier         string
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
	c.SetDefault("DryRun", false)

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

	v := validator.New()
	err = v.Struct(fc.conf)
	if err != nil {
		return nil, err
	}

	err = fc.validateConditionalConfigs()

	if err != nil {
		fmt.Println("error validating conditional configs")
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

func (f *fileConfig) validateConditionalConfigs() error {
	// validate logger config
	loggerType, err := f.GetLoggerType()
	if err != nil {
		return err
	}
	if loggerType == "honeycomb" {
		_, err = f.GetHoneycombLoggerConfig()
		if err != nil {
			return err
		}
	}

	// validate metrics config
	metricsType, err := f.GetMetricsType()
	if err != nil {
		return err
	}
	if metricsType == "honeycomb" {
		_, err = f.GetHoneycombMetricsConfig()
		if err != nil {
			return err
		}
	}
	if metricsType == "prometheus" {
		_, err = f.GetPrometheusMetricsConfig()
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *fileConfig) RegisterReloadCallback(cb func()) {
	f.callbacks = append(f.callbacks, cb)
}

func (f *fileConfig) GetListenAddr() (string, error) {
	_, _, err := net.SplitHostPort(f.conf.ListenAddr)
	if err != nil {
		return "", err
	}
	return f.conf.ListenAddr, nil
}

func (f *fileConfig) GetPeerListenAddr() (string, error) {
	_, _, err := net.SplitHostPort(f.conf.PeerListenAddr)
	if err != nil {
		return "", err
	}
	return f.conf.PeerListenAddr, nil
}

func (f *fileConfig) GetAPIKeys() ([]string, error) {
	return f.conf.APIKeys, nil
}

func (f *fileConfig) GetPeerManagementType() (string, error) {
	return f.conf.PeerManagement.Type, nil
}

func (f *fileConfig) GetPeers() ([]string, error) {
	return f.conf.PeerManagement.Peers, nil
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

func (f *fileConfig) GetHoneycombLoggerConfig() (HoneycombLoggerConfig, error) {
	var hlConfig HoneycombLoggerConfig
	if sub := f.config.Sub("HoneycombLogger"); sub != nil {
		err := sub.UnmarshalExact(hlConfig)
		if err != nil {
			return hlConfig, err
		}

		v := validator.New()
		err = v.Struct(hlConfig)
		if err != nil {
			return hlConfig, err
		}

		return hlConfig, nil
	}
	return hlConfig, errors.New("No config found for HoneycombLogger")
}

func (f *fileConfig) GetCollectorType() (string, error) {
	return f.conf.Collector, nil
}

func (f *fileConfig) GetInMemCollectorCacheCapacity() (InMemoryCollectorCacheCapacity, error) {
	var capacity InMemoryCollectorCacheCapacity
	if sub := f.config.Sub("InMemCollector"); sub != nil {
		err := sub.UnmarshalExact(capacity)
		if err != nil {
			return capacity, err
		}
		return capacity, nil
	}
	return capacity, errors.New("No config found for inMemCollector")
}

var (
	// a list of the fallback keys for sampler type
	samplerFallbacks = []string{"SamplerConfig._default.Sampler", "Sampler"}
)

func (f *fileConfig) GetSamplerTypeForDataset(dataset string) (string, error) {
	key := fmt.Sprintf("SamplerConfig.%s.Sampler", dataset)
	keys := append([]string{key}, samplerFallbacks...)

	for _, k := range keys {
		if ok := f.rules.IsSet(k); ok {
			return f.rules.GetString(k), nil
		}
	}

	return "", errors.New("No SamplerType found")
}

func (f *fileConfig) GetMetricsType() (string, error) {
	return f.conf.Metrics, nil
}

func (f *fileConfig) GetHoneycombMetricsConfig() (HoneycombMetricsConfig, error) {
	var hmConfig HoneycombMetricsConfig
	if sub := f.config.Sub("HoneycombMetrics"); sub != nil {
		err := sub.UnmarshalExact(hmConfig)
		if err != nil {
			spew.Dump(err)
			return hmConfig, err
		}

		v := validator.New()
		err = v.Struct(hmConfig)
		if err != nil {
			return hmConfig, err
		}

		return hmConfig, nil
	}
	return hmConfig, errors.New("No config found for HoneycombMetrics")
}

func (f *fileConfig) GetPrometheusMetricsConfig() (PrometheusMetricsConfig, error) {
	var pcConfig PrometheusMetricsConfig
	if sub := f.config.Sub("PrometheusMetrics"); sub != nil {
		err := sub.UnmarshalExact(pcConfig)
		if err != nil {
			return pcConfig, err
		}

		v := validator.New()
		err = v.Struct(pcConfig)
		if err != nil {
			return pcConfig, err
		}

		return pcConfig, nil
	}
	return pcConfig, errors.New("No config found for PrometheusMetrics")
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

func (f *fileConfig) GetDebugServiceAddr() (string, error) {
	_, _, err := net.SplitHostPort(f.conf.DebugServiceAddr)
	if err != nil {
		return "", err
	}
	return f.conf.DebugServiceAddr, nil
}

func (f *fileConfig) GetIsDryRun() bool {
	return f.conf.DryRun
}
