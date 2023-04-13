package config

import (
	"errors"
	"net"
	"strings"
	"sync"
	"time"
)

// In order to be able to unmarshal "15s" etc. into time.Duration, we need to
// define a new type and implement MarshalText and UnmarshalText.
type Duration time.Duration

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalText(text []byte) error {
	dur, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}

type fileConfig struct {
	mainConfig    *configContents
	mainHash      string
	rulesConfig   map[string]any
	rulesHash     string
	opts          *CmdEnv
	callbacks     []func()
	errorCallback func(error)
	done          chan struct{}
	ticker        *time.Ticker
	mux           sync.RWMutex
	lastLoadTime  time.Time
}

type configContents struct {
	ListenAddr                string                         `default:"0.0.0.0:8080" cmdenv:"HTTPListenAddr" validate:"required"`
	PeerListenAddr            string                         `default:"0.0.0.0:8081" cmdenv:"PeerListenAddr" validate:"required"`
	CompressPeerCommunication bool                           `default:"true"`
	GRPCListenAddr            string                         `cmdenv:"GRPCListenAddr"`
	APIKeys                   []string                       `default:"[\"*\"]" validate:"required"`
	HoneycombAPI              string                         `default:"https://api.honeycomb.io" cmdenv:"HoneycombAPI" validate:"required,url"`
	Logger                    string                         `default:"logrus" validate:"required,oneof= logrus honeycomb"`
	LoggingLevel              string                         `default:"info" validate:"required,oneof= debug info warn error"`
	Collector                 string                         `default:"InMemCollector" validate:"required,oneof= InMemCollector"`
	Metrics                   string                         `default:"honeycomb" validate:"required,oneof= prometheus honeycomb"`
	SendDelay                 Duration                       `default:"2s" validate:"required"`
	BatchTimeout              Duration                       `default:"100ms"`
	TraceTimeout              Duration                       `default:"60s" validate:"required"`
	MaxBatchSize              uint                           `default:"500" validate:"required"`
	SendTicker                Duration                       `default:"100ms" validate:"required"`
	UpstreamBufferSize        int                            `default:"10_000" validate:"required"`
	PeerBufferSize            int                            `default:"10_000" validate:"required"`
	DebugServiceAddr          string                         ``
	PeerManagement            PeerManagementConfig           `validate:"required"`
	InMemCollector            InMemoryCollectorCacheCapacity `validate:"required"`
	AddHostMetadataToTrace    bool                           ``
	AddRuleReasonToTrace      bool                           ``
	EnvironmentCacheTTL       Duration                       `default:"1h"`
	DatasetPrefix             string                         ``
	QueryAuthToken            string                         `cmdenv:"QueryAuthToken"`
	AdditionalErrorFields     []string                       `default:"[\"trace.span_id\"]"`
	AddSpanCountToRoot        bool                           ``
	CacheOverrunStrategy      string                         `default:"impact"`
	SampleCache               SampleCacheConfig              `validate:"required"`
	StressRelief              StressReliefConfig             `validate:"required"`
	AdditionalAttributes      map[string]string              `default:"{}"`
	TraceIdFieldNames         []string                       `default:"[\"trace.trace_id\",\"traceId\"]"`
	ParentIdFieldNames        []string                       `default:"[\"trace.parent_id\",\"parentId\"]"`
	ConfigReloadInterval      Duration                       `default:"30s"`
	DryRun                    bool                           ``
	DryRunFieldName           string                         `default:"refinery_kept"`
	GRPCServerParameters      *GRPCServerParameters
	HoneycombLogger           *HoneycombLoggerConfig
	HoneycombMetrics          *HoneycombMetricsConfig
	PrometheusMetrics         *PrometheusMetricsConfig
}

type InMemoryCollectorCacheCapacity struct {
	// CacheCapacity must be less than math.MaxInt32
	CacheCapacity int    `default:"10_000" validate:"required,lt=2147483647"`
	MaxAlloc      uint64 ``
}

type HoneycombLevel int

type HoneycombLoggerConfig struct {
	LoggerHoneycombAPI      string         `validate:"required,url"`
	LoggerAPIKey            string         `cmdenv:"HoneycombLoggerAPIKey,HoneycombAPIKey" validate:"required"`
	LoggerDataset           string         `default:"Refinery Logs" validate:"required"`
	LoggerSamplerEnabled    bool           ``
	LoggerSamplerThroughput int            `default:"5"`
	Level                   HoneycombLevel `default:"Warn"`
}

type PrometheusMetricsConfig struct {
	MetricsListenAddr string `validate:"required"`
}

type HoneycombMetricsConfig struct {
	MetricsHoneycombAPI      string `validate:"required,url"`
	MetricsAPIKey            string `cmdenv:"HoneycombMetricsAPIKey,HoneycombAPIKey" validate:"required"`
	MetricsDataset           string `validate:"required"`
	MetricsReportingInterval int64  `default:"3s" validate:"required"`
}

type PeerManagementConfig struct {
	Type                    string   `default:"file" validate:"required,oneof= file redis"`
	Peers                   []string `default:"[\"http://127.0.0.1:8081\"]" validate:"dive,url"`
	RedisHost               string   `cmdenv:"RedisHost"`
	RedisUsername           string   `cmdenv:"RedisUsername"`
	RedisPassword           string   `cmdenv:"RedisPassword"`
	RedisPrefix             string   `default:"refinery" validate:"required"`
	RedisDatabase           int      `validate:"gte=0,lte=15"`
	RedisIdentifier         string   ``
	UseTLS                  bool     ``
	UseTLSInsecure          bool     ``
	IdentifierInterfaceName string   ``
	UseIPV6Identifier       bool     ``
	Timeout                 Duration `default:"5s" validate:"gte=1_000_000_000"`
	Strategy                string   `default:"legacy" validate:"required,oneof= legacy hash"`
}

type SampleCacheConfig struct {
	Type              string   `default:"legacy" validate:"required,oneof= legacy cuckoo"`
	KeptSize          uint     `default:"10_000" validate:"gte=500"`
	DroppedSize       uint     `default:"1_000_000" validate:"gte=100_000"`
	SizeCheckInterval Duration `default:"10s" validate:"gte=1_000_000_000"` // 1 second minimum
}

// GRPCServerParameters allow you to configure the GRPC ServerParameters used
// by refinery's own GRPC server:
// https://pkg.go.dev/google.golang.org/grpc/keepalive#ServerParameters
type GRPCServerParameters struct {
	MaxConnectionIdle     Duration `default:"1s"`
	MaxConnectionAge      Duration `default:"5s"`
	MaxConnectionAgeGrace Duration `default:"3s"`
	Time                  Duration `default:"10s"`
	Timeout               Duration `default:"2s"`
}

type StressReliefConfig struct {
	Mode                      string   `default:"never" validate:"required,oneof= always never monitor"`
	ActivationLevel           uint     `default:"90" validate:"gte=0,lte=100"`
	DeactivationLevel         uint     `default:"75" validate:"gte=0,lte=100"`
	StressSamplingRate        uint64   `default:"1000" validate:"gte=1"`
	MinimumActivationDuration Duration `default:"10s"`
	StartStressedDuration     Duration `default:"3s"`
}

// newFileConfig does the work of creating and loading the start of a config object
// from the given arguments.
// It's used by both the main init as well as the reload code.
func newFileConfig(opts *CmdEnv) (*fileConfig, error) {
	mainconf := &configContents{}
	mainhash, err := readConfigInto(mainconf, opts.ConfigLocation, opts)
	if err != nil {
		return nil, err
	}

	var rulesconf map[string]any
	ruleshash, err := readConfigInto(rulesconf, opts.RulesLocation, opts)
	if err != nil {
		return nil, err
	}

	// TODO: this is temporary while we still conform to the old config format;
	// once we're fully migrated, we can remove this
	if dryRun, ok := getValueForCaseInsensitiveKey(rulesconf, "dryrun", false); ok {
		mainconf.DryRun = dryRun
	}
	if dryRunFieldName, ok := getValueForCaseInsensitiveKey(rulesconf, "dryrunfieldname", ""); ok && dryRunFieldName != "" {
		mainconf.DryRunFieldName = dryRunFieldName
	}

	cfg := &fileConfig{
		mainConfig:  mainconf,
		mainHash:    mainhash,
		rulesConfig: rulesconf,
		rulesHash:   ruleshash,
		opts:        opts,
	}

	return cfg, nil
}

// NewConfig creates a new Config object from the given arguments; if args is
// nil, it uses the command line arguments
func NewConfig(opts *CmdEnv, errorCallback func(error)) (Config, error) {
	cfg, err := newFileConfig(opts)
	if err != nil {
		return nil, err
	}

	cfg.callbacks = make([]func(), 0)
	cfg.errorCallback = errorCallback

	go cfg.monitor()

	return cfg, nil
}

func (f *fileConfig) monitor() {
	f.done = make(chan struct{})
	f.ticker = time.NewTicker(time.Duration(f.mainConfig.ConfigReloadInterval))
	for {
		select {
		case <-f.done:
			return
		case <-f.ticker.C:
			// reread the configs
			cfg, err := newFileConfig(f.opts)
			if err != nil {
				f.errorCallback(err)
				continue
			}

			// if nothing's changed, we're fine
			if f.mainHash == cfg.mainHash && f.rulesHash == cfg.rulesHash {
				continue
			}

			// otherwise, update our state and call the callbacks
			f.mux.Lock()
			f.mainConfig = cfg.mainConfig
			f.mainHash = cfg.mainHash
			f.rulesConfig = cfg.rulesConfig
			f.rulesHash = cfg.rulesHash
			for _, cb := range f.callbacks {
				cb()
			}
			f.mux.Unlock() // can't defer since the goroutine never ends
		}
	}
}

// Stop halts the monitor goroutine
func (f *fileConfig) Stop() {
	f.ticker.Stop()
	close(f.done)
	f.done = nil
}

func (f *fileConfig) RegisterReloadCallback(cb func()) {
	f.mux.Lock()
	defer f.mux.Unlock()

	f.callbacks = append(f.callbacks, cb)
}

func (f *fileConfig) GetListenAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.mainConfig.ListenAddr)
	if err != nil {
		return "", err
	}
	return f.mainConfig.ListenAddr, nil
}

func (f *fileConfig) GetPeerListenAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.mainConfig.PeerListenAddr)
	if err != nil {
		return "", err
	}
	return f.mainConfig.PeerListenAddr, nil
}

func (f *fileConfig) GetCompressPeerCommunication() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.CompressPeerCommunication
}

func (f *fileConfig) GetGRPCListenAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	// GRPC listen addr is optional, only check value is valid if not empty
	if f.mainConfig.GRPCListenAddr != "" {
		_, _, err := net.SplitHostPort(f.mainConfig.GRPCListenAddr)
		if err != nil {
			return "", err
		}
	}
	return f.mainConfig.GRPCListenAddr, nil
}

func (f *fileConfig) GetAPIKeys() ([]string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.APIKeys, nil
}

func (f *fileConfig) GetPeerManagementType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Type, nil
}

func (f *fileConfig) GetPeerManagementStrategy() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Strategy, nil
}

func (f *fileConfig) GetPeers() ([]string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Peers, nil
}

func (f *fileConfig) GetRedisHost() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisHost, nil
}

func (f *fileConfig) GetRedisUsername() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisUsername, nil
}

func (f *fileConfig) GetRedisPrefix() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisPrefix
}

func (f *fileConfig) GetRedisPassword() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisPassword, nil
}

func (f *fileConfig) GetRedisDatabase() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisDatabase
}

func (f *fileConfig) GetUseTLS() (bool, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.UseTLS, nil
}

func (f *fileConfig) GetUseTLSInsecure() (bool, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.UseTLSInsecure, nil
}

func (f *fileConfig) GetIdentifierInterfaceName() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.IdentifierInterfaceName, nil
}

func (f *fileConfig) GetUseIPV6Identifier() (bool, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.UseIPV6Identifier, nil
}

func (f *fileConfig) GetRedisIdentifier() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.RedisIdentifier, nil
}

func (f *fileConfig) GetHoneycombAPI() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.HoneycombAPI, nil
}

func (f *fileConfig) GetLoggingLevel() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.LoggingLevel, nil
}

func (f *fileConfig) GetLoggerType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Logger, nil
}

func (f *fileConfig) GetHoneycombLoggerConfig() (HoneycombLoggerConfig, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return *f.mainConfig.HoneycombLogger, nil
}

func (f *fileConfig) GetCollectorType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Collector, nil
}

func (f *fileConfig) GetAllSamplerRules() (map[string]any, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	// This is probably good enough for debug; if not we can extend it.
	return f.rulesConfig, nil
}

// getValueForCaseInsensitiveKey is a generic function that returns the value from a map[string]any
// for the given key, ignoring case of the key. It returns ok=true only if the key was found
// and could be converted to the required type. Otherwise it returns the default value
// and ok=false.
func getValueForCaseInsensitiveKey[T any](m map[string]any, key string, def T) (T, bool) {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			if t, ok := v.(T); ok {
				return t, true
			}
		}
	}
	return def, false
}

// GetSamplerConfigForDataset returns the sampler config for the given dataset,
// as well as the name of the sampler. If the dataset-specific sampler config
// is not found, it returns the default sampler config.
func (f *fileConfig) GetSamplerConfigForDataset(dataset string) (any, string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	config := f.rulesConfig
	// If we have a dataset-specific sampler, we extract the sampler config
	// corresponding to the [dataset]["sampler"] key. Otherwise we try to use
	// the default sampler config corresponding to the "sampler" key. Only if
	// both fail will we return not found.

	const notfound = "not found"
	if v, ok := getValueForCaseInsensitiveKey(config, dataset, map[string]any{}); ok {
		// we have a dataset-specific sampler, so we extract that sampler's config
		config = v
	}

	// now we need the name of the sampler
	samplerName, _ := getValueForCaseInsensitiveKey(config, "sampler", "DeterministicSampler")

	var i any
	switch samplerName {
	case "DeterministicSampler":
		i = &DeterministicSamplerConfig{}
	case "DynamicSampler":
		i = &DynamicSamplerConfig{}
	case "EMADynamicSampler":
		i = &EMADynamicSamplerConfig{}
	case "RulesBasedSampler":
		i = &RulesBasedSamplerConfig{}
	case "TotalThroughputSampler":
		i = &TotalThroughputSamplerConfig{}
	default:
		return nil, notfound, errors.New("no sampler found")
	}

	// now we need to unmarshal the config into the sampler config struct
	err := reloadInto(config, i, f.opts)
	return i, samplerName, err
}

func (f *fileConfig) GetInMemCollectorCacheCapacity() (InMemoryCollectorCacheCapacity, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.InMemCollector, nil
}

func (f *fileConfig) GetMetricsType() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Metrics, nil
}

func (f *fileConfig) GetHoneycombMetricsConfig() (HoneycombMetricsConfig, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return *f.mainConfig.HoneycombMetrics, nil
}

func (f *fileConfig) GetPrometheusMetricsConfig() (PrometheusMetricsConfig, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return *f.mainConfig.PrometheusMetrics, nil
}

func (f *fileConfig) GetSendDelay() (time.Duration, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.SendDelay), nil
}

func (f *fileConfig) GetBatchTimeout() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.BatchTimeout)
}

func (f *fileConfig) GetTraceTimeout() (time.Duration, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.TraceTimeout), nil
}

func (f *fileConfig) GetMaxBatchSize() uint {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.MaxBatchSize
}

func (f *fileConfig) GetUpstreamBufferSize() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.UpstreamBufferSize
}

func (f *fileConfig) GetPeerBufferSize() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerBufferSize
}

func (f *fileConfig) GetSendTickerValue() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.SendTicker)
}

func (f *fileConfig) GetDebugServiceAddr() (string, error) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.mainConfig.DebugServiceAddr)
	if err != nil {
		return "", err
	}
	return f.mainConfig.DebugServiceAddr, nil
}

func (f *fileConfig) GetIsDryRun() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.DryRun
}

func (f *fileConfig) GetDryRunFieldName() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.DryRunFieldName
}

func (f *fileConfig) GetAddHostMetadataToTrace() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AddHostMetadataToTrace
}

func (f *fileConfig) GetAddRuleReasonToTrace() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AddRuleReasonToTrace
}

func (f *fileConfig) GetEnvironmentCacheTTL() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.EnvironmentCacheTTL)
}

func (f *fileConfig) GetDatasetPrefix() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.DatasetPrefix
}

func (f *fileConfig) GetQueryAuthToken() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.QueryAuthToken
}

func (f *fileConfig) GetGRPCMaxConnectionIdle() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.GRPCServerParameters.MaxConnectionIdle)
}

func (f *fileConfig) GetGRPCMaxConnectionAge() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.GRPCServerParameters.MaxConnectionAge)
}

func (f *fileConfig) GetGRPCMaxConnectionAgeGrace() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.GRPCServerParameters.MaxConnectionAgeGrace)
}

func (f *fileConfig) GetGRPCTime() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.GRPCServerParameters.Time)
}

func (f *fileConfig) GetGRPCTimeout() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.GRPCServerParameters.Timeout)
}

func (f *fileConfig) GetPeerTimeout() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.PeerManagement.Timeout)
}

func (f *fileConfig) GetAdditionalErrorFields() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AdditionalErrorFields
}

func (f *fileConfig) GetAddSpanCountToRoot() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AddSpanCountToRoot
}

func (f *fileConfig) GetCacheOverrunStrategy() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.CacheOverrunStrategy
}

func (f *fileConfig) GetSampleCacheConfig() SampleCacheConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.SampleCache
}

func (f *fileConfig) GetStressReliefConfig() StressReliefConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.StressRelief
}

func (f *fileConfig) GetTraceIdFieldNames() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.TraceIdFieldNames
}

func (f *fileConfig) GetParentIdFieldNames() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.ParentIdFieldNames
}

func (f *fileConfig) GetConfigMetadata() []ConfigMetadata {
	ret := make([]ConfigMetadata, 2)
	ret[0] = ConfigMetadata{
		Type:     "config",
		ID:       f.opts.ConfigLocation,
		Hash:     f.mainHash,
		LoadedAt: f.lastLoadTime.Format(time.RFC3339),
	}
	ret[1] = ConfigMetadata{
		Type:     "rules",
		ID:       f.opts.RulesLocation,
		Hash:     f.rulesHash,
		LoadedAt: f.lastLoadTime.Format(time.RFC3339),
	}
	return ret
}

func (f *fileConfig) GetAdditionalAttributes() map[string]string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AdditionalAttributes
}
