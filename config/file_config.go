package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v3"
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
	rulesConfig   *V2SamplerConfig
	rulesHash     string
	opts          *CmdEnv
	callbacks     []ConfigReloadCallback
	errorCallback func(error)
	mux           sync.RWMutex
	lastLoadTime  time.Time
}

// ensure that fileConfig implements Config
var _ Config = (*fileConfig)(nil)

type configContents struct {
	General              GeneralConfig             `yaml:"General"`
	Network              NetworkConfig             `yaml:"Network"`
	AccessKeys           AccessKeyConfig           `yaml:"AccessKeys"`
	Telemetry            RefineryTelemetryConfig   `yaml:"RefineryTelemetry"`
	Traces               TracesConfig              `yaml:"Traces"`
	Debugging            DebuggingConfig           `yaml:"Debugging"`
	Logger               LoggerConfig              `yaml:"Logger"`
	HoneycombLogger      HoneycombLoggerConfig     `yaml:"HoneycombLogger"`
	StdoutLogger         StdoutLoggerConfig        `yaml:"StdoutLogger"`
	PrometheusMetrics    PrometheusMetricsConfig   `yaml:"PrometheusMetrics"`
	LegacyMetrics        LegacyMetricsConfig       `yaml:"LegacyMetrics"`
	OTelMetrics          OTelMetricsConfig         `yaml:"OTelMetrics"`
	OTelTracing          OTelTracingConfig         `yaml:"OTelTracing"`
	PeerManagement       PeerManagementConfig      `yaml:"PeerManagement"`
	RedisPeerManagement  RedisPeerManagementConfig `yaml:"RedisPeerManagement"`
	Collection           CollectionConfig          `yaml:"Collection"`
	BufferSizes          BufferSizeConfig          `yaml:"BufferSizes"`
	Specialized          SpecializedConfig         `yaml:"Specialized"`
	IDFieldNames         IDFieldsConfig            `yaml:"IDFields"`
	GRPCServerParameters GRPCServerParameters      `yaml:"GRPCServerParameters"`
	SampleCache          SampleCacheConfig         `yaml:"SampleCache"`
	StressRelief         StressReliefConfig        `yaml:"StressRelief"`
}

type GeneralConfig struct {
	ConfigurationVersion int      `yaml:"ConfigurationVersion"`
	MinRefineryVersion   string   `yaml:"MinRefineryVersion" default:"v2.0"`
	DatasetPrefix        string   `yaml:"DatasetPrefix" `
	ConfigReloadInterval Duration `yaml:"ConfigReloadInterval" default:"15s"`
}

type NetworkConfig struct {
	ListenAddr      string   `yaml:"ListenAddr" default:"0.0.0.0:8080" cmdenv:"HTTPListenAddr"`
	PeerListenAddr  string   `yaml:"PeerListenAddr" default:"0.0.0.0:8081" cmdenv:"PeerListenAddr"`
	HoneycombAPI    string   `yaml:"HoneycombAPI" default:"https://api.honeycomb.io" cmdenv:"HoneycombAPI"`
	HTTPIdleTimeout Duration `yaml:"HTTPIdleTimeout"`
}

type AccessKeyConfig struct {
	ReceiveKeys          []string `yaml:"ReceiveKeys" default:"[]"`
	SendKey              string   `yaml:"SendKey"`
	SendKeyMode          string   `yaml:"SendKeyMode" default:"none"`
	AcceptOnlyListedKeys bool     `yaml:"AcceptOnlyListedKeys"`
}

// truncate the key to 8 characters for logging
func (a *AccessKeyConfig) sanitize(key string) string {
	return fmt.Sprintf("%.8s...", key)
}

// CheckAndMaybeReplaceKey checks the given API key against the configuration
// and possibly replaces it with the configured SendKey, if the settings so indicate.
// It returns the key to use, or an error if the key is invalid given the settings.
func (a *AccessKeyConfig) CheckAndMaybeReplaceKey(apiKey string) (string, error) {
	// Apply AcceptOnlyListedKeys logic BEFORE we consider replacement
	if a.AcceptOnlyListedKeys && !slices.Contains(a.ReceiveKeys, apiKey) {
		err := fmt.Errorf("api key %s not found in list of authorized keys", a.sanitize(apiKey))
		return "", err
	}

	if a.SendKey != "" {
		overwriteWith := ""
		switch a.SendKeyMode {
		case "none":
			// don't replace keys at all
			// (SendKey is disabled)
		case "all":
			// overwrite all keys, even missing ones, with the configured one
			overwriteWith = a.SendKey
		case "nonblank":
			// only replace nonblank keys with the configured one
			if apiKey != "" {
				overwriteWith = a.SendKey
			}
		case "listedonly":
			// only replace keys that are listed in the `ReceiveKeys` list,
			// otherwise use original key
			overwriteWith = apiKey
			if slices.Contains(a.ReceiveKeys, apiKey) {
				overwriteWith = a.SendKey
			}
		case "missingonly":
			// only inject keys into telemetry that doesn't have a key at all
			// otherwise use original key
			overwriteWith = apiKey
			if apiKey == "" {
				overwriteWith = a.SendKey
			}
		case "unlisted":
			// only replace nonblank keys that are NOT listed in the `ReceiveKeys` list
			// otherwise use original key
			if apiKey != "" {
				overwriteWith = apiKey
				if !slices.Contains(a.ReceiveKeys, apiKey) {
					overwriteWith = a.SendKey
				}
			}
		}
		apiKey = overwriteWith
	}

	if apiKey == "" {
		return "", fmt.Errorf("blank API key is not permitted with this configuration")
	}
	return apiKey, nil
}

type DefaultTrue bool

func (dt *DefaultTrue) Get() (enabled bool) {
	if dt == nil {
		return true
	}
	return bool(*dt)
}

func (dt *DefaultTrue) MarshalText() ([]byte, error) {
	return []byte(strconv.FormatBool(bool(*dt))), nil
}

func (dt *DefaultTrue) UnmarshalText(text []byte) error {
	trueBool, err := strconv.ParseBool(string(text))
	if err != nil {
		return err
	}

	*dt = DefaultTrue(trueBool)

	return nil
}

type RefineryTelemetryConfig struct {
	AddRuleReasonToTrace   bool         `yaml:"AddRuleReasonToTrace"`
	AddSpanCountToRoot     *DefaultTrue `yaml:"AddSpanCountToRoot" default:"true"` // Avoid pointer woe on access, use GetAddSpanCountToRoot() instead.
	AddCountsToRoot        bool         `yaml:"AddCountsToRoot"`
	AddHostMetadataToTrace *DefaultTrue `yaml:"AddHostMetadataToTrace" default:"true"` // Avoid pointer woe on access, use GetAddHostMetadataToTrace() instead.
}

type TracesConfig struct {
	SendDelay    Duration `yaml:"SendDelay" default:"2s"`
	BatchTimeout Duration `yaml:"BatchTimeout" default:"100ms"`
	TraceTimeout Duration `yaml:"TraceTimeout" default:"60s"`
	MaxBatchSize uint     `yaml:"MaxBatchSize" default:"500"`
	SendTicker   Duration `yaml:"SendTicker" default:"100ms"`
	SpanLimit    uint     `yaml:"SpanLimit"`
}

func (t TracesConfig) GetSendDelay() time.Duration {
	return time.Duration(t.SendDelay)
}

func (t TracesConfig) GetBatchTimeout() time.Duration {
	return time.Duration(t.BatchTimeout)
}

func (t TracesConfig) GetTraceTimeout() time.Duration {
	return time.Duration(t.TraceTimeout)
}

func (t TracesConfig) GetMaxBatchSize() uint {
	return t.MaxBatchSize
}

func (t TracesConfig) GetSendTickerValue() time.Duration {
	return time.Duration(t.SendTicker)
}

type DebuggingConfig struct {
	DebugServiceAddr      string   `yaml:"DebugServiceAddr"`
	QueryAuthToken        string   `yaml:"QueryAuthToken" cmdenv:"QueryAuthToken"`
	AdditionalErrorFields []string `yaml:"AdditionalErrorFields" default:"[\"trace.span_id\"]"`
	DryRun                bool     `yaml:"DryRun" `
}

type LoggerConfig struct {
	Type  string `yaml:"Type" default:"stdout"`
	Level Level  `yaml:"Level" default:"warn"`
}

type HoneycombLoggerConfig struct {
	APIHost           string       `yaml:"APIHost" default:"https://api.honeycomb.io"`
	APIKey            string       `yaml:"APIKey" cmdenv:"HoneycombLoggerAPIKey,HoneycombAPIKey"`
	Dataset           string       `yaml:"Dataset" default:"Refinery Logs"`
	SamplerEnabled    *DefaultTrue `yaml:"SamplerEnabled" default:"true"` // Avoid pointer woe on access, use GetSamplerEnabled() instead.
	SamplerThroughput int          `yaml:"SamplerThroughput" default:"10"`
}

// GetSamplerEnabled returns whether configuration has enabled sampling of
// Refinery's own logs destined for Honeycomb.
func (c *HoneycombLoggerConfig) GetSamplerEnabled() (enabled bool) {
	return c.SamplerEnabled.Get()
}

type StdoutLoggerConfig struct {
	Structured        bool `yaml:"Structured" default:"false"`
	SamplerEnabled    bool `yaml:"SamplerEnabled" `
	SamplerThroughput int  `yaml:"SamplerThroughput" default:"10"`
}

type PrometheusMetricsConfig struct {
	Enabled    bool   `yaml:"Enabled" default:"false"`
	ListenAddr string `yaml:"ListenAddr" default:"localhost:2112"`
}

type LegacyMetricsConfig struct {
	Enabled           bool     `yaml:"Enabled" default:"false"`
	APIHost           string   `yaml:"APIHost" default:"https://api.honeycomb.io"`
	APIKey            string   `yaml:"APIKey" cmdenv:"LegacyMetricsAPIKey,HoneycombAPIKey"`
	Dataset           string   `yaml:"Dataset" default:"Refinery Metrics"`
	ReportingInterval Duration `yaml:"ReportingInterval" default:"30s"`
}

type OTelMetricsConfig struct {
	Enabled           bool     `yaml:"Enabled" default:"false"`
	APIHost           string   `yaml:"APIHost" default:"https://api.honeycomb.io"`
	APIKey            string   `yaml:"APIKey" cmdenv:"OTelMetricsAPIKey,HoneycombAPIKey"`
	Dataset           string   `yaml:"Dataset" default:"Refinery Metrics"`
	Compression       string   `yaml:"Compression" default:"gzip"`
	ReportingInterval Duration `yaml:"ReportingInterval" default:"30s"`
}

type OTelTracingConfig struct {
	Enabled    bool   `yaml:"Enabled" default:"false"`
	APIHost    string `yaml:"APIHost" default:"https://api.honeycomb.io"`
	APIKey     string `yaml:"APIKey" cmdenv:"OTelTracesAPIKey,HoneycombAPIKey"`
	Dataset    string `yaml:"Dataset" default:"Refinery Traces"`
	SampleRate uint64 `yaml:"SampleRate" default:"100"`
}

type PeerManagementConfig struct {
	Type                    string   `yaml:"Type" default:"file"`
	Identifier              string   `yaml:"Identifier"`
	IdentifierInterfaceName string   `yaml:"IdentifierInterfaceName"`
	UseIPV6Identifier       bool     `yaml:"UseIPV6Identifier"`
	Peers                   []string `yaml:"Peers"`
}

type RedisPeerManagementConfig struct {
	Host           string   `yaml:"Host" cmdenv:"RedisHost"`
	ClusterHosts   []string `yaml:"ClusterHosts" cmdenv:"RedisClusterHosts"`
	Username       string   `yaml:"Username" cmdenv:"RedisUsername"`
	Password       string   `yaml:"Password" cmdenv:"RedisPassword"`
	AuthCode       string   `yaml:"AuthCode" cmdenv:"RedisAuthCode"`
	Prefix         string   `yaml:"Prefix" default:"refinery"`
	Database       int      `yaml:"Database"`
	UseTLS         bool     `yaml:"UseTLS" `
	UseTLSInsecure bool     `yaml:"UseTLSInsecure" `
	Timeout        Duration `yaml:"Timeout" default:"5s"`
}

type CollectionConfig struct {
	// CacheCapacity must be less than math.MaxInt32
	CacheCapacity         int        `yaml:"CacheCapacity" default:"10_000"`
	PeerQueueSize         int        `yaml:"PeerQueueSize"`
	IncomingQueueSize     int        `yaml:"IncomingQueueSize"`
	AvailableMemory       MemorySize `yaml:"AvailableMemory" cmdenv:"AvailableMemory"`
	MaxMemoryPercentage   int        `yaml:"MaxMemoryPercentage" default:"75"`
	MaxAlloc              MemorySize `yaml:"MaxAlloc"`
	DisableRedistribution bool       `yaml:"DisableRedistribution"`
	ShutdownDelay         Duration   `yaml:"ShutdownDelay" default:"15s"`
}

// GetMaxAlloc returns the maximum amount of memory to use for the cache.
// If AvailableMemory is set, it uses that and MaxMemoryPercentage to calculate
func (c CollectionConfig) GetMaxAlloc() MemorySize {
	if c.AvailableMemory == 0 || c.MaxMemoryPercentage == 0 {
		return c.MaxAlloc
	}
	return c.AvailableMemory * MemorySize(c.MaxMemoryPercentage) / 100
}

// GetPeerBufferCapacity returns the capacity of the in-memory channel for peer traces.
// If PeerBufferCapacity is not set, it uses 3x the cache capacity.
// The minimum value is 3x the cache capacity.
func (c CollectionConfig) GetPeerQueueSize() int {
	if c.PeerQueueSize == 0 || c.PeerQueueSize < c.CacheCapacity*3 {
		return c.CacheCapacity * 3
	}
	return c.PeerQueueSize
}

// GetIncomingBufferCapacity returns the capacity of the in-memory channel for incoming traces.
// If IncomingBufferCapacity is not set, it uses 3x the cache capacity.
// The minimum value is 3x the cache capacity.
func (c CollectionConfig) GetIncomingQueueSize() int {
	if c.IncomingQueueSize == 0 || c.IncomingQueueSize < c.CacheCapacity*3 {
		return c.CacheCapacity * 3
	}
	return c.IncomingQueueSize
}

type BufferSizeConfig struct {
	UpstreamBufferSize int `yaml:"UpstreamBufferSize" default:"10_000"`
	PeerBufferSize     int `yaml:"PeerBufferSize" default:"100_000"`
}

type SpecializedConfig struct {
	EnvironmentCacheTTL       Duration          `yaml:"EnvironmentCacheTTL" default:"1h"`
	CompressPeerCommunication *DefaultTrue      `yaml:"CompressPeerCommunication" default:"true"` // Avoid pointer woe on access, use GetCompressPeerCommunication() instead.
	AdditionalAttributes      map[string]string `yaml:"AdditionalAttributes" default:"{}"`
}

type IDFieldsConfig struct {
	TraceNames  []string `yaml:"TraceNames" default:"[\"trace.trace_id\",\"traceId\"]"`
	ParentNames []string `yaml:"ParentNames" default:"[\"trace.parent_id\",\"parentId\"]"`
}

// GRPCServerParameters allow you to configure the GRPC ServerParameters used
// by refinery's own GRPC server:
// https://pkg.go.dev/google.golang.org/grpc/keepalive#ServerParameters
type GRPCServerParameters struct {
	Enabled               *DefaultTrue `yaml:"Enabled" default:"true"` // Avoid pointer woe on access, use GetGRPCEnabled() instead.
	ListenAddr            string       `yaml:"ListenAddr" cmdenv:"GRPCListenAddr"`
	MaxConnectionIdle     Duration     `yaml:"MaxConnectionIdle" default:"1m"`
	MaxConnectionAge      Duration     `yaml:"MaxConnectionAge" default:"3m"`
	MaxConnectionAgeGrace Duration     `yaml:"MaxConnectionAgeGrace" default:"1m"`
	KeepAlive             Duration     `yaml:"KeepAlive" default:"1m"`
	KeepAliveTimeout      Duration     `yaml:"KeepAliveTimeout" default:"20s"`
	MaxSendMsgSize        MemorySize   `yaml:"MaxSendMsgSize" default:"15MB"`
	MaxRecvMsgSize        MemorySize   `yaml:"MaxRecvMsgSize" default:"15MB"`
}

type SampleCacheConfig struct {
	KeptSize          uint     `yaml:"KeptSize" default:"10_000"`
	DroppedSize       uint     `yaml:"DroppedSize" default:"1_000_000"`
	SizeCheckInterval Duration `yaml:"SizeCheckInterval" default:"10s"`
}

type StressReliefConfig struct {
	Mode                      string   `yaml:"Mode" default:"never"`
	ActivationLevel           uint     `yaml:"ActivationLevel" default:"90"`
	DeactivationLevel         uint     `yaml:"DeactivationLevel" default:"75"`
	SamplingRate              uint64   `yaml:"SamplingRate" default:"100"`
	MinimumActivationDuration Duration `yaml:"MinimumActivationDuration" default:"10s"`
}

type FileConfigError struct {
	ConfigLocations []string
	ConfigFailures  []string
	RulesLocations  []string
	RulesFailures   []string
}

func (e *FileConfigError) Error() string {
	var msg strings.Builder
	if len(e.ConfigFailures) > 0 {
		loc := strings.Join(e.ConfigLocations, ", ")
		msg.WriteString("Validation failed for config [")
		msg.WriteString(loc)
		msg.WriteString("]:\n")
		for _, fail := range e.ConfigFailures {
			msg.WriteString("  ")
			msg.WriteString(fail)
			msg.WriteString("\n")
		}
	}
	if len(e.RulesFailures) > 0 {
		loc := strings.Join(e.RulesLocations, ", ")
		msg.WriteString("Validation failed for config [")
		msg.WriteString(loc)
		msg.WriteString("]:\n")
		for _, fail := range e.RulesFailures {
			msg.WriteString("  ")
			msg.WriteString(fail)
			msg.WriteString("\n")
		}
	}
	return msg.String()
}

// newFileConfig does the work of creating and loading the start of a config object
// from the given arguments.
// It's used by both the main init as well as the reload code.
// In order to do proper validation, we actually read the file twice -- once into
// a map, and once into the actual config object.
func newFileConfig(opts *CmdEnv) (*fileConfig, error) {
	// If we're not validating, skip this part
	if !opts.NoValidate {
		cfgFails, err := validateConfigs(opts)
		if err != nil {
			return nil, err
		}

		ruleFails, err := validateRules(opts.RulesLocations)
		if err != nil {
			return nil, err
		}

		if len(cfgFails) > 0 || len(ruleFails) > 0 {
			return nil, &FileConfigError{
				ConfigLocations: opts.ConfigLocations,
				ConfigFailures:  cfgFails,
				RulesLocations:  opts.RulesLocations,
				RulesFailures:   ruleFails,
			}
		}
	}

	// Now load the files
	mainconf := &configContents{}
	mainhash, err := readConfigInto(mainconf, opts.ConfigLocations, opts)
	if err != nil {
		return nil, err
	}

	var rulesconf *V2SamplerConfig
	ruleshash, err := readConfigInto(&rulesconf, opts.RulesLocations, nil)
	if err != nil {
		return nil, err
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

// writeYAMLToFile renders the given data item to a YAML file
func writeYAMLToFile(data any, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	encoder := yaml.NewEncoder(f)
	encoder.SetIndent(2)
	return encoder.Encode(data)
}

// NewConfig creates a new Config object from the given arguments; if args is
// nil, it uses the command line arguments.
// It also dumps the config and rules to the given files, if specified, which
// will cause the program to exit.
func NewConfig(opts *CmdEnv, errorCallback func(error)) (Config, error) {
	cfg, err := newFileConfig(opts)
	// only exit if we have no config at all; if it fails validation, we'll
	// do the rest and return it anyway
	if err != nil && cfg == nil {
		return nil, err
	}

	if opts.WriteConfig != "" {
		if err := writeYAMLToFile(cfg.mainConfig, opts.WriteConfig); err != nil {
			fmt.Printf("Error writing config: %s\n", err)
			os.Exit(1)
		}
	}
	if opts.WriteRules != "" {
		if err := writeYAMLToFile(cfg.rulesConfig, opts.WriteRules); err != nil {
			fmt.Printf("Error writing rules: %s\n", err)
			os.Exit(1)
		}
	}
	if opts.WriteConfig != "" || opts.WriteRules != "" {
		os.Exit(0)
	}

	cfg.callbacks = make([]ConfigReloadCallback, 0)
	cfg.errorCallback = errorCallback

	return cfg, err
}

// Reload attempts to reload the configuration; if it has changed, it stores the
// new data and calls the reload callbacks.
func (f *fileConfig) Reload() {
	// reread the configs
	cfg, err := newFileConfig(f.opts)
	if err != nil {
		f.errorCallback(err)
		return
	}

	// if nothing's changed, we're fine
	if f.mainHash == cfg.mainHash && f.rulesHash == cfg.rulesHash {
		return
	}

	// otherwise, update our state and call the callbacks
	f.mux.Lock()
	f.mainConfig = cfg.mainConfig
	f.mainHash = cfg.mainHash
	f.rulesConfig = cfg.rulesConfig
	f.rulesHash = cfg.rulesHash
	f.mux.Unlock() // can't defer -- we don't want callbacks to deadlock

	for _, cb := range f.callbacks {
		cb(cfg.mainHash, cfg.rulesHash)
	}
}

// GetHashes returns the current hash values for the main and rules configs.
func (f *fileConfig) GetHashes() (cfg string, rules string) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainHash, f.rulesHash
}

func (f *fileConfig) RegisterReloadCallback(cb ConfigReloadCallback) {
	f.mux.Lock()
	defer f.mux.Unlock()

	f.callbacks = append(f.callbacks, cb)
}

func (f *fileConfig) GetListenAddr() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.mainConfig.Network.ListenAddr)
	if err != nil {
		return ""
	}
	return f.mainConfig.Network.ListenAddr
}

func (f *fileConfig) GetPeerListenAddr() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.mainConfig.Network.PeerListenAddr)
	if err != nil {
		return ""
	}
	return f.mainConfig.Network.PeerListenAddr
}

func (f *fileConfig) GetHTTPIdleTimeout() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.Network.HTTPIdleTimeout)
}

func (f *fileConfig) GetCompressPeerCommunication() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Specialized.CompressPeerCommunication.Get()
}

func (f *fileConfig) GetGRPCEnabled() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.GRPCServerParameters.Enabled.Get()
}

func (f *fileConfig) GetGRPCListenAddr() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	// GRPC listen addr is optional, only check value is valid if not empty
	if f.mainConfig.GRPCServerParameters.ListenAddr != "" {
		_, _, err := net.SplitHostPort(f.mainConfig.GRPCServerParameters.ListenAddr)
		if err != nil {
			return ""
		}
	}
	return f.mainConfig.GRPCServerParameters.ListenAddr
}

func (f *fileConfig) GetGRPCConfig() GRPCServerParameters {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.GRPCServerParameters
}

func (f *fileConfig) GetTracesConfig() TracesConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Traces
}

func (f *fileConfig) GetAccessKeyConfig() AccessKeyConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.AccessKeys
}

func (f *fileConfig) GetPeerManagementType() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Type
}

func (f *fileConfig) GetPeers() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Peers
}

func (f *fileConfig) GetRedisPeerManagement() RedisPeerManagementConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement
}

func (f *fileConfig) GetRedisHost() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement.Host
}

func (f *fileConfig) GetRedisClusterHosts() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement.ClusterHosts
}

func (f *fileConfig) GetRedisUsername() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement.Username
}

func (f *fileConfig) GetRedisPrefix() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement.Prefix
}

func (f *fileConfig) GetRedisPassword() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement.Password
}

func (f *fileConfig) GetRedisAuthCode() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement.AuthCode
}

func (f *fileConfig) GetRedisDatabase() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement.Database
}

func (f *fileConfig) GetUseTLS() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement.UseTLS
}

func (f *fileConfig) GetUseTLSInsecure() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.RedisPeerManagement.UseTLSInsecure
}

func (f *fileConfig) GetIdentifierInterfaceName() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.IdentifierInterfaceName
}

func (f *fileConfig) GetUseIPV6Identifier() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.UseIPV6Identifier
}

func (f *fileConfig) GetRedisIdentifier() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PeerManagement.Identifier
}

func (f *fileConfig) GetHoneycombAPI() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Network.HoneycombAPI
}

func (f *fileConfig) GetLoggerLevel() Level {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Logger.Level
}

func (f *fileConfig) GetLoggerType() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Logger.Type
}

func (f *fileConfig) GetHoneycombLoggerConfig() HoneycombLoggerConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.HoneycombLogger
}

func (f *fileConfig) GetStdoutLoggerConfig() StdoutLoggerConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.StdoutLogger
}

func (f *fileConfig) GetAllSamplerRules() *V2SamplerConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	// This is probably good enough for debug; if not we can extend it.
	return f.rulesConfig
}

// GetSamplerConfigForDestName returns the sampler config for the given
// destination (environment, or dataset in classic mode), as well as the name of
// the sampler type. If the specific destination is not found, it returns the
// default sampler config.
func (f *fileConfig) GetSamplerConfigForDestName(destname string) (any, string) {
	f.mux.RLock()
	defer f.mux.RUnlock()

	nameToUse := "__default__"
	if _, ok := f.rulesConfig.Samplers[destname]; ok {
		nameToUse = destname
	}

	name := "not found"
	var cfg any
	if sampler, ok := f.rulesConfig.Samplers[nameToUse]; ok {
		cfg, name = sampler.Sampler()
	}
	return cfg, name
}

func (f *fileConfig) GetCollectionConfig() CollectionConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Collection
}

func (f *fileConfig) GetLegacyMetricsConfig() LegacyMetricsConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.LegacyMetrics
}

func (f *fileConfig) GetPrometheusMetricsConfig() PrometheusMetricsConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.PrometheusMetrics
}

func (f *fileConfig) GetOTelMetricsConfig() OTelMetricsConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.OTelMetrics
}

func (f *fileConfig) GetUpstreamBufferSize() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.BufferSizes.UpstreamBufferSize
}

func (f *fileConfig) GetPeerBufferSize() int {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.BufferSizes.PeerBufferSize
}

func (f *fileConfig) GetDebugServiceAddr() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	_, _, err := net.SplitHostPort(f.mainConfig.Debugging.DebugServiceAddr)
	if err != nil {
		return ""
	}
	return f.mainConfig.Debugging.DebugServiceAddr
}

func (f *fileConfig) GetIsDryRun() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Debugging.DryRun
}

func (f *fileConfig) GetAddHostMetadataToTrace() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Telemetry.AddHostMetadataToTrace.Get()
}

func (f *fileConfig) GetAddRuleReasonToTrace() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Telemetry.AddRuleReasonToTrace
}

func (f *fileConfig) GetEnvironmentCacheTTL() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.Specialized.EnvironmentCacheTTL)
}

func (f *fileConfig) GetOTelTracingConfig() OTelTracingConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.OTelTracing
}

func (f *fileConfig) GetDatasetPrefix() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.General.DatasetPrefix
}

func (f *fileConfig) GetGeneralConfig() GeneralConfig {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.General
}

func (f *fileConfig) GetQueryAuthToken() string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Debugging.QueryAuthToken
}

func (f *fileConfig) GetPeerTimeout() time.Duration {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return time.Duration(f.mainConfig.RedisPeerManagement.Timeout)
}

func (f *fileConfig) GetAdditionalErrorFields() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Debugging.AdditionalErrorFields
}

func (f *fileConfig) GetAddSpanCountToRoot() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Telemetry.AddSpanCountToRoot.Get()
}

func (f *fileConfig) GetAddCountsToRoot() bool {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Telemetry.AddCountsToRoot
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

	return f.mainConfig.IDFieldNames.TraceNames
}

func (f *fileConfig) GetParentIdFieldNames() []string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.IDFieldNames.ParentNames
}

func (f *fileConfig) GetConfigMetadata() []ConfigMetadata {
	ret := make([]ConfigMetadata, 2)
	ret[0] = ConfigMetadata{
		Type:     "config",
		ID:       strings.Join(f.opts.ConfigLocations, ", "),
		Hash:     f.mainHash,
		LoadedAt: f.lastLoadTime.Format(time.RFC3339),
	}
	ret[1] = ConfigMetadata{
		Type:     "rules",
		ID:       strings.Join(f.opts.RulesLocations, ", "),
		Hash:     f.rulesHash,
		LoadedAt: f.lastLoadTime.Format(time.RFC3339),
	}
	return ret
}

func (f *fileConfig) GetAdditionalAttributes() map[string]string {
	f.mux.RLock()
	defer f.mux.RUnlock()

	return f.mainConfig.Specialized.AdditionalAttributes
}
