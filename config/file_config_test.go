package config

import (
	"errors"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_GetWorkerCount(t *testing.T) {
	for _, tC := range []struct {
		name       string
		numWorkers int
		want       int
	}{
		{"default", 0, runtime.GOMAXPROCS(0)},
		{"configured/one worker", 1, 1},       // minimum allowed
		{"configured/multiple workers", 4, 4}, // custom value
	} {
		t.Run(tC.name, func(t *testing.T) {
			c := &CollectionConfig{WorkerCount: tC.numWorkers}
			assert.Equal(t, tC.want, c.GetWorkerCount())
		})
	}
}

func Test_GetQueueSizesPerWorker(t *testing.T) {
	for _, tC := range []struct {
		name                  string
		incomingQueueSize     int
		peerQueueSize         int
		numWorkers            int
		wantIncomingPerWorker int
		wantPeerPerWorker     int
	}{
		{"single worker/default", 30000, 30000, 1, 30000, 30000},
		{"multiple workers/default", 30000, 30000, 8, 3750, 3750},
		{"multiple workers/even division", 100, 50, 5, 20, 10},
		{"multiple workers/uneven division", 5, 5, 2, 3, 3},              // rounds up
		{"multiple workers/more workers than queue size", 3, 2, 5, 1, 1}, // minimum 1 per worker
	} {
		t.Run(tC.name, func(t *testing.T) {
			c := &CollectionConfig{
				IncomingQueueSize: tC.incomingQueueSize,
				PeerQueueSize:     tC.peerQueueSize,
				WorkerCount:       tC.numWorkers,
			}
			assert.Equal(t, tC.wantIncomingPerWorker, c.GetIncomingQueueSizePerWorker())
			assert.Equal(t, tC.wantPeerPerWorker, c.GetPeerQueueSizePerWorker())
		})
	}
}

func TestAccessKeyConfig_GetReplaceKey(t *testing.T) {
	type fields struct {
		ReceiveKeys          []string
		SendKey              string
		SendKeyMode          string
		AcceptOnlyListedKeys bool
	}

	fSendAll := fields{
		ReceiveKeys: []string{"key1", "key2"},
		SendKey:     "sendkey",
		SendKeyMode: "all",
	}
	fListed := fields{
		ReceiveKeys: []string{"key1", "key2"},
		SendKey:     "sendkey",
		SendKeyMode: "listedonly",
	}
	fMissing := fields{
		ReceiveKeys: []string{"key1", "key2"},
		SendKey:     "sendkey",
		SendKeyMode: "missingonly",
	}
	fUnlisted := fields{
		ReceiveKeys: []string{"key1", "key2"},
		SendKey:     "sendkey",
		SendKeyMode: "unlisted",
	}

	tests := []struct {
		name    string
		fields  fields
		apiKey  string
		want    string
		wantErr bool
	}{
		{"send all known", fSendAll, "key1", "sendkey", false},
		{"send all unknown", fSendAll, "userkey", "sendkey", false},
		{"send all missing", fSendAll, "", "sendkey", false},
		{"listed known", fListed, "key1", "sendkey", false},
		{"listed unknown", fListed, "userkey", "userkey", false},
		{"listed missing", fListed, "", "", true},
		{"missing known", fMissing, "key1", "key1", false},
		{"missing unknown", fMissing, "userkey", "userkey", false},
		{"missing missing", fMissing, "", "sendkey", false},
		{"unlisted known", fUnlisted, "key1", "key1", false},
		{"unlisted unknown", fUnlisted, "userkey", "sendkey", false},
		{"unlisted missing", fUnlisted, "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AccessKeyConfig{
				ReceiveKeys:          tt.fields.ReceiveKeys,
				SendKey:              tt.fields.SendKey,
				SendKeyMode:          tt.fields.SendKeyMode,
				AcceptOnlyListedKeys: tt.fields.AcceptOnlyListedKeys,
			}
			got, err := a.GetReplaceKey(tt.apiKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("AccessKeyConfig.GetReplaceKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AccessKeyConfig.GetReplaceKey() = '%v', want '%v'", got, tt.want)
			}
		})
	}
}

func TestAccessKeyConfig_IsAccepted(t *testing.T) {
	type fields struct {
		ReceiveKeys          []string
		SendKey              string
		SendKeyMode          string
		AcceptOnlyListedKeys bool
	}
	tests := []struct {
		name   string
		fields fields
		key    string
		want   error
	}{
		{"no keys", fields{}, "key1", nil},
		{"known key", fields{ReceiveKeys: []string{"key1"}, AcceptOnlyListedKeys: true}, "key1", nil},
		{"unknown key", fields{ReceiveKeys: []string{"key1"}, AcceptOnlyListedKeys: true}, "key2", errors.New("api key key2... not found in list of authorized keys")},
		{"reject missing key with sendkey configured", fields{ReceiveKeys: []string{"key1"}, AcceptOnlyListedKeys: true, SendKey: "key2"}, "", errors.New("api key ... not found in list of authorized keys")},
		{"reject missing key without sendkey configured", fields{ReceiveKeys: []string{"key1"}, AcceptOnlyListedKeys: true}, "", errors.New("api key ... not found in list of authorized keys")},
		{"accept sendkey", fields{ReceiveKeys: []string{"key1"}, AcceptOnlyListedKeys: true, SendKey: "key2"}, "key2", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AccessKeyConfig{
				ReceiveKeys:          tt.fields.ReceiveKeys,
				SendKey:              tt.fields.SendKey,
				SendKeyMode:          tt.fields.SendKeyMode,
				AcceptOnlyListedKeys: tt.fields.AcceptOnlyListedKeys,
			}
			err := a.IsAccepted(tt.key)
			if tt.want == nil {
				require.NoError(t, err)
				return
			}
			assert.Equal(t, tt.want.Error(), err.Error())
		})
	}
}

func TestCalculateSamplerKey(t *testing.T) {
	testCases := []struct {
		name        string
		apiKey      string
		dataset     string
		environment string
		prefix      string
		expected    string
	}{
		{
			name:        "legacy key with dataset prefix",
			apiKey:      "a1b2c3d4e5f67890abcdef1234567890",
			dataset:     "my-dataset",
			environment: "production",
			prefix:      "test-prefix",
			expected:    "test-prefix.my-dataset",
		},
		{
			name:        "legacy key without dataset prefix",
			apiKey:      "a1b2c3d4e5f67890abcdef1234567890",
			dataset:     "my-dataset",
			environment: "production",
			prefix:      "",
			expected:    "my-dataset",
		},
		{
			name:        "legacy 64-char ingest key with prefix",
			apiKey:      "hcaic_1234567890123456789012345678901234567890123456789012345678",
			dataset:     "my-dataset",
			environment: "production",
			prefix:      "test-prefix",
			expected:    "test-prefix.my-dataset",
		},
		{
			name:        "E&S key returns environment",
			apiKey:      "abc123DEF456ghi789jklm",
			dataset:     "my-dataset",
			environment: "production",
			prefix:      "test-prefix",
			expected:    "production",
		},
		{
			name:        "E&S key with empty environment",
			apiKey:      "abc123DEF456ghi789jklm",
			dataset:     "my-dataset",
			environment: "",
			prefix:      "test-prefix",
			expected:    "",
		},
		{
			name:        "legacy key with empty dataset",
			apiKey:      "a1b2c3d4e5f67890abcdef1234567890",
			dataset:     "",
			environment: "production",
			prefix:      "test-prefix",
			expected:    "test-prefix.",
		},
		{
			name:        "legacy key with empty dataset and no prefix",
			apiKey:      "a1b2c3d4e5f67890abcdef1234567890",
			dataset:     "",
			environment: "production",
			prefix:      "",
			expected:    "",
		},
		{
			name:        "empty api key defaults to E&S behavior",
			apiKey:      "",
			dataset:     "my-dataset",
			environment: "production",
			prefix:      "test-prefix",
			expected:    "production",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := &fileConfig{
				mainConfig: &configContents{
					General: GeneralConfig{
						DatasetPrefix: tc.prefix,
					},
				},
			}

			result := config.DetermineSamplerKey(tc.apiKey, tc.environment, tc.dataset)
			assert.Equal(
				t,
				tc.expected,
				result,
				"CalculateSamplerKey(%q, %q, %q) = %q, want %q",
				tc.apiKey,
				tc.dataset,
				tc.environment,
				result,
				tc.expected)
		})
	}
}

func TestGetSamplingKeyFieldsForDestName(t *testing.T) {
	testCases := []struct {
		name     string
		destName string
		expected []string
	}{
		{"empty dest name", "", nil},
		{"valid dest name", "my-destination", []string{"sampling_key", "service.name"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := &fileConfig{
				rulesConfig: &V2SamplerConfig{
					Samplers: map[string]*V2SamplerChoice{
						"__default__": {
							DeterministicSampler: &DeterministicSamplerConfig{SampleRate: 1},
						},
						"my-destination": {
							DynamicSampler: &DynamicSamplerConfig{
								FieldList: []string{"sampling_key", "service.name"},
							},
						},
					},
				},
			}
			result := config.GetSamplingKeyFieldsForDestName(tc.destName)
			assert.Equal(
				t, tc.expected, result,
				"getSamplingKeyFieldsForDestName(%q) = %v, want %v",
				tc.destName, result, tc.expected,
			)
		})
	}
}
