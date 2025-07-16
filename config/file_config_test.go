package config

import (
	"errors"
	"testing"

	"github.com/honeycombio/husky/otlp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				t.Errorf("AccessKeyConfig.CheckAndMaybeReplaceKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AccessKeyConfig.CheckAndMaybeReplaceKey() = '%v', want '%v'", got, tt.want)
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
		{"accept missing key", fields{ReceiveKeys: []string{"key1"}, AcceptOnlyListedKeys: true}, "", errors.New("api key ... not found in list of authorized keys")},
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

			result := config.CalculateSamplerKey(tc.apiKey, tc.dataset, tc.environment)
			assert.Equal(t, tc.expected, result,
				"CalculateSamplerKey(%q, %q, %q) = %q, want %q",
				tc.apiKey, tc.dataset, tc.environment, result, tc.expected)
		})
	}
}

func TestIsClassicKey(t *testing.T) {
	testCases := []struct {
		name     string
		key      string
		expected bool
	}{
		// 32-character classic API keys (hex digits only)
		{name: "valid 32-char classic key - all lowercase", key: "a1b2c3d4e5f67890abcdef1234567890", expected: true},
		{name: "valid 32-char classic key - all numbers", key: "12345678901234567890123456789012", expected: true},
		{name: "valid 32-char classic key - all lowercase letters", key: "abcdefabcdefabcdefabcdefabcdefab", expected: true},
		{name: "valid 32-char classic key - mixed hex", key: "0123456789abcdef0123456789abcdef", expected: true},
		{name: "invalid 32-char key - uppercase letters", key: "A1B2C3D4E5F67890ABCDEF1234567890", expected: false},
		{name: "invalid 32-char key - contains g", key: "a1b2c3d4e5f67890abcdefg234567890", expected: false},
		{name: "invalid 32-char key - contains special chars", key: "a1b2c3d4e5f67890abcdef123456789!", expected: false},
		{name: "invalid 32-char key - contains space", key: "a1b2c3d4e5f67890abcdef12345 7890", expected: false},

		// 64-character classic ingest keys (pattern: ^hc[a-z]ic_[0-9a-z]*$)
		{name: "valid 64-char ingest key - hcaic", key: "hcaic_1234567890123456789012345678901234567890123456789012345678", expected: true},
		{name: "valid 64-char ingest key - hcbic", key: "hcbic_abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234", expected: true},
		{name: "valid 64-char ingest key - hczic", key: "hczic_0123456789abcdef0123456789abcdef0123456789abcdef0123456789", expected: true},
		{name: "valid 64-char ingest key - mixed", key: "hcxic_1234567890123456789012345678901234567890123456789012345678", expected: true},
		{name: "invalid 64-char ingest key - wrong prefix", key: "hc1ic_1234567890123456789012345678901234567890123456789012345678", expected: false},
		{name: "invalid 64-char ingest key - uppercase in prefix", key: "hcAic_1234567890123456789012345678901234567890123456789012345678", expected: false},
		{name: "invalid 64-char ingest key - missing underscore", key: "hcaic1234567890123456789012345678901234567890123456789012345678", expected: false},
		{name: "invalid 64-char ingest key - uppercase in suffix", key: "hcaic_1234567890123456789012345678901234567890123456789012345A78", expected: false},
		{name: "invalid 64-char ingest key - special char in suffix", key: "hcaic_123456789012345678901234567890123456789012345678901234567!", expected: false},

		// Edge cases for length
		{name: "empty key", key: "", expected: false},
		{name: "too short - 31 chars", key: "a1b2c3d4e5f67890abcdef123456789", expected: false},
		{name: "too long - 33 chars", key: "a1b2c3d4e5f67890abcdef12345678901", expected: false},
		{name: "too short - 63 chars", key: "hcaic_123456789012345678901234567890123456789012345678901234567", expected: false},
		{name: "too long - 65 chars", key: "hcaic_12345678901234567890123456789012345678901234567890123456789", expected: false},

		// Non-classic keys (E&S keys should return false)
		{name: "E&S key", key: "abc123DEF456ghi789jklm", expected: false},
		{name: "E&S ingest key", key: "hcxik_1234567890123456789012345678901234567890123456789012345678", expected: false},

		// Invalid patterns
		{name: "random string", key: "this-is-not-a-key", expected: false},
		{name: "numbers only but wrong length", key: "123456789012", expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsLegacyAPIKey(tc.key)
			assert.Equal(t, tc.expected, result, "Expected IsClassicApiKey(%q) to return %v", tc.key, tc.expected)
		})
	}
}

func BenchmarkIsLegacyAPIKey(b *testing.B) {
	tests := []struct {
		name string
		key  string
	}{
		{"Valid classic key", "a1b2c3d4e5f67890abcdef1234567890"},
		{"Invalid classic key", "abcdef0123456789abcdef01234567zz"},
		{"Valid ingest key", "hcaic_1234567890123456789012345678901234567890123456789012345678"},
		{"Invalid ingest key", "hcaic_1234567890123456789012345678901234567890123456789012345678"},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = IsLegacyAPIKey(tt.key)
			}
		})

		b.Run(tt.name+"/husky", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = otlp.IsClassicApiKey(tt.key)
			}
		})
	}
}
