package config

import (
	"errors"
	"testing"

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
