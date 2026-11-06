package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/honeycombio/refinery/internal/tlsconfig/tlsconfigtest"
)

// startServer responds with the client certificate's common name, or "none".
func startServer(t *testing.T, cfg *tls.Config) *httptest.Server {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TLS.PeerCertificates) == 0 {
			io.WriteString(w, "none")
			return
		}
		io.WriteString(w, r.TLS.PeerCertificates[0].Subject.CommonName)
	}))
	srv.TLS = cfg
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, cfg *tls.Config, url string) (string, error) {
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
	t.Cleanup(client.CloseIdleConnections)
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return string(body), err
}

func clientCN(t *testing.T, cfg *tls.Config) string {
	cert, err := cfg.GetClientCertificate(&tls.CertificateRequestInfo{})
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)
	return leaf.Subject.CommonName
}

func touch(t *testing.T, ahead time.Duration, paths ...string) {
	future := time.Now().Add(ahead)
	for _, p := range paths {
		require.NoError(t, os.Chtimes(p, future, future))
	}
}

func TestNewEmpty(t *testing.T) {
	cfg, err := New(Options{}, nil)
	require.NoError(t, err)
	assert.Nil(t, cfg)
}

func TestNewCAOnly(t *testing.T) {
	pki := tlsconfigtest.New(t)
	serverCfg := pki.ServerTLSConfig()
	serverCfg.ClientAuth = tls.NoClientCert
	srv := startServer(t, serverCfg)

	cfg, err := New(Options{CAFile: pki.CAFile}, nil)
	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
	assert.Nil(t, cfg.GetClientCertificate)

	body, err := get(t, cfg, srv.URL)
	require.NoError(t, err)
	assert.Equal(t, "none", body)

	_, err = get(t, &tls.Config{}, srv.URL)
	require.Error(t, err)
}

func TestNewMutualTLS(t *testing.T) {
	pki := tlsconfigtest.New(t)
	srv := startServer(t, pki.ServerTLSConfig())

	cfg, err := New(Options{CertFile: pki.ClientCertFile, KeyFile: pki.ClientKeyFile, CAFile: pki.CAFile}, nil)
	require.NoError(t, err)
	body, err := get(t, cfg, srv.URL)
	require.NoError(t, err)
	assert.Equal(t, "client", body)

	noCert, err := New(Options{CAFile: pki.CAFile}, nil)
	require.NoError(t, err)
	_, err = get(t, noCert, srv.URL)
	require.Error(t, err)
}

func TestNewErrors(t *testing.T) {
	pki := tlsconfigtest.New(t)
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.pem")
	garbage := filepath.Join(dir, "garbage.pem")
	require.NoError(t, os.WriteFile(garbage, []byte("not a pem file"), 0o600))

	testCases := []struct {
		name    string
		opts    Options
		wantErr string
	}{
		{"missing CA file", Options{CAFile: missing}, "CA file"},
		{"CA file without certificates", Options{CAFile: garbage}, "no valid PEM certificates"},
		{"cert without key", Options{CertFile: pki.ClientCertFile}, "must be set together"},
		{"key without cert", Options{KeyFile: pki.ClientKeyFile}, "must be set together"},
		{"missing cert file", Options{CertFile: missing, KeyFile: pki.ClientKeyFile}, "cert file"},
		{"missing key file", Options{CertFile: pki.ClientCertFile, KeyFile: missing}, "key file"},
		{"invalid key", Options{CertFile: pki.ClientCertFile, KeyFile: garbage}, "client certificate"},
		{"key does not match cert", Options{CertFile: pki.ClientCertFile, KeyFile: pki.CAFile}, "client certificate"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := New(tc.opts, nil)
			require.ErrorContains(t, err, tc.wantErr)
			assert.Nil(t, cfg)
		})
	}
}

func TestClientCertificateRotation(t *testing.T) {
	pki := tlsconfigtest.New(t)
	cfg, err := New(Options{CertFile: pki.ClientCertFile, KeyFile: pki.ClientKeyFile}, nil)
	require.NoError(t, err)
	assert.Equal(t, "client", clientCN(t, cfg))

	pki.WriteClientCert(t, "rotated")
	touch(t, time.Minute, pki.ClientCertFile, pki.ClientKeyFile)
	assert.Equal(t, "rotated", clientCN(t, cfg))

	srv := startServer(t, pki.ServerTLSConfig())
	cfg.RootCAs = x509.NewCertPool()
	cfg.RootCAs.AppendCertsFromPEM(mustRead(t, pki.CAFile))
	body, err := get(t, cfg, srv.URL)
	require.NoError(t, err)
	assert.Equal(t, "rotated", body)
}

type warning struct {
	msg    string
	fields map[string]any
}

func recordWarnings() (*[]warning, Warner) {
	var warnings []warning
	return &warnings, func(msg string, fields map[string]any) {
		warnings = append(warnings, warning{msg, fields})
	}
}

func mustRead(t *testing.T, path string) []byte {
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

func TestClientCertificateReloadFailureKeepsLastGood(t *testing.T) {
	pki := tlsconfigtest.New(t)
	warnings, warn := recordWarnings()
	cfg, err := New(Options{CertFile: pki.ClientCertFile, KeyFile: pki.ClientKeyFile}, warn)
	require.NoError(t, err)
	assert.Equal(t, "client", clientCN(t, cfg))
	assert.Empty(t, *warnings)

	// cert rotated but key corrupt
	pki.WriteClientCert(t, "rotated")
	require.NoError(t, os.WriteFile(pki.ClientKeyFile, []byte("corrupt"), 0o600))
	touch(t, time.Minute, pki.ClientCertFile, pki.ClientKeyFile)
	assert.Equal(t, "client", clientCN(t, cfg))
	assert.Equal(t, "client", clientCN(t, cfg))
	require.Len(t, *warnings, 1)
	assert.Contains(t, (*warnings)[0].msg, "failed to reload")
	assert.Contains(t, (*warnings)[0].fields["error"], "client certificate")

	require.NoError(t, os.Remove(pki.ClientKeyFile))
	assert.Equal(t, "client", clientCN(t, cfg))
	require.Len(t, *warnings, 2)
	assert.Contains(t, (*warnings)[1].fields["error"], "key file")

	pki.WriteClientCert(t, "recovered")
	touch(t, 2*time.Minute, pki.ClientCertFile, pki.ClientKeyFile)
	assert.Equal(t, "recovered", clientCN(t, cfg))

	require.NoError(t, os.Remove(pki.ClientKeyFile))
	assert.Equal(t, "recovered", clientCN(t, cfg))
	assert.Len(t, *warnings, 3)
}

func TestClientCertificateExpiryWarnings(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		name      string
		notBefore time.Time
		notAfter  time.Time
		wantMsg   string
	}{
		{"healthy", now.Add(-time.Hour), now.Add(time.Hour), ""},
		{"long-lived with a day left", now.Add(-90 * 24 * time.Hour), now.Add(24 * time.Hour), "expires soon"},
		{"short-lived with a third left", now.Add(-100 * time.Second), now.Add(50 * time.Second), ""},
		{"short-lived with a twentieth left", now.Add(-190 * time.Second), now.Add(10 * time.Second), "expires soon"},
		{"expired", now.Add(-2 * time.Hour), now.Add(-time.Hour), "has expired"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pki := tlsconfigtest.New(t)
			pki.WriteClientCertValidity(t, "client", tc.notBefore, tc.notAfter)
			warnings, warn := recordWarnings()
			cfg, err := New(Options{CertFile: pki.ClientCertFile, KeyFile: pki.ClientKeyFile}, warn)
			require.NoError(t, err)
			clientCN(t, cfg)
			clientCN(t, cfg)

			if tc.wantMsg == "" {
				assert.Empty(t, *warnings)
				return
			}
			require.Len(t, *warnings, 1, "warned once, not on each handshake")
			assert.Contains(t, (*warnings)[0].msg, tc.wantMsg)
			assert.Equal(t, tc.notAfter.UTC().Format(time.RFC3339), (*warnings)[0].fields["not_after"])
		})
	}
}

func TestClientCertificateExpiryWarningClearsAfterRenewal(t *testing.T) {
	pki := tlsconfigtest.New(t)
	pki.WriteClientCertValidity(t, "old", time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	warnings, warn := recordWarnings()
	cfg, err := New(Options{CertFile: pki.ClientCertFile, KeyFile: pki.ClientKeyFile}, warn)
	require.NoError(t, err)
	assert.Empty(t, *warnings, "New must not warn; Refinery's logger isn't started yet")
	assert.Equal(t, "old", clientCN(t, cfg))
	require.Len(t, *warnings, 1)

	pki.WriteClientCert(t, "renewed")
	touch(t, time.Minute, pki.ClientCertFile, pki.ClientKeyFile)
	assert.Equal(t, "renewed", clientCN(t, cfg))
	assert.Len(t, *warnings, 1)

	pki.WriteClientCertValidity(t, "expired-again", time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	touch(t, 2*time.Minute, pki.ClientCertFile, pki.ClientKeyFile)
	assert.Equal(t, "expired-again", clientCN(t, cfg))
	require.Len(t, *warnings, 2)
	assert.Contains(t, (*warnings)[1].msg, "has expired")
}
