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

func TestNewInsecureSkipVerify(t *testing.T) {
	pki := tlsconfigtest.New(t)
	srv := startServer(t, pki.ServerTLSConfig())

	cfg, err := New(Options{CertFile: pki.ClientCertFile, KeyFile: pki.ClientKeyFile, InsecureSkipVerify: true}, nil)
	require.NoError(t, err)
	body, err := get(t, cfg, srv.URL)
	require.NoError(t, err)
	assert.Equal(t, "client", body)
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
	cfg.InsecureSkipVerify = true
	body, err := get(t, cfg, srv.URL)
	require.NoError(t, err)
	assert.Equal(t, "rotated", body)
}

func TestClientCertificateReloadFailureKeepsLastGood(t *testing.T) {
	pki := tlsconfigtest.New(t)
	var reloadErrs []error
	cfg, err := New(Options{CertFile: pki.ClientCertFile, KeyFile: pki.ClientKeyFile}, func(err error) {
		reloadErrs = append(reloadErrs, err)
	})
	require.NoError(t, err)
	assert.Equal(t, "client", clientCN(t, cfg))
	assert.Empty(t, reloadErrs)

	// cert rotated but key corrupt
	pki.WriteClientCert(t, "rotated")
	require.NoError(t, os.WriteFile(pki.ClientKeyFile, []byte("corrupt"), 0o600))
	touch(t, time.Minute, pki.ClientCertFile, pki.ClientKeyFile)
	assert.Equal(t, "client", clientCN(t, cfg))
	assert.Equal(t, "client", clientCN(t, cfg))
	require.Len(t, reloadErrs, 1)
	assert.ErrorContains(t, reloadErrs[0], "client certificate")

	require.NoError(t, os.Remove(pki.ClientKeyFile))
	assert.Equal(t, "client", clientCN(t, cfg))
	require.Len(t, reloadErrs, 2)
	assert.ErrorContains(t, reloadErrs[1], "key file")

	pki.WriteClientCert(t, "recovered")
	touch(t, 2*time.Minute, pki.ClientCertFile, pki.ClientKeyFile)
	assert.Equal(t, "recovered", clientCN(t, cfg))

	require.NoError(t, os.Remove(pki.ClientKeyFile))
	assert.Equal(t, "recovered", clientCN(t, cfg))
	assert.Len(t, reloadErrs, 3)
}
