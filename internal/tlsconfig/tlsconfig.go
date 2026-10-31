package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

type Options struct {
	CertFile           string
	KeyFile            string
	CAFile             string
	InsecureSkipVerify bool
}

// New returns a new *tls.Config built from opts, or nil if opts is empty.
// onReloadError, if non-nil, is called when the client certificate changes on
// disk but can't be reloaded; repeats of the same error are not reported.
func New(opts Options, onReloadError func(error)) (*tls.Config, error) {
	if opts == (Options{}) {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: opts.InsecureSkipVerify,
	}

	if opts.CAFile != "" {
		pem, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, fmt.Errorf("reading upstream TLS CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("upstream TLS CA file %q contains no valid PEM certificates", opts.CAFile)
		}
		cfg.RootCAs = pool
	}

	if opts.CertFile != "" || opts.KeyFile != "" {
		if opts.CertFile == "" || opts.KeyFile == "" {
			return nil, errors.New("upstream TLS cert file and key file must be set together")
		}
		r := &certReloader{certFile: opts.CertFile, keyFile: opts.KeyFile, onReloadError: onReloadError}
		if err := r.load(); err != nil {
			return nil, err
		}
		cfg.GetClientCertificate = r.getClientCertificate
	}

	return cfg, nil
}

// certReloader reloads the client certificate when its files change and keeps
// serving the last good certificate if a reload fails.
type certReloader struct {
	certFile, keyFile string
	onReloadError     func(error)

	mu              sync.Mutex
	cert            *tls.Certificate
	certMod, keyMod time.Time
	lastErr         string
}

func (r *certReloader) load() error {
	certInfo, err := os.Stat(r.certFile)
	if err != nil {
		return fmt.Errorf("reading upstream TLS cert file: %w", err)
	}
	keyInfo, err := os.Stat(r.keyFile)
	if err != nil {
		return fmt.Errorf("reading upstream TLS key file: %w", err)
	}
	if r.cert != nil && certInfo.ModTime().Equal(r.certMod) && keyInfo.ModTime().Equal(r.keyMod) {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("loading upstream TLS client certificate: %w", err)
	}
	r.cert = &cert
	r.certMod = certInfo.ModTime()
	r.keyMod = keyInfo.ModTime()
	return nil
}

func (r *certReloader) getClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		if err.Error() != r.lastErr && r.onReloadError != nil {
			r.onReloadError(err)
		}
		r.lastErr = err.Error()
	} else {
		r.lastErr = ""
	}
	return r.cert, nil
}
