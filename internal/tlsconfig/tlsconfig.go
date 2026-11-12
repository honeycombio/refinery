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
	CertFile string
	KeyFile  string
	CAFile   string
}

// Warner reports a problem with the client certificate that doesn't stop
// Refinery from sending, such as a failed reload or an upcoming expiry.
type Warner func(msg string, fields map[string]any)

// New returns a new *tls.Config built from opts, or nil if opts is empty.
// warn, if non-nil, is called during TLS handshakes, once for each new client
// certificate problem.
func New(opts Options, warn Warner) (*tls.Config, error) {
	if opts == (Options{}) {
		return nil, nil
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

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
		r := &certReloader{certFile: opts.CertFile, keyFile: opts.KeyFile, warn: warn}
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
	warn              Warner

	mu              sync.Mutex
	cert            *tls.Certificate
	certMod, keyMod time.Time
	lastReloadErr   string
	lastExpiryWarn  string
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

// checkExpiry warns when the current certificate has expired or has less than
// a tenth of its lifetime left, which is well past when renewal tools such as
// cert-manager replace it.
func (r *certReloader) checkExpiry() {
	leaf := r.cert.Leaf
	if leaf == nil {
		return
	}
	now := time.Now()
	var msg string
	switch {
	case now.After(leaf.NotAfter):
		msg = "upstream TLS client certificate has expired"
	case leaf.NotAfter.Sub(now) < leaf.NotAfter.Sub(leaf.NotBefore)/10:
		msg = "upstream TLS client certificate expires soon"
	}
	key := msg + leaf.NotAfter.String()
	if msg != "" && key != r.lastExpiryWarn && r.warn != nil {
		r.warn(msg, map[string]any{
			"cert_file": r.certFile,
			"subject":   leaf.Subject.String(),
			"not_after": leaf.NotAfter.UTC().Format(time.RFC3339),
		})
	}
	r.lastExpiryWarn = key
}

func (r *certReloader) getClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.load(); err != nil {
		if err.Error() != r.lastReloadErr && r.warn != nil {
			r.warn("failed to reload upstream TLS client certificate; continuing with the previous certificate", map[string]any{
				"error": err.Error(),
			})
		}
		r.lastReloadErr = err.Error()
	} else {
		r.lastReloadErr = ""
	}
	r.checkExpiry()
	return r.cert, nil
}
