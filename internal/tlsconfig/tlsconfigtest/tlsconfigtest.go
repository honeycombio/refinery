package tlsconfigtest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// PKI is a test CA with a server certificate for 127.0.0.1 and a client certificate.
type PKI struct {
	CAFile         string
	ClientCertFile string
	ClientKeyFile  string

	ca         *x509.Certificate
	caKey      *ecdsa.PrivateKey
	caPool     *x509.CertPool
	serverCert tls.Certificate
	serial     int64
}

func New(t testing.TB) *PKI {
	t.Helper()
	dir := t.TempDir()
	p := &PKI{
		CAFile:         filepath.Join(dir, "ca.pem"),
		ClientCertFile: filepath.Join(dir, "client.pem"),
		ClientKeyFile:  filepath.Join(dir, "client-key.pem"),
	}

	p.caKey = newKey(t)
	caDER := p.sign(t, &x509.Certificate{
		Subject:               pkix.Name{CommonName: "test-ca"},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}, &p.caKey.PublicKey)
	var err error
	if p.ca, err = x509.ParseCertificate(caDER); err != nil {
		t.Fatal(err)
	}
	p.caPool = x509.NewCertPool()
	p.caPool.AddCert(p.ca)
	writePEM(t, p.CAFile, "CERTIFICATE", caDER)

	serverKey := newKey(t)
	serverDER := p.sign(t, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "server"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, &serverKey.PublicKey)
	p.serverCert = tls.Certificate{Certificate: [][]byte{serverDER}, PrivateKey: serverKey}

	p.WriteClientCert(t, "client")
	return p
}

// WriteClientCert replaces the client certificate and key files.
func (p *PKI) WriteClientCert(t testing.TB, commonName string) {
	t.Helper()
	key := newKey(t)
	der := p.sign(t, &x509.Certificate{
		Subject:     pkix.Name{CommonName: commonName},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, &key.PublicKey)
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	writePEM(t, p.ClientCertFile, "CERTIFICATE", der)
	writePEM(t, p.ClientKeyFile, "EC PRIVATE KEY", keyDER)
}

// ServerTLSConfig requires a client certificate signed by the test CA.
func (p *PKI) ServerTLSConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{p.serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    p.caPool,
	}
}

func (p *PKI) sign(t testing.TB, tmpl *x509.Certificate, pub *ecdsa.PublicKey) []byte {
	t.Helper()
	p.serial++
	tmpl.SerialNumber = big.NewInt(p.serial)
	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().Add(time.Hour)
	parent := p.ca
	if parent == nil {
		parent = tmpl
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, p.caKey)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func writePEM(t testing.TB, path, blockType string, der []byte) {
	t.Helper()
	data := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
