package quic

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var errFingerprint = errors.New("channel quic: certificate fingerprint mismatch")

// clientTLS trusts the system roots, or ca, or only the certificate whose SHA-256 is fingerprint.
func clientTLS(host string, options Options) (*tls.Config, error) {
	config := &tls.Config{NextProtos: []string{ALPN}, MinVersion: tls.VersionTLS13, ServerName: host}
	if options.ServerName != "" {
		config.ServerName = options.ServerName
	}
	switch {
	case options.Fingerprint != "":
		want, err := hex.DecodeString(strings.ToLower(strings.ReplaceAll(options.Fingerprint, ":", "")))
		if err != nil || len(want) != sha256.Size {
			return nil, fmt.Errorf("channel quic: fingerprint isn't a SHA-256 in hex: %q", options.Fingerprint)
		}
		// Pinning replaces the other checks, as in the TypeScript client
		config.InsecureSkipVerify = true
		config.VerifyPeerCertificate = func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return errFingerprint
			}
			if sum := sha256.Sum256(raw[0]); string(sum[:]) != string(want) {
				return errFingerprint
			}
			return nil
		}
	case len(options.CA) > 0:
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(options.CA) {
			return nil, errors.New("channel quic: CA has no PEM certificate")
		}
		config.RootCAs = pool
	}
	return config, nil
}

func serverTLS(options ListenOptions) (*tls.Config, error) {
	certificate, err := tls.X509KeyPair(options.Cert, options.Key)
	if err != nil {
		return nil, fmt.Errorf("channel quic: %w", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{certificate}, NextProtos: []string{ALPN}, MinVersion: tls.VersionTLS13}, nil
}
