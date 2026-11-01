package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeTLSFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func makeTLSFiles(t *testing.T) (string, string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	client := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "test client"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, client, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return writeTLSFile(t, "ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})),
		writeTLSFile(t, "client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER})),
		writeTLSFile(t, "client.key", pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func TestEtcdTLSConfiguration(t *testing.T) {
	ca, cert, key := makeTLSFiles(t)
	_, _, otherKey := makeTLSFiles(t)
	badPEM := writeTLSFile(t, "invalid.pem", []byte("not a certificate"))
	missing := filepath.Join(t.TempDir(), "missing")
	tests := []struct {
		name                      string
		ca, cert, key, serverName string
		endpoints                 []string
		wantErr                   string
		wantTLS, wantSystemRoots  bool
		wantCerts                 int
	}{
		{name: "unconfigured"},
		{name: "plaintext endpoint", endpoints: []string{"http://localhost:2379"}},
		{name: "CA only", ca: ca, wantTLS: true},
		{name: "mutual TLS", ca: ca, cert: cert, key: key, wantTLS: true, wantCerts: 1},
		{name: "system roots", cert: cert, key: key, wantTLS: true, wantSystemRoots: true, wantCerts: 1},
		{name: "server name", ca: ca, serverName: "etcd.internal", wantTLS: true},
		{name: "https endpoint", ca: ca, endpoints: []string{"https://localhost:2379"}, wantTLS: true},
		{name: "bare endpoint", ca: ca, endpoints: []string{"localhost:2379"}, wantTLS: true},
		{name: "server name without TLS", serverName: "etcd.internal", wantErr: "requires"},
		{name: "certificate without key", ca: ca, cert: cert, wantErr: "must be set together"},
		{name: "key without certificate", key: key, wantErr: "must be set together"},
		{name: "missing CA", ca: missing, wantErr: "failed to access"},
		{name: "directory", ca: t.TempDir(), wantErr: "expected a file"},
		{name: "invalid CA", ca: badPEM, wantErr: "failed to parse"},
		{name: "invalid key", ca: ca, cert: cert, key: badPEM, wantErr: "failed to load"},
		{name: "mismatched key", ca: ca, cert: cert, key: otherKey, wantErr: "failed to load"},
		{name: "http downgrade", ca: ca, endpoints: []string{"http://localhost:2379"}, wantErr: "uses http://"},
		{name: "mixed endpoints", ca: ca, endpoints: []string{"https://localhost:2379", "http://localhost:2380"}, wantErr: "uses http://"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for name, value := range map[string]string{
				mooncakeEtcdCACertEnv: tt.ca, mooncakeEtcdClientCertEnv: tt.cert,
				mooncakeEtcdClientKeyEnv: tt.key, mooncakeEtcdServerNameEnv: tt.serverName,
			} {
				t.Setenv(name, value)
			}
			endpoints := tt.endpoints
			if len(endpoints) == 0 {
				endpoints = []string{"localhost:2379"}
			}
			config, err := newStoreClientConfig(endpoints)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (config.TLS != nil) != tt.wantTLS {
				t.Fatalf("TLS enabled = %v, want %v", config.TLS != nil, tt.wantTLS)
			}
			if !tt.wantTLS {
				return
			}
			if config.TLS.InsecureSkipVerify || config.TLS.MinVersion != tls.VersionTLS12 {
				t.Fatal("TLS must verify server certificates and require TLS 1.2 or newer")
			}
			if (config.TLS.RootCAs == nil) != tt.wantSystemRoots {
				t.Fatal("unexpected CA trust configuration")
			}
			if len(config.TLS.Certificates) != tt.wantCerts || config.TLS.ServerName != tt.serverName {
				t.Fatal("unexpected client certificate or ServerName")
			}
		})
	}
}
