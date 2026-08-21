package main

import (
<<<<<<< HEAD
	"context"
=======
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
>>>>>>> 2b0ef06b ([Common] Narrow etcd RBAC/TLS wrapper scope and add tests)
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
<<<<<<< HEAD
	"go.etcd.io/etcd/client/v3/concurrency"
)

func TestNewMaintenanceSessionCancelsStartup(t *testing.T) {
	original := startMaintenanceSession
	defer func() { startMaintenanceSession = original }()

	started := make(chan struct{})
	startMaintenanceSession = func(ctx context.Context, _ *clientv3.Client,
		_ int) (*concurrency.Session, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	result := make(chan error, 1)
	go func() {
		_, err := newMaintenanceSession(nil, 30, 10*time.Millisecond)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("maintenance session startup was not attempted")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected startup cancellation error")
		}
	case <-time.After(time.Second):
		t.Fatal("maintenance session startup did not cancel")
=======
)

func resetSecurityStateForTest(t *testing.T) {
	t.Helper()
	securityOnce = sync.Once{}
	cachedSecurity = securitySettings{}
}

func writeTempFile(t *testing.T, name string, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	return path
}

func writeTempCACert(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "mooncake-test-ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return writeTempFile(t, "ca.crt", string(pemBytes))
}

func TestParseRbacCredentialsFile(t *testing.T) {
	path := writeTempFile(t, "credentials.conf", "# comment\n\nusername = mooncake\npassword= secret-value \n")

	username, password, err := parseRbacCredentialsFile(path)
	if err != nil {
		t.Fatalf("parseRbacCredentialsFile returned error: %v", err)
	}
	if username != "mooncake" {
		t.Fatalf("unexpected username: %q", username)
	}
	if password != "secret-value" {
		t.Fatalf("unexpected password: %q", password)
	}
}

func TestParseRbacCredentialsFileRejectsUnknownKey(t *testing.T) {
	path := writeTempFile(t, "credentials.conf", "username=mooncake\nrole=admin\npassword=secret\n")

	_, _, err := parseRbacCredentialsFile(path)
	if err == nil || !strings.Contains(err.Error(), "unknown key") {
		t.Fatalf("expected unknown key error, got: %v", err)
	}
}

func TestApplySecurityConfigDefaultsToPlaintext(t *testing.T) {
	resetSecurityStateForTest(t)
	t.Setenv("MC_ETCD_CONF_FILE", "")
	t.Setenv("MC_ETCD_TLS_CA_CERT", "")
	t.Setenv("MC_ETCD_TLS_SERVER_NAME", "")
	t.Setenv("MC_ETCD_TLS_INSECURE_SKIP_VERIFY", "")

	cfg := clientv3.Config{}
	if err := applySecurityConfig(&cfg); err != nil {
		t.Fatalf("applySecurityConfig returned error: %v", err)
	}
	if cfg.Username != "" || cfg.Password != "" {
		t.Fatalf("expected empty auth config, got user=%q", cfg.Username)
	}
	if cfg.TLS != nil {
		t.Fatalf("expected nil TLS config by default")
	}
}

func TestApplySecurityConfigRejectsIncompleteTLSOverride(t *testing.T) {
	resetSecurityStateForTest(t)
	t.Setenv("MC_ETCD_CONF_FILE", "")
	t.Setenv("MC_ETCD_TLS_CA_CERT", "")
	t.Setenv("MC_ETCD_TLS_SERVER_NAME", "etcd.internal")
	t.Setenv("MC_ETCD_TLS_INSECURE_SKIP_VERIFY", "")

	cfg := clientv3.Config{}
	err := applySecurityConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), "requires TLS to be enabled") {
		t.Fatalf("expected incomplete TLS config error, got: %v", err)
	}
	if cfg.TLS != nil {
		t.Fatalf("expected TLS config to remain nil on error")
	}
}

func TestApplySecurityConfigAllowsInsecureTLSWithoutCA(t *testing.T) {
	resetSecurityStateForTest(t)
	t.Setenv("MC_ETCD_CONF_FILE", "")
	t.Setenv("MC_ETCD_TLS_CA_CERT", "")
	t.Setenv("MC_ETCD_TLS_SERVER_NAME", "")
	t.Setenv("MC_ETCD_TLS_INSECURE_SKIP_VERIFY", "true")

	cfg := clientv3.Config{}
	if err := applySecurityConfig(&cfg); err != nil {
		t.Fatalf("applySecurityConfig returned error: %v", err)
	}
	if cfg.TLS == nil {
		t.Fatalf("expected TLS config to be populated")
	}
	if !cfg.TLS.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify to be true")
	}
	if cfg.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatalf("unexpected TLS min version: %v", cfg.TLS.MinVersion)
	}
	if cfg.TLS.RootCAs != nil {
		t.Fatalf("expected RootCAs to be nil when no CA is provided")
	}
}

func TestApplySecurityConfigRejectsInvalidInsecureSkipVerifyValue(t *testing.T) {
	resetSecurityStateForTest(t)
	caPath := writeTempCACert(t)
	t.Setenv("MC_ETCD_CONF_FILE", "")
	t.Setenv("MC_ETCD_TLS_CA_CERT", caPath)
	t.Setenv("MC_ETCD_TLS_SERVER_NAME", "")
	t.Setenv("MC_ETCD_TLS_INSECURE_SKIP_VERIFY", "maybe")

	cfg := clientv3.Config{}
	err := applySecurityConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), "expected true or false") {
		t.Fatalf("expected invalid boolean error, got: %v", err)
	}
}

func TestApplySecurityConfigLoadsRbacAndTLS(t *testing.T) {
	resetSecurityStateForTest(t)
	credsPath := writeTempFile(t, "credentials.conf", "username=mooncake\npassword=secret\n")
	caPath := writeTempCACert(t)
	t.Setenv("MC_ETCD_CONF_FILE", credsPath)
	t.Setenv("MC_ETCD_TLS_CA_CERT", caPath)
	t.Setenv("MC_ETCD_TLS_SERVER_NAME", "etcd.internal")
	t.Setenv("MC_ETCD_TLS_INSECURE_SKIP_VERIFY", "true")

	cfg := clientv3.Config{}
	if err := applySecurityConfig(&cfg); err != nil {
		t.Fatalf("applySecurityConfig returned error: %v", err)
	}
	if cfg.Username != "mooncake" || cfg.Password != "secret" {
		t.Fatalf("unexpected auth config: user=%q password=%q", cfg.Username, cfg.Password)
	}
	if cfg.TLS == nil {
		t.Fatalf("expected TLS config to be populated")
	}
	if cfg.TLS.ServerName != "etcd.internal" {
		t.Fatalf("unexpected TLS server name: %q", cfg.TLS.ServerName)
	}
	if !cfg.TLS.InsecureSkipVerify {
		t.Fatalf("expected InsecureSkipVerify to be true")
	}
	if cfg.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatalf("unexpected TLS min version: %v", cfg.TLS.MinVersion)
	}
	if cfg.TLS.RootCAs == nil {
		t.Fatalf("expected RootCAs to be populated")
>>>>>>> 2b0ef06b ([Common] Narrow etcd RBAC/TLS wrapper scope and add tests)
	}
}
