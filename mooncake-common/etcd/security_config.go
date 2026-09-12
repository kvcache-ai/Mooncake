package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"unicode"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// securitySettings holds the cached RBAC and TLS configuration parsed from
// environment variables. It is populated exactly once by loadSecurityConfig via
// sync.Once.
type securitySettings struct {
	username  string
	password  string
	tlsConfig *tls.Config
	loadErr   error
}

var (
	securityOnce   sync.Once
	cachedSecurity securitySettings
)

// parseRbacCredentialsFile reads a credentials file in key=value format and
// returns the username and password. Expected format:
//
//	username=<user>
//	password=<secret>
func parseRbacCredentialsFile(path string) (string, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", "", fmt.Errorf("cannot open credentials file %s: %w", path, err)
	}
	defer file.Close()

	var username, password string
	scanner := bufio.NewScanner(file)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("credentials file %s line %d: invalid format, expected key=value", path, lineNo)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		switch key {
		case "username":
			username = value
		case "password":
			password = value
		default:
			return "", "", fmt.Errorf("credentials file %s line %d: unknown key %q, expected username or password", path, lineNo, key)
		}
	}
	if err := scanner.Err(); err != nil {
		return "", "", fmt.Errorf("error reading credentials file %s: %w", path, err)
	}
	if username == "" {
		return "", "", fmt.Errorf("username not found in credentials file %s", path)
	}
	if password == "" {
		return "", "", fmt.Errorf("password not found in credentials file %s", path)
	}
	return username, password, nil
}

// loadSecurityConfig reads environment variables and populates the cached
// security settings. Diagnostic messages are written to stderr so they never
// pollute stdout, which callers may rely on for data.
func loadSecurityConfig() {
	credsFile := os.Getenv("MC_ETCD_CONF_FILE")
	if credsFile != "" {
		username, password, err := parseRbacCredentialsFile(credsFile)
		if err != nil {
			cachedSecurity.loadErr = fmt.Errorf("failed to load RBAC credentials: %w", err)
			fmt.Fprintf(os.Stderr, "[etcd_wrapper] ERROR: %v\n", cachedSecurity.loadErr)
			return
		}
		cachedSecurity.username = username
		cachedSecurity.password = password
		fmt.Fprintf(os.Stderr, "[etcd_wrapper] RBAC authentication enabled (user: %s)\n", username)
	}

	normalizeEnvValue := func(v string) string {
		v = strings.Map(func(r rune) rune {
			if unicode.IsControl(r) {
				return -1
			}
			return r
		}, v)
		v = strings.TrimSpace(v)
		v = strings.Trim(v, "\"")
		v = strings.Trim(v, "\x00")
		return strings.TrimSpace(v)
	}

	caCertFile := normalizeEnvValue(os.Getenv("MC_ETCD_TLS_CA_CERT"))
	serverName := normalizeEnvValue(os.Getenv("MC_ETCD_TLS_SERVER_NAME"))
	insecureSkipVerify := strings.ToLower(strings.TrimSpace(os.Getenv("MC_ETCD_TLS_INSECURE_SKIP_VERIFY")))
	if caCertFile == "" {
		switch insecureSkipVerify {
		case "", "false":
			if serverName != "" {
				cachedSecurity.loadErr = errors.New("MC_ETCD_TLS_SERVER_NAME requires TLS to be enabled (set MC_ETCD_TLS_CA_CERT or MC_ETCD_TLS_INSECURE_SKIP_VERIFY=true)")
				fmt.Fprintf(os.Stderr, "[etcd_wrapper] ERROR: %v\n", cachedSecurity.loadErr)
				return
			}
			return
		case "true":
			tlsCfg := &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true,
			}
			if serverName != "" {
				tlsCfg.ServerName = serverName
				fmt.Fprintf(os.Stderr, "[etcd_wrapper] TLS ServerName override: %s\n", serverName)
			}
			fmt.Fprintf(os.Stderr, "[etcd_wrapper] WARNING: TLS InsecureSkipVerify is enabled without a CA certificate — not safe for production\n")
			cachedSecurity.tlsConfig = tlsCfg
			fmt.Fprintf(os.Stderr, "[etcd_wrapper] TLS enabled (insecure, no CA)\n")
			return
		default:
			cachedSecurity.loadErr = fmt.Errorf(
				"invalid MC_ETCD_TLS_INSECURE_SKIP_VERIFY value %q, expected true or false",
				insecureSkipVerify,
			)
			fmt.Fprintf(os.Stderr, "[etcd_wrapper] ERROR: %v\n", cachedSecurity.loadErr)
			return
		}
	}

	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		cachedSecurity.loadErr = fmt.Errorf("failed to read CA certificate %q: %w", caCertFile, err)
		fmt.Fprintf(os.Stderr, "[etcd_wrapper] ERROR: %v\n", cachedSecurity.loadErr)
		return
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		cachedSecurity.loadErr = fmt.Errorf("failed to parse CA certificate from %s (not valid PEM)", caCertFile)
		fmt.Fprintf(os.Stderr, "[etcd_wrapper] ERROR: %v\n", cachedSecurity.loadErr)
		return
	}

	tlsCfg := &tls.Config{
		RootCAs:    caCertPool,
		MinVersion: tls.VersionTLS12,
	}
	if serverName != "" {
		tlsCfg.ServerName = serverName
		fmt.Fprintf(os.Stderr, "[etcd_wrapper] TLS ServerName override: %s\n", serverName)
	}

	switch insecureSkipVerify {
	case "":
	case "false":
	case "true":
		if insecureSkipVerify == "true" {
			tlsCfg.InsecureSkipVerify = true
			fmt.Fprintf(os.Stderr, "[etcd_wrapper] WARNING: TLS InsecureSkipVerify is enabled — not safe for production\n")
		}
	default:
		cachedSecurity.loadErr = fmt.Errorf(
			"invalid MC_ETCD_TLS_INSECURE_SKIP_VERIFY value %q, expected true or false",
			insecureSkipVerify,
		)
		fmt.Fprintf(os.Stderr, "[etcd_wrapper] ERROR: %v\n", cachedSecurity.loadErr)
		return
	}

	cachedSecurity.tlsConfig = tlsCfg
	fmt.Fprintf(os.Stderr, "[etcd_wrapper] TLS enabled (CA: %s)\n", caCertFile)
}

// applySecurityConfig applies the cached security configuration (RBAC
// credentials and TLS settings) to the given clientv3.Config.
func applySecurityConfig(cfg *clientv3.Config) error {
	securityOnce.Do(loadSecurityConfig)

	if cachedSecurity.loadErr != nil {
		return cachedSecurity.loadErr
	}

	cfg.Username = cachedSecurity.username
	cfg.Password = cachedSecurity.password
	cfg.TLS = cachedSecurity.tlsConfig
	return nil
}
