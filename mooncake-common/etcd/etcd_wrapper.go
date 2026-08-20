package main

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Trampolines to invoke C/C++ function pointers safely from Go via cgo.
// NOTE: Calling a C function pointer by converting it to a Go func is undefined
// and can crash. Always go through a C helper like these.

#ifndef MOONCAKE_ETCD_CALLBACK_TRAMPOLINES
#define MOONCAKE_ETCD_CALLBACK_TRAMPOLINES

typedef void (*watch_cb_t)(void* ctx,
                             const char* key, size_t keySize,
                             const char* value, size_t valueSize,
                             int eventType,
                             long long modRev);

static inline void call_watch_cb(void* func,
                                    void* ctx,
                                    const char* key, size_t keySize,
                                    const char* value, size_t valueSize,
                                    int eventType,
                                    long long modRev) {
  ((watch_cb_t)func)(ctx, key, keySize, value, valueSize, eventType, modRev);
}

#endif  // MOONCAKE_ETCD_CALLBACK_TRAMPOLINES
*/
import "C"

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	rpctypes "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type prefixWatchInfo struct {
	cancel          context.CancelFunc
	callbackContext unsafe.Pointer
	// done is closed when the watch goroutine fully exits (no more callbacks).
	done chan struct{}
	// broken means this watch ended because reset/error made it untrustworthy.
	broken bool
	// brokenNotified prevents duplicate WATCH_BROKEN callbacks from exit races.
	brokenNotified bool
}

type maintenanceSession struct {
	session *concurrency.Session
	cancel  context.CancelFunc
}

func (s *maintenanceSession) close() error {
	err := s.session.Close()
	s.cancel()
	return err
}

var startMaintenanceSession = func(ctx context.Context, cli *clientv3.Client,
	ttl int) (*concurrency.Session, error) {
	return concurrency.NewSession(cli, concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
}

func newMaintenanceSession(cli *clientv3.Client, ttl int,
	startupTimeout time.Duration) (*maintenanceSession, error) {
	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(startupTimeout, cancel)
	session, err := startMaintenanceSession(ctx, cli, ttl)
	timedOut := !timer.Stop()
	if err != nil {
		cancel()
		return nil, err
	}
	if session == nil {
		cancel()
		return nil, errors.New("maintenance session creation returned nil")
	}
	if timedOut {
		_ = session.Close()
		cancel()
		return nil, context.DeadlineExceeded
	}
	return &maintenanceSession{session: session, cancel: cancel}, nil
}

// Use different etcd client so they are not affected by each other,
// and can be configured separately.
var (
	// etcd client for transform engine
	globalClient   *clientv3.Client
	globalMutex    sync.Mutex
	globalRefCount int
	// etcd client for store
	storeClient *clientv3.Client
	storeMutex  sync.Mutex
	// keep alive contexts for store
	storeKeepAliveCtx   = make(map[int64]context.CancelFunc)
	storeKeepAliveMutex sync.Mutex
	// maintenance sessions own their keepalive and lease lifecycle in Go.
	storeMaintenanceSessions   = make(map[int64]*maintenanceSession)
	storeMaintenanceNextHandle int64
	storeMaintenanceMutex      sync.Mutex
	// watch contexts for store
	storeWatchCtx   = make(map[string]context.CancelFunc)
	storeWatchMutex sync.Mutex
	// etcd client for HA snapshot
	snapshotClient *clientv3.Client
	snapshotMutex  sync.Mutex
	// watch contexts for prefix watch
	storePrefixWatchCtx   = make(map[string]prefixWatchInfo)
	storePrefixWatchMutex sync.Mutex
)

const (
	// Snapshot client config (for GB-level snapshot files)
	snapshotMaxMsgSize = 2000 * 1000 * 1000 // 2GB
	snapshotTimeout    = 60 * time.Second   // 1 minute for large files
)

const (
	storeDialKeepAliveTime    = 10 * time.Second
	storeDialKeepAliveTimeout = 3 * time.Second
	maintenanceStartupTimeout = 5 * time.Second
)

// --- Security configuration (RBAC + TLS) loaded from environment variables ---
//
// Environment variables:
//   MC_ETCD_CONF_FILE - Path to a file containing username=... and password=... lines.
//   MC_ETCD_TLS_CA_CERT           - Path to the CA certificate file for server verification (one-way TLS).
//   MC_ETCD_TLS_SERVER_NAME        - Optional: override the server name used for TLS SNI and certificate
//                                     hostname verification. Useful when connecting via IP or a service
//                                     name that differs from the certificate's SAN.
//   MC_ETCD_TLS_INSECURE_SKIP_VERIFY - Set to "true" to skip server certificate verification. Only for
//                                     testing environments; never use in production.
//
// All variables are optional. When none are set the behaviour is identical to before this feature.
//
// The configuration is loaded exactly once via sync.Once and cached, so even EtcdStoreResetClientWrapper
// reuses the same parsed values without re-reading files.

// securitySettings holds the cached RBAC and TLS configuration parsed from environment variables.
// It is populated exactly once by loadSecurityConfig via sync.Once.
type securitySettings struct {
	username  string
	password  string
	tlsConfig *tls.Config
	loadErr   error // non-nil when a configured resource failed to load
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
//
// Lines starting with '#' are treated as comments and ignored. Empty lines are skipped.
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
		// Skip empty lines and comment lines.
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
// securitySettings. This function is called exactly once via sync.Once.
// Diagnostic messages are written to stderr so they never pollute stdout,
// which callers may rely on for data.
func loadSecurityConfig() {
	// --- RBAC credentials from file ---
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

	// --- TLS configuration (one-way: verify server) ---
	caCertFile := os.Getenv("MC_ETCD_TLS_CA_CERT")
	if caCertFile == "" {
		// No TLS configured — this is fine, plaintext is the default.
		return
	}

	caCert, err := os.ReadFile(caCertFile)
	if err != nil {
		cachedSecurity.loadErr = fmt.Errorf("failed to read CA certificate %s: %w", caCertFile, err)
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

	// Optional: override the server name used for SNI and hostname verification.
	if serverName := os.Getenv("MC_ETCD_TLS_SERVER_NAME"); serverName != "" {
		tlsCfg.ServerName = serverName
		fmt.Fprintf(os.Stderr, "[etcd_wrapper] TLS ServerName override: %s\n", serverName)
	}

	// Optional: skip certificate verification (testing only).
	if os.Getenv("MC_ETCD_TLS_INSECURE_SKIP_VERIFY") == "true" {
		tlsCfg.InsecureSkipVerify = true
		fmt.Fprintf(os.Stderr, "[etcd_wrapper] WARNING: TLS InsecureSkipVerify is enabled — not safe for production\n")
	}

	cachedSecurity.tlsConfig = tlsCfg
	fmt.Fprintf(os.Stderr, "[etcd_wrapper] TLS enabled (CA: %s)\n", caCertFile)
}

// applySecurityConfig applies the cached security configuration (RBAC credentials
// and TLS settings) to the given clientv3.Config. The configuration is loaded
// lazily on first call via sync.Once.
//
// Returns nil on success or when no security environment variables are set.
// Returns an error when a configured resource (RBAC file, CA certificate) fails
// to load — the caller must abort client creation in this case to avoid falling
// back to an unauthenticated connection.
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

// newStoreClientConfig builds the base clientv3.Config for the store etcd client
// and applies any security configuration (RBAC / TLS) from environment variables.
// The caller must check the returned error and abort client creation on failure:
// falling back to an unauthenticated connection when credentials are configured
// would be a security issue.
func newStoreClientConfig(validEndpoints []string) (clientv3.Config, error) {
	cfg := clientv3.Config{
		Endpoints:            validEndpoints,
		DialTimeout:          5 * time.Second,
		DialKeepAliveTime:    storeDialKeepAliveTime,
		DialKeepAliveTimeout: storeDialKeepAliveTimeout,
		PermitWithoutStream:  true,
	}
	if err := applySecurityConfig(&cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// parseEtcdEndpoints converts the C endpoint string into a []string suitable
// for clientv3.Config.Endpoints. It normalises comma separators to semicolons,
// trims whitespace, and filters empty entries. The scheme prefixes (etcd://,
// http://) are handled by the C++ callers, which already pass bare host:port
// endpoints, so no stripping is done here.
func parseEtcdEndpoints(endpoints *C.char) []string {
	if endpoints == nil {
		return nil
	}
	endpointStr := C.GoString(endpoints)
	endpointStr = strings.ReplaceAll(endpointStr, ",", ";")
	endpointList := strings.Split(endpointStr, ";")

	var validEndpoints []string
	for _, ep := range endpointList {
		ep = strings.TrimSpace(ep)
		if ep != "" {
			validEndpoints = append(validEndpoints, ep)
		}
	}
	return validEndpoints
}

//export NewEtcdClient
func NewEtcdClient(endpoints *C.char, errMsg **C.char) int {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	if globalClient != nil {
		globalRefCount++
		return 0
	}

	MaxMsgSize := 32 * 1024 * 1024
	validEndpoints := parseEtcdEndpoints(endpoints)
	if len(validEndpoints) == 0 {
		*errMsg = C.CString("no valid endpoints provided")
		return -1
	}

	cfg := clientv3.Config{
		Endpoints:          validEndpoints,
		DialTimeout:        5 * time.Second,
		MaxCallSendMsgSize: MaxMsgSize,
		MaxCallRecvMsgSize: MaxMsgSize,
	}
	if err := applySecurityConfig(&cfg); err != nil {
		*errMsg = C.CString(fmt.Sprintf("security config error: %v", err))
		return -1
	}
	cli, err := clientv3.New(cfg)

	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	globalClient = cli
	globalRefCount++
	return 0
}

//export EtcdPutWrapper
func EtcdPutWrapper(key *C.char, value *C.char, errMsg **C.char) int {
	if globalClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoString(key)
	v := C.GoString(value)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := globalClient.Put(ctx, k, v)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export EtcdGetWrapper
func EtcdGetWrapper(key *C.char, value **C.char, errMsg **C.char) int {
	if globalClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoString(key)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := globalClient.Get(ctx, k)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	if len(resp.Kvs) == 0 {
		*value = nil
	} else {
		kv := resp.Kvs[0]
		*value = C.CString(string(kv.Value))
	}
	return 0
}

//export EtcdDeleteWrapper
func EtcdDeleteWrapper(key *C.char, errMsg **C.char) int {
	if globalClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoString(key)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := globalClient.Delete(ctx, k)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export EtcdCloseWrapper
func EtcdCloseWrapper() {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	if globalClient != nil {
		globalRefCount--
		if globalRefCount == 0 {
			globalClient.Close()
			globalClient = nil
		}
	}
}

func getStoreClient() *clientv3.Client {
	storeMutex.Lock()
	defer storeMutex.Unlock()
	return storeClient
}

//export NewStoreEtcdClient
func NewStoreEtcdClient(endpoints *C.char, errMsg **C.char) int {
	storeMutex.Lock()
	defer storeMutex.Unlock()
	if storeClient != nil {
		*errMsg = C.CString("etcd client can be initialized only once")
		return -2
	}

	validEndpoints := parseEtcdEndpoints(endpoints)
	if len(validEndpoints) == 0 {
		*errMsg = C.CString("no valid endpoints provided")
		return -1
	}

	cfg, cfgErr := newStoreClientConfig(validEndpoints)
	if cfgErr != nil {
		*errMsg = C.CString(fmt.Sprintf("etcd store client config error: %v", cfgErr))
		return -1
	}
	cli, err := clientv3.New(cfg)

	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	storeClient = cli
	return 0
}

// rebuildStoreClient creates a brand-new store etcd client and atomically
// swaps it into storeClient. All keep-alive goroutines, per-key watches and
// prefix watches bound to the old client are torn down first; prefix watches
// are marked broken so their C++ owners receive a WATCH_BROKEN callback and
// re-arm against the new client. The old client is closed only after the swap.
//
// This is used both by the exported reset wrapper and by the prefix-watch
// goroutine when an etcd auth token expires: clientv3 never refreshes the
// token for long-lived watch streams (etcd-io/etcd#12385, etcd-io/etcd#17623),
// so the only reliable client-side fix is a brand-new client whose token
// bundle is authenticated on first use.
func rebuildStoreClient(endpoints []string) error {
	cfg, cfgErr := newStoreClientConfig(endpoints)
	if cfgErr != nil {
		return cfgErr
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return err
	}

	cancelAllStoreKeepAlives()
	closeAllStoreMaintenanceSessions()
	cancelAllStoreWatches()
	cancelAllStorePrefixWatches()

	storeMutex.Lock()
	oldClient := storeClient
	storeClient = cli
	storeMutex.Unlock()

	if oldClient != nil {
		oldClient.Close()
	}
	return nil
}

//export EtcdStoreResetClientWrapper
func EtcdStoreResetClientWrapper(endpoints *C.char, errMsg **C.char) int {
	validEndpoints := parseEtcdEndpoints(endpoints)
	if len(validEndpoints) == 0 {
		*errMsg = C.CString("no valid endpoints provided")
		return -1
	}

	if err := rebuildStoreClient(validEndpoints); err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export NewSnapshotEtcdClient
func NewSnapshotEtcdClient(endpoints *C.char, errMsg **C.char) int {
	snapshotMutex.Lock()
	defer snapshotMutex.Unlock()
	if snapshotClient != nil {
		*errMsg = C.CString("etcd snapshot client can be initialized only once")
		return -2
	}

	validEndpoints := parseEtcdEndpoints(endpoints)
	if len(validEndpoints) == 0 {
		*errMsg = C.CString("no valid endpoints provided")
		return -1
	}

	cfg := clientv3.Config{
		Endpoints:          validEndpoints,
		DialTimeout:        10 * time.Second,
		MaxCallSendMsgSize: snapshotMaxMsgSize,
		MaxCallRecvMsgSize: snapshotMaxMsgSize,
	}
	if err := applySecurityConfig(&cfg); err != nil {
		*errMsg = C.CString(fmt.Sprintf("security config error: %v", err))
		return -1
	}
	cli, err := clientv3.New(cfg)

	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	snapshotClient = cli
	return 0
}

//export EtcdStoreGetWrapper
func EtcdStoreGetWrapper(key *C.char, keySize C.int, value **C.char,
	valueSize *C.int, revisionId *int64, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, k)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	if len(resp.Kvs) == 0 {
		*errMsg = C.CString("key not found in etcd")
		return -2
	} else {
		kv := resp.Kvs[0]
		*value = C.CString(string(kv.Value))
		*valueSize = C.int(len(kv.Value))
		*revisionId = kv.CreateRevision
		return 0
	}
}

//export EtcdStoreGrantLeaseWrapper
func EtcdStoreGrantLeaseWrapper(ttl int64, leaseId *int64, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Grant(ctx, ttl)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	*leaseId = int64(resp.ID)
	return 0
}

//export EtcdStoreRevokeLeaseWrapper
func EtcdStoreRevokeLeaseWrapper(leaseId int64, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.Revoke(ctx, clientv3.LeaseID(leaseId))
	if err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			return 0
		}
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export EtcdStoreCreateWithLeaseWrapper
func EtcdStoreCreateWithLeaseWrapper(key *C.char, keySize C.int, value *C.char, valueSize C.int,
	leaseId int64, revisionId *int64, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	v := C.GoStringN(value, valueSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a transaction
	txn := cli.Txn(ctx)

	// Only put the key if it does not exist
	resp, err := txn.If(clientv3.Compare(clientv3.CreateRevision(k), "=", 0)).
		Then(clientv3.OpPut(k, v, clientv3.WithLease(clientv3.LeaseID(leaseId)))).
		Commit()

	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	// If the key already existed, resp.Succeeded will be false
	// If we created the key, resp.Succeeded will be true
	if resp.Succeeded {
		*revisionId = resp.Header.Revision
		return 0
	} else {
		*errMsg = C.CString("etcd transaction failed")
		return -2
	}
}

//export EtcdStoreAcquireMaintenanceSessionWrapper
func EtcdStoreAcquireMaintenanceSessionWrapper(key *C.char, keySize C.int, ttl int64,
	sessionHandle *int64, leaseId *int64, createRevision *int64, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	if ttl <= 0 {
		*errMsg = C.CString("maintenance session TTL must be positive")
		return -1
	}

	session, err := newMaintenanceSession(cli, int(ttl), maintenanceStartupTimeout)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	k := C.GoStringN(key, keySize)
	id := int64(session.session.Lease())
	ownerToken := strconv.FormatInt(id, 10)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(k), "=", 0)).
		Then(clientv3.OpPut(k, ownerToken, clientv3.WithLease(session.session.Lease()))).
		Commit()
	cancel()
	if err != nil {
		_ = session.close()
		*errMsg = C.CString(err.Error())
		return -1
	}
	if !resp.Succeeded {
		_ = session.close()
		*errMsg = C.CString("maintenance lock is already held")
		return -2
	}

	storeMaintenanceMutex.Lock()
	storeMaintenanceNextHandle++
	handle := storeMaintenanceNextHandle
	storeMaintenanceSessions[handle] = session
	storeMaintenanceMutex.Unlock()

	*sessionHandle = handle
	*leaseId = id
	*createRevision = resp.Header.Revision
	return 0
}

//export EtcdStoreCloseMaintenanceSessionWrapper
func EtcdStoreCloseMaintenanceSessionWrapper(sessionHandle int64, errMsg **C.char) int {
	storeMaintenanceMutex.Lock()
	session, exists := storeMaintenanceSessions[sessionHandle]
	if exists {
		delete(storeMaintenanceSessions, sessionHandle)
	}
	storeMaintenanceMutex.Unlock()
	if !exists {
		return 0
	}
	if err := session.close(); err != nil && !errors.Is(err, rpctypes.ErrLeaseNotFound) {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export EtcdStoreMaintenanceSessionAliveWrapper
func EtcdStoreMaintenanceSessionAliveWrapper(sessionHandle int64, errMsg **C.char) int {
	storeMaintenanceMutex.Lock()
	session, exists := storeMaintenanceSessions[sessionHandle]
	storeMaintenanceMutex.Unlock()
	if !exists {
		*errMsg = C.CString("maintenance session handle not found")
		return -1
	}
	select {
	case <-session.session.Done():
		return 0
	default:
		return 1
	}
}

/*
* @brief First cancel the watch context, then delete it from the map.
*        Cancel must be called before delete in case this is a new context
*        other than the one we want to delete. In that case, that context will
*        be deleted before being cancelled and will not be able to be cancelled
*        anymore.
 */
func cancelAndDeleteWatch(k string) int {
	storeWatchMutex.Lock()
	defer storeWatchMutex.Unlock()

	if cancel, exists := storeWatchCtx[k]; exists {
		cancel()
		delete(storeWatchCtx, k)
		return 0
	}
	return -1
}

func cancelAllStoreKeepAlives() {
	storeKeepAliveMutex.Lock()
	cancels := make([]context.CancelFunc, 0, len(storeKeepAliveCtx))
	for leaseId, cancel := range storeKeepAliveCtx {
		cancels = append(cancels, cancel)
		delete(storeKeepAliveCtx, leaseId)
	}
	storeKeepAliveMutex.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func closeAllStoreMaintenanceSessions() {
	storeMaintenanceMutex.Lock()
	sessions := make([]*maintenanceSession, 0, len(storeMaintenanceSessions))
	for handle, session := range storeMaintenanceSessions {
		sessions = append(sessions, session)
		delete(storeMaintenanceSessions, handle)
	}
	storeMaintenanceMutex.Unlock()

	for _, session := range sessions {
		_ = session.close()
	}
}

func cancelAllStoreWatches() {
	storeWatchMutex.Lock()
	cancels := make([]context.CancelFunc, 0, len(storeWatchCtx))
	for key, cancel := range storeWatchCtx {
		cancels = append(cancels, cancel)
		delete(storeWatchCtx, key)
	}
	storeWatchMutex.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

func cancelAllStorePrefixWatches() {
	storePrefixWatchMutex.Lock()
	cancels := make([]context.CancelFunc, 0, len(storePrefixWatchCtx))
	for prefix, watchInfo := range storePrefixWatchCtx {
		watchInfo.broken = true
		storePrefixWatchCtx[prefix] = watchInfo
		cancels = append(cancels, watchInfo.cancel)
		// Do not delete map entries here; let goroutine defer clean up.
	}
	storePrefixWatchMutex.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
}

// notifyStorePrefixWatchBrokenOnce delivers a WATCH_BROKEN callback for the
// prefix watch `p` at most once. If markBroken is true the watch is first
// marked broken (used by reset/error exit paths); explicit cancel passes
// markBroken=false so a normal shutdown does not generate a spurious
// WATCH_BROKEN. The cgo callback is invoked outside storePrefixWatchMutex
// to avoid deadlocks from cgo re-entry while holding a Go mutex.
func notifyStorePrefixWatchBrokenOnce(p string, callbackContext unsafe.Pointer, callbackFunc unsafe.Pointer, markBroken bool) {
	shouldNotify := false

	storePrefixWatchMutex.Lock()
	if watchInfo, exists := storePrefixWatchCtx[p]; exists {
		if markBroken {
			watchInfo.broken = true
		}
		if watchInfo.broken && !watchInfo.brokenNotified {
			watchInfo.brokenNotified = true
			shouldNotify = true
		}
		storePrefixWatchCtx[p] = watchInfo
	}
	storePrefixWatchMutex.Unlock()

	if shouldNotify {
		C.call_watch_cb(callbackFunc, callbackContext, nil, 0, nil, 0, C.int(2) /*WATCH_BROKEN*/, C.longlong(0))
	}
}

//export EtcdStoreWatchUntilDeletedWrapper
func EtcdStoreWatchUntilDeletedWrapper(key *C.char, keySize C.int, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)

	// Create a context with cancel function
	ctx, cancel := context.WithCancel(context.Background())

	// Store the cancel function
	storeWatchMutex.Lock()
	if _, exists := storeWatchCtx[k]; exists {
		storeWatchMutex.Unlock()
		*errMsg = C.CString("This key is already being watched")
		return -1
	}
	storeWatchCtx[k] = cancel
	storeWatchMutex.Unlock()

	// Make sure to delete from the map before returning
	defer cancelAndDeleteWatch(k)

	// Start watching the key
	watchChan := cli.Watch(ctx, k)

	// Wait for the key to be deleted
	for {
		select {
		case watchResp, ok := <-watchChan:
			if !ok {
				// Channel closed unexpectedly
				*errMsg = C.CString("watch channel closed unexpectedly")
				return -1
			}
			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypeDelete {
					// Clean up the context when done
					return 0
				}
			}
		case <-ctx.Done():
			// Context was cancelled
			*errMsg = C.CString("watch context cancelled")
			return -2
		}
	}
}

//export EtcdStoreCancelWatchWrapper
func EtcdStoreCancelWatchWrapper(key *C.char, keySize C.int, errMsg **C.char) int {
	k := C.GoStringN(key, keySize)
	if cancelAndDeleteWatch(k) == -1 {
		*errMsg = C.CString("no watch context found for the given key")
		return -1
	}
	return 0
}

/*
* @brief First cancel the keep alive context, then delete it from the map.
*        Cancel must be called before deleting in case this is a new context
*        other than the one we want to delete. In that case, that context will
*        be deleted before being cancelled and will not be able to be cancelled
*        anymore.
 */
func cancelAndDeleteKeepAlive(leaseId int64) int {
	storeKeepAliveMutex.Lock()
	defer storeKeepAliveMutex.Unlock()

	if cancel, exists := storeKeepAliveCtx[leaseId]; exists {
		cancel()
		delete(storeKeepAliveCtx, leaseId)
		return 0
	}
	return -1
}

func hasKeepAliveContext(leaseId int64) bool {
	storeKeepAliveMutex.Lock()
	defer storeKeepAliveMutex.Unlock()

	_, exists := storeKeepAliveCtx[leaseId]
	return exists
}

//export EtcdStoreKeepAliveWrapper
func EtcdStoreKeepAliveWrapper(leaseId int64, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}

	// Create a context with cancel function
	ctx, cancel := context.WithCancel(context.Background())

	// Store the cancel function
	storeKeepAliveMutex.Lock()
	if _, exists := storeKeepAliveCtx[leaseId]; exists {
		storeKeepAliveMutex.Unlock()
		*errMsg = C.CString("This lease id is already being kept alive")
		return -1
	}
	storeKeepAliveCtx[leaseId] = cancel
	storeKeepAliveMutex.Unlock()
	// Make sure to delete from the map before returning
	defer cancelAndDeleteKeepAlive(leaseId)

	// Start keep alive
	keepAliveChan, err := cli.KeepAlive(ctx, clientv3.LeaseID(leaseId))
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	// Wait for keep alive responses
	for {
		select {
		case resp, ok := <-keepAliveChan:
			if !ok {
				*errMsg = C.CString("keep alive channel closed")
				return -1
			}
			if resp == nil {
				*errMsg = C.CString("keep alive response is nil")
				return -1
			}
			// Keep alive successful, continue
		case <-ctx.Done():
			// Context cancelled
			*errMsg = C.CString("keep alive context cancelled")
			return -2
		}
	}
}

//export EtcdStoreCancelKeepAliveWrapper
func EtcdStoreCancelKeepAliveWrapper(leaseId int64, errMsg **C.char) int {
	if cancelAndDeleteKeepAlive(leaseId) == -1 {
		*errMsg = C.CString("no keep alive context found for the given lease ID")
		return -1
	}
	return 0
}

//export EtcdStoreWaitKeepAliveReadyWrapper
func EtcdStoreWaitKeepAliveReadyWrapper(leaseId int64, timeoutMs int, errMsg **C.char) int {
	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	for {
		if hasKeepAliveContext(leaseId) {
			return 0
		}
		if timeoutMs <= 0 || !time.Now().Before(deadline) {
			*errMsg = C.CString("keep alive context did not become ready before timeout")
			return -1
		}
		time.Sleep(time.Millisecond)
	}
}

//export EtcdStorePutWrapper
func EtcdStorePutWrapper(key *C.char, keySize C.int, value *C.char, valueSize C.int, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	v := C.GoStringN(value, valueSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := cli.Put(ctx, k, v)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

// Create key if absent (CAS on CreateRevision==0).
// Return:
// - 0 on success
// - -2 if key already exists
// - -1 on error
//
//export EtcdStoreCreateWrapper
func EtcdStoreCreateWrapper(key *C.char, keySize C.int, value *C.char, valueSize C.int, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	v := C.GoStringN(value, valueSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txn := cli.Txn(ctx)
	resp, err := txn.If(clientv3.Compare(clientv3.CreateRevision(k), "=", 0)).
		Then(clientv3.OpPut(k, v)).
		Commit()
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	if resp.Succeeded {
		return 0
	}
	*errMsg = C.CString("key already exists")
	return -2
}

//export EtcdStoreBatchCreateWrapper
func EtcdStoreBatchCreateWrapper(keys **C.char, values **C.char, count C.int, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}

	n := int(count)
	if n == 0 {
		return 0
	}

	// Unsafe casting to access C arrays as Go slices
	keyPtrs := (*[1 << 28]*C.char)(unsafe.Pointer(keys))[:n:n]
	valPtrs := (*[1 << 28]*C.char)(unsafe.Pointer(values))[:n:n]

	ops := make([]clientv3.Op, 0, n)
	cmps := make([]clientv3.Cmp, 0, n)
	for i := 0; i < n; i++ {
		k := C.GoString(keyPtrs[i])
		v := C.GoString(valPtrs[i])
		ops = append(ops, clientv3.OpPut(k, v))
		// Ensure none of the keys exist
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(k), "=", 0))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Use Txn to ensure atomicity of the batch
	resp, err := cli.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	if !resp.Succeeded {
		*errMsg = C.CString("transaction failed: one or more keys already exist")
		return -2
	}
	return 0
}

//export EtcdStoreTxnCompareAndPutWrapper
func EtcdStoreTxnCompareAndPutWrapper(compareKeys **C.char, compareKeySizes *C.int, compareKinds *C.int, compareValues **C.char, compareValueSizes *C.int, compareRevisions *int64, compareCount C.int, putKeys **C.char, putKeySizes *C.int, putValues **C.char, putValueSizes *C.int, putCount C.int, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}

	cmpN := int(compareCount)
	putN := int(putCount)

	cmps := make([]clientv3.Cmp, 0, cmpN)
	if cmpN > 0 {
		compareKeyPtrs := (*[1 << 28]*C.char)(unsafe.Pointer(compareKeys))[:cmpN:cmpN]
		compareKeySizeList := (*[1 << 28]C.int)(unsafe.Pointer(compareKeySizes))[:cmpN:cmpN]
		compareKindList := (*[1 << 28]C.int)(unsafe.Pointer(compareKinds))[:cmpN:cmpN]
		compareValuePtrs := (*[1 << 28]*C.char)(unsafe.Pointer(compareValues))[:cmpN:cmpN]
		compareValueSizeList := (*[1 << 28]C.int)(unsafe.Pointer(compareValueSizes))[:cmpN:cmpN]
		compareRevisionList := (*[1 << 28]int64)(unsafe.Pointer(compareRevisions))[:cmpN:cmpN]
		for i := 0; i < cmpN; i++ {
			k := C.GoStringN(compareKeyPtrs[i], compareKeySizeList[i])
			switch int(compareKindList[i]) {
			case 0:
				v := C.GoStringN(compareValuePtrs[i], compareValueSizeList[i])
				cmps = append(cmps, clientv3.Compare(clientv3.Value(k), "=", v))
			case 1:
				cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(k), "=", 0))
			case 2:
				cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(k), "=", compareRevisionList[i]))
			default:
				*errMsg = C.CString("unsupported compare kind")
				return -1
			}
		}
	}

	ops := make([]clientv3.Op, 0, putN)
	if putN > 0 {
		putKeyPtrs := (*[1 << 28]*C.char)(unsafe.Pointer(putKeys))[:putN:putN]
		putKeySizeList := (*[1 << 28]C.int)(unsafe.Pointer(putKeySizes))[:putN:putN]
		putValuePtrs := (*[1 << 28]*C.char)(unsafe.Pointer(putValues))[:putN:putN]
		putValueSizeList := (*[1 << 28]C.int)(unsafe.Pointer(putValueSizes))[:putN:putN]
		for i := 0; i < putN; i++ {
			k := C.GoStringN(putKeyPtrs[i], putKeySizeList[i])
			v := C.GoStringN(putValuePtrs[i], putValueSizeList[i])
			ops = append(ops, clientv3.OpPut(k, v))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	if !resp.Succeeded {
		*errMsg = C.CString("transaction compare failed")
		return -2
	}
	return 0
}

//export EtcdStoreGetWithPrefixWrapper
func EtcdStoreGetWithPrefixWrapper(prefix *C.char, prefixSize C.int, keys **C.char, keySizes **C.int, values **C.char, valueSizes **C.int, count *C.int, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	p := C.GoStringN(prefix, prefixSize)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, p, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	if len(resp.Kvs) == 0 {
		*count = 0
		return 0
	}

	// Allocate arrays for keys and values
	keyCount := len(resp.Kvs)
	*count = C.int(keyCount)

	// Allocate memory for arrays
	keysArray := (*[1 << 30]*C.char)(C.malloc(C.size_t(keyCount) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	keySizesArray := (*[1 << 30]C.int)(C.malloc(C.size_t(keyCount) * C.size_t(unsafe.Sizeof(C.int(0)))))
	valuesArray := (*[1 << 30]*C.char)(C.malloc(C.size_t(keyCount) * C.size_t(unsafe.Sizeof((*C.char)(nil)))))
	valueSizesArray := (*[1 << 30]C.int)(C.malloc(C.size_t(keyCount) * C.size_t(unsafe.Sizeof(C.int(0)))))

	for i, kv := range resp.Kvs {
		keysArray[i] = C.CString(string(kv.Key))
		keySizesArray[i] = C.int(len(kv.Key))
		valuesArray[i] = C.CString(string(kv.Value))
		valueSizesArray[i] = C.int(len(kv.Value))
	}

	*keys = (*C.char)(unsafe.Pointer(keysArray))
	*keySizes = (*C.int)(unsafe.Pointer(keySizesArray))
	*values = (*C.char)(unsafe.Pointer(valuesArray))
	*valueSizes = (*C.int)(unsafe.Pointer(valueSizesArray))

	return 0
}

//export EtcdStoreGetRangeAsJsonWrapper
func EtcdStoreGetRangeAsJsonWrapper(startKey *C.char, startKeySize C.int, endKey *C.char, endKeySize C.int, limit C.int, outJson **C.char, outJsonSize *C.int, revisionId *C.longlong, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	start := C.GoStringN(startKey, startKeySize)
	end := C.GoStringN(endKey, endKeySize)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	opts := []clientv3.OpOption{
		clientv3.WithRange(end),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}
	if limit > 0 {
		opts = append(opts, clientv3.WithLimit(int64(limit)))
	}
	resp, err := cli.Get(ctx, start, opts...)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	if resp != nil && resp.Header != nil {
		*revisionId = C.longlong(resp.Header.Revision)
	} else {
		*revisionId = 0
	}

	type kvPair struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	kvs := make([]kvPair, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		kvs = append(kvs, kvPair{Key: string(kv.Key), Value: string(kv.Value)})
	}
	b, jerr := json.Marshal(kvs)
	if jerr != nil {
		*errMsg = C.CString(jerr.Error())
		return -1
	}

	*outJson = C.CString(string(b))
	*outJsonSize = C.int(len(b))
	return 0
}

//export EtcdStoreGetFirstKeyWithPrefixWrapper
func EtcdStoreGetFirstKeyWithPrefixWrapper(prefix *C.char, prefixSize C.int, firstKey **C.char, firstKeySize *C.int, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	p := C.GoStringN(prefix, prefixSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, p, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(1))
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	if len(resp.Kvs) == 0 {
		*errMsg = C.CString("no key found with prefix")
		return -2
	}
	kv := resp.Kvs[0]
	*firstKey = C.CString(string(kv.Key))
	*firstKeySize = C.int(len(kv.Key))
	return 0
}

//export EtcdStoreGetLastKeyWithPrefixWrapper
func EtcdStoreGetLastKeyWithPrefixWrapper(prefix *C.char, prefixSize C.int, lastKey **C.char, lastKeySize *C.int, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	p := C.GoStringN(prefix, prefixSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.Get(
		ctx, p,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
		clientv3.WithLimit(1),
	)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	if len(resp.Kvs) == 0 {
		*errMsg = C.CString("no key found with prefix")
		return -2
	}
	kv := resp.Kvs[0]
	*lastKey = C.CString(string(kv.Key))
	*lastKeySize = C.int(len(kv.Key))
	return 0
}

//export EtcdStoreDeleteRangeWrapper
func EtcdStoreDeleteRangeWrapper(startKey *C.char, startKeySize C.int, endKey *C.char, endKeySize C.int, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	start := C.GoStringN(startKey, startKeySize)
	end := C.GoStringN(endKey, endKeySize)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := cli.Delete(ctx, start, clientv3.WithRange(end))
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export EtcdStoreWatchWithPrefixFromRevisionWrapper
func EtcdStoreWatchWithPrefixFromRevisionWrapper(prefix *C.char, prefixSize C.int, startRevision C.longlong, callbackContext unsafe.Pointer, callbackFunc unsafe.Pointer, errMsg **C.char) int {
	cli := getStoreClient()
	if cli == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	if callbackFunc == nil {
		*errMsg = C.CString("callback function is nil")
		return -1
	}
	p := C.GoStringN(prefix, prefixSize)

	ctx, cancel := context.WithCancel(context.Background())

	storePrefixWatchMutex.Lock()
	if _, exists := storePrefixWatchCtx[p]; exists {
		storePrefixWatchMutex.Unlock()
		*errMsg = C.CString("This prefix is already being watched")
		cancel()
		return -1
	}
	doneCh := make(chan struct{})
	// createdCh reports whether the server-side watch was successfully
	// established. The goroutine sends exactly one value: true once etcd
	// confirms the watch (clientv3.WithCreatedNotify), or false if the
	// goroutine exits before that confirmation. The wrapper blocks on it before
	// returning, so callers can rely on "watch is live server-side" the moment
	// WatchWithPrefixFromRevision returns OK. This closes the race where the
	// goroutine had not yet issued storeClient.Watch() when the caller
	// proceeded to read the current view. Buffered (size 1) so the goroutine
	// never blocks on the send even if the wrapper has already timed out.
	createdCh := make(chan bool, 1)
	storePrefixWatchCtx[p] = prefixWatchInfo{
		cancel:          cancel,
		callbackContext: callbackContext,
		done:            doneCh,
		broken:          false,
		brokenNotified:  false,
	}
	storePrefixWatchMutex.Unlock()

	go func(doneCh chan struct{}, createdCh chan bool) {
		// createdSignalled guards against sending on createdCh more than once.
		createdSignalled := false
		signalCreated := func(ok bool) {
			if !createdSignalled {
				createdSignalled = true
				createdCh <- ok
			}
		}
		defer func() {
			// Report failure if the watch was never confirmed (e.g. the
			// goroutine exits before any response), then remove the watch
			// entry and signal completion. Ordering matters: the failure
			// signal is sent before `done` is closed.
			signalCreated(false)
			storePrefixWatchMutex.Lock()
			delete(storePrefixWatchCtx, p)
			storePrefixWatchMutex.Unlock()
			close(doneCh)
		}()

		// Auth tokens issued by etcd have a server-side TTL (default 300s for
		// the `simple` token type). When one expires the server kills the
		// long-lived watch stream with `Unauthenticated: invalid auth token`.
		// Re-issuing cli.Watch() opens a NEW watch stream whose client
		// interceptor calls getToken() and attaches a fresh token, so the
		// watch recovers here without tearing down and re-arming from C++.
		// authRetries caps *consecutive* reconnects triggered solely by token
		// expiry: it is reset on every successful Created, so a credential
		// problem still surfaces as WATCH_BROKEN instead of retrying forever.
		const maxAuthRetries = 5
		authRetries := maxAuthRetries

		// resumeRev is the next revision to watch from on reconnect so no
		// events are missed between the stream dying and the reconnect.
		// 0 means "from the current revision" (or the caller's startRevision).
		resumeRev := int64(startRevision)
		if resumeRev < 0 {
			resumeRev = 0
		}

		// startWatch (re)creates the watch stream. Every call goes through the
		// client interceptor, which re-authenticates when credentials are set.
		startWatch := func() clientv3.WatchChan {
			// WithCreatedNotify makes etcd send an initial response with
			// Created == true as soon as the watch is registered server-side.
			opts := []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithCreatedNotify()}
			if resumeRev > 0 {
				opts = append(opts, clientv3.WithRev(resumeRev))
			}
			return cli.Watch(ctx, p, opts...)
		}
		watchChan := startWatch()

		for {
			select {
			case watchResp, ok := <-watchChan:
				if !ok {
					// Channel closed. Check if context was cancelled.
					select {
					case <-ctx.Done():
						notifyStorePrefixWatchBrokenOnce(p, callbackContext, callbackFunc, false)
						return
					default:
						// Channel closed unexpectedly (not cancelled). Notify C++ watcher to reconnect.
						notifyStorePrefixWatchBrokenOnce(p, callbackContext, callbackFunc, true)
						return
					}
				}
				// The first response with Created == true confirms the
				// server-side watch is established; release the waiter.
				if watchResp.Created {
					signalCreated(true)
					// A successful (re)connect resets the auth-retry budget so
					// only *consecutive* token failures can exhaust it.
					authRetries = maxAuthRetries
				}
				if watchResp.Err() != nil {
					// Watch error. Check if context was cancelled.
					select {
					case <-ctx.Done():
						notifyStorePrefixWatchBrokenOnce(p, callbackContext, callbackFunc, false)
						return
					default:
					}
					// Detect auth-token expiry. Both rpctypes.Error() and
					// status.FromError() can return codes.Unknown for the same
					// Unauthenticated failure, so fall back to matching the
					// stable server-side description text as well.
					st, _ := status.FromError(watchResp.Err())
					errText := watchResp.Err().Error()
					isAuthTokenErr := st.Code() == codes.Unauthenticated ||
						strings.Contains(errText, "etcdserver: invalid auth token")
					if isAuthTokenErr && authRetries > 0 {
						// Probe with a plain unary Authenticate RPC to tell
						// "server rejects our credentials / auth is broken"
						// (probe_err != nil) apart from "only this client's
						// cached token is stale" (probe_err == nil).
						authRetries--
						probeCtx, probeCancel := context.WithTimeout(ctx, 3*time.Second)
						_, probeErr := cli.Auth.Authenticate(probeCtx, cli.Username, cli.Password)
						probeCancel()
						if probeErr == nil {
							// Server is healthy and willing to issue a fresh
							// token; the stale token lives only in this client's
							// authTokenBundle, which clientv3 never refreshes
							// for long-lived watch streams (etcd-io/etcd#12385,
							// etcd-io/etcd#17623). Rebuilding the store client
							// creates a fresh bundle that is authenticated on
							// first use.
							fmt.Fprintf(os.Stderr, "[etcd_wrapper] prefix=%s: auth token expired, rebuilding store client (retries left=%d): %v\n",
								p, authRetries, watchResp.Err())
							if rebuildErr := rebuildStoreClient(cli.Endpoints()); rebuildErr != nil {
								fmt.Fprintf(os.Stderr, "[etcd_wrapper] prefix=%s: store client rebuild failed (%v), falling back to in-place reconnect\n",
									p, rebuildErr)
							} else {
								// The rebuild cancelled every prefix watch
								// (including this one) and swapped in a fresh
								// client. Signal WATCH_BROKEN so the C++ side
								// re-arms against the new client; any events
								// missed between stream death and re-arm are
								// replayed by the caller's resync logic.
								notifyStorePrefixWatchBrokenOnce(p, callbackContext, callbackFunc, true)
								return
							}
						} else {
							fmt.Fprintf(os.Stderr, "[etcd_wrapper] prefix=%s: auth token expired, reconnecting (retries left=%d, reauth_probe_err=%v): %v\n",
								p, authRetries, probeErr, watchResp.Err())
						}
						select {
						case <-ctx.Done():
							notifyStorePrefixWatchBrokenOnce(p, callbackContext, callbackFunc, false)
							return
						case <-time.After(200 * time.Millisecond):
						}
						watchChan = startWatch()
						continue
					}
					// Log the concrete error before notifying C++ so the root
					// cause (e.g. PermissionDenied, ErrCompacted, rpc error) is
					// visible instead of being swallowed.
					fmt.Fprintf(os.Stderr, "[etcd_wrapper] prefix=%s: watch response error (created=%v, rev=%d, auth_retries_left=%d): %v\n",
						p, watchResp.Created, watchResp.Header.Revision, authRetries, watchResp.Err())
					notifyStorePrefixWatchBrokenOnce(p, callbackContext, callbackFunc, true)
					return
				}

				// Use response-level revision as a more stable resume point.
				respRev := int64(0)
				if watchResp.Header.Revision > 0 {
					respRev = watchResp.Header.Revision
				}
				// The whole response is processed synchronously before the next
				// channel read, so on a reconnect we can safely resume right
				// after this response's revision without loss or duplication.
				if respRev+1 > resumeRev {
					resumeRev = respRev + 1
				}

				for _, event := range watchResp.Events {
					select {
					case <-ctx.Done():
						notifyStorePrefixWatchBrokenOnce(p, callbackContext, callbackFunc, false)
						return
					default:
					}

					keyStr := string(event.Kv.Key)
					keyPtr := C.CString(keyStr)
					keySize := C.size_t(len(keyStr))

					var valuePtr *C.char
					var valueSize C.size_t
					var eventType C.int

					if event.Type == clientv3.EventTypePut {
						eventType = C.int(0)
						valueStr := string(event.Kv.Value)
						valuePtr = C.CString(valueStr)
						valueSize = C.size_t(len(valueStr))
					} else if event.Type == clientv3.EventTypeDelete {
						eventType = C.int(1)
						valuePtr = nil
						valueSize = 0
					}

					modRev := C.longlong(0)
					if event.Kv != nil {
						evRev := event.Kv.ModRevision
						if respRev > evRev {
							evRev = respRev
						}
						modRev = C.longlong(evRev)
					} else if respRev > 0 {
						modRev = C.longlong(respRev)
					}

					C.call_watch_cb(callbackFunc, callbackContext, keyPtr, keySize, valuePtr, valueSize, eventType, modRev)

					C.free(unsafe.Pointer(keyPtr))
					if valuePtr != nil {
						C.free(unsafe.Pointer(valuePtr))
					}
				}
			case <-ctx.Done():
				notifyStorePrefixWatchBrokenOnce(p, callbackContext, callbackFunc, false)
				return
			}
		}
	}(doneCh, createdCh)

	// Block until the server confirms the watch is established. Returning OK
	// then guarantees the watch is live server-side, so a caller that arms the
	// watch before reading state will not miss an event in between.
	const watchCreatedTimeout = 5 * time.Second
	select {
	case created := <-createdCh:
		if !created {
			// The goroutine exited before the watch was confirmed (e.g. the
			// Watch RPC failed). It has already torn itself down; wait for it
			// to finish so the prefix entry is gone and no callback is in
			// flight, then report failure.
			cancel()
			<-doneCh
			*errMsg = C.CString("etcd watch goroutine exited before the watch was established")
			return -1
		}
		return 0
	case <-time.After(watchCreatedTimeout):
		// The watch was not established within the timeout. Tear down the
		// goroutine and report failure so the caller can fall back. Wait for
		// the goroutine to fully exit so the prefix entry is removed and no
		// callback can be in flight when we return.
		cancel()
		<-doneCh
		*errMsg = C.CString("timeout waiting for etcd watch to be created")
		return -1
	}
}

func cancelAndDeletePrefixWatch(p string) int {
	// NOTE: We intentionally do NOT delete the prefix entry here.
	// The watch goroutine owns deletion + closing `done`, so callers can Wait safely.
	storePrefixWatchMutex.Lock()
	watchInfo, exists := storePrefixWatchCtx[p]
	storePrefixWatchMutex.Unlock()

	if !exists {
		return -1
	}

	watchInfo.cancel()
	return 0
}

//export EtcdStoreWaitWatchWithPrefixStoppedWrapper
func EtcdStoreWaitWatchWithPrefixStoppedWrapper(prefix *C.char, prefixSize C.int, timeoutMs C.int, errMsg **C.char) int {
	p := C.GoStringN(prefix, prefixSize)
	storePrefixWatchMutex.Lock()
	watchInfo, exists := storePrefixWatchCtx[p]
	storePrefixWatchMutex.Unlock()

	// If there is no watch, it's already stopped (idempotent).
	if !exists {
		return 0
	}

	timeout := time.Duration(timeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5000 * time.Millisecond
	}

	select {
	case <-watchInfo.done:
		return 0
	case <-time.After(timeout):
		if errMsg != nil {
			*errMsg = C.CString("timeout waiting for prefix watch to stop")
		}
		return -1
	}
}

//export EtcdStoreCancelWatchWithPrefixWrapper
func EtcdStoreCancelWatchWithPrefixWrapper(prefix *C.char, prefixSize C.int, errMsg **C.char) int {
	p := C.GoStringN(prefix, prefixSize)
	// Idempotent cancel: callers may cancel pre-emptively before starting a watch.
	// If no context exists, treat it as success.
	_ = cancelAndDeletePrefixWatch(p)
	// Intentionally does not wait; use EtcdStoreWaitWatchWithPrefixStoppedWrapper.
	_ = errMsg
	return 0
}

//export SnapshotStorePutWrapper
func SnapshotStorePutWrapper(key *C.char, keySize C.int, value *C.char, valueSize C.int, errMsg **C.char) int {
	if snapshotClient == nil {
		*errMsg = C.CString("etcd snapshot client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	v := C.GoStringN(value, valueSize)
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	_, err := snapshotClient.Put(ctx, k, v)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export SnapshotStoreGetWrapper
func SnapshotStoreGetWrapper(key *C.char, keySize C.int, value **C.char,
	valueSize *C.int, revisionId *int64, errMsg **C.char) int {
	if snapshotClient == nil {
		*errMsg = C.CString("etcd snapshot client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	resp, err := snapshotClient.Get(ctx, k)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	if len(resp.Kvs) == 0 {
		*errMsg = C.CString("key not found in etcd")
		return -2
	} else {
		kv := resp.Kvs[0]
		*value = (*C.char)(C.CBytes(kv.Value))
		*valueSize = C.int(len(kv.Value))
		*revisionId = kv.CreateRevision
		return 0
	}
}

//export SnapshotStoreDeleteWrapper
func SnapshotStoreDeleteWrapper(key *C.char, keySize C.int, usePrefix C.int, errMsg **C.char) int {
	if snapshotClient == nil {
		*errMsg = C.CString("etcd snapshot client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()

	var opts []clientv3.OpOption
	if usePrefix != 0 {
		opts = append(opts, clientv3.WithPrefix())
	}

	_, err := snapshotClient.Delete(ctx, k, opts...)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

func main() {}
