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

typedef void (*watch_cb_v1_t)(void* ctx,
                             const char* key, size_t keySize,
                             const char* value, size_t valueSize,
                             int eventType);

static inline void call_watch_cb_v1(void* func,
                                    void* ctx,
                                    const char* key, size_t keySize,
                                    const char* value, size_t valueSize,
                                    int eventType) {
  ((watch_cb_v1_t)func)(ctx, key, keySize, value, valueSize, eventType);
}

typedef void (*watch_cb_v2_t)(void* ctx,
                             const char* key, size_t keySize,
                             const char* value, size_t valueSize,
                             int eventType,
                             long long modRev);

static inline void call_watch_cb_v2(void* func,
                                    void* ctx,
                                    const char* key, size_t keySize,
                                    const char* value, size_t valueSize,
                                    int eventType,
                                    long long modRev) {
  ((watch_cb_v2_t)func)(ctx, key, keySize, value, valueSize, eventType, modRev);
}

#endif  // MOONCAKE_ETCD_CALLBACK_TRAMPOLINES
*/
import "C"

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"
	"unsafe"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// prefixWatchInfo stores cancel function and callback context for a prefix watch
type prefixWatchInfo struct {
	cancel          context.CancelFunc
	callbackContext unsafe.Pointer
	// done is closed when the watch goroutine fully exits (no more callbacks).
	done chan struct{}
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
	// watch contexts for store
	storeWatchCtx   = make(map[string]context.CancelFunc)
	storeWatchMutex sync.Mutex
	// watch contexts for prefix watch
	storePrefixWatchCtx   = make(map[string]prefixWatchInfo)
	storePrefixWatchMutex sync.Mutex
	// Valid callback contexts - track which C++ objects are still alive
	// When a watch is cancelled, we remove the context from this map
	// Before calling callback, we check if context is still valid
	validCallbackContexts     = make(map[unsafe.Pointer]bool)
	validCallbackContextMutex sync.RWMutex
)

//export NewEtcdClient
func NewEtcdClient(endpoints *C.char, errMsg **C.char) int {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	if globalClient != nil {
		globalRefCount++
		return 0
	}

	MaxMsgSize := 32 * 1024 * 1024
	endpointStr := C.GoString(endpoints)
	// Support multiple endpoints separated by comma or semicolon
	// Normalize separators to semicolon first, then split
	endpointStr = strings.ReplaceAll(endpointStr, ",", ";")
	parts := strings.Split(endpointStr, ";")
	var validEndpoints []string
	for _, ep := range parts {
		ep = strings.TrimSpace(ep)
		if ep != "" {
			validEndpoints = append(validEndpoints, ep)
		}
	}
	if len(validEndpoints) == 0 {
		*errMsg = C.CString("no valid endpoints provided")
		return -1
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:          validEndpoints,
		DialTimeout:        5 * time.Second,
		MaxCallSendMsgSize: MaxMsgSize,
		MaxCallRecvMsgSize: MaxMsgSize,
	})

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

//export NewStoreEtcdClient
func NewStoreEtcdClient(endpoints *C.char, errMsg **C.char) int {
	storeMutex.Lock()
	defer storeMutex.Unlock()
	if storeClient != nil {
		*errMsg = C.CString("etcd client can be initialized only once")
		return -2
	}

	endpointStr := C.GoString(endpoints)
	// Support multiple endpoints separated by comma or semicolon.
	endpointStr = strings.ReplaceAll(endpointStr, ",", ";")
	endpointList := strings.Split(endpointStr, ";")

	// Filter out any empty strings that might result from splitting
	var validEndpoints []string
	for _, ep := range endpointList {
		ep = strings.TrimSpace(ep)
		if ep != "" {
			validEndpoints = append(validEndpoints, ep)
		}
	}

	if len(validEndpoints) == 0 {
		*errMsg = C.CString("no valid endpoints provided")
		return -1
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   validEndpoints,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	storeClient = cli
	return 0
}

//export EtcdStoreGetWrapper
func EtcdStoreGetWrapper(key *C.char, keySize C.int, value **C.char,
	valueSize *C.int, revisionId *int64, errMsg **C.char) int {
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := storeClient.Get(ctx, k)
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
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := storeClient.Grant(ctx, ttl)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	*leaseId = int64(resp.ID)
	return 0
}

//export EtcdStoreCreateWithLeaseWrapper
func EtcdStoreCreateWithLeaseWrapper(key *C.char, keySize C.int, value *C.char, valueSize C.int,
	leaseId int64, revisionId *int64, errMsg **C.char) int {
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	v := C.GoStringN(value, valueSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a transaction
	txn := storeClient.Txn(ctx)

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

//export EtcdStoreWatchUntilDeletedWrapper
func EtcdStoreWatchUntilDeletedWrapper(key *C.char, keySize C.int, errMsg **C.char) int {
	if storeClient == nil {
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
	watchChan := storeClient.Watch(ctx, k)

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

//export EtcdStoreKeepAliveWrapper
func EtcdStoreKeepAliveWrapper(leaseId int64, errMsg **C.char) int {
	if storeClient == nil {
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
	keepAliveChan, err := storeClient.KeepAlive(ctx, clientv3.LeaseID(leaseId))
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

//export EtcdStorePutWrapper
func EtcdStorePutWrapper(key *C.char, keySize C.int, value *C.char, valueSize C.int, errMsg **C.char) int {
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	v := C.GoStringN(value, valueSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := storeClient.Put(ctx, k, v)
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
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	v := C.GoStringN(value, valueSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txn := storeClient.Txn(ctx)
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

//export EtcdStoreGetWithPrefixWrapper
func EtcdStoreGetWithPrefixWrapper(prefix *C.char, prefixSize C.int, keys **C.char, keySizes **C.int, values **C.char, valueSizes **C.int, count *C.int, errMsg **C.char) int {
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	p := C.GoStringN(prefix, prefixSize)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := storeClient.Get(ctx, p, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
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
	if storeClient == nil {
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
	resp, err := storeClient.Get(ctx, start, opts...)
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
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	p := C.GoStringN(prefix, prefixSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := storeClient.Get(ctx, p, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(1))
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
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	p := C.GoStringN(prefix, prefixSize)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := storeClient.Get(
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
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	start := C.GoStringN(startKey, startKeySize)
	end := C.GoStringN(endKey, endKeySize)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := storeClient.Delete(ctx, start, clientv3.WithRange(end))
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export EtcdStoreWatchWithPrefixWrapper
func EtcdStoreWatchWithPrefixWrapper(prefix *C.char, prefixSize C.int, callbackContext unsafe.Pointer, callbackFunc unsafe.Pointer, errMsg **C.char) int {
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	if callbackFunc == nil {
		*errMsg = C.CString("callback function is nil")
		return -1
	}
	p := C.GoStringN(prefix, prefixSize)

	// Create a context with cancel function
	ctx, cancel := context.WithCancel(context.Background())

	// Store the cancel function and callback context
	storePrefixWatchMutex.Lock()
	if _, exists := storePrefixWatchCtx[p]; exists {
		storePrefixWatchMutex.Unlock()
		*errMsg = C.CString("This prefix is already being watched")
		return -1
	}
	doneCh := make(chan struct{})
	storePrefixWatchCtx[p] = prefixWatchInfo{
		cancel:          cancel,
		callbackContext: callbackContext,
		done:            doneCh,
	}
	storePrefixWatchMutex.Unlock()

	// Register callback context as valid
	validCallbackContextMutex.Lock()
	validCallbackContexts[callbackContext] = true
	validCallbackContextMutex.Unlock()

	// Start watching in a goroutine
	go func(doneCh chan struct{}) {
		defer func() {
			// Unregister callback context when goroutine exits
			validCallbackContextMutex.Lock()
			delete(validCallbackContexts, callbackContext)
			validCallbackContextMutex.Unlock()

			// Remove watch entry and signal completion
			storePrefixWatchMutex.Lock()
			delete(storePrefixWatchCtx, p)
			storePrefixWatchMutex.Unlock()
			close(doneCh)
		}()

		// Start watching the prefix
		watchChan := storeClient.Watch(ctx, p, clientv3.WithPrefix())

		for {
			select {
			case watchResp, ok := <-watchChan:
				if !ok {
					// Channel closed unexpectedly
					return
				}
				if watchResp.Err() != nil {
					// Watch error, stop watching
					return
				}

				// Process each event
				for _, event := range watchResp.Events {
					keyStr := string(event.Kv.Key)
					keyPtr := C.CString(keyStr)
					keySize := C.size_t(len(keyStr))

					var valuePtr *C.char
					var valueSize C.size_t
					var eventType C.int

					if event.Type == clientv3.EventTypePut {
						eventType = C.int(0) // WatchEventTypePut
						valueStr := string(event.Kv.Value)
						valuePtr = C.CString(valueStr)
						valueSize = C.size_t(len(valueStr))
					} else if event.Type == clientv3.EventTypeDelete {
						eventType = C.int(1) // WatchEventTypeDelete
						valuePtr = nil
						valueSize = 0
					}

					// Call the C callback function via C trampoline (safe ABI)
					C.call_watch_cb_v1(callbackFunc, callbackContext, keyPtr, keySize, valuePtr, valueSize, eventType)

					// Free the C strings
					C.free(unsafe.Pointer(keyPtr))
					if valuePtr != nil {
						C.free(unsafe.Pointer(valuePtr))
					}
				}
			case <-ctx.Done():
				// Context was cancelled
				return
			}
		}
	}(doneCh)

	return 0
}

//export EtcdStoreWatchWithPrefixFromRevisionWrapper
func EtcdStoreWatchWithPrefixFromRevisionWrapper(prefix *C.char, prefixSize C.int, startRevision C.longlong, callbackContext unsafe.Pointer, callbackFunc unsafe.Pointer, errMsg **C.char) int {
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	if callbackFunc == nil {
		*errMsg = C.CString("callback function is nil")
		return -1
	}
	p := C.GoStringN(prefix, prefixSize)

	// Create a context with cancel function
	ctx, cancel := context.WithCancel(context.Background())

	// Store the cancel function and callback context
	storePrefixWatchMutex.Lock()
	if _, exists := storePrefixWatchCtx[p]; exists {
		storePrefixWatchMutex.Unlock()
		*errMsg = C.CString("This prefix is already being watched")
		return -1
	}
	doneCh := make(chan struct{})
	storePrefixWatchCtx[p] = prefixWatchInfo{
		cancel:          cancel,
		callbackContext: callbackContext,
		done:            doneCh,
	}
	storePrefixWatchMutex.Unlock()

	// Register callback context as valid
	validCallbackContextMutex.Lock()
	validCallbackContexts[callbackContext] = true
	validCallbackContextMutex.Unlock()

	go func(doneCh chan struct{}) {
		defer func() {
			// Unregister callback context when goroutine exits
			validCallbackContextMutex.Lock()
			delete(validCallbackContexts, callbackContext)
			validCallbackContextMutex.Unlock()

			// Remove watch entry and signal completion
			storePrefixWatchMutex.Lock()
			delete(storePrefixWatchCtx, p)
			storePrefixWatchMutex.Unlock()
			close(doneCh)
		}()

		opts := []clientv3.OpOption{clientv3.WithPrefix()}
		if startRevision > 0 {
			opts = append(opts, clientv3.WithRev(int64(startRevision)))
		}
		watchChan := storeClient.Watch(ctx, p, opts...)

		for {
			select {
			case watchResp, ok := <-watchChan:
				if !ok {
					return
				}
				if watchResp.Err() != nil {
					return
				}

				for _, event := range watchResp.Events {
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

					// Call the C callback function via C trampoline (safe ABI)
					C.call_watch_cb_v1(callbackFunc, callbackContext, keyPtr, keySize, valuePtr, valueSize, eventType)

					C.free(unsafe.Pointer(keyPtr))
					if valuePtr != nil {
						C.free(unsafe.Pointer(valuePtr))
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}(doneCh)

	return 0
}

//export EtcdStoreWatchWithPrefixFromRevisionV2Wrapper
func EtcdStoreWatchWithPrefixFromRevisionV2Wrapper(prefix *C.char, prefixSize C.int, startRevision C.longlong, callbackContext unsafe.Pointer, callbackFunc unsafe.Pointer, errMsg **C.char) int {
	if storeClient == nil {
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
		return -1
	}
	doneCh := make(chan struct{})
	storePrefixWatchCtx[p] = prefixWatchInfo{
		cancel:          cancel,
		callbackContext: callbackContext,
		done:            doneCh,
	}
	storePrefixWatchMutex.Unlock()

	// Register callback context as valid
	validCallbackContextMutex.Lock()
	validCallbackContexts[callbackContext] = true
	validCallbackContextMutex.Unlock()

	go func(doneCh chan struct{}) {
		defer func() {
			// Unregister callback context when goroutine exits
			validCallbackContextMutex.Lock()
			delete(validCallbackContexts, callbackContext)
			validCallbackContextMutex.Unlock()

			// Remove watch entry and signal completion
			storePrefixWatchMutex.Lock()
			delete(storePrefixWatchCtx, p)
			storePrefixWatchMutex.Unlock()
			close(doneCh)
		}()

		opts := []clientv3.OpOption{clientv3.WithPrefix()}
		if startRevision > 0 {
			opts = append(opts, clientv3.WithRev(int64(startRevision)))
		}
		watchChan := storeClient.Watch(ctx, p, opts...)

		for {
			select {
			case watchResp, ok := <-watchChan:
				if !ok {
					// Channel closed. Check if context was cancelled.
					// If cancelled, don't call callback as C++ object may be destroyed.
					select {
					case <-ctx.Done():
						// Context was cancelled, just return without calling callback
						return
					default:
						// Check if callback context is still valid
						validCallbackContextMutex.RLock()
						_, stillValid := validCallbackContexts[callbackContext]
						validCallbackContextMutex.RUnlock()

						if !stillValid {
							// Context is no longer valid, skip callback
							return
						}

						// Channel closed unexpectedly (not cancelled). Notify C++ watcher to reconnect.
						func() {
							defer func() {
								if r := recover(); r != nil {
									// C++ callback caused panic (likely object destroyed)
									// Remove from valid contexts
									validCallbackContextMutex.Lock()
									delete(validCallbackContexts, callbackContext)
									validCallbackContextMutex.Unlock()
								}
							}()
							// Call the C callback function via C trampoline (safe ABI)
							C.call_watch_cb_v2(callbackFunc, callbackContext, nil, 0, nil, 0, C.int(2) /*WATCH_BROKEN*/, C.longlong(0))
						}()
						return
					}
				}
				if watchResp.Err() != nil {
					// Watch error. Check if context was cancelled before calling callback.
					select {
					case <-ctx.Done():
						// Context was cancelled, just return without calling callback
						return
					default:
						// Check if callback context is still valid
						validCallbackContextMutex.RLock()
						_, stillValid := validCallbackContexts[callbackContext]
						validCallbackContextMutex.RUnlock()

						if !stillValid {
							// Context is no longer valid, skip callback
							return
						}

						// Watch error (not cancelled). Notify C++ watcher to reconnect.
						func() {
							defer func() {
								if r := recover(); r != nil {
									// C++ callback caused panic (likely object destroyed)
									// Remove from valid contexts
									validCallbackContextMutex.Lock()
									delete(validCallbackContexts, callbackContext)
									validCallbackContextMutex.Unlock()
								}
							}()
							// Call the C callback function via C trampoline (safe ABI)
							C.call_watch_cb_v2(callbackFunc, callbackContext, nil, 0, nil, 0, C.int(2) /*WATCH_BROKEN*/, C.longlong(0))
						}()
						return
					}
				}

				// Use response-level revision as a more stable resume point.
				// (It can be >= individual event's ModRevision.)
				// Note: watchResp.Header is a value type, not a pointer, so we can directly access it.
				respRev := int64(0)
				if watchResp.Header.Revision > 0 {
					respRev = watchResp.Header.Revision
				}

				for _, event := range watchResp.Events {
					// Check if context was cancelled before processing each event
					select {
					case <-ctx.Done():
						// Context was cancelled, stop processing events
						return
					default:
						// Continue processing
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

					// Check context again before calling callback
					select {
					case <-ctx.Done():
						// Context was cancelled, free allocated memory and return
						C.free(unsafe.Pointer(keyPtr))
						if valuePtr != nil {
							C.free(unsafe.Pointer(valuePtr))
						}
						return
					default:
						// Check if callback context is still valid before calling
						// This prevents calling callback after C++ object is destroyed
						validCallbackContextMutex.RLock()
						_, stillValid := validCallbackContexts[callbackContext]
						validCallbackContextMutex.RUnlock()

						if !stillValid {
							// Context is no longer valid (object destroyed), skip callback
							C.free(unsafe.Pointer(keyPtr))
							if valuePtr != nil {
								C.free(unsafe.Pointer(valuePtr))
							}
							return
						}

						// Double-check context wasn't cancelled between check and call
						select {
						case <-ctx.Done():
							// Context was cancelled, free memory and return
							C.free(unsafe.Pointer(keyPtr))
							if valuePtr != nil {
								C.free(unsafe.Pointer(valuePtr))
							}
							return
						default:
							// Final check: verify context is still valid immediately before calling
							// This minimizes the time window between check and call
							validCallbackContextMutex.RLock()
							_, stillValid := validCallbackContexts[callbackContext]
							validCallbackContextMutex.RUnlock()

							if !stillValid {
								// Context was invalidated between previous check and now, skip callback
								C.free(unsafe.Pointer(keyPtr))
								if valuePtr != nil {
									C.free(unsafe.Pointer(valuePtr))
								}
								return
							}

							// Call callback with panic recovery to prevent crash if C++ object is destroyed
							func() {
								defer func() {
									if r := recover(); r != nil {
										// C++ callback caused panic (likely object destroyed)
										// Remove from valid contexts to prevent future callbacks
										validCallbackContextMutex.Lock()
										delete(validCallbackContexts, callbackContext)
										validCallbackContextMutex.Unlock()
									}
								}()
								// Callback signature:
								// void cb(void* ctx, char* key, size_t keySize, char* value, size_t valueSize, int eventType, long long modRev)
								// Call the C callback function via C trampoline (safe ABI)
								C.call_watch_cb_v2(callbackFunc, callbackContext, keyPtr, keySize, valuePtr, valueSize, eventType, modRev)
							}()
						}
					}

					C.free(unsafe.Pointer(keyPtr))
					if valuePtr != nil {
						C.free(unsafe.Pointer(valuePtr))
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}(doneCh)

	return 0
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

	// CRITICAL: Invalidate callback context first, then cancel watch.
	validCallbackContextMutex.Lock()
	delete(validCallbackContexts, watchInfo.callbackContext)
	validCallbackContextMutex.Unlock()

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

func main() {}
