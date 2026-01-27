package main

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"context"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Use different etcd client so they are not affected by each other,
// and can be configured separately.
var (
	// etcd client for transform engine
	globalClient *clientv3.Client
	globalMutex        sync.Mutex
	globalRefCount     int
	// etcd client for store (HA)
	storeClient  *clientv3.Client
	storeMutex   sync.Mutex
	// keep alive contexts for store
	storeKeepAliveCtx = make(map[int64]context.CancelFunc)
	storeKeepAliveMutex    sync.Mutex
	// watch contexts for store
	storeWatchCtx = make(map[string]context.CancelFunc)
	storeWatchMutex    sync.Mutex
	// etcd client for HA snapshot
	snapshotClient  *clientv3.Client
	snapshotMutex   sync.Mutex
)

const (
	// Snapshot client config (for GB-level snapshot files)
	snapshotMaxMsgSize = 2000 * 1000 * 1000  // 2GB
	snapshotTimeout    = 60 * time.Second   // 1 minute for large files
)

//export NewEtcdClient
func NewEtcdClient(endpoints *C.char, errMsg **C.char) int {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	if globalClient != nil {
		globalRefCount++
		return 0
	}

	MaxMsgSize := 32*1024*1024
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
	endpointList := strings.Split(endpointStr, ";")
	
	// Filter out any empty strings that might result from splitting
	var validEndpoints []string
	for _, ep := range endpointList {
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

//export NewSnapshotEtcdClient
func NewSnapshotEtcdClient(endpoints *C.char, errMsg **C.char) int {
	snapshotMutex.Lock()
	defer snapshotMutex.Unlock()
	if snapshotClient != nil {
		*errMsg = C.CString("etcd snapshot client can be initialized only once")
		return -2
	}

	endpointStr := C.GoString(endpoints)
	// Support multiple endpoints separated by comma or semicolon
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
		Endpoints:          validEndpoints,
		DialTimeout:        10 * time.Second,
		MaxCallSendMsgSize: snapshotMaxMsgSize,
		MaxCallRecvMsgSize: snapshotMaxMsgSize,
	})

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
		return 0;
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
func SnapshotStoreDeleteWrapper(key *C.char, keySize C.int, errMsg **C.char) int {
	if snapshotClient == nil {
		*errMsg = C.CString("etcd snapshot client not initialized")
		return -1
	}
	k := C.GoStringN(key, keySize)
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	_, err := snapshotClient.Delete(ctx, k)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

func main() {}
