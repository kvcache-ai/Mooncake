package main

/*
#include <stdint.h>
*/
import "C"

import (
	"context"
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
	// etcd client for store
	storeClient  *clientv3.Client
	storeMutex   sync.Mutex
)

//export NewEtcdClient
func NewEtcdClient(endpoints *C.char, errMsg **C.char) int {
	globalMutex.Lock()
	defer globalMutex.Unlock()
	if globalClient != nil {
		*errMsg = C.CString("etcd client can be initialized only once")
		return -1
	}

	endpoint := C.GoString(endpoints)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	globalClient = cli
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
		globalClient.Close()
		globalClient = nil
	}
}

//export NewStoreEtcdClient
func NewStoreEtcdClient(endpoints *C.char, errMsg **C.char) int {
	storeMutex.Lock()
	defer storeMutex.Unlock()
	if storeClient != nil {
		// return 0 since the goal is achieved
		return 0
	}

	endpoint := C.GoString(endpoints)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
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
func EtcdStoreGetWrapper(key *C.char, value **C.char, errMsg **C.char) int {
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoString(key)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := storeClient.Get(ctx, k)
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

//export EtcdStoreGrantLeaseWrapper
func EtcdStoreGrantLeaseWrapper(ttl int64, leaseID *int64, errMsg **C.char) int {
	// TODO: make timeout a configurable parameter
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
	*leaseID = int64(resp.ID)
	return 0
}

//export EtcdStoreCreateWithLeaseWrapper
func EtcdStoreCreateWithLeaseWrapper(key *C.char, value *C.char, leaseID int64, tx_success *int, errMsg **C.char) int {
    if storeClient == nil {
        *errMsg = C.CString("etcd client not initialized")
        return -1
    }
    k := C.GoString(key)
    v := C.GoString(value)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Create a transaction
    txn := storeClient.Txn(ctx)

    // First check if the key exists
    resp, err := txn.If(clientv3.Compare(clientv3.CreateRevision(k), "=", 0)).
        Then(clientv3.OpPut(k, v, clientv3.WithLease(clientv3.LeaseID(leaseID)))).
        Commit()

    if err != nil {
        *errMsg = C.CString(err.Error())
        return -1
    }

    // If the key already existed, resp.Succeeded will be false
    // If we created the key, resp.Succeeded will be true
	if resp.Succeeded {
		*tx_success = 1
	} else {
		*tx_success = 0
	}
    return 0
}

//export EtcdStoreWatchUntilDeletedWrapper
func EtcdStoreWatchUntilDeletedWrapper(key *C.char, errMsg **C.char) int {
	if storeClient == nil {
		*errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoString(key)
	ctx := context.Background()
	
	// Start watching the key
	watchChan := storeClient.Watch(ctx, k)
	
	// Wait for the key to be deleted
	for watchResp := range watchChan {
		for _, event := range watchResp.Events {
			if event.Type == clientv3.EventTypeDelete {
				return 0
			}
		}
	}

	*errMsg = C.CString("watch channel closed unexpectedly")
	return -1
}

//export EtcdStoreKeepAliveWrapper
func EtcdStoreKeepAliveWrapper(leaseID int64, errMsg **C.char) int {
    if storeClient == nil {
        *errMsg = C.CString("etcd client not initialized")
        return -1
    }

    // Create a context without timeout since we want to keep alive indefinitely
    ctx := context.Background()
    
    // Start keep alive
    keepAliveChan, err := storeClient.KeepAlive(ctx, clientv3.LeaseID(leaseID))
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
            *errMsg = C.CString("context cancelled")
            return -1
        }
    }
}

func main() {}
