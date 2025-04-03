package main

/*
#include <stdint.h> 
*/
import "C"

import (
	"context"
	"time"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	globalClient *clientv3.Client
	mutex        sync.Mutex
)

//export NewEtcdClient
func NewEtcdClient(endpoints *C.char, errMsg *C.char) int {
	mutex.Lock()
	defer mutex.Unlock()
	if globalClient != nil {
		errMsg = C.CString("etcd client can be initialized only once")
		return -1
	}

	endpoint := C.GoString(endpoints)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		errMsg = C.CString(err.Error())
		return -1
	}

	globalClient = cli
	return 0
}

//export EtcdPutWrapper
func EtcdPutWrapper(key *C.char, value *C.char, errMsg *C.char) int {
	if globalClient == nil {
		errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoString(key)
	v := C.GoString(value)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
	_, err := globalClient.Put(ctx, k, v)
	if err != nil {
		errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export EtcdGetWrapper
func EtcdGetWrapper(key *C.char, value **C.char, errMsg *C.char) int {
	if globalClient == nil {
		errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoString(key)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
	resp, err := globalClient.Get(ctx, k)
	if err != nil {
		errMsg = C.CString(err.Error())
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
func EtcdDeleteWrapper(key *C.char, errMsg *C.char) int {
	if globalClient == nil {
		errMsg = C.CString("etcd client not initialized")
		return -1
	}
	k := C.GoString(key)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
	_, err := globalClient.Delete(ctx, k)
	if err != nil {
		errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export EtcdCloseWrapper
func EtcdCloseWrapper() {
	mutex.Lock()
	defer mutex.Unlock()
	if globalClient != nil {
		globalClient.Close()
		globalClient = nil
	}
}

func main() {}