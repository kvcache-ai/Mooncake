package main

/*
#include <stdint.h> 
*/
import "C"

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"unsafe"
)

var globalClient *CClient

type CClient struct {
	client *clientv3.Client
}

//export NewEtcdClient
func NewEtcdClient(endpoints *C.char) C.uintptr_t {
	if globalClient != nil {
		return C.uintptr_t(uintptr(unsafe.Pointer(globalClient)))
	}

	endpoint := C.GoString(endpoints)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return 0
	}

	globalClient = &CClient{client: cli}
	return C.uintptr_t(uintptr(unsafe.Pointer(globalClient)))
}

//export EtcdPutWrapper
func EtcdPutWrapper(clientPtr C.uintptr_t, key *C.char, value *C.char) int {
	if clientPtr == 0 {
		return -1
	}
	client := *(**CClient)(unsafe.Pointer(&clientPtr))
	k := C.GoString(key)
	v := C.GoString(value)
	_, err := client.client.Put(context.Background(), k, v)
	if err != nil {
		return -1
	}
	return 0
}

//export EtcdGetWrapper
func EtcdGetWrapper(clientPtr C.uintptr_t, key *C.char, value **C.char) int {
	if clientPtr == 0 {
		return -1
	}
	client := *(**CClient)(unsafe.Pointer(&clientPtr))
	k := C.GoString(key)
	resp, err := client.client.Get(context.Background(), k)
	if err != nil {
		return -1
	}
	if len(resp.Kvs) == 0 {
		return -2
	}
	kv := resp.Kvs[0]
	*value = C.CString(string(kv.Value))
	return 0
}

//export EtcdDeleteWrapper
func EtcdDeleteWrapper(clientPtr C.uintptr_t, key *C.char) int {
	if clientPtr == 0 {
		return -1
	}
	client := *(**CClient)(unsafe.Pointer(&clientPtr))
	k := C.GoString(key)
	_, err := client.client.Delete(context.Background(), k)
	if err != nil {
		return -1
	}
	return 0
}

//export EtcdCloseWrapper
func EtcdCloseWrapper(clientPtr C.uintptr_t) {
	if clientPtr == 0 {
		return
	}
	client := *(**CClient)(unsafe.Pointer(&clientPtr))
	if client == globalClient {
		client.client.Close()
		globalClient = nil
	}
}

func main() {}