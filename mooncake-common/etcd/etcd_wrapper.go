package main

/*
#include <stdint.h> 
*/
import "C"

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"unsafe"
)

type CClient struct {
	client *clientv3.Client
}

//export NewEtcdClient
func NewEtcdClient(endpoints *C.char) C.uintptr_t {
	endpoint := C.GoString(endpoints)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to etcd: %v", err))
	}
	client := &CClient{client: cli}
	fmt.Println("connect to etcd server", uintptr(unsafe.Pointer(client)), client)
	return C.uintptr_t(uintptr(unsafe.Pointer(client)))
}

//export Put
func Put(clientPtr C.uintptr_t, key *C.char, value *C.char) int {
	client := *(**CClient)(unsafe.Pointer(&clientPtr))
	k := C.GoString(key)
	v := C.GoString(value)
	_, err := client.client.Put(context.Background(), k, v)
	if err != nil {
		return -1
	}
	return 0
}

//export Get
func Get(clientPtr C.uintptr_t, key *C.char, value **C.char) int {
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

//export Delete
func Delete(clientPtr C.uintptr_t, key *C.char) int {
	client := *(**CClient)(unsafe.Pointer(&clientPtr))
	k := C.GoString(key)
	_, err := client.client.Delete(context.Background(), k)
	if err != nil {
		return -1
	}
	return 0
}

//export Close
func Close(clientPtr C.uintptr_t) {
	client := *(**CClient)(unsafe.Pointer(&clientPtr))
	client.client.Close()
}

func main() {}