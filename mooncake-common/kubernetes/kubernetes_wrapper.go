package main

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"context"
	"fmt"
	"log"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/tools/leaderelection"
)

const (
	leaseDuration = 5 * time.Second
	renewDeadline = 3 * time.Second
	retryPeriod   = 2 * time.Second
)

// Use different etcd client so they are not affected by each other,
// and can be configured separately.
var (
	// leader elector for store
	storeLeaderElector                              *leaderelection.LeaderElector
	onStartedLeadingChan                            = make(chan struct{})
	onStoppedLeadingChan                            = make(chan struct{})
	storeLeaderElectorCtx, storeLeaderElectorCancel = context.WithCancel(context.Background())
)

//export InitLeaderElectorGo
func InitLeaderElectorGo(leaseName *C.char, identity *C.char, errMsg **C.char) int {
	cfg := GetConfigOrDie()
	resourceLock, err := NewResourceLock(cfg, C.GoString(leaseName), C.GoString(identity))
	if err != nil {
		*errMsg = C.CString("New ResourceLock Failed,Error: " + err.Error())
		return -1
	}

	leaderElector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          resourceLock,
		LeaseDuration: leaseDuration,
		RenewDeadline: renewDeadline,
		RetryPeriod:   retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(_ context.Context) {
				onStartedLeadingChan <- struct{}{}
			},
			OnStoppedLeading: func() {
				onStoppedLeadingChan <- struct{}{}
			},
			OnNewLeader: func(identity string) {
			},
		},
		ReleaseOnCancel: true,
		Name:            C.GoString(leaseName),
	})
	if err != nil {
		*errMsg = C.CString("New LeaderElector Failed,Error: " + err.Error())
		return -1
	}
	storeLeaderElector = leaderElector
	return 0
}

//export ElectLeaderGo
func ElectLeaderGo(errMsg **C.char) int {
	if storeLeaderElector == nil {
		*errMsg = C.CString("LeaderElector not initialized")
		return -1
	}
	go func() {
		storeLeaderElector.Run(storeLeaderElectorCtx)
	}()
	<-onStartedLeadingChan
	return 0
}

//export KeepAliveGo
func KeepAliveGo() int {
	<-onStoppedLeadingChan
	return 0
}

//export StopLeaderElectorGo
func StopLeaderElectorGo(errMsg **C.char) int {
	if storeLeaderElector == nil {
		*errMsg = C.CString("LeaderElector not initialized")
		return -1
	}
	storeLeaderElectorCancel()
	return 0
}

//export GetMasterAddressGo
func GetMasterAddressGo(leaseNamespace *C.char, leaseName *C.char, address **C.char, addressSize *C.int, errMsg **C.char) int {
	leaseNameString := C.GoString(leaseName)
	cfg := GetConfigOrDie()
	coordinationClient, err := coordinationv1client.NewForConfig(cfg)
	if err != nil {
		*errMsg = C.CString("Failed to create coordinationClient,error: " + err.Error())
		return -1
	}
	namespace := C.GoString(leaseNamespace)
	if len(namespace) == 0 {
		namespace, err = GetInClusterNamespace()
		if err != nil {
			*errMsg = C.CString("Failed to get namespace,error: " + err.Error())
			return -1
		}
	}
	log.Printf("Try to get lease %s/%s", namespace, leaseNameString)
	lease, err := coordinationClient.Leases(namespace).Get(context.Background(), leaseNameString, metav1.GetOptions{})
	if err != nil {
		*errMsg = C.CString(fmt.Sprintf("Failed to get lease %s/%s,error: %s", namespace, leaseNameString, err))
		return -1
	}
	if lease.Spec.HolderIdentity == nil {
	    *errMsg = C.CString(fmt.Sprintf("Lease %s/%s has no holder identity", namespace, leaseNameString))
		return -1
	}
	*address = C.CString(*lease.Spec.HolderIdentity)
	*addressSize = C.int(len(*lease.Spec.HolderIdentity))
	return 0
}

func main() {}
