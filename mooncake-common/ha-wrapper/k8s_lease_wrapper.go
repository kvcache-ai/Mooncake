package main

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Trampoline to invoke C/C++ callback safely from Go via cgo.
#ifndef MOONCAKE_K8S_CALLBACK_TRAMPOLINES
#define MOONCAKE_K8S_CALLBACK_TRAMPOLINES

typedef void (*holder_change_cb_t)(void* ctx,
                                    const char* holder, size_t holderSize,
                                    int64_t leaseTransitions);

static inline void call_holder_change_cb(holder_change_cb_t func, void* ctx,
                                          const char* holder, size_t holderSize,
                                          int64_t leaseTransitions) {
  func(ctx, holder, holderSize, leaseTransitions);
}

#endif  // MOONCAKE_K8S_CALLBACK_TRAMPOLINES
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// electionState holds the runtime state for a single leader election.
type electionState struct {
	cancel      context.CancelFunc
	elected     chan struct{} // closed when OnStartedLeading fires
	lost        chan struct{} // closed when OnStoppedLeading fires
	err         error         // set before lost is closed, if any
	transitions int64         // set before elected is closed
}

// watchState holds the runtime state for a single Lease watch.
type watchState struct {
	cancel context.CancelFunc
}

var (
	k8sGlobalClient kubernetes.Interface
	k8sClientMutex  sync.Mutex
	k8sInitClientFn = initK8sClient

	k8sElections     = make(map[string]*electionState)
	k8sElectionMutex sync.Mutex

	k8sWatches    = make(map[string]*watchState)
	k8sWatchMutex sync.Mutex
)

func electionKey(namespace, leaseName string) string {
	return namespace + "/" + leaseName
}

func ensureK8sClientInitialized() error {
	k8sClientMutex.Lock()
	initialized := k8sGlobalClient != nil
	k8sClientMutex.Unlock()
	if initialized {
		return nil
	}
	return k8sInitClientFn()
}

// initK8sClient creates the K8s clientset from in-cluster config or KUBECONFIG.
func initK8sClient() error {
	k8sClientMutex.Lock()
	defer k8sClientMutex.Unlock()
	if k8sGlobalClient != nil {
		return nil
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		// Fall back to KUBECONFIG
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			home := os.Getenv("HOME")
			if home != "" {
				kubeconfig = home + "/.kube/config"
			}
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return fmt.Errorf("failed to build k8s config: %w", err)
		}
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create k8s clientset: %w", err)
	}
	k8sGlobalClient = client
	return nil
}

// runElection starts a leader election goroutine for the given namespace/leaseName.
func runElection(namespace, leaseName, identity string,
	leaseDurationSec, renewDeadlineSec, retryPeriodSec int) error {
	if err := ensureK8sClientInitialized(); err != nil {
		return err
	}

	key := electionKey(namespace, leaseName)

	k8sElectionMutex.Lock()
	if _, exists := k8sElections[key]; exists {
		k8sElectionMutex.Unlock()
		return fmt.Errorf("election already running for %s", key)
	}

	ctx, cancel := context.WithCancel(context.Background())
	state := &electionState{
		cancel:  cancel,
		elected: make(chan struct{}),
		lost:    make(chan struct{}),
	}
	k8sElections[key] = state
	k8sElectionMutex.Unlock()

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Client: k8sGlobalClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   time.Duration(leaseDurationSec) * time.Second,
		RenewDeadline:   time.Duration(renewDeadlineSec) * time.Second,
		RetryPeriod:     time.Duration(retryPeriodSec) * time.Second,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				_, transitions, err := getHolder(namespace, leaseName)
				if err == nil {
					state.transitions = transitions
				}
				close(state.elected)
				// Block until context is cancelled (leadership lost or explicit cancel)
				<-ctx.Done()
			},
			OnStoppedLeading: func() {
				close(state.lost)
				// Auto-cleanup: remove from map so the same key can be reused.
				k8sElectionMutex.Lock()
				if k8sElections[key] == state {
					delete(k8sElections, key)
				}
				k8sElectionMutex.Unlock()
			},
		},
	})
	if err != nil {
		k8sElectionMutex.Lock()
		delete(k8sElections, key)
		k8sElectionMutex.Unlock()
		cancel()
		return fmt.Errorf("failed to create leader elector: %w", err)
	}

	go le.Run(ctx)
	return nil
}

// getHolder reads the current Lease holder identity and transitions.
func getHolder(namespace, leaseName string) (string, int64, error) {
	if err := ensureK8sClientInitialized(); err != nil {
		return "", 0, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lease, err := k8sGlobalClient.CoordinationV1().Leases(namespace).Get(ctx, leaseName, metav1.GetOptions{})
	if err != nil {
		return "", 0, fmt.Errorf("failed to get lease: %w", err)
	}

	holder := ""
	if lease.Spec.HolderIdentity != nil {
		holder = *lease.Spec.HolderIdentity
	}
	transitions := int64(0)
	if lease.Spec.LeaseTransitions != nil {
		transitions = int64(*lease.Spec.LeaseTransitions)
	}

	// Treat expired leases as having no holder so that the C++ supervisor
	// will attempt acquisition instead of going to standby.
	if holder != "" && lease.Spec.RenewTime != nil && lease.Spec.LeaseDurationSeconds != nil {
		expiry := lease.Spec.RenewTime.Time.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
		if time.Now().After(expiry) {
			holder = ""
		}
	}
	return holder, transitions, nil
}

//export K8sLeaseInit
func K8sLeaseInit(errMsg **C.char) C.int {
	if err := ensureK8sClientInitialized(); err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export K8sLeaseRunElection
func K8sLeaseRunElection(
	ns, leaseName, identity *C.char,
	leaseDurationSec, renewDeadlineSec, retryPeriodSec C.int,
	errMsg **C.char,
) C.int {
	nsStr := C.GoString(ns)
	ln := C.GoString(leaseName)
	id := C.GoString(identity)

	err := runElection(nsStr, ln, id,
		int(leaseDurationSec), int(renewDeadlineSec), int(retryPeriodSec))
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export K8sLeaseWaitElected
func K8sLeaseWaitElected(
	ns, leaseName *C.char,
	timeoutSec C.int,
	leaseTransitions *C.longlong,
	errMsg **C.char,
) C.int {
	key := electionKey(C.GoString(ns), C.GoString(leaseName))

	k8sElectionMutex.Lock()
	state, exists := k8sElections[key]
	k8sElectionMutex.Unlock()

	if !exists {
		*errMsg = C.CString("no election running for " + key)
		return -1
	}

	timeout := time.Duration(timeoutSec) * time.Second

	// Wait for elected, lost, or timeout
	select {
	case <-state.elected:
		*leaseTransitions = C.longlong(state.transitions)
		return 0
	case <-state.lost:
		*errMsg = C.CString("election lost before becoming leader")
		return -1
	case <-time.After(timeout):
		state.cancel()
		<-state.lost
		*errMsg = C.CString("election timed out after " + fmt.Sprintf("%d", int(timeoutSec)) + "s")
		return -1
	}
}

//export K8sLeaseWaitLost
func K8sLeaseWaitLost(
	ns, leaseName *C.char,
	errMsg **C.char,
) C.int {
	key := electionKey(C.GoString(ns), C.GoString(leaseName))

	k8sElectionMutex.Lock()
	state, exists := k8sElections[key]
	k8sElectionMutex.Unlock()

	if !exists {
		// Already cleaned up by OnStoppedLeading — election is over.
		return 0
	}

	<-state.lost

	if state.err != nil {
		*errMsg = C.CString(state.err.Error())
		return -1
	}
	return 0
}

//export K8sLeaseCancelElection
func K8sLeaseCancelElection(
	ns, leaseName *C.char,
	errMsg **C.char,
) C.int {
	key := electionKey(C.GoString(ns), C.GoString(leaseName))

	k8sElectionMutex.Lock()
	state, exists := k8sElections[key]
	k8sElectionMutex.Unlock()

	if !exists {
		// Idempotent — no error if no election
		return 0
	}

	state.cancel()
	return 0
}

//export K8sLeaseGetHolder
func K8sLeaseGetHolder(
	ns, leaseName *C.char,
	holderIdentity **C.char,
	leaseTransitions *C.longlong,
	errMsg **C.char,
) C.int {
	nsStr := C.GoString(ns)
	ln := C.GoString(leaseName)

	holder, transitions, err := getHolder(nsStr, ln)
	if err != nil {
		if apierrors.IsNotFound(err) {
			*holderIdentity = nil
			*leaseTransitions = 0
			return 1
		}
		errStr := err.Error()
		*errMsg = C.CString(errStr)
		return -1
	}

	if holder == "" {
		*holderIdentity = nil
	} else {
		*holderIdentity = C.CString(holder)
	}
	*leaseTransitions = C.longlong(transitions)
	return 0
}

//export K8sLeaseWatchHolder
func K8sLeaseWatchHolder(
	ns, leaseName *C.char,
	callbackCtx unsafe.Pointer,
	callbackFunc C.holder_change_cb_t,
	errMsg **C.char,
) C.int {
	nsStr := C.GoString(ns)
	ln := C.GoString(leaseName)
	key := electionKey(nsStr, ln)

	if callbackFunc == nil {
		*errMsg = C.CString("callback function is nil")
		return -1
	}
	if err := ensureK8sClientInitialized(); err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}

	k8sWatchMutex.Lock()
	if _, exists := k8sWatches[key]; exists {
		k8sWatchMutex.Unlock()
		*errMsg = C.CString("watch already running for " + key)
		return -1
	}

	ctx, cancel := context.WithCancel(context.Background())
	k8sWatches[key] = &watchState{cancel: cancel}
	k8sWatchMutex.Unlock()

	go func() {
		defer func() {
			k8sWatchMutex.Lock()
			delete(k8sWatches, key)
			k8sWatchMutex.Unlock()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			watcher, err := k8sGlobalClient.CoordinationV1().Leases(nsStr).Watch(ctx, metav1.ListOptions{
				FieldSelector: "metadata.name=" + ln,
			})
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					time.Sleep(time.Second)
					continue
				}
			}

			for event := range watcher.ResultChan() {
				select {
				case <-ctx.Done():
					watcher.Stop()
					return
				default:
				}

				if event.Type == watch.Modified || event.Type == watch.Added {
					lease, ok := event.Object.(*coordinationv1.Lease)
					if !ok {
						continue
					}
					holder := ""
					if lease.Spec.HolderIdentity != nil {
						holder = *lease.Spec.HolderIdentity
					}
					transitions := int64(0)
					if lease.Spec.LeaseTransitions != nil {
						transitions = int64(*lease.Spec.LeaseTransitions)
					}

					var holderPtr *C.char
					var holderSize C.size_t
					if holder != "" {
						holderPtr = C.CString(holder)
						holderSize = C.size_t(len(holder))
					}

					C.call_holder_change_cb(callbackFunc, callbackCtx,
						holderPtr, holderSize, C.int64_t(transitions))

					if holderPtr != nil {
						C.free(unsafe.Pointer(holderPtr))
					}
				}
			}

			// Watch channel closed — retry unless cancelled
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(time.Second)
			}
		}
	}()

	return 0
}

//export K8sLeaseCancelWatch
func K8sLeaseCancelWatch(
	ns, leaseName *C.char,
	errMsg **C.char,
) C.int {
	key := electionKey(C.GoString(ns), C.GoString(leaseName))

	k8sWatchMutex.Lock()
	state, exists := k8sWatches[key]
	k8sWatchMutex.Unlock()

	if !exists {
		// Idempotent
		return 0
	}

	state.cancel()
	return 0
}

// patchPodLabel sets or removes a label on a pod using a JSON merge patch.
// If value is non-nil, the label is set; if nil, the label is removed.
func patchPodLabel(namespace, podName, labelKey string, value interface{}) error {
	if err := ensureK8sClientInitialized(); err != nil {
		return err
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]interface{}{
				labelKey: value,
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("failed to marshal label patch: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = k8sGlobalClient.CoreV1().Pods(namespace).Patch(
		ctx, podName, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to patch pod label: %w", err)
	}
	return nil
}

//export K8sPatchPodLabel
func K8sPatchPodLabel(
	ns, podName, labelKey, labelValue *C.char,
	errMsg **C.char,
) C.int {
	err := patchPodLabel(C.GoString(ns), C.GoString(podName),
		C.GoString(labelKey), C.GoString(labelValue))
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}

//export K8sRemovePodLabel
func K8sRemovePodLabel(
	ns, podName, labelKey *C.char,
	errMsg **C.char,
) C.int {
	err := patchPodLabel(C.GoString(ns), C.GoString(podName),
		C.GoString(labelKey), nil)
	if err != nil {
		*errMsg = C.CString(err.Error())
		return -1
	}
	return 0
}
