//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var (
	testEnv    *envtest.Environment
	testConfig *rest.Config
)

type electionStateNoRelease struct {
	cancel  context.CancelFunc
	elected chan struct{}
	lost    chan struct{}
}

func TestMain(m *testing.M) {
	testEnv = &envtest.Environment{}

	var err error
	testConfig, err = testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start envtest: %v\n", err)
		os.Exit(1)
	}

	// Set up global client for the wrapper
	client, err := kubernetes.NewForConfig(testConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create clientset: %v\n", err)
		testEnv.Stop()
		os.Exit(1)
	}
	clientMutex.Lock()
	globalClient = client
	clientMutex.Unlock()

	code := m.Run()

	testEnv.Stop()
	os.Exit(code)
}

func runElectionWithoutRelease(namespace, leaseName, identity string,
	leaseDurationSec, renewDeadlineSec, retryPeriodSec int) (*electionStateNoRelease, error) {
	if err := ensureClientInitialized(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	state := &electionStateNoRelease{
		cancel:  cancel,
		elected: make(chan struct{}),
		lost:    make(chan struct{}),
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Client: globalClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   time.Duration(leaseDurationSec) * time.Second,
		RenewDeadline:   time.Duration(renewDeadlineSec) * time.Second,
		RetryPeriod:     time.Duration(retryPeriodSec) * time.Second,
		ReleaseOnCancel: false,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				close(state.elected)
				<-ctx.Done()
			},
			OnStoppedLeading: func() {
				close(state.lost)
			},
		},
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create leader elector: %w", err)
	}

	go le.Run(ctx)
	return state, nil
}

// TestSingleLeaderElection verifies a single candidate becomes leader.
func TestSingleLeaderElection(t *testing.T) {
	ns := "default"
	lease := "single-election-test"
	identity := "node-1:8080"

	err := runElection(ns, lease, identity, 5, 4, 1)
	if err != nil {
		t.Fatalf("runElection failed: %v", err)
	}

	// Wait for elected
	key := electionKey(ns, lease)
	electionMutex.Lock()
	state := elections[key]
	electionMutex.Unlock()

	select {
	case <-state.elected:
		// success
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	// Verify holder via getHolder
	holder, transitions, err := getHolder(ns, lease)
	if err != nil {
		t.Fatalf("getHolder failed: %v", err)
	}
	if holder != identity {
		t.Errorf("expected holder %q, got %q", identity, holder)
	}
	// First election — transitions should be 0 or 1
	if transitions < 0 {
		t.Errorf("expected non-negative transitions, got %d", transitions)
	}

	// Cancel the election
	electionMutex.Lock()
	state = elections[key]
	electionMutex.Unlock()
	state.cancel()

	select {
	case <-state.lost:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for election loss after cancel")
	}
}

// TestLeaderEpoch verifies leaseTransitions increments across elections.
func TestLeaderEpoch(t *testing.T) {
	ns := "default"
	lease := "epoch-test"

	// First election
	err := runElection(ns, lease, "node-epoch-1:8080", 5, 4, 1)
	if err != nil {
		t.Fatalf("first runElection failed: %v", err)
	}

	key := electionKey(ns, lease)
	electionMutex.Lock()
	state1 := elections[key]
	electionMutex.Unlock()

	select {
	case <-state1.elected:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out on first election")
	}

	_, trans1, _ := getHolder(ns, lease)

	// Cancel first election and wait for loss
	state1.cancel()
	select {
	case <-state1.lost:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for first election loss")
	}

	// Wait for lease to expire / be released
	time.Sleep(2 * time.Second)

	// Second election
	err = runElection(ns, lease, "node-epoch-2:8080", 5, 4, 1)
	if err != nil {
		t.Fatalf("second runElection failed: %v", err)
	}

	electionMutex.Lock()
	state2 := elections[key]
	electionMutex.Unlock()

	select {
	case <-state2.elected:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out on second election")
	}

	_, trans2, _ := getHolder(ns, lease)
	if trans2 <= trans1 {
		t.Errorf("expected transitions to increment: first=%d, second=%d", trans1, trans2)
	}

	state2.cancel()
	select {
	case <-state2.lost:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for second election loss")
	}
}

// TestSequentialLeadershipHandoff tests that a second candidate can acquire
// leadership after the first one releases it.
func TestSequentialLeadershipHandoff(t *testing.T) {
	ns := "default"
	lease := "two-candidate-test"

	err1 := runElection(ns, lease, "candidate-a:8080", 5, 4, 1)
	if err1 != nil {
		t.Fatalf("first runElection failed: %v", err1)
	}

	key := electionKey(ns, lease)
	electionMutex.Lock()
	stateA := elections[key]
	electionMutex.Unlock()

	// Wait for first candidate to win
	select {
	case <-stateA.elected:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for first candidate")
	}

	// Verify holder is candidate-a
	holder, _, err := getHolder(ns, lease)
	if err != nil {
		t.Fatalf("getHolder failed: %v", err)
	}
	if holder != "candidate-a:8080" {
		t.Errorf("expected candidate-a, got %q", holder)
	}

	// Cancel candidate-a
	stateA.cancel()
	select {
	case <-stateA.lost:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for candidate-a loss")
	}

	// Wait for lease to expire
	time.Sleep(2 * time.Second)

	// Start candidate-b
	err2 := runElection(ns, lease, "candidate-b:8080", 5, 4, 1)
	if err2 != nil {
		t.Fatalf("second runElection failed: %v", err2)
	}

	electionMutex.Lock()
	stateB := elections[key]
	electionMutex.Unlock()

	select {
	case <-stateB.elected:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for candidate-b")
	}

	holder, _, err = getHolder(ns, lease)
	if err != nil {
		t.Fatalf("getHolder after takeover failed: %v", err)
	}
	if holder != "candidate-b:8080" {
		t.Errorf("expected candidate-b, got %q", holder)
	}

	stateB.cancel()
	select {
	case <-stateB.lost:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for candidate-b loss")
	}
}

// TestConcurrentCandidateElection starts two candidates simultaneously and
// verifies that exactly one wins leadership.
func TestConcurrentCandidateElection(t *testing.T) {
	ns := "default"
	lease := "concurrent-election-test"

	type result struct {
		identity string
		elected  bool
	}

	candidates := []string{"candidate-a:8080", "candidate-b:8080"}
	results := make(chan result, len(candidates))

	lock := func(identity string) *resourcelock.LeaseLock {
		return &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      lease,
				Namespace: ns,
			},
			Client: globalClient.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: identity,
			},
		}
	}

	var wg sync.WaitGroup
	for _, id := range candidates {
		wg.Add(1)
		go func(identity string) {
			defer wg.Done()

			// Short timeout: enough for one to acquire, but the loser
			// times out before the winner's lease could expire.
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()

			elected := make(chan struct{})
			le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
				Lock:            lock(identity),
				LeaseDuration:   5 * time.Second,
				RenewDeadline:   3 * time.Second,
				RetryPeriod:     1 * time.Second,
				ReleaseOnCancel: true,
				Callbacks: leaderelection.LeaderCallbacks{
					OnStartedLeading: func(ctx context.Context) {
						close(elected)
						<-ctx.Done()
					},
					OnStoppedLeading: func() {},
				},
			})
			if err != nil {
				t.Errorf("NewLeaderElector(%s): %v", identity, err)
				return
			}

			go le.Run(ctx)

			select {
			case <-elected:
				results <- result{identity, true}
				// Keep holding until context expires (8s total).
				// Winner does NOT release early, so loser cannot
				// re-acquire within its own 8s window.
				<-ctx.Done()
			case <-ctx.Done():
				results <- result{identity, false}
			}
		}(id)
	}

	wg.Wait()
	close(results)

	winners := 0
	for r := range results {
		if r.elected {
			winners++
			t.Logf("winner: %s", r.identity)
		}
	}

	if winners != 1 {
		t.Fatalf("expected exactly 1 winner, got %d", winners)
	}
}

// TestCancelElection tests that cancelling an election makes WaitLost return.
func TestCancelElection(t *testing.T) {
	ns := "default"
	lease := "cancel-test"

	err := runElection(ns, lease, "cancel-node:8080", 5, 4, 1)
	if err != nil {
		t.Fatalf("runElection failed: %v", err)
	}

	key := electionKey(ns, lease)
	electionMutex.Lock()
	state := elections[key]
	electionMutex.Unlock()

	// Wait for elected
	select {
	case <-state.elected:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	// Cancel
	state.cancel()

	// WaitLost should return promptly
	select {
	case <-state.lost:
		// success
	case <-time.After(10 * time.Second):
		t.Fatal("WaitLost did not return after cancel")
	}
}

// TestGetHolderDuringElection verifies getHolder works while election is active.
func TestGetHolderDuringElection(t *testing.T) {
	ns := "default"
	lease := "active-get-holder-test"
	identity := "active-node:8080"

	err := runElection(ns, lease, identity, 5, 4, 1)
	if err != nil {
		t.Fatalf("runElection failed: %v", err)
	}

	key := electionKey(ns, lease)
	electionMutex.Lock()
	state := elections[key]
	electionMutex.Unlock()

	select {
	case <-state.elected:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	// Concurrent getHolder calls during active election
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			holder, _, err := getHolder(ns, lease)
			if err != nil {
				t.Errorf("getHolder during election failed: %v", err)
				return
			}
			if holder != identity {
				t.Errorf("expected %q, got %q", identity, holder)
			}
		}()
	}
	wg.Wait()

	state.cancel()
	<-state.lost
}

// TestGetHolderReturnsEmptyAfterLeaderDeath verifies that after a leader stops
// renewing its lease without releasing it, getHolder returns an empty holder
// once the lease expires. This is the integration-level counterpart to the
// unit test TestGetHolderReturnsEmptyForExpiredLease.
func TestGetHolderReturnsEmptyAfterLeaderDeath(t *testing.T) {
	ns := "default"
	lease := "expired-leader-test"
	identity := "doomed-leader:8080"

	// Acquire leadership without ReleaseOnCancel so canceling simulates a dead
	// leader that stops renewing and leaves the old holder until expiry.
	state, err := runElectionWithoutRelease(ns, lease, identity, 5, 4, 1)
	if err != nil {
		t.Fatalf("runElection failed: %v", err)
	}

	select {
	case <-state.elected:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for election")
	}

	// Verify holder while active.
	holder, _, err := getHolder(ns, lease)
	if err != nil {
		t.Fatalf("getHolder (active) failed: %v", err)
	}
	if holder != identity {
		t.Fatalf("expected active holder %q, got %q", identity, holder)
	}

	// Simulate leader death: stop renewing without explicitly releasing.
	state.cancel()
	select {
	case <-state.lost:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for loss")
	}

	// Wait for the lease to expire (leaseDuration=5s, add margin).
	time.Sleep(7 * time.Second)

	// After expiry, getHolder must return empty holder so that the
	// supervisor will attempt acquisition.
	holder, _, err = getHolder(ns, lease)
	if err != nil {
		t.Fatalf("getHolder (expired) failed: %v", err)
	}
	if holder != "" {
		t.Errorf("expected empty holder after lease expiry, got %q", holder)
	}
}

// TestFailoverAfterLeaderDeath verifies that a new candidate can acquire
// leadership after the previous leader dies and its lease expires.
func TestFailoverAfterLeaderDeath(t *testing.T) {
	ns := "default"
	lease := "failover-test"

	// First leader acquires without ReleaseOnCancel so canceling leaves the
	// old holder in place until the lease naturally expires.
	state1, err := runElectionWithoutRelease(ns, lease, "leader-1:8080", 5, 4, 1)
	if err != nil {
		t.Fatalf("first runElection failed: %v", err)
	}

	select {
	case <-state1.elected:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for first election")
	}

	// Simulate crash: cancel without release, wait for expiry.
	state1.cancel()
	<-state1.lost
	time.Sleep(7 * time.Second)

	// Second candidate should be able to acquire.
	err = runElection(ns, lease, "leader-2:8080", 5, 4, 1)
	if err != nil {
		t.Fatalf("second runElection failed: %v", err)
	}

	key := electionKey(ns, lease)
	electionMutex.Lock()
	state2 := elections[key]
	electionMutex.Unlock()

	select {
	case <-state2.elected:
		// success — failover worked
	case <-time.After(15 * time.Second):
		t.Fatal("second candidate failed to acquire after leader death")
	}

	holder, _, err := getHolder(ns, lease)
	if err != nil {
		t.Fatalf("getHolder after failover failed: %v", err)
	}
	if holder != "leader-2:8080" {
		t.Errorf("expected new leader %q, got %q", "leader-2:8080", holder)
	}

	state2.cancel()
	<-state2.lost
}
