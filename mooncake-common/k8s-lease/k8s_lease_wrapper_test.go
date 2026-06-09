package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
)

// swapClient replaces globalClient and returns the old one.
func swapClient(newClient kubernetes.Interface) kubernetes.Interface {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	old := globalClient
	globalClient = newClient
	return old
}

// TestGetHolderWithFakeClient tests getHolder using a fake K8s clientset.
func TestGetHolderWithFakeClient(t *testing.T) {
	holderID := "node-1:8080"
	transitions := int32(3)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-lease",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:   &holderID,
			LeaseTransitions: &transitions,
		},
	}

	fakeClient := fake.NewSimpleClientset(lease)
	old := swapClient(fakeClient)
	defer swapClient(old)

	holder, trans, err := getHolder("default", "test-lease")
	if err != nil {
		t.Fatalf("getHolder failed: %v", err)
	}
	if holder != holderID {
		t.Errorf("expected holder %q, got %q", holderID, holder)
	}
	if trans != int64(transitions) {
		t.Errorf("expected transitions %d, got %d", transitions, trans)
	}
}

// TestGetHolderNotFound tests getHolder when the Lease does not exist.
func TestGetHolderNotFound(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	old := swapClient(fakeClient)
	defer swapClient(old)

	_, _, err := getHolder("default", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent lease, got nil")
	}
}

// TestGetHolderEmptyIdentity tests getHolder when holder is nil.
func TestGetHolderEmptyIdentity(t *testing.T) {
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "empty-lease",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{},
	}
	fakeClient := fake.NewSimpleClientset(lease)
	old := swapClient(fakeClient)
	defer swapClient(old)

	holder, trans, err := getHolder("default", "empty-lease")
	if err != nil {
		t.Fatalf("getHolder failed: %v", err)
	}
	if holder != "" {
		t.Errorf("expected empty holder, got %q", holder)
	}
	if trans != 0 {
		t.Errorf("expected 0 transitions, got %d", trans)
	}
}

// TestGetHolderReturnsEmptyForExpiredLease verifies that getHolder treats a
// lease whose renewTime + leaseDuration is in the past as having no holder.
// This is critical for failover: when a leader pod dies without releasing the
// lease, standbys must see an empty holder so the supervisor attempts
// acquisition instead of looping in standby.
func TestGetHolderReturnsEmptyForExpiredLease(t *testing.T) {
	holderID := "dead-leader:8080"
	leaseDuration := int32(5)
	transitions := int32(2)
	expiredRenewTime := metav1.NewMicroTime(time.Now().Add(-10 * time.Second))

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "expired-lease",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holderID,
			LeaseDurationSeconds: &leaseDuration,
			LeaseTransitions:     &transitions,
			RenewTime:            &expiredRenewTime,
		},
	}

	fakeClient := fake.NewSimpleClientset(lease)
	old := swapClient(fakeClient)
	defer swapClient(old)

	holder, trans, err := getHolder("default", "expired-lease")
	if err != nil {
		t.Fatalf("getHolder failed: %v", err)
	}
	if holder != "" {
		t.Errorf("expected empty holder for expired lease, got %q", holder)
	}
	// Transitions should still be reported even for expired leases.
	if trans != int64(transitions) {
		t.Errorf("expected transitions %d, got %d", transitions, trans)
	}
}

// TestGetHolderReturnsHolderForActiveLease verifies that getHolder returns the
// holder identity when the lease is still active (renewTime + leaseDuration is
// in the future).
func TestGetHolderReturnsHolderForActiveLease(t *testing.T) {
	holderID := "active-leader:8080"
	leaseDuration := int32(15)
	transitions := int32(1)
	recentRenewTime := metav1.NewMicroTime(time.Now())

	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active-lease",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:       &holderID,
			LeaseDurationSeconds: &leaseDuration,
			LeaseTransitions:     &transitions,
			RenewTime:            &recentRenewTime,
		},
	}

	fakeClient := fake.NewSimpleClientset(lease)
	old := swapClient(fakeClient)
	defer swapClient(old)

	holder, trans, err := getHolder("default", "active-lease")
	if err != nil {
		t.Fatalf("getHolder failed: %v", err)
	}
	if holder != holderID {
		t.Errorf("expected holder %q, got %q", holderID, holder)
	}
	if trans != int64(transitions) {
		t.Errorf("expected transitions %d, got %d", transitions, trans)
	}
}

// TestElectionKeyFormat tests the election key construction.
func TestElectionKeyFormat(t *testing.T) {
	tests := []struct {
		ns, name, want string
	}{
		{"default", "leader", "default/leader"},
		{"kube-system", "my-lock", "kube-system/my-lock"},
		{"", "bare", "/bare"},
	}
	for _, tc := range tests {
		got := electionKey(tc.ns, tc.name)
		if got != tc.want {
			t.Errorf("electionKey(%q, %q) = %q, want %q", tc.ns, tc.name, got, tc.want)
		}
	}
}

// TestLeaseCRUDWithFakeClient tests basic Lease CRUD via the K8s API.
func TestLeaseCRUDWithFakeClient(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	holderID := "node-a:9090"
	transitions := int32(0)
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "crud-test",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:   &holderID,
			LeaseTransitions: &transitions,
		},
	}
	created, err := fakeClient.CoordinationV1().Leases("default").Create(ctx, lease, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create lease failed: %v", err)
	}
	if *created.Spec.HolderIdentity != holderID {
		t.Errorf("created holder = %q, want %q", *created.Spec.HolderIdentity, holderID)
	}

	newHolder := "node-b:9090"
	newTransitions := int32(1)
	created.Spec.HolderIdentity = &newHolder
	created.Spec.LeaseTransitions = &newTransitions
	updated, err := fakeClient.CoordinationV1().Leases("default").Update(ctx, created, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("update lease failed: %v", err)
	}
	if *updated.Spec.HolderIdentity != newHolder {
		t.Errorf("updated holder = %q, want %q", *updated.Spec.HolderIdentity, newHolder)
	}
	if *updated.Spec.LeaseTransitions != newTransitions {
		t.Errorf("updated transitions = %d, want %d", *updated.Spec.LeaseTransitions, newTransitions)
	}

	got, err := fakeClient.CoordinationV1().Leases("default").Get(ctx, "crud-test", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get lease failed: %v", err)
	}
	if *got.Spec.HolderIdentity != newHolder {
		t.Errorf("got holder = %q, want %q", *got.Spec.HolderIdentity, newHolder)
	}

	err = fakeClient.CoordinationV1().Leases("default").Delete(ctx, "crud-test", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("delete lease failed: %v", err)
	}

	_, err = fakeClient.CoordinationV1().Leases("default").Get(ctx, "crud-test", metav1.GetOptions{})
	if err == nil {
		t.Fatal("expected error after delete, got nil")
	}
}

// TestConcurrentGetHolder tests concurrent calls to getHolder.
func TestConcurrentGetHolder(t *testing.T) {
	holderID := "concurrent-node:8080"
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "concurrent-lease",
			Namespace: "default",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity:   &holderID,
			LeaseTransitions: ptr.To(int32(5)),
		},
	}
	fakeClient := fake.NewSimpleClientset(lease)
	old := swapClient(fakeClient)
	defer swapClient(old)

	const n = 10
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			holder, trans, err := getHolder("default", "concurrent-lease")
			if err != nil {
				errCh <- err
				return
			}
			if holder != holderID {
				errCh <- fmt.Errorf("expected holder %q, got %q", holderID, holder)
				return
			}
			if trans != 5 {
				errCh <- fmt.Errorf("expected transitions 5, got %d", trans)
				return
			}
			errCh <- nil
		}()
	}

	for i := 0; i < n; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent getHolder failed: %v", err)
		}
	}
}
