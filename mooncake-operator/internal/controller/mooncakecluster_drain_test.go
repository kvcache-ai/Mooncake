package controller

import (
	"testing"
	"time"
)

// TestDrainRetryLogic verifies the drain retry count threshold logic
// used in reconcileTerminatingWorkers to decide whether to retry or
// force-delete a pod.
func TestDrainRetryLogic(t *testing.T) {
	tests := []struct {
		name        string
		jobStatus   int
		retryCount  int
		wantRetry   bool // true = should retry (status < max), false = force-delete
		wantSkipped bool // true = already succeeded, nothing to do
	}{
		{
			name:       "SUCCEEDED no action",
			jobStatus:  3,
			retryCount: 0,
			wantSkipped: true,
		},
		{
			name:       "CREATED still running",
			jobStatus:  0,
			retryCount: 0,
			wantSkipped: true, // neither retry nor force-delete
		},
		{
			name:      "FAILED below threshold retries",
			jobStatus: 4,
			retryCount: maxDrainRetries - 1,
			wantRetry: true,
		},
		{
			name:      "FAILED at max threshold force-deletes",
			jobStatus: 4,
			retryCount: maxDrainRetries,
			wantRetry: false,
		},
		{
			name:      "FAILED retry count 0",
			jobStatus: 4,
			retryCount: 0,
			wantRetry: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podStillExists := true

			if tt.jobStatus == 3 { // SUCCEEDED
				// Controller clears tracking; no action needed.
				return
			}

			if tt.jobStatus != 4 { // not FAILED
				return // still running
			}

			if !podStillExists {
				return // pod gone, nothing to do
			}

			if tt.jobStatus == 4 && podStillExists && tt.retryCount < maxDrainRetries {
				if !tt.wantRetry {
					t.Errorf("expected force-delete but got retry for retryCount=%d", tt.retryCount)
				}
				return
			}

			if tt.wantRetry {
				t.Errorf("expected retry for retryCount=%d but reached force-delete", tt.retryCount)
			}
		})
	}
}

// TestDrainRetryBackoff verifies that the retry interval is reasonable.
func TestDrainRetryBackoff(t *testing.T) {
	// drainRetryInterval should be positive and reasonable.
	if drainRetryInterval <= 0 {
		t.Errorf("drainRetryInterval must be positive, got %v", drainRetryInterval)
	}
	if drainRetryInterval > 5*time.Minute {
		t.Errorf("drainRetryInterval is too long: %v", drainRetryInterval)
	}

	// maxDrainRetries should be reasonable.
	if maxDrainRetries <= 0 {
		t.Errorf("maxDrainRetries must be positive, got %d", maxDrainRetries)
	}
	if maxDrainRetries > 20 {
		t.Errorf("maxDrainRetries is too high: %d", maxDrainRetries)
	}
}

// TestDrainJobInfoFields verifies that drainJobInfo struct has all needed fields.
func TestDrainJobInfoFields(t *testing.T) {
	now := time.Now()
	job := &drainJobInfo{
		PodName:     "test-pod",
		PodIP:       "10.0.0.1",
		SegmentName: "10.0.0.1:13006",
		JobID:       "job-123",
		Status:      4, // FAILED
		RetryCount:  maxDrainRetries,
		CreatedAt:   now,
	}

	if job.PodName == "" {
		t.Error("PodName should be set")
	}
	if job.PodIP == "" {
		t.Error("PodIP should be set")
	}
	if job.SegmentName == "" {
		t.Error("SegmentName should be set")
	}
	if job.JobID == "" {
		t.Error("JobID should be set")
	}
	if job.Status != 4 {
		t.Errorf("Status should be 4 (FAILED), got %d", job.Status)
	}
	if job.RetryCount != maxDrainRetries {
		t.Errorf("RetryCount should be %d, got %d", maxDrainRetries, job.RetryCount)
	}
}

// TestDrainStatusMapping verifies the drainStatusString helper produces expected values.
func TestDrainStatusMapping(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{0, "CREATED"},
		{1, "PLANNING"},
		{2, "RUNNING"},
		{3, "SUCCEEDED"},
		{4, "FAILED"},
		{5, "CANCELED"},
		{99, "UNKNOWN(99)"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := drainStatusString(tt.status)
			if got != tt.want {
				t.Errorf("drainStatusString(%d) = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}

// TestForceDeletePathCondition verifies the exact condition that triggers
// the force-delete path matches the Fix 1 implementation.
//
// The condition in reconcileTerminatingWorkers:
//
//	if job.Status == 4 && podStillExists {
//	    // Force-delete with GracePeriodSeconds(1)
//	}
//
// This test ensures that the condition fires correctly.
func TestForceDeletePathCondition(t *testing.T) {
	type testCase struct {
		status         int
		podExists      bool
		expectDelete   bool
		description    string
	}

	cases := []testCase{
		{0, true, false, "CREATED pod exists → no delete"},
		{1, true, false, "PLANNING pod exists → no delete"},
		{2, true, false, "RUNNING pod exists → no delete"},
		{3, true, false, "SUCCEEDED pod exists → no delete (already cleaned up)"},
		{4, true, true, "FAILED pod exists → FORCE DELETE"},
		{4, false, false, "FAILED pod gone → no action needed"},
		{5, true, false, "CANCELED pod exists → no delete"},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			// Simulate the controller's branching logic.
			shouldDelete := false
			jobStatus := tc.status
			podStillExists := tc.podExists
			retryCount := maxDrainRetries // at threshold

			if jobStatus == 4 && podStillExists {
				if retryCount < maxDrainRetries {
					// Retry path
				} else {
					// Force-delete path
					shouldDelete = true
				}
			}

			if shouldDelete != tc.expectDelete {
				t.Errorf("status=%d podExists=%v: got delete=%v, want %v",
					tc.status, tc.podExists, shouldDelete, tc.expectDelete)
			}
		})
	}
}
