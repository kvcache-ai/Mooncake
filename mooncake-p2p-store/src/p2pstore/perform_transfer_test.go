// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package p2pstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errBatchBusy = errors.New("BatchID cannot be freed until all tasks are done")

// fakeBatchTransport mirrors the engine's batch lifecycle: a submitted task
// stays in flight for pollsUntilDone status polls, and freeBatchID refuses
// with BatchBusy while the task is in flight, as MultiTransport::freeBatchID does.
type fakeBatchTransport struct {
	pollsUntilDone int
	finalStatus    int
	submitErr      error

	polls          int
	submitted      bool
	freed          bool
	busyRejections int
}

func (f *fakeBatchTransport) allocateBatchID(int) (BatchID, error) { return BatchID(42), nil }

func (f *fakeBatchTransport) openSegment(string, bool) (int64, error) { return 1, nil }

func (f *fakeBatchTransport) submitTransfer(BatchID, []TransferRequest) error {
	if f.submitErr != nil {
		return f.submitErr
	}
	f.submitted = true
	return nil
}

func (f *fakeBatchTransport) inFlight() bool {
	return f.submitted && f.polls < f.pollsUntilDone
}

func (f *fakeBatchTransport) getTransferStatus(BatchID, int) (int, uint64, error) {
	f.polls++
	if f.inFlight() {
		return STATUS_PENDING, 0, nil
	}
	return f.finalStatus, 0, nil
}

func (f *fakeBatchTransport) freeBatchID(BatchID) error {
	if f.inFlight() {
		f.busyRejections++
		return errBatchBusy
	}
	f.freed = true
	return nil
}

func testLocation() *Location {
	return &Location{SegmentName: "peer:12345", Offset: 0}
}

// TestPerformTransferOnceCancellationFreesBatch is the regression for the
// cancellation leak: with the task still pending, returning on ctx.Done()
// without draining makes the deferred free hit BatchBusy and leak the batch.
func TestPerformTransferOnceCancellationFreesBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fake := &fakeBatchTransport{pollsUntilDone: 3, finalStatus: STATUS_COMPLETED}
	_, err := performTransferOnce(ctx, fake, 0, 4096, testLocation(), true)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if !fake.freed {
		t.Fatal("batch ID leaked on cancellation")
	}
	if fake.busyRejections != 0 {
		t.Fatalf("free attempted %d time(s) while the task was in flight", fake.busyRejections)
	}
}

func TestPerformTransferOnceCompletedFreesBatch(t *testing.T) {
	fake := &fakeBatchTransport{pollsUntilDone: 2, finalStatus: STATUS_COMPLETED}
	status, err := performTransferOnce(context.Background(), fake, 0, 4096, testLocation(), true)
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if status != STATUS_COMPLETED {
		t.Fatalf("status = %d, want STATUS_COMPLETED", status)
	}
	if !fake.freed {
		t.Fatal("batch ID leaked on completion")
	}
}

func TestPerformTransferOnceSubmitErrorFreesBatch(t *testing.T) {
	submitErr := errors.New("submit failed")
	fake := &fakeBatchTransport{submitErr: submitErr}
	_, err := performTransferOnce(context.Background(), fake, 0, 4096, testLocation(), true)
	if !errors.Is(err, submitErr) {
		t.Fatalf("err = %v, want submit error", err)
	}
	if !fake.freed {
		t.Fatal("batch ID leaked on submit failure")
	}
}

// recordSleeps swaps sleepFn for a recorder so tests can assert the exact
// backoff schedule instead of measuring wall-clock time.
func recordSleeps(t *testing.T) *[]time.Duration {
	t.Helper()
	var slept []time.Duration
	prev := sleepFn
	sleepFn = func(d time.Duration) { slept = append(slept, d) }
	t.Cleanup(func() { sleepFn = prev })
	return &slept
}

// TestWaitTransferBacksOffAfterSpinBudget checks the polling schedule: within
// the spin budget polls are back-to-back, after it every in-flight poll is
// followed by one sleep, growing 100µs, 200µs, 400µs, 800µs, then capped at 1ms.
func TestWaitTransferBacksOffAfterSpinBudget(t *testing.T) {
	const extraPolls = 40
	slept := recordSleeps(t)
	fake := &fakeBatchTransport{pollsUntilDone: pollSpinBudget + extraPolls, finalStatus: STATUS_COMPLETED}
	fake.submitted = true

	status, err := waitTransfer(context.Background(), fake, BatchID(1))
	if err != nil || status != STATUS_COMPLETED {
		t.Fatalf("status=%d err=%v", status, err)
	}
	// The fake reports terminal state on poll number pollsUntilDone.
	if fake.polls != pollSpinBudget+extraPolls {
		t.Fatalf("polls = %d, want %d", fake.polls, pollSpinBudget+extraPolls)
	}

	var want []time.Duration
	interval := pollMinInterval
	for i := 0; i < extraPolls-1; i++ {
		want = append(want, interval)
		if interval < pollMaxInterval {
			interval *= 2
			if interval > pollMaxInterval {
				interval = pollMaxInterval
			}
		}
	}
	if len(*slept) != len(want) {
		t.Fatalf("sleep count = %d, want %d: %v", len(*slept), len(want), *slept)
	}
	for i := range want {
		if (*slept)[i] != want[i] {
			t.Fatalf("sleep[%d] = %v, want %v (schedule %v)", i, (*slept)[i], want[i], *slept)
		}
	}
	if (*slept)[len(*slept)-1] != pollMaxInterval {
		t.Fatalf("backoff did not reach the cap: last sleep %v", (*slept)[len(*slept)-1])
	}
}

func TestWaitTransferWithinSpinBudgetDoesNotSleep(t *testing.T) {
	slept := recordSleeps(t)
	fake := &fakeBatchTransport{pollsUntilDone: pollSpinBudget / 2, finalStatus: STATUS_COMPLETED}
	fake.submitted = true
	if _, err := waitTransfer(context.Background(), fake, BatchID(1)); err != nil {
		t.Fatal(err)
	}
	if len(*slept) != 0 {
		t.Fatalf("spin-budget path slept %d time(s): %v", len(*slept), *slept)
	}
}

// TestWaitTransferCancelledContextWinsOverTerminalStatus pins the edge case
// where ctx is already cancelled and the very first poll reports a terminal
// status: cancellation is authoritative and ctx.Err() is returned.
func TestWaitTransferCancelledContextWinsOverTerminalStatus(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &fakeBatchTransport{pollsUntilDone: 0, finalStatus: STATUS_COMPLETED}
	fake.submitted = true

	status, err := waitTransfer(ctx, fake, BatchID(1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if status != STATUS_FAILED {
		t.Fatalf("status = %d, want STATUS_FAILED", status)
	}
	if fake.polls != 1 {
		t.Fatalf("polls = %d, want 1 (terminal on first poll)", fake.polls)
	}
}
