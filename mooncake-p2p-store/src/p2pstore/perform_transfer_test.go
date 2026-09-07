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
