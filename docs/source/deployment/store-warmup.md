# Store Connection Warmup

Store warmup is an optional, best-effort transport connection warmup. During
client setup, the Store client asks the master for eligible remote segments,
opens each selected segment, and submits a small READ-only probe. The master
only returns targets; it does not perform the warmup.

Warmup is not KV data or cache preloading. It does not write remote memory, and
failure to discover or probe an individual target does not fail client setup.
Not every target is guaranteed to succeed. The current automated integration
coverage primarily validates TCP.

## Configuration

Warmup is disabled by default. Enable it with the Store setup option or one of
the following environment values:

```bash
export MC_STORE_WARMUP=1
```

`true`, `yes`, and `on` are also accepted, case-insensitively.

| Variable | Default | Valid values | Meaning |
| --- | ---: | --- | --- |
| `MC_STORE_WARMUP` | disabled | `1`, `true`, `yes`, `on` | Enables startup warmup. |
| `MC_STORE_WARMUP_READ_SIZE` | `64` | `1..1048576` bytes | READ probe size. This is a connection probe, not a business data block. |
| `MC_STORE_WARMUP_TIMEOUT_MS` | `1000` | `1..60000` ms | Per-target wait time before cleanup continues asynchronously. |
| `MC_STORE_WARMUP_CONCURRENCY` | `16` | positive integer; effective maximum `128` | Number of concurrent client-side probe workers. Values above `128` are clamped. |
| `MC_STORE_WARMUP_MAX_TARGETS` | `16` | `0..65536` | Maximum targets requested from the master. `0` requests all eligible targets, subject to the Master service cap of `1024`. |

Malformed, negative, overflowing, zero where disallowed, or out-of-range
values fall back to the documented default and emit a warning. The effective
values are logged when warmup starts.

Increasing target count, concurrency, read size, or timeout can increase setup
latency, memory use, batch use, handshake load, and incoming work on peers.
The finite default target count avoids an all-to-all startup wave. The Master
applies a defensive hard limit of `1024` targets to both unlimited (`0`) and
oversized requests, so it never constructs or returns more than `1024` warmup
targets for one request. Explicit `0` should still be used cautiously because
it can create a much larger startup wave than the default `16`.

The Master filters the requester's own segments, unavailable segment states,
empty endpoints, and protocols that do not match an exact protocol token. It
prefers the Transfer Engine endpoint over the segment name and deduplicates the
result by endpoint. A finite request is selected deterministically for the
requester and receives priorities starting at zero.

`MC_STORE_WARMUP_CONCURRENCY` controls outgoing probe work in the Store client.
It does not add receiver-side handshake workers or change Transfer Engine's
general handshake listener concurrency model.

If a probe times out, its batch and destination buffer remain owned by the
client cleanup path until Transfer Engine reports a terminal state. Once the
transfer is terminal, the probe buffer is returned immediately, even if
`freeBatchID()` temporarily fails; only a lightweight batch-ID record remains
for retry.

A single cleanup worker services both non-terminal transfers and lightweight
batch-ID retries. Each cleanup round handles at most one item from each queue,
so a transfer that remains pending cannot starve an already-terminal batch ID
that is ready to be freed. Transfer status checks and `freeBatchID()` calls run
outside the cleanup mutex.

To avoid accumulating several long-running probes in one startup wave, the
client stops submitting new targets after the first probe enters pending
cleanup. The summary log reports:

- `discovered`: eligible targets returned by the master.
- `attempted`: targets for which this warmup invocation started processing.
- `not_attempted`: discovered targets skipped after pending cleanup stopped
  new submissions. `discovered = attempted + not_attempted`.

Teardown calls `Client::ShutdownWarmup()` before unregistering probe memory.
Shutdown stops new warmup calls and uses a fixed five-second deadline to wait
for transfers that may still write probe buffers. If the deadline expires,
`RealClient` returns a transfer failure and keeps the client, allocator, and
memory registration alive for a later teardown retry. Transfer Engine has no
reliable batch cancel operation, so the implementation does not claim that an
unfinished transfer was cancelled. A direct `Client` destructor treats an
unresolved transfer as a fatal unsafe-teardown error.

Lightweight batch-ID records continue retrying during normal cleanup. Once
shutdown has confirmed that no transfer can write probe memory, any batch ID
that still cannot be freed is logged and left for final Transfer Engine
teardown; it does not keep the probe allocation or block memory unregistration.

## When To Use It

Warmup is most useful when clients are long-lived, peers are relatively stable,
first-READ latency matters, and the target count is controlled.

It is usually a poor fit for large simultaneous client startup waves, rarely
used peers, short-lived clients, frequent elastic scaling, or deployments that
leave target count unlimited without an external bound.

## Tests

Build and run the unit and TCP loopback integration tests:

```bash
cmake --build build-warmup \
  --target store_warmup_test store_warmup_integration_test \
  -j"$(nproc)"

ctest --test-dir build-warmup \
  -R '^store_warmup(_integration)?_test$' \
  --output-on-failure
```

The unit tests cover self and unavailable-segment filtering, protocol-token
matching, endpoint deduplication, stable finite selection, the Master service
cap, configuration bounds, and four cleanup cases: pending-transfer buffer
retention, fairness between cleanup queues, terminal-transfer buffer release
while batch-ID cleanup retries, and shutdown-before-unregister ordering. The
integration tests cover single-client no-target behavior and TCP loopback
probes between multiple clients. They do not cover real cross-physical-node
deployments, RDMA/UB hardware, connection reuse counts, or large-scale stress.
