# TENT High-Performance TCP

`hp_tcp` is a standalone TENT transport for CPU DRAM transfers over
data-center TCP. Standard `tcp` remains the RPC-based compatibility path.
The first version intentionally excludes GPU memory, TLS, WRITE striping, rail
failover, transparent replay after an ambiguous WRITE and dynamic lane
scheduling.

## Architecture

Each worker owns one `asio::io_context` and one thread. Each peer has a
configured number of persistent lanes. A separate sequence for each peer
rotates operations across them, so interleaved traffic to other peers cannot
pin a peer to one lane. Each sequence starts at that peer's first request ID
to preserve its initial lane choice. Request IDs remain globally unique.
A stable hash of peer and lane selects the owner; socket state
never moves between workers, and operations on a lane are FIFO. ASIO provides
the event queue; process-wide task and byte admission limits bound all accepted
work, including callbacks waiting in that queue.

Both ends disable Nagle's algorithm: headers and payloads use separate writes,
so a delayed ACK must not hold up a short payload on a persistent socket.

The server uses the same worker pool. Accepted sockets are assigned to workers
and stored in worker-owned session sets. A global connection limit bounds live
sessions; closing a session removes it immediately rather than retaining one
thread per historical connection.

```text
TENT request -> bounded admission -> owner worker -> persistent lane
             -> versioned TCP protocol -> registered remote buffer
```

### Static multi-rail routing

`rail_addresses` optionally assigns persistent lanes across an ordered set of
local and remote TCP addresses. Each entry must be a numeric address assigned
to the local host. The current implementation accepts IPv4 addresses only.
When the list is non-empty it supplies the published
endpoints instead of `advertise_address`. Lane `i` uses rail
`i % rail_count`, binds its socket to the matching local address, and keeps the
existing peer-and-lane worker ownership. Both peers must configure the same
non-zero rail count. A specific listener address must match the sole rail;
multiple local rails require a wildcard listener.

Routing is deliberately static. Small transfers and all WRITEs stay on one
persistent lane. A READ large enough to contribute at least one internal I/O
step per configured rail is split into one contiguous slice per rail. The
slices reuse the persistent lanes and complete as one TENT task. `hp_tcp` does
not rebalance traffic or fail over between rails.

The current internal step is 1 MiB: with two rails, READs of at least 2 MiB
are sliced; smaller READs and single-rail READs are not. Remainder bytes are
assigned to the first slices, so uneven lengths still cover the request
exactly. Multiple independent WRITEs can use different rails, but each WRITE
stays on one lane and waits for its own remote completion ACK.

`connections_per_peer` is the **total** lane budget, not a per-rail multiplier.
For example, four lanes and two rails give two lanes per rail. Worker count is
independent of rail count: several lanes may share an owner worker. Increasing
lanes on a single rail does not split a single READ into multiple streams.

## Protocol and memory safety

Requests contain a version, opcode, request ID, registration ID, remote
address and length. Responses contain the request ID, status and committed
byte count. A WRITE completes only after the target has copied the full payload
and returned an acknowledgement. A READ completes after the full response
payload arrives.

Every registered buffer has an ID formed from a per-registry random namespace
and a monotonic sequence, plus a remote permission. This prevents a stale ID
from a previous server incarnation from becoming valid after restart. The
target validates the ID, range and permission before access. An operation holds
a lease until its final I/O callback retires; unregister hides the range from
new work and waits for existing leases. Stale registration metadata causes one
bounded metadata refresh and retry on the same transport. Permission and range
failures are terminal.

If a WRITE request may have reached the peer but no valid acknowledgement is
received, the remote outcome is unknown. That failure is terminal and is not
replayed through another transport; otherwise a committed WRITE whose ACK was
lost could execute twice.

## Timeouts and shutdown

Resolve/connect use `connect_timeout_ms`. Header, payload and response progress
use `progress_timeout_ms` on both client and server. A newly accepted connection
must send its first header byte before the deadline, and every partial header
or payload must continue to make progress. After a valid request completes,
pure idle time on its persistent connection is not treated as stalled I/O; the
deadline resumes as soon as the next header begins. A timeout cancels the
resolver or socket; terminal completion is published only after the
corresponding callback retires.

A failed READ slice cancels its siblings; the logical task settles only after
all slice callbacks retire. A stale-registration result can trigger the
existing bounded metadata refresh/retry, not migration onto another rail.
Independent peers can continue while a peer is waiting for its progress
timeout. FIFO sharing within one peer's lanes can still delay small requests
behind large ones; slicing is not a priority or preemption mechanism.

The client separately closes a pooled socket after
`idle_connection_timeout_ms` without active or queued work on that lane
(default 60 seconds). New work before expiry cancels this timer and reuses the
socket; work after expiry reconnects. This releases receiver connection slots
held by idle updated clients. The server does not evict established idle sockets:
it cannot know whether a client has just started another WRITE. Older clients
that keep sockets open indefinitely still require their own pool cleanup.
Tasks attempted while the receiver connection limit is full can still fail;
idle cleanup is not task backpressure or an automatic retry policy.

Shutdown closes admission and the listener, drains queued dispatch callbacks,
cancels every client lane and server session on its owner, waits for operations
and leases, then stops and joins worker threads. This makes shutdown bounded
even when a peer sends only part of a request.

This ordering is a lifecycle invariant, not an incidental destructor detail:
the client and server are destroyed before the worker contexts they use. In a
debug build, normal teardown asserts that admission, client operations and
server sessions have all drained before their owners are destroyed.

An exception escaping an ASIO handler marks the runtime failed and blocks
further admission. The owner event loop continues only to retire previously
committed work and process teardown cancellation with the same affinity. Once
those resources drain, shutdown joins the workers and reports the failure.
Likewise, admission-release underflow is fail-closed: counters are preserved,
new work is rejected, and drain returns an error instead of treating live work
as complete.

## Configuration

The transport is configured under `transports.hp_tcp`:

| Field | Meaning |
| --- | --- |
| `enable` | Enable `hp_tcp`; set `transports.tcp.enable` to `false`. The two transports cannot be enabled together because control-plane notification ownership is singular. |
| `bind_address`, `advertise_address`, `port` | Listener and published endpoint. |
| `rail_addresses` | Ordered numeric IPv4 source addresses for static lane-to-rail routing. The list must be unique, no longer than `connections_per_peer`, and have the same length on both peers. A non-wildcard `bind_address` must equal the sole rail address; multiple rails require it to be empty or `0.0.0.0`. |
| `worker_count` | ASIO event-loop threads. |
| `connections_per_peer` | Persistent lanes per peer. |
| `max_outstanding_tasks`, `max_outstanding_bytes` | Global admission bounds. |
| `max_transfer_bytes` | Maximum request size. When HP TCP is enabled, coalescing of HP TCP/UNSPEC requests respects both local and advertised remote limits; an individually oversized request is still rejected. |
| `connect_timeout_ms`, `progress_timeout_ms` | Connection and I/O deadlines. |
| `idle_connection_timeout_ms` | Positive client idle-pool retention time; default 60000 ms. Active or queued requests are never expired by this timer. Shorter retention frees receiver slots sooner but requires more reconnections for intermittent traffic. |

### Single-rail and paired-rail examples

For a single rail, put this in the server's `MC_TENT_CONF` JSON file (replace
the example address with an address assigned to the host):

```json
{
  "transports": {
    "tcp": {"enable": false},
    "rdma": {"enable": false},
    "shm": {"enable": false},
    "hp_tcp": {
      "enable": true,
      "bind_address": "",
      "rail_addresses": ["10.0.0.2"],
      "worker_count": 4,
      "connections_per_peer": 4
    }
  }
}
```

Use the same configuration on the client with its local address `10.0.0.1`.
For two rails, change only the lists:

| Host | `rail_addresses` |
| --- | --- |
| Client | `["10.0.0.1", "10.1.0.1"]` |
| Server | `["10.0.0.2", "10.1.0.2"]` |

Entries pair by index, and both hosts need working source-address routes for
those pairs. Reused sockets retain that mapping; failed sockets are closed
before later requests reconnect.

`MC_TENT_CONF` loads a complete configuration, so include the transport enable
flags even when using tebench's `--xport_type=hp_tcp`. To check data with the
existing benchmark, start its target, then run an initiator with its advertised
segment name:

```bash
MC_TENT_CONF=client.json tebench --backend=tent --xport_type=hp_tcp \
  --tent_transport_hint=hp_tcp --target_seg_name=SERVER_SEGMENT \
  --seg_type=DRAM --op_type=mix --check_consistency=true \
  --start_block_size=67108864 --max_block_size=67108864 --duration=3
```

Repeat with both block-size flags set to `4096` for the unsliced path.
With `--xport_type=hp_tcp --check_consistency=true`, the CPU checker uses
seed-reproducible, non-constant data and a full byte comparison to detect
reordered slices. Run throughput separately without this checking overhead.
Ordinary `mix`, other backends and `write_seed`/`read_verify` retain their
existing data patterns.

### Checking rail use

Test-local socket relays record the peer/local addresses and completed slice
ranges. Full-engine tests check payloads, guards, the slicing threshold and
connection reuse; two-process E2Es also cover unequal transfer-size limits.

On two machines, inspect connections with `ss -tnp`, source-address routes with
`ip route get REMOTE from LOCAL`, and per-interface byte counters before/after
a transfer. READ payload moves from server TX to client RX. Compare both
rails' deltas with successful application bytes; account for protocol overhead
and unrelated traffic. Two open connections alone do not prove payload use.

Loopback proves routing, not physical NIC use. Check PCI devices and shared
host/fabric limits. Compare one rail/one lane, one rail/multiple lanes, and two
rails/the same total lanes: the last two differ in single-READ slicing as well
as rail placement.

### Measured scope

On two Xeon 8457C virtual machines, with four workers and four total lanes,
three interleaved runs (1-second warmup, 3-second measurement) gave the
following medians:

| READ workload | Metric | One rail | Two rails |
| --- | --- | ---: | ---: |
| 64 MiB, one concurrent task | Throughput (GB/s) | 3.22 | 6.46 |
| 64 MiB, four concurrent tasks | Throughput (GB/s) | 11.13 | 10.96 |
| 4 KiB, one concurrent task | Mean latency (microseconds) | 72 | 78 |

Both interfaces carried payload-direction traffic, but their underlying
resource independence is not guaranteed. Four concurrent READs showed no
additional gain. A same-pool 4 KiB/64 MiB closed-loop mix still delayed small
tasks behind large ones: static slicing offers neither latency isolation nor
universal bandwidth scaling.
