# TENT High-Performance TCP

`hp_tcp` is a standalone TENT transport for CPU DRAM transfers over
data-center TCP. Standard `tcp` remains the RPC-based compatibility path.
The first version intentionally excludes GPU memory, TLS, per-transfer
striping, rail failover, transparent replay after an ambiguous WRITE and
dynamic lane scheduling.

## Architecture

Each worker owns one `asio::io_context` and one thread. Each peer has a
configured number of persistent lanes, and request IDs distribute operations
across them. A stable hash of peer and lane selects the owner; socket state
never moves between workers, and operations on a lane are FIFO. ASIO provides
the event queue; process-wide task and byte admission limits bound all accepted
work, including callbacks waiting in that queue.

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

Routing is deliberately static. A transfer stays on one persistent lane and
therefore one rail: `hp_tcp` does not stripe a transfer, rebalance traffic, or
fail over between rails.

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
| `max_transfer_bytes` | Maximum request size. I/O progress is tracked in fixed internal steps. |
| `connect_timeout_ms`, `progress_timeout_ms` | Connection and I/O deadlines. |

Tests cover wire validation, rail metadata and source binding, admission,
buffer leases, connection reuse, session reaping, client/server timeout,
stale-registration recovery, ambiguous WRITE completion and a two-process
READ/WRITE smoke test.
