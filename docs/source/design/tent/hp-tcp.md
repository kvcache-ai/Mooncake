# TENT High-Performance TCP

`hp_tcp` is a standalone TENT transport for CPU DRAM transfers over
data-center TCP. Standard `tcp` remains the RPC-based compatibility path.
The first version intentionally excludes GPU memory, TLS, multi-endpoint
routing, multi-NIC striping, socket tuning, automatic fallback and dynamic
lane scheduling.

## Architecture

Each worker owns one `asio::io_context` and one thread. A stable hash of peer
and lane selects the owner; socket state never moves between workers. Each peer
has a configured number of persistent lanes, and operations on a lane are
FIFO. ASIO provides the event queue; process-wide task and byte admission
limits bound all accepted work, including callbacks waiting in that queue.

The server uses the same worker pool: worker zero owns the listener, and
accepted sockets are assigned to workers and stored in worker-owned session
sets. A global connection limit bounds live sessions; closing a session
removes it immediately rather than retaining one thread per historical
connection.

```text
TENT request -> bounded admission -> owner worker -> persistent lane
             -> versioned TCP protocol -> registered remote buffer
```

## Protocol and memory safety

Requests contain a version, opcode, request ID, registration ID, remote
address and length. Responses contain the request ID, status and committed
byte count. A WRITE completes only after the target has copied the full payload
and returned an acknowledgement. A READ completes after the full response
payload arrives.

Every registered buffer has a monotonically increasing registration ID and a
remote permission. The target validates the ID, range and permission before
access. An operation holds a lease until its final I/O callback retires;
unregister hides the range from new work and waits for existing leases.
Stale registration metadata causes one bounded metadata refresh and retry on
the same transport. Permission and range failures are terminal.

## Timeouts and shutdown

Resolve/connect use `connect_timeout_ms`. Header, payload and response progress
use `progress_timeout_ms` on both client and server, including an empty or
partial request header. A timeout cancels the resolver or socket; terminal
completion is published only after the corresponding callback retires.

Shutdown closes admission and the listener, drains queued dispatch callbacks,
cancels every client lane and server session on its owner, waits for operations
and leases, then stops and joins worker threads. This makes shutdown bounded
even when a peer sends only part of a request.

## Configuration

The transport is configured under `transports.hp_tcp`:

| Field | Meaning |
| --- | --- |
| `enable` | Enable `hp_tcp`. |
| `bind_address`, `advertise_address`, `port` | Listener and published endpoint. |
| `worker_count` | ASIO event-loop threads. |
| `connections_per_peer` | Persistent lanes per peer. |
| `max_outstanding_tasks`, `max_outstanding_bytes` | Global admission bounds. |
| `max_transfer_bytes`, `chunk_size` | Request and I/O step limits. |
| `connect_timeout_ms`, `progress_timeout_ms` | Connection and I/O deadlines. |

Tests cover wire validation, admission, buffer leases, connection reuse,
session reaping, client/server timeout, stale-registration recovery and a
two-process READ/WRITE smoke test.
