# TENT High-Performance TCP Transport

`hp_tcp` is a dedicated TENT transport for high-throughput data movement over
TCP inside a data center. It uses a bounded asynchronous data path and keeps
the control plane separate from bulk payload transfer.

The standard `tcp` transport remains the compatibility-oriented RPC backend.
`hp_tcp` has its own transport type, configuration block, endpoint metadata,
buffer metadata, loader entry, and build target. In the current version they
cannot be enabled together because both would own the single bulk-data
notification callback in `ControlService`.

## Scope

The first version supports CPU DRAM-to-DRAM transfers. Its main goals are:

- persistent TCP connections with a fixed number of lanes per peer;
- bounded task, byte, mailbox, and accepted-connection resources;
- asynchronous connect, read, and write operations with timeouts;
- explicit remote buffer registration, permission checks, and lifetime
  protection;
- deterministic cancellation and shutdown;
- a remote commit acknowledgement for writes.

The first version does not implement GPU-direct transfer, TLS, retry,
multi-endpoint routing, multi-NIC striping, or dynamic load balancing between
lanes. Those features can be added without changing the ownership and
admission rules described below.

## Data Path

```text
Transfer request
    |
    v
whole-batch admission (task and byte limits)
    |
    v
bounded per-worker mailbox
    |
    v
fixed lane selected by (peer, endpoint, lane, incarnation)
    |
    v
persistent asynchronous TCP connection
    |
    v
remote bounded session on the target worker
    |
    v
registered remote buffer + operation-scoped lease
```

Each worker owns one `asio::io_context` and one thread. Client lanes and server
sessions assigned to that worker are only mutated on its event loop. The
mailbox is the cross-thread handoff into the event loop; it is not an
unbounded pending queue.

## Admission and Resource Bounds

Admission is atomic for a submitted batch. Before any command becomes visible
to a worker, the transport checks all affected mailbox capacities and reserves
the batch's task and byte budget. If any check fails, no command is committed
and no budget is consumed.

The reservation remains active while work is in a mailbox, waiting on a lane,
connecting, or performing I/O. It is released only when the task reaches a
terminal state. The relevant limits are:

- `queue_capacity_per_worker`: commands waiting in each worker mailbox;
- `max_outstanding_tasks`: accepted tasks across the transport;
- `max_outstanding_bytes`: bytes represented by accepted tasks;
- `max_transfer_bytes`: maximum size of one wire request;
- `max_connections`: a server-side derived limit that bounds active accepted
  sessions.

The server reserves an active-session slot before assigning an accepted socket
to a worker. A connection over the limit is closed. A completed or cancelled
session removes itself from its worker-owned session set and releases the slot,
so normal traffic does not retain thread or session resources.

## Lanes and Connection Ownership

`connections_per_peer` creates a fixed set of logical lanes. A request ID
selects a lane, and the tuple `(peer, endpoint, lane, incarnation)` selects its
worker and connection state. Requests on one lane are serialized; different
lanes can make progress on different workers.

A lane reuses its TCP connection after a clean operation. Protocol errors,
timeouts, cancellation, endpoint incarnation changes, and I/O errors make the
connection dirty and close it. The next operation on that lane establishes a
new connection.

The endpoint incarnation is published in metadata when the transport starts.
Including it in the lane key prevents a restarted peer from reusing a
connection associated with stale endpoint state.

## Wire Protocol

The protocol uses fixed-size, versioned request and response headers in network
byte order. A request contains:

- magic and protocol version;
- READ or WRITE opcode;
- request ID;
- remote buffer registration ID;
- remote address and transfer length.

The response contains the request ID, status, and committed byte count. Both
sides validate the version, opcode, length, request ID, and committed byte
count before reporting success.

For WRITE, the initiator sends the request header and payload. The target
reports success only after the complete payload has been copied into the
registered destination buffer. The initiator reports completion only after it
receives this response. A local socket write completion alone is not a remote
commit.

For READ, the target validates and leases the source buffer, sends a successful
response, and then sends the payload. The initiator reports completion only
after it has received the complete payload.

## Buffer Registration and Leases

Registering a buffer creates a monotonically increasing registration ID and
publishes its remote permission. Every remote request must match the registered
address range, current registration ID, requested operation, and permission.
This prevents a stale metadata record from authorizing access to a newly
registered buffer at the same address.

An accepted operation holds a lease on the local or remote buffer until its
last I/O callback has quiesced and the task has reached a terminal state.
Unregister first hides the entry from new acquisitions, then waits for existing
leases to drain. Therefore an application cannot free a registered buffer while
the data path may still access it.

## Timeout and Failure Semantics

Client resolve and connect use `connect_timeout_ms`. Header, payload, and
response progress use `progress_timeout_ms`. The server also applies
`progress_timeout_ms` while receiving a partial request or sending a response
or payload.

On timeout, the owner event loop cancels the resolver or socket and marks the
operation terminal. Completion and lease release happen after the cancelled
I/O callback returns. This ordering prevents a late callback from touching a
buffer after the caller observes completion.

Every operation has an epoch in addition to its request ID. Timer and I/O
callbacks check the epoch before changing state, so callbacks from a cancelled
or previous operation cannot complete the next operation on a reused lane.

## Shutdown

Shutdown follows an explicit ownership order:

1. Stop accepting new transport work and close admission.
2. Stop accepting new TCP connections.
3. Cancel commands that have not entered an owner event loop.
4. Cancel every client lane and every accepted server session on its owner
   worker. This closes sockets blocked on partial requests or stalled payloads.
5. Wait for active operations and sessions to reach zero.
6. Stop and join worker threads, remove the published endpoint metadata, and
   release the runtime objects.

`stop()` must not block a worker thread, because the cancellation callbacks
needed to finish shutdown run on those workers. Teardown is idempotent; a fully
stopped worker runtime is not restarted in place.

## Configuration

The transport is configured under `transports.hp_tcp`:

| Field | Meaning |
| --- | --- |
| `enable` | Enable the standalone `hp_tcp` transport. |
| `bind_address` | Local address used by the data-plane listener. |
| `advertise_address` | Address published to remote peers. |
| `port` | Listener port; zero requests an ephemeral port. |
| `worker_count` | Number of worker event loops and threads. |
| `queue_capacity_per_worker` | Capacity of each worker mailbox. |
| `connections_per_peer` | Persistent lanes available to each peer. |
| `max_outstanding_tasks` | Global accepted-task limit. |
| `max_outstanding_bytes` | Global accepted-byte limit. |
| `max_transfer_bytes` | Maximum payload represented by one request. |
| `chunk_size` | Maximum payload processed by one asynchronous I/O step. |
| `connect_timeout_ms` | Deadline for resolve and connection establishment. |
| `progress_timeout_ms` | Maximum time without progress during data-plane I/O. |

Because standard `tcp` is enabled by default, an `hp_tcp` configuration must
explicitly set `transports.tcp.enable` to `false` in this version.

## Testing

The transport has deterministic tests for protocol encoding and validation,
buffer permissions and lease draining, atomic bounded admission, worker
affinity, connection reuse and retirement, session reaping, malformed and
partial peers, connect/progress timeout, shutdown, transport metadata, and
two-process READ/WRITE operation at multiple concurrency levels.
