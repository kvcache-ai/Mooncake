(tent-notification)=
# TENT Notifications

A notification is a small `{name, msg}` message from one engine to the engine that owns a segment. It is sent on its own with `sendNotification()`, or passed to `submitTransfer()` with a batch of requests and sent once they complete; the receiver takes it with `receiveNotification()`. This document describes what the engine guarantees about delivery, how a notification travels and recovers from faults, which knobs control that, how to observe it, and what an application built on it still has to do.

## Semantics

The engine delivers a notification at least once and the receiver drops copies, so the application sees each one exactly once:

* For every `submitTransfer()` call with a notification whose requests the sender saw `COMPLETED`, each segment those requests target receives exactly one notification.
* For every standalone `sendNotification()` that returned OK, the target receives exactly one notification.
* A call with a request that ended `FAILED` or `CANCELED` sends none.

This holds under two preconditions:

1. **The receiver keeps polling.** The application calls `receiveNotification()` often enough that no receive queue reaches `notification/recv_backlog_max`. A full queue drops its oldest unread notification for each new arrival and counts it.
2. **Both ends stamp.** `notification/proto` is 1, the default, on both engines, and over RDMA the peer advertised it when the notify QP was set up. Without the stamp the receiver cannot recognize a copy, so a retry may hand the application a notification twice, and a notification lost with a failing notify QP is not resent.

Known Limitations lists the cases these preconditions do not cover: a Notify RPC a receiver could not take for longer than the deadlines, a notification too large to carry its stamp, and a session the receiver forgot while the sender still used it.

Retries are bounded in time: a notification the engine gives up on is dropped, counted and logged rather than retried forever. Non-goals:

* **No order across notifications.** The receiver identifies a notification by its `name`, not by its position. `receiveNotification()` returns the transports' queues one after another, which never promised an order.
* **No exactly-once across a process crash.** A receiver that dies loses what its NIC acked or its control plane accepted and the application had not yet taken. A sender that dies loses what it had not handed over: up to 256 sends in flight per notify QP, plus its outbox and resend queues. An application that needs more acknowledges at its own level.
* **No GPU memory visibility guarantee.** Whether data written to GPU memory is visible to the receiver's kernels when the notification arrives is outside this design.
* **TCP-only engines still try on the polling thread.** Without the RDMA transport, the status poll that completes a transfer still makes the Notify RPC itself; what that try cannot hand over goes to the outbox as with RDMA (see Known Limitations).

## Notifications Bound to Transfers

The requests of one `submitTransfer()` call form its interval. Its notification is due once a status poll sees every request of the interval `COMPLETED`, and then goes to every segment those requests target. If any request ends `FAILED` or `CANCELED`, the interval sends nothing, so a receiver is never told about data that did not land. Other intervals of the batch neither hold it back nor are held back by it. Any status poll can send it: `getTransferStatus()` in all three forms, `progressBatch()` and the calls built on it, the poll `freeBatch()` makes before freeing, and the progress worker when it is enabled. A notification of a transfer never polled to completion before teardown is not sent.

A request whose status query fails does not stop `getTransferStatus(batch_id, status)` or `progressBatch(batch_id, status)`: the other requests are still polled, and the notification of an interval whose requests all completed goes out before the first error is returned. `getTransferStatus(batch_id, status_list)`, and the progress pass of the runtime queue (`enable_runtime_queue`), stop at that request; the ones behind it are polled, and their notifications sent, once a later call gets past it.

## Paths

A notification tries these paths in order, each only after the previous one reported its channel unavailable; any other error is returned to the caller:

1. The notify QP of pair 0: the first enabled local RDMA NIC to the peer's NIC 0. Every NIC pair's endpoint carries one notify QP next to its data QPs, but the data path picks its own pairs (for host memory, the NIC the rail map prefers for the NUMA node of the memory written), so pair 0 is up only once the data path used it or an earlier send dialed it.
2. The other RDMA NIC pairs.
3. The TCP transport (HP TCP when TCP is not loaded): the Notify RPC to the peer's control plane.
4. With neither loaded, the same RPC made by the engine itself.

With `notification/rpc_fallback` set to `false` the engine returns the first transport's error instead of moving on. A standalone `sendNotification()` runs the paths on the calling thread and returns OK once a notify QP or the peer's control plane took the notification, and otherwise the status of the last path tried.

### Other NIC pairs

Unless pair 0 failed because the peer's control plane did not answer, a new notification that pair 0 cannot take gets at most one try on each other pair before it moves on, and a full send queue there is not waited on. The other pairs are dialed if pair 0 could not be connected; if pair 0 is up but cannot post, only pairs already up are tried. If pair 0's send queue stalled instead (no free slot for 1 s), the peer's NIC 0 is suspect and no pair to it is tried, so for a peer with one RDMA NIC the notification moves on at once. Pairs to the peer's other NICs that are already up go first, then those pairs are dialed. After a stall, a dial that ends in an RPC error returns it without moving on to the RPC paths, which would wait on the same control plane, so a standalone send waits at most about 1 s plus one RPC request timeout. `notification/path_probe` set to `false` turns this step off.

### Bound notifications and the outbox

The poll that finds a bound notification due makes one try. With the RDMA transport loaded, the try uses only a notify QP that is already up, on pair 0 or another pair; it never fetches the peer's segment description, dials the peer or waits on its control plane, though on pair 0 it can wait up to 1 s for a free send slot. When pair 0 is not up, for example because the data path writes only to the peer's other NICs, the try takes the first other pair that is up and has a free send slot, without waiting for a slot. Without RDMA the try is the Notify RPC on the polling thread. What the try cannot hand over goes to the engine's outbox, unless it failed as invalid, and so does every later bound notification to a peer that the outbox still holds entries for:

* One outbox thread, started with the first entry, sends each peer's oldest entry over the full path, as a standalone send would. A peer that fails is retried after `notification/outbox_retry_ms`, doubling up to 2 s, and its later entries wait behind the oldest.
* An entry older than `notification/outbox_deadline_ms` is dropped. At `notification/outbox_max` entries, the oldest entry of any peer is dropped to make room. The entry out on a send counts toward that bound but is never the one dropped, so with an `outbox_max` of 1 a new entry waits next to the one in flight. A notification whose send fails as `InvalidArgument`, or that the control-plane leg cannot encode, is dropped at once, on the poll's try or later.
* Freeing the batch does not cancel what the outbox holds. At teardown the outbox finishes the send in flight and drops the rest.

Delivery of a bound notification is therefore asynchronous: it may arrive after a standalone notification sent later, and notifications to different peers have no common order. Among bound notifications to one peer the order is best effort: one that becomes due while the outbox holds older ones for that peer queues behind them, but two that become due at the same time, on concurrent polls, have no defined order. A peer without an RDMA notify channel up, for example a TCP-only peer of an RDMA engine, gets every bound notification through the outbox thread.

### Resends over RDMA

A notify QP that fails, or whose endpoint is torn down, hands the stamped notifications it had not completed back to the RDMA transport's resend thread. The thread posts each one again, trying every pair once, starting after the pair that failed and dialing as needed. If no pair takes it, or once it has been through three such rounds, the thread makes the Notify RPC instead (unless `notification/rpc_fallback` is `false`). If the peer's control plane does not answer, or answers that it cannot take the notification yet, the resend is held rather than dropped: only the oldest held resend per peer is tried again, every 2 s, until `notification/resend_deadline_ms` has passed since it was first held. Resends still queued or held when the transport stops are dropped.

### Peer health

`PeerHealth` is a process-wide table of control-plane addresses that stopped answering, so that finding a dead peer costs one RPC timeout rather than one per call. An address is marked when a dial to it or a call to it times out. While an address is marked, the control-plane calls notifications make to it (fetching its segment description, bootstrapping a notify QP, the Notify RPC) fail at once with `marked unreachable, not dialing`; a probe, including the one `probePeerAliveByID()` makes, always dials. The mark lasts `peer_health/cooldown_ms`, doubled each time a call to it times out again, up to 60 s. A prober thread dials marked addresses every `peer_health/probe_interval_ms`, any call that succeeds clears the mark, and an entry is forgotten 10 minutes after its first failure. To the paths above, a call to a marked peer is a control plane that did not answer: a standalone send returns the error, the outbox backs off, and a resend is held.

Only a timeout marks a peer. The RPC library reports any failed dial other than its own timeout as not connected, and a refused connection, or one closed without an answer, may be a peer restarting on the same address or a stale pooled connection. Such a failure neither marks nor clears an address: a peer that refused calls while it restarted is usable at once, and one marked after a timeout that then refuses stays on the prober's list until it answers or its entry is forgotten.

### Receiver acknowledgement

The receiver's control plane answers a Notify RPC it could not take: before the engine registered its callback (the RPC server starts first), after teardown cleared it, when the request does not decode, or when the callback throws or returns non-zero. The sender turns a non-empty answer into an error: `InvalidArgument` for a request that does not decode, which the outbox drops, and `InternalError` otherwise, which the outbox retries and a resend holds (see Resends over RDMA); a standalone send returns either error to its caller. The receiver logs these, except a non-zero return, and, with metrics built in, counts the two it refuses outright (`recv_no_callback`, `recv_decode`). Older receivers answer empty and older senders ignore the answer, so mixed versions work together.

## Sessions, Sequence Numbers and Missing Ranges

The sender stamps every notification to a remote segment with a session, random per target segment name, and the next sequence number of that session. A resend or an outbox retry keeps its stamp. The stamp travels in the notify QP frame and in the Notify RPC, so the receiver can drop a copy that already arrived over another path.

The receiver keeps, per sender session, the highest sequence number seen and an ordered set of missing ranges (the holes of the `dedup_*` keys), each with the time it was opened:

* An arrival above the highest opens one range for the gap, however large, and is delivered.
* An arrival inside a range splits the range and is delivered, however late, while the range is open. Any other arrival at or below the highest is a duplicate and is dropped.
* A range is given up once it is older than `notification/dedup_hole_ttl_ms`, or, oldest first, when a session has more than `notification/dedup_max_holes` ranges; a later arrival in it is dropped. Keep the ttl above the sum of the outbox and resend deadlines (240 s by default), so that no range is given up while the sender may still deliver into it.
* A session first seen mid-stream, as by a receiver that restarted, opens the range below its first sequence number. Each number in it is admitted once, and the range is given up without being counted as a loss.

Memory grows with the number of open ranges, not with throughput: without loss or reordering a session holds none, apart from the range a session first seen mid-stream opens. The sender forgets a target segment it has not notified for 10 minutes, and the receiver a session it has not heard from for the longer of 10 minutes and the ttl; each checks once every 4096 notifications. With `notification/proto` 0 notifications go unstamped and are never dropped as duplicates.

## Receive Queues

What arrives waits for `receiveNotification()` in the queue of the transport that took it (RDMA, TCP or HP TCP), or in the engine's own queue: notifications to its own segment and, with neither TCP transport loaded, those that came over the control plane. Each queue holds at most `notification/recv_backlog_max` notifications; an arrival at a full queue drops the oldest unread one. Size it as the rate at which all senders together send to this receiver times the longest pause in its polling it has to ride out, a stopped receiver included: when it resumes, the senders deliver what they held for it at once. The default holds about 65 s at 1,000 per second, or about 22 s from three senders at that rate. The bound counts notifications, not bytes; it keeps any peer that can reach the control plane from growing the receiver's memory without limit.

A transport's queue drops a stamped notification before duplicate detection sees it, so its sequence number stays missing: a copy that arrives later, over another path or as a resend, is still delivered; without one, the missing range is given up and the loss is counted a second time. The engine's own queue checks what came over the control plane on arrival, so a later copy of one it dropped is refused as a duplicate.

## Configuration

Keys are read from the engine configuration, for example the JSON in `MC_TENT_CONF`, where `notification/outbox_max` is `outbox_max` inside a `notification` object. For the numeric `notification/*` keys other than `notification/proto`, a value of 0 or less keeps the default, and larger values are clamped where noted. The `rpc/*` and `peer_health/*` keys are process-wide; the engine constructed last sets them. For example:

```json
{
  "notification": {"recv_backlog_max": 262144, "outbox_deadline_ms": 60000},
  "rpc": {"request_timeout_ms": 5000},
  "peer_health": {"cooldown_ms": 2000}
}
```

`MC_TENT_CONF` takes this JSON or the path of a file holding it. It replaces the whole configuration except the keys the caller set when it built the engine (`metadata_type`, `metadata_servers`, `local_segment_name`, `rpc_server_hostname`, `rpc_server_port`, and the switches set in code such as `transports/force_tcp` and `topology/rdma_whitelist`), so keep the deployment's other keys in it as well.

| Key | Default | Meaning |
|-----|---------|---------|
| `notification/proto` | `1` | `1` stamps notifications for duplicate detection; `0` sends them unstamped. |
| `notification/rpc_fallback` | `true` | Move on to the next transport and finally the engine's own Notify RPC when a transport reports its channel unavailable; also lets a resend fall back to that RPC. |
| `notification/path_probe` | `true` | Try the other RDMA NIC pairs when pair 0 cannot take a new notification. |
| `notification/outbox_max` | `65536` | Entries the outbox holds before it drops its oldest. |
| `notification/outbox_deadline_ms` | `120000` | How long a bound notification may wait in the outbox. At most 24 h. |
| `notification/outbox_retry_ms` | `50` | First retry delay for a peer, doubled per failure. At most 2000. |
| `notification/resend_deadline_ms` | `120000` | How long an RDMA resend is held for a peer whose control plane does not answer or does not take it yet. At most 24 h. |
| `notification/dedup_hole_ttl_ms` | `600000` | How long the receiver waits for a missing sequence number. Keep it above the sum of the two deadlines. |
| `notification/dedup_max_holes` | `65536` | Missing ranges the receiver keeps per sender session. |
| `notification/recv_backlog_max` | `65536` | Notifications each receive queue holds before an arrival drops the oldest unread one. 1 to 1048576; any other integer keeps the default, with a warning. |
| `rpc/request_timeout_ms` | `-1` | Timeout, for connect and call each, of the control-plane calls notifications make: fetching a peer's segment description, bootstrapping a notify QP, the Notify RPC and `probePeerAliveByID()`. 0 or less uses the RPC library's default, 30 s, which other control-plane calls always use. |
| `rpc/probe_timeout_ms` | `2000` | Timeout of the prober's dials. 0 or less uses the request timeout. |
| `peer_health/enable` | `true` | Mark control planes that do not answer and fail calls to them fast. |
| `peer_health/cooldown_ms` | `5000` | How long a mark lasts, doubled per repeated timeout. At most 60000. |
| `peer_health/probe_interval_ms` | `1000` | How often the prober dials marked addresses. 0 or less: no prober. |

## Observability

### Metrics

With TENT built with `-DTENT_METRICS_ENABLED=ON`, ten metric families follow a notification from the sender to the application; without it their recording compiles away. Their exact semantics, label values and drop reasons are in {ref}`TENT Metrics <tent-metrics-notifications>`.

| Metric | Type, label | What it shows |
|--------|-------------|---------------|
| `tent_notify_sent_total` | Counter, `path` | Notifications handed over, by the path that took them |
| `tent_notify_received_total` | Counter, `queue` | Arrivals in each receive queue, copies included |
| `tent_notify_resent_total` | Counter | Resends that went through after a notify QP failed |
| `tent_notify_dropped_total` | Counter, `reason` | Notifications given up on, by reason |
| `tent_notify_dedup_dropped_total` | Counter, `reason` | Arrivals refused as a `duplicate` or `expired` |
| `tent_notify_dedup_holes_dropped_total` | Counter, `reason` | Missing ranges the receiver gave up on (`ttl`, `cap`) |
| `tent_notify_queue_depth` | Gauge, `queue` | Length of the outbox, the resend queues and the receive queues |
| `tent_notify_inflight` | Gauge, `transport` | Sends posted on notify QPs and not yet completed |
| `tent_notify_ack_latency_us` | Histogram, `transport` | Notify QP post to send completion, that is, to the peer NIC's ack |
| `tent_notify_bound_delay_us` | Histogram, `path` | Bound notification: transfer seen complete to its first successful send |

A loss shows up in `tent_notify_dropped_total` on the sender, or on the receiver for a notification that did not decode, arrived on a notify QP whose endpoint was gone, or found its receive queue full; a Notify RPC the receiver could not take yet is held by the sender up to its deadline (a standalone send returns the error instead), counted on the receiver only when no callback was registered; one that did not decode is counted on the receiver and dropped by the sender. `tent_notify_dedup_holes_dropped_total` on the receiver counts missing ranges given up. An unstamped notification lost with its notify QP shows in no counter; backlog in `tent_notify_queue_depth`; delivery latency in the two histograms.

### Log keywords

Most drop and fallback lines are logged once per 100 occurrences (the receive-queue line once per 1000) and carry the count so far.

| Keyword | Interpretation |
|---------|----------------|
| `Notification transport X unavailable for segment S, falling back to the next path` | A transport reported its channel unavailable; the next path is tried. |
| `Notification send queue stalled on endpoint E` | A notify QP had no free send slot for 1 s (a new notification on pair 0, or a resend on any pair). When it is pair 0, new notifications skip pairs to the peer's NIC 0 until one of its sends completes. |
| `Peer control plane A marked unreachable for N ms` | `PeerHealth` marked the address; `... reachable again` clears it. |
| `Notification outbox full at N, dropped the oldest` | `notification/outbox_max` reached. |
| `Dropped a notification to segment S past the outbox deadline` | `notification/outbox_deadline_ms` passed. |
| `... unretryable so far` / `... unencodable so far` | A bound notification failed as invalid, or its control-plane leg could not encode it. |
| `Dropped N undelivered notification(s) at shutdown` | Left in the outbox at teardown. |
| `Dropped a notification to segment S after N attempt(s)` | A resend out of paths or refused as malformed, or past `notification/resend_deadline_ms` for a peer that did not answer or did not take it. |
| `Dropped a notification to segment S, N so far: ...` | A resend the control-plane leg could not encode. |
| `Endpoint E hands back N in-flight notification(s) for resend` | A notify QP failed or its endpoint was torn down; its stamped sends in flight go to the resend thread. |
| `Dropped N notification resend(s) ...` | Resends past the deadline behind a held one, or left when the RDMA transport stopped. |
| `Gave up on notifications A..B of session S` | Receiver: a missing range expired or was evicted. |
| `Notification receive queue Q full at N, dropped the oldest unread` | Receiver: `notification/recv_backlog_max` reached; `Q` is the queue, as in the metrics. |
| `Notification dropped: endpoint E was torn down before the completion was handled` / `Received notification from unknown QP` | Receiver: a notification arrived on a notify QP whose endpoint was already gone; its NIC acked it, so the sender does not resend it. |
| `Dropped notifications delivered before or given up on` | Receiver: a duplicate, or a late arrival in a range given up; refused in a poll, or on arrival for the engine's own queue. |
| `onNotify failed` | Receiver: a Notify RPC whose request did not decode (answered as malformed; the sender drops it), or whose receive callback threw (answered; the sender keeps it, or a standalone send returns the error). A callback that returns non-zero is answered the same way, without this line. |
| `Dropped a Notify RPC that arrived with no notification callback registered` | Receiver: a Notify RPC came before the engine registered its callback or after teardown cleared it; the sender is told and keeps it, or a standalone send returns the error. |

## Advice for PD Adapter Layers

A prefill/decode connector can carry its "KV written" signal as a bound notification instead of a side channel: the prefill side passes it to the `submitTransfer()` call of the KV writes and the decode side polls `receiveNotification()`. From Python (`mooncake.engine.TransferEngine` with `MC_USE_TENT=1`), pass `notify=TransferNotify(name, msg)` to `transfer_sync()` or `batch_transfer_sync()` (the async forms take none), poll `get_notifies()`, and set the keys above through `MC_TENT_CONF`; see the [Transfer Engine Python API](../../api-reference/python/transfer-engine.md). The guarantees above cover delivery; the connector still has to:

* **Name the notification after the request.** Put the request id in `name` and match on it. Never rely on arrival order. Drop a notification whose request is no longer waiting because it already finished or failed.
* **Poll the transfer to completion on the prefill side.** The notification leaves from the status poll that sees the transfer complete; unless the progress worker is enabled, that is the connector's own poll. The Python sync forms poll for the caller.
* **Wait with its own timeout, sized to the deadlines.** A late notification is not lost while the sender lives: the outbox may deliver it up to `notification/outbox_deadline_ms` after the transfer completed, and a resend up to `notification/resend_deadline_ms` later still, so a shorter timeout fails some requests whose KV did land. A sender that died or gave up never sends it, and a failed transfer sends nothing by design.
* **On timeout, fail the request but keep its blocks.** The engine has no fencing: nothing cancels RDMA writes the prefill side already posted, and a prefill side that is paused, overloaded or cut off only on the control-plane network keeps its QPs, so its writes can still land. `TransferEngine::probePeerAliveByID(segment)` (in Python `send_probe(peer_server_name)`, 0 when alive) tells a slow peer from one that does not answer, and neither answer makes the blocks safe: reuse them only once the prefill side confirms its transfer has ended, or its process is known to have exited, which destroys its QPs.
* **Keep polling.** Call `receiveNotification()` from a loop that runs while any request is outstanding; each call returns only what arrived since the last one. A queue at `notification/recv_backlog_max` drops its oldest unread notification per arrival, which the connector then sees only as a timeout, so keep the bound above the number of requests a decode instance can have outstanding.
* **Lower `rpc/request_timeout_ms` to what PD tolerates.** The default of 30 s bounds how long a standalone send, an outbox send and `probePeerAliveByID()` can wait on a silent peer. The key is process-wide, and a value below what a loaded peer needs to answer marks that peer unreachable for `peer_health/cooldown_ms`.
* **Report failures as failures.** Tell the decode side explicitly when the prefill side cannot serve a request, and finish a request on the decode side when it learns of a failure or its timeout fires.

At the time of writing, the vLLM connector in `mooncake-wheel/mooncake/mooncake_connector_v1.py` does not use notifications; it signals completion over a ZMQ side channel.

## Known Limitations

* **TCP-only engines.** Without the RDMA transport, the poll that completes a transfer makes the Notify RPC itself, so a silent peer holds that poll for about one RPC request timeout.
* **One outbox thread.** Peers are served one send at a time. A silent peer's first dial holds the thread for one RPC request timeout (later dials fail fast while the peer is marked), and peers reached only over RPC get one RPC at a time; on one host over loopback, with small notifications, the thread made about 18,000 to 20,000 such RPCs per second.
* **Teardown.** Engine teardown joins the outbox thread and may wait one RPC request timeout for the send in flight.
* **Outbox bound.** Beyond `notification/outbox_max` (65536) entries the oldest is dropped and counted. Size it as the rate of bound notifications times the longest peer stop it has to ride out: the default holds about 65 s at 1,000 per second. The outbox sends one entry at a time and later entries for a peer wait behind it, so while that peer's control plane is slow, each entry that needs the RPC leg holds up the rest.
* **Order.** Order among bound notifications to one peer is best effort, as described above.
* **Session tables are pruned independently.** The sender and the receiver forget idle sessions on their own schedules. If the receiver forgets a session that the sender still uses, a late copy of an old sequence number is admitted once, as the start of a new session.
* **A receiver that dies.** What its NIC acked or its control plane accepted and the application had not yet taken is lost: the sender saw it delivered and does not resend it. With a receiver that kept polling under load, that was at most one notification per kill, or up to four when the receiver ran slower under a sanitizer build, against 257 before notify QP failures were resent; closing it needs an acknowledgement at the application level.
* **A receiver that cannot take a Notify RPC for long.** A bound notification or resend whose Notify RPC the receiver answers it cannot take yet (no callback registered, or the callback failed) is held and retried by the sender's outbox or resend queue, up to `notification/outbox_deadline_ms` or `notification/resend_deadline_ms`; past that it is dropped and counted on the sender. A standalone send returns the error to its caller instead. An older receiver answers empty, so a notification it could not take is lost unseen.
* **After a stall of pair 0.** A dial that fails with anything but a timeout is not remembered, so the next notification dials that pair again. The peer's NIC 0 is blamed even when the local NIC of pair 0 is at fault.
* **A peer NIC that stops acknowledging behind another pair.** Only a slot wait on pair 0 makes a peer NIC suspect. A notification on another pair, as a bound notification takes when pair 0 is not up, never waits for a slot there, so a pair that is up to a peer NIC that stopped acknowledging takes up to 256 sends before the next pair is tried. Stamped ones come back for a resend when that QP times out (in tests, 3.5 to 3.8 s after the peer NIC stopped) or its endpoint is torn down: late, not lost.
* **Stale pooled connections.** A notify QP bootstrap over a pooled connection that went stale when the peer restarted returns its RPC error without the RPC fallback, which would have retried it once; the next attempt uses a fresh connection. This holds for pair 0's own dial as well as for the dials after a stall.
* **Runtime queue.** Its progress pass stops at the first queued transfer whose poll fails, as described above.
* **Large notifications.** One that fits a 64 KB notify QP slot only without the 44-byte frame header is sent unstamped, outside duplicate detection; one that does not fit at all goes over the control plane.
* **Peer health.** Only a timeout marks a peer (see Peer health). A peer that refuses connections is dialed by every call to it, twice by a notify, a probe or a segment description fetch, which try once more after such a failure.
* **A host down on the local subnet.** A dial to it fails with "no route to host" after about 3 s, once ARP gives up. That is not a timeout, so the host is not marked and every dial to it waits that long; a notify, a probe or a segment description fetch dials twice, so each call to the host takes about 6 s, the outbox thread's and the resend thread's included. Setting `rpc/request_timeout_ms` below 3000 makes the dial time out first, and the peer is then marked.
* **Memory.** Each RDMA endpoint holds two 16 MiB notify buffers (`kNotifyBufferSize` x `kNotifyMaxPendingSends`, one per direction). After an endpoint is rebuilt, glibc keeps the freed pair as heap high water, so resident memory can grow by up to 32 MiB per rebuilt endpoint until the allocator reuses it; in the measured runs memory in use stayed flat (pinning glibc's mmap threshold removed the steps).
