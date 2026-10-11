# TENT Notifications

A notification is a small `{name, msg}` message from one engine to the engine
that owns a segment. It is either sent on its own with `sendNotification()` or
bound to transfers with `submitTransfer(batch_id, request_list, notifi)`, and
the receiver takes it with `receiveNotification()`.

## Paths

Notification transports are tried in slot order. RDMA, when loaded, sends over
a notify queue pair next to the data queue pairs. TCP sends over the peer's
control plane (the Notify RPC). If every loaded transport reports its channel
unavailable, the engine makes that RPC itself, unless
`notification/rpc_fallback` is `false`.

A standalone `sendNotification()` runs on the calling thread and returns the
status of the path that took the notification, or of the last one tried.

## Notifications bound to transfers

A bound notification is owed once a status poll sees every request of its
`submitTransfer()` call complete. If any of them failed or was canceled,
nothing is owed. The poll that sees the last request complete makes one try.
With the RDMA transport loaded, it goes over a notify channel that is already
up and never dials the peer or waits on its control plane. An engine without
RDMA makes it with its TCP transport (HP TCP if only that is loaded), that is,
the Notify RPC on the polling thread, which waits on the peer's control plane
as a standalone send does. What the try cannot hand over goes to the engine's
outbox:

- One outbox thread, started with the first entry, sends each peer's oldest
  entry over the full path. A peer that fails is retried after
  `notification/outbox_retry_ms`, doubling up to 2 s, and its later entries
  wait behind the oldest, so they leave in the order they came in.
- An entry that has waited `notification/outbox_deadline_ms` is dropped. When
  the outbox holds `notification/outbox_max` entries, the oldest of any peer is
  dropped to make room. The entry out on a send counts toward that bound but is
  never the one dropped, so the outbox exceeds the bound, by one, only with an
  `outbox_max` of 1: a new entry then waits next to the one in flight.
- Freeing the batch does not cancel what the outbox holds. At engine teardown
  the outbox waits for its send in flight, at most one RPC request timeout, and
  drops the rest.

Delivery is therefore asynchronous: a bound notification may arrive after a
standalone notification sent later, and notifications to different peers have
no common order. Order among bound notifications to one peer is best effort:
one owed while the outbox holds the peer's older ones queues behind them, but
two owed at the same time, on concurrent status polls, have no defined order.
With RDMA loaded, a peer whose RDMA notify channel is not up, for example a
TCP-only peer, gets every bound notification through the outbox thread, one
RPC at a time.

A notification counts as delivered once the RDMA send completes or the peer's
control plane answers the Notify RPC, so one the receiver's NIC took just
before the receiving process died is lost. A request that fails to poll does
not stop `getTransferStatus(batch_id, status)` or
`progressBatch(batch_id, status)`: the other requests are still polled, and a
notification whose requests have all completed is owed before the first error
is returned. `getTransferStatus(batch_id, status_list)` still stops at that
request: the requests behind it are polled, and their notifications owed, only
once a later call gets past it. Bound notifications of a transfer never polled
to completion before teardown are not sent.

## Duplicates

With `notification/proto` 1 (the default) the sender stamps each notification
with a random session per target segment and a sequence number. A resend keeps
its stamp, so the receiver can drop a copy that already arrived over another
path. The receiver keeps the sequences it has not seen yet as ranges per
session; a late one is admitted however far behind. A session first seen
mid-stream, as by a receiver that restarted, admits any sequence below the
first one it saw, once. A range is given up after
`notification/dedup_hole_ttl_ms`, or, oldest first, when a session has more
than `notification/dedup_max_holes` of them; a late arrival in it is then
dropped. A session idle for the longer of 10 minutes and that ttl is
forgotten. `notification/proto` 0 sends unstamped notifications, which are
never dropped as duplicates.

## Drops

Apart from notifications of transfers never polled to completion (above),
every notification the engine gives up on is counted and logged; a line that
can repeat is rate-limited and carries the count so far:

| Log line | Cause |
|----------|-------|
| `Notification outbox full at N, dropped the oldest` | `notification/outbox_max` reached |
| `Dropped a notification to segment S past the outbox deadline` | `notification/outbox_deadline_ms` passed |
| `... unretryable so far` | the peer or transport rejected it as invalid |
| `... unencodable so far` | the RPC leg could not encode it |
| `Dropped N undelivered notification(s) at shutdown` | left in the outbox at teardown |
| `Gave up on notifications A..B of session S` | receiver: a missing range expired or was evicted |
| `Dropped notifications delivered before or given up on` | receiver: a duplicate, or a late arrival in a range given up |

## Configuration

For the numeric keys other than `notification/proto`, a value of 0 or less
keeps the default; larger values are clamped where noted.

| Key | Default | Meaning |
|-----|---------|---------|
| `notification/proto` | `1` | `1` stamps notifications for duplicate detection; `0` sends them unstamped. |
| `notification/rpc_fallback` | `true` | Make the Notify RPC when every notification transport reports its channel unavailable. |
| `notification/outbox_max` | `65536` | Entries the outbox holds before it drops its oldest. |
| `notification/outbox_deadline_ms` | `120000` | How long a bound notification may wait in the outbox. At most 24 h. |
| `notification/outbox_retry_ms` | `50` | First retry delay for a peer, doubled per failure. At most 2000. |
| `notification/dedup_hole_ttl_ms` | `600000` | How long the receiver waits for a missing sequence. Keep it above the sender's deadlines. |
| `notification/dedup_max_holes` | `65536` | Missing ranges the receiver keeps per sender session. |
