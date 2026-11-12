# Traffic Recording and Replay

Mooncake Store Master can record the RPC requests it receives to local files and
replay them later. Recording runs inside the Master process, records requests
only, and does not change the Master service API.

## Quick Start

Create a configuration file:

```json
{
  "output_dir": "/var/log/mooncake/traffic"
}
```

Start the Master with the configuration path:

```bash
export MOONCAKE_TRAFFIC_CONFIG=/etc/mooncake/traffic.json
./build/mooncake-store/src/mooncake_master \
  --port=50051 \
  --rpc_address=127.0.0.1
```

Recording starts automatically. The Master writes
`/var/log/mooncake/traffic/control.token` with mode `0600`.

Query the recorder:

```bash
TOKEN=$(cat /var/log/mooncake/traffic/control.token)
curl -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:37901/v1/recording/status
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:37901/v1/recording/stop
```

`stop` answers `202` and the writer drains the in-memory ring before the state
changes from `stopping` to `idle`; an idle recorder answers `409` with
`{"error":"not recording"}`.

Set `MOONCAKE_TRAFFIC_AUTOSTART=0` to leave the recorder idle until
`POST /v1/recording/start`. `start` answers `200` with the current status, `409
{"error":"busy"}` when a session is already running, and `500
{"error":"cannot start recording; see the log"}` when the session cannot be
opened.

## Configuration

| Key | Required | Description |
|-----|----------|-------------|
| `output_dir` | yes | Directory for the control token and session directories. |
| `max_total_bytes` | no | Per-session output limit; `0` disables it. Defaults to 10 GiB. |
| `routes` | no | Route allowlist of numeric keys or method names. Defaults to all routes; an empty list is rejected. |
| `redaction` | no | `none` (default), `metadata`, or `redacted`. |
| `ring_bytes` | no | In-memory ring budget shared by the RPC threads. Defaults to 1 GiB. |
| `max_record_bytes` | no | Largest accepted request. Defaults to 4 MiB, at most `block_bytes`. |
| `block_bytes` | no | Largest block written at once. Defaults to 4 MiB, at most `segment_bytes` and 64 MiB. |
| `segment_bytes` | no | Segment rotation size. Defaults to 256 MiB. |
| `segment_seconds` | no | Segment rotation age; `0` disables it. Defaults to 300. |
| `stop_timeout_ms` | no | Age after which `stop` marks the session incomplete. Defaults to 30000. |
| `control.host` | no | Control address; `127.0.0.1`, `localhost`, or `::1`. Defaults to `127.0.0.1`. |
| `control.port` | no | Control port. Defaults to `37901`. |
| `control.token` | no | Bearer token supplied inline instead of read from a file. |
| `control.token_file` | no | Token file. Defaults to `<output_dir>/control.token`. |

| Variable | Description |
|----------|-------------|
| `MOONCAKE_TRAFFIC_CONFIG` | Configuration file. Recording stays disabled when it is unset. |
| `MOONCAKE_TRAFFIC_AUTOSTART` | `0` leaves the recorder idle until `POST /v1/recording/start`. |
| `MOONCAKE_TRAFFIC_CONTROL_PORT` | Overrides `control.port`. |
| `MOONCAKE_TRAFFIC_WRITER_CPU` | Pins the writer thread to this CPU index on Linux. |

Unknown keys and invalid values fail Master startup. Route names in `routes`
are validated against the registered methods, so a typo fails startup instead
of recording nothing.

The recorder keeps bounded per-thread rings and a single writer thread. A full
ring drops the request and counts it; the RPC handler still runs. `metadata`
copies the body into the ring and omits it from the file, so its memory cost
matches `none`.

Each session directory holds `segment-*.mcr`, `routes.json` (route key to method
name), and `summary.json` (counters and completion flags). `output_dir` and the
session directories are created with mode `0700`; segment, summary, and route
files with `0600`.

Reaching `max_total_bytes` finalizes the segment normally: `format_complete`
stays true and the session replays, while `capture_complete` becomes false.

## Control API

The control server listens on the loopback address. `GET /v1/recording/health`
needs no token; `GET /v1/recording/status`, `POST /v1/recording/start`, and
`POST /v1/recording/stop` require `Authorization: Bearer <token>`. A control
port that cannot be bound fails Master startup and the log names the address.

`format_complete` reports that every file was finalized. `capture_complete` also
requires that no request was dropped, that none failed redaction, that the stop
did not time out, and that the session did not stop at `max_total_bytes`.

## Redaction

`none` writes the serialized request body unchanged. `metadata` writes the
record header without the body. `redacted` decodes allowlisted request types and
replaces each string field with a fixed-length token derived from its content,
so equal strings get equal tokens and the key cardinality and hot-key shape of
the workload survive.

A token is a hash of the value, so the same key shows the same token in every
session and a guessed value can be confirmed by hashing it.

`redacted` requires an explicit `routes` list, and every entry needs a
registered redactor; a route without one fails startup. A body that cannot be
decoded is dropped and counted in `redaction_failed_requests` rather than
written raw. The redactors cover the Put, Upsert, read, existence, and remove
families; other routes cannot be recorded in this mode.

Masking runs on the writer thread, so the raw body passes through the in-memory
ring and a core dump of the Master can still contain request bodies.

## Replay

```bash
cmake --build build --target traffic_replay

./build/mooncake-store/tools/traffic/traffic_replay inspect \
  --dir /var/log/mooncake/traffic/session-1234567890-1234

./build/mooncake-store/tools/traffic/traffic_replay run \
  --dir /var/log/mooncake/traffic/session-1234567890-1234 \
  --target 127.0.0.1:50051 \
  --allow-route BatchGetReplicaList \
  --limit 1000
```

`inspect` prints the segment, block, and record counts, the payload bytes, the
time span and request rate, the redaction mode, and a per-route count.

`--allow-route` accepts a numeric route key, a full method name, or a unique
short name from `routes.json`; repeat it to select several routes. `--allow-all`
sends every route and belongs on a disposable target.

| Option | Description |
|--------|-------------|
| `--dry-run` | Select records without opening a connection. |
| `--rate N` | Limit the send rate to N requests per second. |
| `--speed N` | Replay at N times the recorded pace. |
| `--timeout-ms N` | Bounds connection setup and each response; defaults to 5000. |
| `--limit N` | Stop after N selected records; must be at least 1. |
| `--verbose` | Print per-request progress. |

The tool speaks the Master client framing over one TCP connection and does not
link the Master service library. `metadata` sessions cannot be replayed because
the body was not stored.

To share a session, tar its directory. The archive carries `segment-*.mcr`,
`routes.json`, and `summary.json`, so the receiver can select routes by name.
Record a short window with a small `max_total_bytes` when the session is meant
to travel. Replaying needs a Master in a comparable state: `run` reports `ok`
for every well-formed response and does not decode handler results, so a trace
that reads objects answers `object_not_found` on an empty Master. Check the
Master log for the handler-level outcome.

## Limits

- Only requests are recorded. Results that depend on a response value cannot be
  reconstructed, and service time is not stored: a session shows the request mix
  and arrival pattern behind a slow period.
- A slow or failed output device stops recording and marks the session
  incomplete; it does not block the RPC handler.
- The replay tool uses one connection and sends requests in recorded order, so
  cross-connection concurrency is not recreated.
- `max_total_bytes` applies to one session; old session directories are the
  operator's responsibility.
- A Master killed before it closes the session leaves the last segment without a
  footer. `inspect` and `run` read the blocks that were written, `inspect`
  prints `incomplete=yes`, and the records after the cut are lost.
