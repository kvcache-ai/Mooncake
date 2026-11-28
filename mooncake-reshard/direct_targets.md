# Registered dense chunk targets

`RegisteredTransferEngine` retains allocation registrations and owners over the
existing Transfer Engine executor. `RegisteredDenseTargets` scatters typed
`TiledCOODenseFragment` values into canonical two-dimensional
`RuntimeBindingFragment` views. A chunk is a partial source directory; this API
does not construct or claim a complete weight placement.

Create the transport with `engine`, `endpoint`, and optionally an existing
executor. Supply each allocation's process-local owner to
`register_buffer(address, capacity, owner)`. Repeated registration requires the
same address, capacity, and owner identity. `unregister_buffer` and `close` use
the shared pending-registration cleanup path.

Call `RegisteredDenseTargets(transport).begin(bindings)` before reading a
generation. Each binding identifies one canonical view and retains its actual
allocation owner; aliases share one registration. Frameworks normalize vectors
to their canonical two-dimensional geometry. `read_dense(endpoint,
source_address, capacity, fragments, itemsize, validate_target=callback)` checks
all spans before submitting any transfer. The optional callback revalidates each
used framework view once per call, under the framework's allocation mutation
fence. It must raise if storage, dtype, shape, or strides changed.

Lowering retains no per-row runtime fragments and submits at most 8192 physical
segments per batch. Source offsets describe the packed chunk, while target
offsets use the binding's byte strides and storage offset. Bounds are checked by
the shared `TransferBatchRange` contract; batches use `TransferBatch.from_ranges`
and the existing terminal-aware executor.

Call `end(terminal=True, retain_registration=True)` only after application and
postload fences complete. Reuse requires a fresh `begin(bindings)` snapshot.
`release()` drops cached registrations before framework sleep or replacement.
Any failure after transfer submission poisons the generation. Unknown or
interrupted completion retains owners through the executor's pending manager;
`drain_pending()` restores transport usability only after terminal completion.
It does not clear a poisoned dense generation or acknowledge consumed weights.
Registration cleanup failures also retain owners until pending cleanup drains.

The framework owns tensor/view mapping, allocation mutation exclusion, producer
and consumer stream fences, metadata and source lease identity, directory/RPC
operations, ACK ordering, sleep/wakeup, padding, and model postload. An adapter
must keep source publications leased until transfer and consumer completion.
No Ray, torch, Store descriptor, or second transport protocol is introduced.
