# Scatter-Gather Control Plane for Engram and Beyond

## Why a new design is needed

The current cross-node Engram remote-gather path is functionally correct, but its
control path is still heavier than the data path we want to drive. The expensive
part is no longer the gather callback itself. It is the protocol around it:

- large sparse requests still need to be described and submitted
- descriptor payloads should not travel through the hot control socket
- the producer should be able to start work without a large synchronous request/reply exchange
- the submission substrate should be reusable by workloads beyond Engram

The next design therefore has two goals:

1. make the scatter-gather submission path a general Mooncake capability
2. add an Engram-specific planner on top of that capability

## Goals

### Performance goals

- move cross-node scatter-gather control latency from `~1.2-1.5 ms` toward
  `100-200 us`
- eliminate large JSON encode/decode from the hot path
- stop sending large descriptor payloads over the socket control plane
- leave the data path on RDMA / TE transfer primitives

### Architecture goals

- make the transport generic enough for workloads beyond Engram
- separate logical planning from physical scatter-gather execution
- support future zero-copy and low-copy submit paths
- keep the doorbell path small enough to later map to lower-level signaling
  primitives

## Design overview

The design has three layers.

```text
logical request (optional)
    |
    v
planner layer
    |
    v
binary SG descriptor
    |
    v
descriptor ring / request pool
    |
    v
small doorbell submit
    |
    v
remote executor
    |
    v
completion / ack
```

### Layer 1: Binary SG protocol

This layer defines a transport-neutral binary descriptor format for one scatter-
gather request.

It is the generic physical plan representation.

### Layer 2: Descriptor ring + tiny doorbell

This layer defines how descriptors are submitted to the remote node.

Large descriptors do not travel through the control socket. They are written into
a pre-registered descriptor ring or request pool. The control plane only sends a
small doorbell that identifies the descriptor slot.

### Layer 3: Planner

This layer is workload-specific.

For Engram, the planner receives a logical request and produces one or more binary
SG descriptors. Other workloads may build descriptors directly and bypass the
planner.

## Binary SG protocol

The binary SG protocol is the long-term common descriptor format used by Mooncake's
scatter-gather path.

### Requirements

- fixed-size header for cheap validation
- packed POD arrays for cache-friendly parsing
- explicit versioning
- no JSON or schema parsing in the hot path
- flexible enough for gather, scatter, and gather-then-write requests

### Descriptor model

A descriptor is made of:

1. a fixed header
2. a source-object table
3. a destination-object table
4. a packed range array

Conceptually:

```text
SgDescriptorHeader
SgObjectDesc src_objects[src_object_count]
SgObjectDesc dst_objects[dst_object_count]
SgRangeDesc  ranges[range_count]
```

### Suggested header fields

- `version`
- `opcode`
- `flags`
- `request_id`
- `src_object_count`
- `dst_object_count`
- `range_count`
- `completion_target`
- `reserved`

### Suggested object descriptor fields

- `object_id` or `buffer_handle`
- `segment_id`
- `base_offset`
- `length`
- `transport_flags`

### Suggested range descriptor fields

- `src_object_idx`
- `dst_object_idx`
- `src_offset`
- `dst_offset`
- `length`

For the hot path, this should stay plain-old-data and naturally aligned.

### Immediate follow-up optimizations

Once the binary format is in place, we can add:

- adjacent-range merge
- same-object delta encoding
- 32-bit compact offsets for small descriptors
- plan caching or template reuse

These are optional follow-up optimizations, not prerequisites.

## Descriptor ring and tiny doorbell

The descriptor ring is the transport-level submit queue.

### Core idea

- each producer-consumer pair owns one or more request rings
- the requester writes the binary descriptor into a free slot
- the requester sends a tiny doorbell containing only slot metadata
- the producer reads the descriptor from the ring and executes it
- completion is reported separately

### Why this is needed

Binary descriptors alone remove JSON overhead, but if the full descriptor still
travels through a TCP request/reply path, the control plane still pays for copying
and blocking on a large message.

The ring + doorbell split removes that cost.

### Doorbell contents

The doorbell should be intentionally small, for example:

- `queue_id`
- `slot_id`
- `seq`
- `descriptor_bytes`
- `flags`

The doorbell should be stable enough to later map onto:

- current TE notify path
- long-lived TCP control channels
- eventfd or epoll style local wakeup
- RDMA write-with-imm or CQ data style signaling when the transport stack allows it

### Completion model

Completions are separate from submit doorbells.

Each completion should contain:

- `request_id`
- `status`
- `error_code`
- `bytes_done`
- optional stage timings

The important design rule is:

- submit should become as close to one-way as possible
- the requester should not wait for a synchronous `"success"` reply before the
  remote executor starts working

## Engram planner

Engram should not permanently expose its logical request format as Mooncake's
universal SG API. Instead, it should be implemented as a planner on top of the
common SG substrate.

### Planner inputs

The planner receives:

- layer id
- batch token ids or hashed row ids
- current table layout or version
- destination workspace information
- endpoint placement information

### Planner outputs

The planner produces one or more SG descriptors grouped by remote endpoint.

### Planner responsibilities

- compute physical row ranges
- merge adjacent rows into larger ranges
- bucket work by remote endpoint
- choose staging or destination layout
- optionally use cached plans when the pattern is reusable

### What stays out of the planner

- transport submission details
- notify or doorbell details
- completion polling logic

That separation keeps the planner workload-specific while the transport remains
reusable.

## Execution model

### Same-node path

Same-node reads continue to use the existing local-direct or local buffer fast
paths whenever possible.

### Cross-node path

Cross-node requests use the generic SG submission stack:

1. build physical descriptor directly, or build it via a planner
2. write descriptor into a ring slot
3. send tiny doorbell
4. remote executor resolves source buffers and performs transfer or gather work
5. requester polls or waits for completion

## Rollout plan

The implementation should be done in phases so each step is measurable.

### Phase 0: transport foundation

- keep existing semantics
- replace the notify path's JSON envelope with a binary notify envelope
- keep current request and reply structure for compatibility

This phase does not solve the whole problem, but it gives a lower-overhead notify
substrate that later phases can reuse.

### Phase 1: binary SG descriptor

- replace the current JSON remote-gather request and ack payloads with binary SG
  descriptors and binary completions
- keep the current notify submit path temporarily
- measure how much latency is removed by eliminating JSON planning payloads

### Phase 2: descriptor ring + tiny doorbell

- move large descriptors out of the control socket path
- add per-peer request rings or request pools
- submit with small doorbells only
- move completion to a separate completion queue

This is the main architectural step needed to attack the remaining control-send
latency.

### Phase 3: Engram planner

- add a logical Engram planner that can build descriptors on the requester side
- optionally add a remote planner mode if layout ownership and versioning are
  stable enough
- evaluate whether planner-side compression further reduces descriptor size

### Phase 4: generic API exposure

- expose the scatter-gather substrate as a general Mooncake capability
- keep Engram as the first workload-specific planner built on top

## Failure handling and lifecycle

The final implementation must explicitly define:

- slot ownership and reclamation
- queue overflow or backpressure
- request timeout and retry policy
- peer restart or reconnection semantics
- descriptor version compatibility
- planner layout-version mismatch handling

These semantics are more important than micro-optimizations because the design is
meant to become a generic substrate.

## Current implementation status

This document describes the target architecture.

The first development step on this branch is to introduce a binary notify
envelope in the transfer-engine notify path. That is a transport foundation step:

- it keeps the current API shape
- it removes JSON from the notify envelope itself
- it prepares the notify path to later carry binary SG submissions and doorbells

The later phases still need to be implemented:

- binary SG descriptor for remote gather requests
- descriptor ring or request pool
- tiny doorbell submit path
- Engram planner

## Relationship to Engram

Engram remains the first and most important motivating workload because it exposes
large sparse requests with many small ranges. But the design is deliberately more
general:

- Engram uses the planner layer
- other workloads can submit binary SG descriptors directly
- Mooncake gets a reusable low-latency scatter-gather substrate instead of an
  Engram-only protocol
