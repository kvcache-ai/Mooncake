---
orphan: true
---

# Design Documents

Architecture and implementation details for Mooncake's storage, transfer, and
distributed execution components.

## Core Architecture

| Document | Description |
|----------|-------------|
| [Mooncake Architecture](architecture) | KVCache-centric disaggregated serving architecture. |
| [Mooncake Store](mooncake-store) | Distributed object and KV cache storage design. |
| [Transfer Engine](transfer-engine/index) | High-performance data movement architecture and transports. |
| [P2P Store](p2p-store) | Peer-to-peer checkpoint and object transfer design. |

## Serving and Cache Systems

| Document | Description |
|----------|-------------|
| [HiCache](hicache-design) | Hierarchical KV cache design. |
| [Engram](engram) | Distributed serving and cache architecture. |
| [Unified Parallel Tensor I/O](unified-parallel-tensor-io) | Parallel tensor storage and transfer model. |
| [SSD Offload](ssd-offload) | SSD-backed cache hierarchy design. |
| [SSD Free-Ratio-First Allocation](ssd-free-ratio-first-allocation) | Capacity-aware replica placement strategy. |

## Distributed Execution and Routing

| Document | Description |
|----------|-------------|
| [Mooncake Backend (PG)](mooncake-backend-pg) | Fault-tolerant PyTorch process-group backend. |
| [Mooncake EP](mooncake-ep) | Expert-parallel communication and recovery. |
| [TENT](tent/overview) | Next-generation transfer engine design. |
| [TENT Benchmark](tent/tebench) | TENT benchmark framework and methodology. |
| [Conductor](conductor/conductor-architecture-design) | Cache-aware request routing architecture. |
