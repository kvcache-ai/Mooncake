# TENT: Transfer Engine NEXT

TENT (Transfer Engine NEXT) is a runtime for point-to-point data movement in heterogeneous AI clusters.
It is the successor to the classic Mooncake Transfer Engine (TE) and is designed to better handle heterogeneous interconnects, dynamic topology, and partial failures.

TENT focuses on one goal: **move data efficiently and reliably without requiring applications to manage transport-specific details**.

## Background

In early deployments, Mooncake TE assumed that a process would bind to a single transport backend (for example, RDMA or NVLink) and use it for all transfers. This model works well in homogeneous environments, but it becomes limiting in modern clusters.

In practice, clusters often combine multiple interconnects. Even within a single job, some peers may be connected by NVLink, others by RDMA, and some only through host memory. Link quality can also change over time due to congestion, hardware resets, or transient failures.

Under these conditions, static backend selection and static multi-rail striping lead to two problems:

1. Transfers cannot adapt to changing connectivity.
2. Slow or degraded links dominate tail latency and reduce effective bandwidth.

TENT was introduced to address these issues by moving more decisions into the runtime.

## Core Design

TENT is based on three design choices.

### Dynamic Transport Selection

Applications using TENT do not select a transport backend directly. Instead, they submit transfer requests that describe **what data to move**, not **how to move it**.

For each request, the TENT runtime determines which transport backends and paths are available between the source and destination. It selects an execution plan at runtime and can change this decision for later requests if conditions change.

If a direct path is not available, TENT automatically constructs a staged transfer, such as moving data through host memory. This logic is handled entirely inside the runtime and does not require application changes.

### Fine-Grained Scheduling with Telemetry

When multiple paths or rails are available, TENT does not rely on static striping. Large transfers are divided into smaller slices, and each slice is scheduled independently.

The runtime uses simple telemetry, such as observed completion time and queue depth, to decide where to send each slice. Slower or congested paths naturally receive fewer slices, while faster paths receive more.

This approach allows TENT to use multiple rails efficiently while avoiding head-of-line blocking caused by a single slow path.

### In-Runtime Failure Handling

Partial failures are common in large clusters. Instead of surfacing these failures to applications, TENT handles them inside the data path.

If a path becomes slow or unavailable, the runtime temporarily stops scheduling slices on that path and continues using other available paths. If an entire backend becomes unavailable, another backend is selected automatically.

Slices are retried when necessary, and recovered paths are added back once they become stable. From the application's perspective, transfers continue to work, possibly with reduced performance for a short period.

## Architecture Overview

At a high level, TENT consists of:

* A declarative API for submitting transfer requests
* A segment abstraction that represents data locations
* A set of pluggable transport backends (RDMA, NVLink, shared memory, etc.)
* A runtime that performs path selection, scheduling, and failure handling
* A low-overhead datapath implemented with worker threads and lock-free queues

Transport backends are intentionally small and focused on data movement. Scheduling and policy decisions are centralized in the runtime.

## Typical Use Cases

TENT is intended for cases where data transfer is on the critical path, such as:

* KVCache movement in large language model inference
* Data exchange between pipeline stages
* Frequent model or parameter updates
* Transfers across mixed accelerator and memory configurations

## Summary

TENT extends the classic Mooncake Transfer Engine by moving transport selection, scheduling, and failure handling into the runtime. This allows applications to run efficiently on heterogeneous and changing hardware without embedding transport-specific logic.

The design favors predictable behavior and operational simplicity over manual tuning and static configuration.

## TENT C++ API Reference 

:::{toctree}
:maxdepth: 1

cpp-api
:::

## TENT Metrics System

:::{toctree}
:maxdepth: 1

metrics
:::