# TENT Documentation

TENT (Transfer Engine NEXT) is a runtime for point-to-point data movement in heterogeneous AI clusters. It is the successor to the classic Mooncake Transfer Engine.

## Documentation Structure

### 📚 Getting Started

- **[Quick Start Guide](getting-started/quick-start.md)** - Get started with TENT C++ API
  - Installation and building
  - Your first TENT program
  - Complete client-server example
  - Common patterns

### 🔧 API Reference

- **[C++ API Reference](api/cpp-api.md)** - Complete C++ API documentation
  - Core APIs: engine creation, memory management, data transfer
  - Advanced APIs: notifications, introspection
  - Type reference

- **[C API Reference](api/c-api.md)** - Complete C API documentation
  - Types and constants
  - All C functions
  - Complete example

- **[Python API Reference](api/python-api.md)** - TENT Python bindings (`tent` module)
  - Exception-based error handling
  - RAII context managers
  - Complete examples

### ⚙️ Configuration

- **[Configuration Reference](configuration/configuration.md)** - Complete configuration options
  - Top-level configuration
  - Transport configuration
  - Policy configuration
  - Environment variables
  - Configuration examples

### ✨ Advanced Features

- **[Transport Selector](features/transport-selector.md)** - Transport selection policies
  - Priority-based selection
  - Configuration-based policies
  - Transport fallback
  - Complete examples

- **[Failover](features/failover.md)** - Failure handling and recovery
  - Cross-transport failover
  - RDMA rail recovery
  - State machine
  - Configuration

- **[QoS (Quality of Service)](features/qos.md)** - Priority and isolation
  - Priority levels
  - Per-worker queues
  - Global coordination
  - Configuration

- **[Metrics System](features/metrics.md)** - Performance monitoring
  - Metrics types
  - Configuration
  - Prometheus integration
  - Performance optimization

- **[Slice Spraying](features/slice-spraying.md)** - Multi-rail optimization
  - Device selector
  - NUMA-aware selection
  - EWMA-based scheduling
  - Smart mode

### 🎯 Operations

- **[Performance Tuning](operations/performance-tuning.md)** - Performance optimization guide
  - Transport optimization
  - Memory optimization
  - Batch optimization
  - Platform-specific tuning
  - Performance scenarios

- **[Troubleshooting](operations/troubleshooting.md)** - Common issues and solutions
  - Engine initialization failures
  - Memory registration failures
  - Transfer failures
  - Performance issues
  - Diagnostic tools

### 📊 Benchmarks

- **[TE Bench Guide](tebench.md)** - Benchmarking tool documentation

## Quick Navigation

### By Use Case

| Use Case | Recommended Reading |
|----------|-------------------|
| **New to TENT** | [Quick Start](getting-started/quick-start.md) → [C++ API](api/cpp-api.md) |
| **Migrating from TE** | [Migration Guide](getting-started/migration.md) → [C++ API](api/cpp-api.md) |
| **Configuration** | [Configuration Reference](configuration/configuration.md) |
| **Performance Issues** | [Performance Tuning](operations/performance-tuning.md) → [Troubleshooting](operations/troubleshooting.md) |
| **RDMA Setup** | [Configuration Reference](configuration/configuration.md) |
| **Advanced Features** | [Failover](features/failover.md) → [QoS](features/qos.md) → [Metrics](features/metrics.md) |

### By Component

| Component | Documentation |
|-----------|---------------|
| **API** | [C++ API](api/cpp-api.md), [C API](api/c-api.md), [Python API](api/python-api.md) |
| **Transport** | [Selector](features/transport-selector.md) |
| **Features** | [Failover](features/failover.md), [QoS](features/qos.md), [Metrics](features/metrics.md) |
| **Operations** | [Performance](operations/performance-tuning.md), [Troubleshooting](operations/troubleshooting.md) |

## Background

### What is TENT?

TENT extends the classic Mooncake Transfer Engine by moving transport selection, scheduling, and failure handling into the runtime. This allows applications to run efficiently on heterogeneous and changing hardware without embedding transport-specific logic.

### Key Features

- **Dynamic Transport Selection**: Automatically selects optimal transport based on conditions
- **Fine-Grained Scheduling**: Slice-based transfers with adaptive load balancing
- **In-Runtime Failure Handling**: Automatic failover without application changes
- **Multi-Platform Support**: CUDA, ROCm, Ascend, and CPU
- **High Performance**: Zero-copy transfers, GPUDirect RDMA, multi-rail support

### Design Philosophy

1. **Declarative over Imperative**: Applications describe *what* data to move, not *how*
2. **Single Initialization**: Configuration objects fully describe engine state
3. **Internal Metadata Management**: Discovery handled inside runtime
4. **Consistent Error Model**: All APIs return `Status` objects

## Related Documentation

- [Transfer Engine (Classic)](../transfer-engine/index.md) - Classic TE documentation
- [Mooncake Store](../mooncake-store.md) - Distributed KVCache store
- [Python API Reference](../../../python-api-reference/transfer-engine.md) - Python bindings

## Getting Help

- **GitHub Issues**: [https://github.com/kvcache-ai/Mooncake/issues](https://github.com/kvcache-ai/Mooncake/issues)
- **Documentation**: [https://kvcache-ai.github.io/Mooncake/](https://kvcache-ai.github.io/Mooncake/)
- **Paper**: [FAST 2025 Award-Winning Paper](https://www.usenix.org/conference/fast25/presentation-qin)
