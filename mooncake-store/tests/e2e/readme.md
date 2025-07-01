# Mooncake Store E2E Tests

This directory contains end-to-end (E2E) tests for the Mooncake Store system. These tests verify the functionality, reliability, and fault tolerance of the distributed storage system under various conditions.

## Overview

The E2E test suite includes several executable programs designed to test different aspects of Mooncake Store:

- **clientctl**: An interactive client control tool for manual testing.
- **chaosctl**: A highly configurable tool for conducting chaos tests.
- **e2e_rand_test**: Long-term randomized end-to-end testing.
- **chaos_test**: Short-term chaos testing with predefined scenarios.
- **chaos_rand_test**: Long-term randomized chaos testing with configurable parameters.

## Parameters

All test programs support configurable parameters, such as `--protocol`, `--master_path`, etc. Use the `--help` flag to see detailed information.

## Executable Programs

### clientctl

**Brief**: An interactive client control tool for convenient and straightforward manual testing and debugging. This tool can start multiple clients and perform various operations like `put`, `get`, and `mount` from a specified client.

**Usage**:
1. Start the transfer-engine's meta server and `mooncake_master`. If using HA mode, also start the etcd servers.
2. Launch `clientctl` and manually type in commands. Several predefined scenarios can be found in the `client_ctl_cases` directory.

**Commands**:
- `create [name] [port]`: Create a new client instance.
- `put [client_name] [key] [value]`: Store a key-value pair via the specified client.
- `get [client_name] [key]`: Retrieve a value by key via the specified client.
- `mount [client_name] [segment_name] [size]`: Mount a memory segment from the specified client.
- `remove [client_name]`: Remove a client instance.
- `sleep [seconds]`: Pause execution for the specified duration.
- `terminate`: Exit the program.

### chaosctl

**Brief**: A highly configurable tool for conducting chaos tests. This program starts multiple master instances and clients. The clients continuously send requests to the masters and verify the results. During the test, the program randomly kills or restarts master and client processes. After the test, it prints a test report.

**Usage**:
1. Start the transfer-engine's meta server and etcd servers.
2. Run the chaos test with the desired parameters.
3. After the test completes, a test report will be generated. If `TEST_ERROR_STR` is not zero, it indicates undesirable errors occurred — possibly due to misconfigurations or underlying bugs. Users can check the log files for detailed information.

### e2e_rand_test

**Brief**: This randomized end-to-end test starts one or several masters and clients, sends requests from clients to masters, and verifies the results. It simulates a stable cluster (no crashes or network partitions) to test normal behavior. This is a long-term test that may run for hours.

**Usage**:
1. Start the transfer-engine's meta server and etcd servers.
2. Run the test.

**[WIP]**:
Currently it only has few test cases. Will add more in the future.

### chaos_test

**Brief**: Chaos testing using predefined fault scenarios.

**Usage**:
1. Start the transfer-engine's meta server and etcd servers.
2. Run the test.

### chaos_rand_test

**Brief**: Randomized chaos testing with configurable parameters. This test launches multiple servers and clients, then verifies their behavior under fault conditions. Currently, only master crashes are simulated. This is a long-term test that may run for hours.

**Usage**:
1. Start the transfer-engine's meta server and etcd servers.
2. Run the test.

**[WIP]**:
Currently it only has few test cases. Will add more in the future.