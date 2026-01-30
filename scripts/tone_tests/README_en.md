# Mooncake E2E Test Guide

## Overview

This directory contains end-to-end (E2E) test cases for the Mooncake project. These test cases are used for CI testing and are executed on the [T-One](https://tone.openanolis.cn) platform.

## Test Environment

- **Current Support**: 2 A10 GPU servers
- **Future Expansion**: May support more machine configurations

## Supported Test Cases

### 1. test_1p1d_erdma.sh

**Description**: Distributed inference test with Prefill-Decode disaggregation architecture

### 2. test_hicache_storage_mooncake_backend.sh

**Description**: Integration test for HiCache storage system with Mooncake backend

### 3. test_disaggregation_different_tp.sh

**Description**: Prefill-Decode disaggregation test with different TP configurations

## T-One/tone-cli Support

[tone-cli](https://gitee.com/anolis/tone-cli/tree/master/tests/mooncake-ci-test) provides the following automation features:

1. **Code Fetching**: Automatically pulls E2E test code from GitHub
2. **Test Execution**: Automatically executes test case sets
3. **Result Parsing**: Reads `test_results.json` file to parse test results
4. **Log Collection**: Copies test logs to T-One platform accessible paths

## E2E Test Case Development Guidelines

### 1. Script Wrapper Requirements

- **Must Use Shell Script Wrapper**: Regardless of using Python or other languages, tests must be wrapped in Shell scripts
- **Script Location**: Place in the `scripts/` directory
- **Naming Convention**: Start with `test_` (e.g., `test_1p1d_erdma.sh`)

### 2. Variable Declaration Requirements

Declare the following variables at the beginning of the script:

```bash
# Test case name (required)
test_case_name="test_demo"

# Test type (required): single (single machine) or double (dual machine)
TEST_TYPE="single"  # or "double"
```

**Important Notes**:
- `test_case_name`: Must match the script file name (without `.sh`)
- `TEST_TYPE`: Used by `run_test.sh` to determine environment preparation type
  - `single`: Single machine test, runs only on local machine
  - `double`: Dual machine test, requires coordination between local and remote machines

### 3. Path Usage Guidelines

- **Avoid Relative Paths**: Don't use `./` or `../` relative paths
- **Use Environment Variables**: Utilize `$BASE_DIR`, `$TEST_CASE_RESULT_DIR`, etc.
- **Recommended Practice**:
  ```bash
  BASE_DIR=${TONE_TESTS_DIR}
  TEST_CASE_RESULT_DIR=${TONE_TESTS_DIR}/${TEST_CASE_RESULT_PATH}
  ```

### 4. Directory Structure Requirements

All test-generated files must be saved in the `tone_tests/run/` directory:

```
tone_tests/run/
├── logs/                           # Test logs root directory
│   ├── test_1p1d_erdma/           # Log directory for each test case
│   │   ├── test_results.json      # Test results (required)
│   │   ├── Qwen__Qwen3-8B/        # Model-related logs (if applicable)
│   │   │   ├── sglang_server_local.log
│   │   │   ├── sglang_server_remote.log
│   │   │   ├── load_balancer.log
│   │   │   └── curl_response.log
│   │   └── ...
│   └── test_disaggregation_different_tp/
│       ├── test_results.json
│       └── test_disaggregation_different_tp.log
├── whls/                           # Wheel packages
│   └── mooncake-*.whl
├── pids/                           # Process PID records (for cleanup)
│   └── test_case_name/
│       ├── server_prefill.pid
│       └── proxy.pid
└── .shrc                           # Environment variable configuration file
```

**Path Specifications**:
- Use variables for log paths: `${BASE_DIR}/run/logs/${test_case_name}/`
- Reference via `TEST_CASE_RESULT_PATH` variable: `run/logs/${test_case_name}`
- Avoid hardcoded paths, use environment variables to ensure portability

### 5. Self-Contained Test Principle

Test case scripts should follow the self-contained principle, but environment preparation is managed uniformly by `run_test.sh`:

**Managed by `run_test.sh`**:
- Environment initialization (create directories, generate `.shrc` configuration file)
- Docker container management (pull images, start/stop containers)
- Wheel package download and installation
- Single/dual machine environment determination and preparation
- ERDMA driver installation
- Remote machine synchronization and configuration (dual machine tests)

**Test scripts need to implement**:
- `run_test()` function: Execute specific test logic
- `parse()` function: Parse test results and generate `test_results.json`
- Optional other functions (e.g., `start_server`, `run_proxy`, etc.)

**Reuse Common Functions** (from `common.sh`):
  - `setup_directory` - Create directory
  - `setup_log_directory` - Create/clean log directory
  - `get_whl` - Download wheel package
  - `get_image` - Pull Docker image
  - `docker_launch` - Launch Docker container
  - `clean_container` - Clean up container
  - `stop_container` - Stop container
  - `check_server_ready` - Check server readiness
  - `check_proxy_ready` - Check proxy readiness
  - `save_test_result` - Save test result to JSON file
  - `launch_and_track_process` - Launch process and record PID
  - `kill_process` - Stop process based on PID file

### 6. Test Result Format Requirements

**Required File**: `tone_tests/run/logs/${test_case_name}/test_results.json`

**JSON Format**:

```json
{
  "test_case": "test_1p1d_erdma",
  "status": "Pass",
  "timestamp": "2025-12-08T12:50:20Z"
}
```

Or when failed:

```json
{
  "test_case": "test_1p1d_erdma",
  "status": "Fail",
  "timestamp": "2025-12-08T12:50:20Z"
}
```

**Field Descriptions**:
- `test_case`: Test case name (required)
- `status`: Test status, `Pass` or `Fail` (required)
- `timestamp`: UTC timestamp in ISO 8601 format (optional)

### 7. Script Structure Example

#### Test Script Example

```bash
#!/bin/bash

# Test case basic information
test_case_name="test_demo"
TEST_TYPE="single"  # Single machine test

# Initialize paths and load common functions
BASE_DIR=${BASE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)}
. ${BASE_DIR}/scripts/common.sh

run_test()
{
    echo "===== Running pytest tests ====="
    local log_file="${BASE_DIR}/${TEST_CASE_RESULT_PATH}/${test_case_name}.log"

    echo "Running tests in container and saving output to: $log_file"
    ${docker_exec} "\
        cd /test_workspace && \
        python3 -m pytest test_demo.py -v -s --tb=long" | tee "$log_file"
    
    return ${PIPESTATUS[0]}
}

parse()
{
    local test_exit_code=$1

    echo "===== Parsing test results ====="
    if [ $test_exit_code -eq 0 ]; then
        save_test_result "$test_case_name" "Pass" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        echo "✓ Test PASSED"
        return 0
    else
        save_test_result "$test_case_name" "Fail" "${BASE_DIR}/${TEST_CASE_RESULT_PATH}"
        echo "✗ Test FAILED"
        return 1
    fi
}

# Script entry point
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    exit_code=0
    if ! run_test; then
        exit_code=1
    fi
    
    parse $exit_code
    exit $?
fi
```

## Usage

### Via run_test.sh (Recommended)

`run_test.sh` is the unified entry point for test cases, responsible for the complete lifecycle management of environment preparation, test execution, and resource cleanup.

#### Run a Single Test Case

```bash
cd scripts/tone_tests/scripts
./run_test.sh run-single test_hicache_storage_mooncake_backend.sh
```

Execution flow:
1. Prepare environment based on test type (`TEST_TYPE`) (single or dual machine)
2. Create and configure Docker containers
3. Download and install Mooncake wheel package
4. Execute the test script's `run_test()` function
5. Call the `parse()` function to parse results
6. Clean up environment and containers

#### Run All SGLANG Test Cases

```bash
cd scripts/tone_tests/scripts
./run_test.sh run-all SGLANG
```

Will execute all tests in the `All_TEST_SCRIPTS_SGLANG` array in order:
- `test_hicache_storage_mooncake_backend.sh`
- `test_disaggregation_different_tp.sh`
- `test_1p1d_erdma.sh`

#### View Help Information

```bash
./run_test.sh
```

Output:
```
Mooncake CI Controller
Usage: ./run_test.sh <command> [args]

Commands:
  run-single <test_name>             - Full lifecycle: setup -> run -> parse -> cleanup
  run-all [SGLANG|VLLM]              - Run all tests for specific framework
  run-all SGLANG                     - Run all SGLANG tests (using SGLANG image)
  run-all VLLM                       - Run all VLLM tests (using VLLM image)
```

## Important Notes

1. **Environment Variable Usage**: Use environment variables like `$BASE_DIR`, `$TEST_CASE_RESULT_PATH`, avoid hardcoded paths
2. **Log Completeness**: Ensure all important logs are saved to the `${BASE_DIR}/run/logs/${test_case_name}/` directory
3. **Resource Cleanup**: Test scripts should manage processes via PID files to ensure proper resource cleanup
4. **JSON Format**: Strictly follow JSON format specifications, must include `test_case` and `status` fields
5. **Test Type Declaration**: Must declare `TEST_TYPE` variable at the beginning of the script for `run_test.sh` to determine environment preparation type

## Related Links

- [T-One Platform](https://tone.openanolis.cn)
- [tone-cli Repository](https://gitee.com/anolis/tone-cli/tree/master/tests/mooncake-ci-test)
