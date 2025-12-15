# Mooncake E2E Test Guide

## Overview

This directory contains end-to-end (E2E) test cases for the Mooncake project. These test cases are used for CI testing and are executed on the [T-One](https://tone.openanolis.cn) platform.

## Test Environment

- **Current Support**: 2 A10 GPU servers
- **Future Expansion**: May support more machine configurations

## Supported Test Cases

### 1. test_1p1d_erdma.sh

**Description**: Distributed inference test with Prefill-Decode disaggregation architecture

- **Test Scenario**: Deploy Prefill and Decode servers on two separate machines communicating via ERDMA (Elastic RDMA) network
- **Test Coverage**:
  - Local machine runs SGLang server in Prefill mode
  - Remote machine runs SGLang server in Decode mode
  - Proxy requests through a load balancer
  - Send test requests to verify distributed inference functionality
- **Supported Models**: Qwen/Qwen3-8B, deepseek-ai/DeepSeek-V2-Lite
- **Test Environment**: Dual-machine setup with 2 GPUs per machine (TP=2)

### 2. test_hicache_storage_mooncake_backend.sh

**Description**: Integration test for HiCache storage system with Mooncake backend

- **Test Scenario**: Verify the functionality of HiCache storage system using Mooncake as backend
- **Test Coverage**:
  - Start Mooncake metadata and master services
  - Run pytest tests in Docker container
  - Test different memory layouts (page_first, page_first_direct, etc.)
  - Test different models (standard models, MLA models)
  - Verify cache accuracy
- **Test Environment**: Single machine with 2 GPUs (TP=2)

## T-One/tone-cli Support

[tone-cli](https://gitee.com/anolis/tone-cli/tree/master/tests/mooncake-ci-test) provides the following automation features:

1. **Code Fetching**: Automatically pulls E2E test code from GitHub
2. **Variable Replacement**: Scans scripts starting with `test_` in the `scripts/` directory and replaces the following variables:
   - `LOCAL_IP` - Local machine IP
   - `REMOTE_IP` - Remote machine IP
   - `ARTIFACT_ID` - GitHub Actions artifact ID
   - `GIT_REPO` - Git repository address for downloading wheel packages
3. **Test Execution**: Automatically executes compliant test scripts
4. **Result Parsing**: Reads `test_results.json` file to parse test results
5. **Log Collection**: Copies test logs to T-One platform accessible paths

## E2E Test Case Development Guidelines

### 1. Script Wrapper Requirements

- **Must Use Shell Script Wrapper**: Regardless of using Python or other languages, tests must be wrapped in Shell scripts
- **Script Location**: Place in the `scripts/` directory
- **Naming Convention**: Start with `test_` (e.g., `test_1p1d_erdma.sh`)

### 2. Variable Declaration Requirements

Declare the following variables at the beginning of the script:

```bash
# For dual-machine tests
LOCAL_IP=
REMOTE_IP=

# Required for both single and multi-machine tests
ARTIFACT_ID=
GIT_REPO=
test_case_name=
```

### 3. Path Usage Guidelines

- **Avoid Relative Paths**: Don't use `./` or `../` relative paths
- **Use Environment Variables**: Utilize `$BASE_DIR`, `$TEST_CASE_RESULT_DIR`, etc.
- **Recommended Practice**:
  ```bash
  BASE_DIR=${TONE_TESTS_DIR}
  TEST_CASE_RESULT_DIR=${TONE_TESTS_DIR}/${TEST_CASE_RESULT_PATH}
  ```

### 4. Directory Structure Requirements

All test-generated files must be saved in `tone_tests/run/${test_case_name}/` directory:

```
tone_tests/run/test_1p1d_erdma/
├── logs/                    # Test logs
├── whls/                    # Wheel packages
│   └── mooncake-*.whl
├── .shrc                    # Environment variable configuration
└── test_results.json        # Test results (required)
```

### 5. Self-Contained Test Principle

- **Container Management**: Scripts must manage Docker containers independently (pull images, configure parameters, start/stop)
- **Environment Isolation**: Single and multi-machine tests must be completed independently by scripts
- **Reuse Common Functions**: Can use common functions from `common.sh`:
  - `setup_test_result_directory` - Create test result directory
  - `setup_log_directory` - Create log directory
  - `get_whl` - Download wheel package
  - `get_image` - Pull Docker image
  - `docker_launch` - Launch Docker container
  - `clean_container` - Clean up container
  - `stop_container` - Stop container
  - `check_server_ready` - Check server readiness
  - `check_proxy_ready` - Check proxy readiness

### 6. Test Result Format Requirements

**Required File**: `tone_tests/run/${test_case_name}/test_results.json`

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

Recommended script structure:

```bash
#!/bin/bash

test_case_name="test_demo"
TONE_TESTS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && cd .. && pwd)

# Variable declarations
LOCAL_IP=
REMOTE_IP=
ARTIFACT_ID=
GIT_REPO=

. ${TONE_TESTS_DIR}/scripts/common.sh

setup() {
    # Container launch and environment configuration
}

run_test() {
    # Execute tests
}

parse() {
    # Parse results and generate test_results.json
}

cleanup() {
    # Clean up resources
}

main() {
    setup
    run_test
    parse
    cleanup
}

main
```

## Usage

### Execute Full Test

```bash
cd scripts && ./test_demo.sh
```

### Execute on T-One Platform

Test cases will be automatically scanned and executed by tone-cli without manual intervention.

## Important Notes

1. **Environment Variable Priority**: tone-cli will replace variables in scripts
2. **Log Completeness**: Ensure all important logs are saved to the specified directory for troubleshooting
3. **Resource Cleanup**: Execute cleanup even if tests fail to clean up resources
4. **JSON Format**: Strictly follow JSON format specifications to avoid syntax errors

## Related Links

- [T-One Platform](https://tone.openanolis.cn)
- [tone-cli Repository](https://gitee.com/anolis/tone-cli/tree/master/tests/mooncake-ci-test)
