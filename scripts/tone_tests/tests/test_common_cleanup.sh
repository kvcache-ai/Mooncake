#!/usr/bin/env bash
set -euo pipefail

TEST_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
COMMON_SH="${TEST_DIR}/../scripts/common.sh"

fail() {
    echo "FAIL: $*" >&2
    return 1
}

test_cuda_cleanup_aggregates_both_nodes() (
    export CI_ACCELERATOR=cuda
    export CONTAINER_NAME=mooncake-cleanup-test
    export REMOTE_IP=192.0.2.2
    test_case_name=cleanup_unit
    # shellcheck disable=SC1090
    source "$COMMON_SH"

    call_log=$(mktemp)
    trap 'rm -f "$call_log"' EXIT
    stop_container() {
        printf '%s\n' "$*" >> "$call_log"
        if [ "$#" -eq 1 ]; then
            return 9
        fi
        return 0
    }

    if cleanup_test_env double; then
        fail "CUDA cleanup succeeded after the local stop failed"
    fi
    [ "$(wc -l < "$call_log" | tr -d ' ')" -eq 2 ] \
        || fail "CUDA cleanup did not attempt both nodes"
)

test_cuda_process_launch_keeps_legacy_pid_tracking() (
    export CI_ACCELERATOR=cuda
    export CONTAINER_NAME=mooncake-cleanup-test
    test_case_name=cleanup_unit
    # shellcheck disable=SC1090
    source "$COMMON_SH"

    temp_dir=$(mktemp -d)
    trap 'rm -rf "$temp_dir"' EXIT
    call_log="$temp_dir/docker.log"
    pid_file="$temp_dir/server.pid"
    process_cmd='CUDA_VISIBLE_DEVICES=6,7 python3 -m server --config '\''{"workers":2}'\'''

    docker() {
        printf '%s\n' "$*" >> "$call_log"
        if [ "$1" = inspect ]; then
            printf '100\n'
        fi
        return 0
    }
    ps() {
        printf '200 100 python3 -m server --config {"workers":2}\n'
    }
    sleep() { return 0; }

    launch_and_track_process "$process_cmd" /tmp/server.log "$pid_file" \
        'python3 -m server' || fail "CUDA legacy process launch failed"
    grep -Fq -- "--config '{\"workers\":2}'" "$call_log" \
        || fail "CUDA launch command lost its quoted JSON"
    [ "$(cat "$pid_file")" = 200 ] \
        || fail "CUDA launch did not record the legacy host PID"
)

test_run_all_propagates_cleanup_failure() (
    export CI_ACCELERATOR=cuda
    export LOCAL_IP=127.0.0.1
    export REMOTE_IP=
    export ARTIFACT_ID=unit-test
    export GIT_REPO=example/Mooncake
    # shellcheck disable=SC1090
    source "${TEST_DIR}/../scripts/run_test.sh"

    temp_dir=$(mktemp -d)
    trap 'rm -rf "$temp_dir"' EXIT
    TONE_TESTS_DIR=$temp_dir
    RUN_DIR="$temp_dir/run"
    mkdir -p "$RUN_DIR" "$TONE_TESTS_DIR/scripts"
    printf 'test_case_name="cleanup_caller"\nTEST_TYPE="double"\nrun_test() { return 0; }\nparse() { return 0; }\n' \
        > "$TONE_TESTS_DIR/scripts/fake_test.sh"
    UNIT_TESTS=(fake_test.sh)

    setup_env_for_test() {
        printf 'export BASE_DIR=%s\nexport TEST_RUN_DIR=%s\n' \
            "$TONE_TESTS_DIR" "$RUN_DIR" > "$RUN_DIR/.shrc"
        return 0
    }
    cleanup_test_env() { return 1; }

    if run_all_tests UNIT_TESTS; then
        fail "run_all_tests masked a cleanup failure"
    fi
    [ "$MOONCAKE_ENV_UNHEALTHY" = true ] \
        || fail "run_all_tests did not quarantine the shared environment"
)

run_test_case() {
    local name=$1
    if "$name"; then
        echo "PASS: $name"
    else
        echo "FAIL: $name" >&2
        return 1
    fi
}

run_test_case test_cuda_cleanup_aggregates_both_nodes
run_test_case test_cuda_process_launch_keeps_legacy_pid_tracking
run_test_case test_run_all_propagates_cleanup_failure
