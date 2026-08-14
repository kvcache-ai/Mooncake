#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <summary-title> <failed-tests-log> <last-test-log>" >&2
    exit 2
fi

: "${GITHUB_STEP_SUMMARY:?GITHUB_STEP_SUMMARY must be set}"

readonly SUMMARY_TITLE="$1"
readonly FAILED_TESTS_LOG="$2"
readonly LAST_TEST_LOG="$3"

{
    echo "## $SUMMARY_TITLE"
    echo

    if [[ -s "$FAILED_TESTS_LOG" ]]; then
        echo "### Failed CTest targets"
        echo '```text'
        cat "$FAILED_TESTS_LOG"
        echo '```'
    fi

    if [[ -s "$LAST_TEST_LOG" ]]; then
        failed_cases=$(grep -E '^\[  FAILED  \]' "$LAST_TEST_LOG" | sort -u || true)
        if [[ -n "$failed_cases" ]]; then
            echo "### Failed test cases"
            echo '```text'
            echo "$failed_cases"
            echo '```'
        fi

        failure_context=$(grep -n -B2 -A8 -E \
            ':[0-9]+: Failure$|ERROR: (AddressSanitizer|LeakSanitizer|ThreadSanitizer|UndefinedBehaviorSanitizer)|runtime error:|Segmentation fault|terminate called' \
            "$LAST_TEST_LOG" | sed -n '1,200p' || true)
        if [[ -n "$failure_context" ]]; then
            echo "### Failure context"
            echo '```text'
            echo "$failure_context"
            echo '```'
        fi
    else
        echo "CTest did not produce LastTest.log. Check the failed step for setup errors."
    fi

    echo
    echo "Download the CTest diagnostics artifact for the complete log and JUnit report."
} >> "$GITHUB_STEP_SUMMARY"
