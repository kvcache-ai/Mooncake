#!/usr/bin/env bash

set -euo pipefail

readonly ACTION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly FIXTURE_DIR="$ACTION_DIR/testdata"
readonly SUMMARY_FILE="$(mktemp)"

trap 'rm -f "$SUMMARY_FILE"' EXIT

GITHUB_STEP_SUMMARY="$SUMMARY_FILE" "$ACTION_DIR/summarize.sh" \
    "CTest failure fixture" \
    "$FIXTURE_DIR/last-tests-failed.txt" \
    "$FIXTURE_DIR/last-test.txt"

diff -u "$FIXTURE_DIR/expected-summary.md" "$SUMMARY_FILE"
