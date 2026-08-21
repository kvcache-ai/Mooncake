#!/usr/bin/env bash
# Validate the canonical reshard contracts and their required negative examples.
set -euo pipefail

pyright --project python/pyrightconfig.json

if negative_output=$(pyright --project python/tests/typecheck/reshard/negative/pyrightconfig.json 2>&1); then
    echo "Expected invalid reshard contract examples to fail static checking."
    exit 1
fi

printf '%s\n' "${negative_output}"
printf '%s\n' "${negative_output}" | grep -F "ParticipantId" >/dev/null
printf '%s\n' "${negative_output}" | grep -F "SplitAxisKind" >/dev/null
