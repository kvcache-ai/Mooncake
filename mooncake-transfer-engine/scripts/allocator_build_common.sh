#!/bin/bash

resolve_allocator_script_dir() {
    cd "$(dirname "$(readlink -f "$0")")" &>/dev/null && pwd
}

prepare_allocator_build_env() {
    OUTPUT_DIR=${1:-.}
    local include_list=${2:-}

    SCRIPT_DIR=$(resolve_allocator_script_dir)
    include_list="${include_list:+${include_list} }${SCRIPT_DIR}/../include"

    INCLUDE_FLAGS=""
    if [ -n "$include_list" ]; then
        INCLUDE_FLAGS=$(printf '%s\n' "$include_list" | tr ' ' '\n' | sed '/^$/d; s/^/-I/' | paste -sd' ' -)
    fi

    mkdir -p "$OUTPUT_DIR"
}
