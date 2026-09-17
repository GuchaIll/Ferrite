#!/usr/bin/env bash

set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
    echo "ripgrep (rg) is not installed; this gate cannot run without it" >&2
    exit 1
fi

raft_dir="src/raft"

fail_if_found() {
    local description="$1"
    local pattern="$2"

    if rg --line-number --glob '*.rs' -- "$pattern" "$raft_dir"; then
        echo "forbidden in $raft_dir: $description" >&2
        exit 1
    fi
}

fail_if_found "asynchronous consensus code" 'async fn|\.await'
fail_if_found "Tokio in the consensus core" 'tokio'
fail_if_found "wall-clock reads in the consensus core" 'Instant::now'
