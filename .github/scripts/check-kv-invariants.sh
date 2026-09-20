#!/usr/bin/env bash

set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
    echo "ripgrep (rg) is not installed; this gate cannot run without it" >&2
    exit 1
fi

kv_dir="src/kv"

# Matches in code only. Line comments and rustdoc are skipped so that a doc
# comment may name a banned construct in order to explain why it is banned.
fail_if_found() {
    local description="$1"
    local pattern="$2"
    local matches

    matches="$(rg --line-number --glob '*.rs' -- "$pattern" "$kv_dir" \
        | grep -Ev '^[^:]+:[0-9]+:[[:space:]]*//' || true)"

    if [[ -n "$matches" ]]; then
        printf '%s\n' "$matches" >&2
        echo "forbidden in $kv_dir: $description" >&2
        exit 1
    fi
}

fail_if_found "unordered HashMap or HashSet" 'HashMap|HashSet'
fail_if_found "wall-clock reads in the state machine" 'Instant::now|SystemTime::now'
fail_if_found "ambient or unstable RNG" 'thread_rng|StdRng|getrandom|rand::random'
