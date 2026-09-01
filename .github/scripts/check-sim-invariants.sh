#!/usr/bin/env bash

set -euo pipefail

sim_dir="src/sim"

fail_if_found() {
    local description="$1"
    local pattern="$2"

    if rg --line-number --glob '*.rs' -- "$pattern" "$sim_dir"; then
        echo "forbidden in $sim_dir: $description" >&2
        exit 1
    fi
}

fail_if_found "unordered HashMap or HashSet" 'HashMap|HashSet'
fail_if_found "ambient or unstable RNG" 'thread_rng|StdRng|getrandom|rand::random'
fail_if_found "asynchronous simulation code" 'async fn|\.await'
fail_if_found "wall-clock sleep or Tokio time" 'thread::sleep|tokio::time'

instant_calls="$(rg --line-number --glob '*.rs' -- 'Instant::now' "$sim_dir" || true)"
if [[ "$instant_calls" != 'src/sim/clock.rs:'* ]] || [[ "$(printf '%s\n' "$instant_calls" | grep -c .)" -ne 1 ]]; then
    printf '%s\n' "$instant_calls" >&2
    echo 'Instant::now is allowed exactly once, in src/sim/clock.rs' >&2
    exit 1
fi