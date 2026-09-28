#!/usr/bin/env bash

set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
    echo "ripgrep (rg) is not installed; this gate cannot run without it" >&2
    exit 1
fi

raft_dir="src/raft"

# Matches in code only. Line comments and rustdoc are skipped so that a doc
# comment may name a banned construct in order to explain why it is banned —
# the storage backends must tell callers that `commit`/`recover` block and have
# to be offloaded by the runtime driver, and they cannot say so without naming
# it. Trailing comments after code are still matched.
fail_if_found() {
    local description="$1"
    local pattern="$2"
    local matches

    matches="$(rg --line-number --glob '*.rs' -- "$pattern" "$raft_dir" \
        | grep -Ev '^[^:]+:[0-9]+:[[:space:]]*//' || true)"

    if [[ -n "$matches" ]]; then
        printf '%s\n' "$matches" >&2
        echo "forbidden in $raft_dir: $description" >&2
        exit 1
    fi
}

fail_if_found "asynchronous consensus code" '\basync\b|\.await'
fail_if_found "Tokio in the consensus core" 'tokio'
fail_if_found "wall-clock reads in the consensus core" 'Instant::now'
