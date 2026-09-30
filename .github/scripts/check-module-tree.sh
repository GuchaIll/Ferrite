#!/usr/bin/env bash

set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
    echo "ripgrep (rg) is not installed; this gate cannot run without it" >&2
    exit 1
fi

roots=()
for root in src/lib.rs src/main.rs src/bin/*.rs src/bin/*/main.rs; do
    [[ -f "$root" ]] && roots+=("$root")
done

# Newline-delimited set; macOS ships bash 3.2, which has no associative arrays.
reachable=$'\n'
queue=("${roots[@]}")
failed=0

while ((${#queue[@]} > 0)); do
    file="${queue[0]}"
    queue=("${queue[@]:1}")
    [[ "$reachable" == *$'\n'"$file"$'\n'* ]] && continue
    reachable+="$file"$'\n'

    # Crate roots and mod.rs own their directory; foo.rs owns foo/.
    case "$file" in
        src/lib.rs | src/main.rs | src/bin/*.rs | */mod.rs) dir="$(dirname "$file")" ;;
        *) dir="${file%.rs}" ;;
    esac

    while read -r name; do
        [[ -z "$name" ]] && continue
        if [[ -f "$dir/$name.rs" ]]; then
            queue+=("$dir/$name.rs")
        elif [[ -f "$dir/$name/mod.rs" ]]; then
            queue+=("$dir/$name/mod.rs")
        else
            echo "$file declares \`mod $name;\` but neither $dir/$name.rs nor $dir/$name/mod.rs exists" >&2
            failed=1
        fi
    done < <(rg --no-filename --only-matching --replace '$1' \
        '^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;' "$file" || true)
done

while read -r file; do
    if [[ "$reachable" != *$'\n'"$file"$'\n'* ]]; then
        echo "unreachable module file: $file is not declared by any module reachable from a crate root" >&2
        failed=1
    fi
done < <(find src -name '*.rs' | sort)

if ((failed)); then
    exit 1
fi
