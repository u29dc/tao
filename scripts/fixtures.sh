#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-vault/generated}"
mkdir -p "$ROOT"

make_vault () {
  local name="$1"
  local count="$2"
  local dir="$ROOT/$name"
  mkdir -p "$dir"

  local i
  for ((i=1; i<=count; i++)); do
    local prev=$((i-1))
    local next=$((i+1))
    cat > "$dir/note-$i.md" <<MD
---
id: note-$i
type: benchmark
rating: $((i % 5))
---

# Note $i

This is synthetic benchmark content for note $i.

[[note-$prev]]
[[note-$next]]
MD
  done
}

make_vault vault-1k 1000
make_vault vault-5k 5000
make_vault vault-10k 10000
make_vault vault-25k 25000

echo "fixtures generated in $ROOT"
