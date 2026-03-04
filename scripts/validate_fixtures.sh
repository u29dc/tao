#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-vault/generated}"

if [[ ! -d "$ROOT" ]]; then
  echo "fixture root not found: $ROOT" >&2
  exit 1
fi

fail() {
  echo "fixture validation failed: $1" >&2
  exit 1
}

frontmatter_link_count() {
  local file="$1"
  awk '
    BEGIN { in_fm=0; fences=0; count=0 }
    /^---$/ {
      fences++
      if (fences == 1) { in_fm=1; next }
      if (fences == 2) { in_fm=0; exit }
    }
    in_fm && /\[\[/ { count++ }
    END { print count }
  ' "$file"
}

list_vaults() {
  local root="$1"
  local dirs
  dirs=$(find "$root" -mindepth 1 -maxdepth 1 -type d | sort)
  if [[ -z "$dirs" ]]; then
    fail "no generated vault directories under $root"
  fi
  echo "$dirs"
}

validate_vault() {
  local vault="$1"
  echo "validating $(basename "$vault")"

  [[ -d "$vault/notes" ]] || fail "missing notes dir in $vault"
  [[ -d "$vault/views" ]] || fail "missing views dir in $vault"
  [[ -d "$vault/daily" ]] || fail "missing daily dir in $vault"
  [[ -d "$vault/templates" ]] || fail "missing templates dir in $vault"

  local required_bases=(contacts.base companies.base projects.base meetings.base)
  local base
  for base in "${required_bases[@]}"; do
    [[ -f "$vault/views/$base" ]] || fail "missing base file $vault/views/$base"
  done

  if command -v fd >/dev/null 2>&1; then
    local hub_hits
    hub_hits=$(fd -HI -t f -i hub "$vault")
    [[ -z "$hub_hits" ]] || fail "hub-like files are forbidden but found in $vault"
  else
    if find "$vault" -type f -iname '*hub*' | grep -q .; then
      fail "hub-like files are forbidden but found in $vault"
    fi
  fi

  local markdown_files
  markdown_files=$(find "$vault" -type f -name '*.md' | sort)
  [[ -n "$markdown_files" ]] || fail "no markdown files in $vault"

  local total_links
  total_links=$(rg -n '\[\[' "$vault" -g '*.md' | wc -l | tr -d ' ')
  (( total_links > 0 )) || fail "no wikilinks found in $vault"

  local fm_links=0
  local file
  while IFS= read -r file; do
    fm_links=$(( fm_links + $(frontmatter_link_count "$file") ))
  done <<< "$markdown_files"
  (( fm_links > 0 )) || fail "no frontmatter wikilinks found in $vault"

  local unresolved_links
  unresolved_links=$(rg -n '\[\[missing-' "$vault" -g '*.md' | wc -l | tr -d ' ')
  (( unresolved_links > 0 )) || fail "no unresolved synthetic links found in $vault"

  local tasks_count
  tasks_count=$(rg -n '^\s*- \[( |x|X|-)\] ' "$vault" -g '*.md' | wc -l | tr -d ' ')
  (( tasks_count > 0 )) || fail "no markdown tasks found in $vault"

  local tag_count
  tag_count=$(rg -n '#[a-zA-Z0-9_-]+' "$vault" -g '*.md' | wc -l | tr -d ' ')
  (( tag_count > 0 )) || fail "no tag tokens found in $vault"

  local ratio
  ratio=$(awk -v unresolved="$unresolved_links" -v total="$total_links" 'BEGIN { if (total == 0) { print 0 } else { print unresolved / total } }')
  awk -v r="$ratio" 'BEGIN { exit !(r > 0.005 && r < 0.40) }' || fail "unresolved ratio out of bounds in $vault: $ratio"

  if rg -n 'Dropbox/VAULT|/Users/han/' "$vault" -g '*.md' >/dev/null 2>&1; then
    fail "potential personal path leakage detected in $vault"
  fi

  echo "validated $(basename "$vault"): links=$total_links frontmatter_links=$fm_links unresolved=$unresolved_links tasks=$tasks_count tags=$tag_count"
}

while IFS= read -r vault; do
  validate_vault "$vault"
done <<< "$(list_vaults "$ROOT")"

echo "fixture validation passed for root: $ROOT"
