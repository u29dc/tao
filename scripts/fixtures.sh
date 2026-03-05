#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
source "${SCRIPT_DIR}/safety.sh"

usage() {
  cat <<USAGE
Usage: scripts/fixtures.sh [--profile PROFILE] [--notes N] [--seed N] [--output DIR] [--skip-validate]

Profiles:
  all      Generate vault-1k, vault-2k, vault-5k, vault-10k, vault-25k (default)
  parity   Generate graph-parity and base-parity deterministic fixture sets
  1k       Generate vault-1k
  2k       Generate vault-2k
  5k       Generate vault-5k
  10k      Generate vault-10k
  25k      Generate vault-25k

Examples:
  scripts/fixtures.sh
  scripts/fixtures.sh --profile 10k --seed 7
  scripts/fixtures.sh --notes 2000 --seed 99 --output vault/generated
  scripts/fixtures.sh --profile 10k --skip-validate
USAGE
}

PROFILE="all"
NOTES=""
SEED="42"
OUTPUT_ROOT="vault/generated"
VALIDATE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="${2:-}"
      shift 2
      ;;
    --notes)
      NOTES="${2:-}"
      shift 2
      ;;
    --seed)
      SEED="${2:-}"
      shift 2
      ;;
    --output)
      OUTPUT_ROOT="${2:-}"
      shift 2
      ;;
    --skip-validate)
      VALIDATE=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      # Backward compatibility with positional output root.
      if [[ "$1" == -* ]]; then
        echo "unknown argument: $1" >&2
        usage >&2
        exit 1
      fi
      OUTPUT_ROOT="$1"
      shift
      ;;
  esac
done

if [[ -n "$NOTES" ]] && ! [[ "$NOTES" =~ ^[0-9]+$ ]]; then
  echo "--notes must be an integer" >&2
  exit 1
fi
if ! [[ "$SEED" =~ ^[0-9]+$ ]]; then
  echo "--seed must be an integer" >&2
  exit 1
fi

assert_safe_path "$OUTPUT_ROOT" "fixture output root"
mkdir -p "$OUTPUT_ROOT"

fail_validation() {
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

list_generated_vaults() {
  local root="$1"
  local dirs
  dirs=$(find "$root" -mindepth 1 -maxdepth 1 -type d | sort)
  if [[ -z "$dirs" ]]; then
    fail_validation "no generated vault directories under $root"
  fi
  echo "$dirs"
}

validate_generated_vault() {
  local vault="$1"
  echo "validating $(basename "$vault")"

  [[ -d "$vault/notes" ]] || fail_validation "missing notes dir in $vault"
  [[ -d "$vault/views" ]] || fail_validation "missing views dir in $vault"
  [[ -d "$vault/daily" ]] || fail_validation "missing daily dir in $vault"
  [[ -d "$vault/templates" ]] || fail_validation "missing templates dir in $vault"

  local required_bases=(contacts.base companies.base projects.base meetings.base)
  local base
  for base in "${required_bases[@]}"; do
    [[ -f "$vault/views/$base" ]] || fail_validation "missing base file $vault/views/$base"
  done

  if command -v fd >/dev/null 2>&1; then
    local hub_hits
    hub_hits=$(fd -HI -t f -i hub "$vault")
    [[ -z "$hub_hits" ]] || fail_validation "hub-like files are forbidden but found in $vault"
  else
    if find "$vault" -type f -iname '*hub*' | grep -q .; then
      fail_validation "hub-like files are forbidden but found in $vault"
    fi
  fi

  local markdown_files
  markdown_files=$(find "$vault" -type f -name '*.md' | sort)
  [[ -n "$markdown_files" ]] || fail_validation "no markdown files in $vault"

  local total_links
  total_links=$(rg -n '\[\[' "$vault" -g '*.md' | wc -l | tr -d ' ')
  (( total_links > 0 )) || fail_validation "no wikilinks found in $vault"

  local fm_links=0
  local file
  while IFS= read -r file; do
    fm_links=$(( fm_links + $(frontmatter_link_count "$file") ))
  done <<< "$markdown_files"
  (( fm_links > 0 )) || fail_validation "no frontmatter wikilinks found in $vault"

  local unresolved_links
  unresolved_links=$(rg -n '\[\[missing-' "$vault" -g '*.md' | wc -l | tr -d ' ')
  (( unresolved_links > 0 )) || fail_validation "no unresolved synthetic links found in $vault"

  local tasks_count
  tasks_count=$(rg -n '^\s*- \[( |x|X|-)\] ' "$vault" -g '*.md' | wc -l | tr -d ' ')
  (( tasks_count > 0 )) || fail_validation "no markdown tasks found in $vault"

  local tag_count
  tag_count=$(rg -n '#[a-zA-Z0-9_-]+' "$vault" -g '*.md' | wc -l | tr -d ' ')
  (( tag_count > 0 )) || fail_validation "no tag tokens found in $vault"

  local ratio
  ratio=$(awk -v unresolved="$unresolved_links" -v total="$total_links" 'BEGIN { if (total == 0) { print 0 } else { print unresolved / total } }')
  awk -v r="$ratio" 'BEGIN { exit !(r > 0.005 && r < 0.40) }' || fail_validation "unresolved ratio out of bounds in $vault: $ratio"

  if rg -n 'Dropbox/VAULT|/Users/han/' "$vault" -g '*.md' >/dev/null 2>&1; then
    fail_validation "potential personal path leakage detected in $vault"
  fi

  echo "validated $(basename "$vault"): links=$total_links frontmatter_links=$fm_links unresolved=$unresolved_links tasks=$tasks_count tags=$tag_count"
}

validate_generated_root() {
  local root="$1"
  assert_safe_path "$root" "fixture validation root"
  while IFS= read -r vault; do
    validate_generated_vault "$vault"
  done <<< "$(list_generated_vaults "$root")"
  echo "fixture validation passed for root: $root"
}

write_base_files() {
  local root="$1"
  mkdir -p "$root/views"

  cat > "$root/views/contacts.base" <<'BASE'
views:
  - name: Contacts
    type: table
    source: notes/contacts
    columns:
      - title
      - company
      - role
      - tags
BASE

  cat > "$root/views/companies.base" <<'BASE'
views:
  - name: Companies
    type: table
    source: notes/companies
    columns:
      - title
      - sector
      - stage
      - tags
BASE

  cat > "$root/views/projects.base" <<'BASE'
views:
  - name: Projects
    type: table
    source: notes/projects
    filters:
      - key: status
        op: neq
        value: archived
    sorts:
      - key: priority
        direction: desc
    columns:
      - title
      - status
      - priority
      - company
BASE

  cat > "$root/views/meetings.base" <<'BASE'
views:
  - name: Meetings
    type: table
    source: notes/meetings
    sorts:
      - key: date
        direction: desc
    columns:
      - title
      - date
      - project
      - attendees
BASE
}

make_note_frontmatter() {
  local category="$1"
  local idx="$2"
  local seed="$3"
  local project_count="$4"
  local contact_count="$5"
  local company_count="$6"

  local project_ref=$(( (idx + seed) % project_count + 1 ))
  local contact_ref=$(( (idx * 3 + seed) % contact_count + 1 ))
  local company_ref=$(( (idx * 5 + seed) % company_count + 1 ))
  local unresolved=""
  if (( (idx + seed) % 19 == 0 )); then
    unresolved="[[missing-${category}-${idx}]]"
  fi

  cat <<FM
---
seed: ${seed}
category: ${category}
project: "[[projects/project-${project_ref}.md]]"
company: "[[companies/company-${company_ref}.md]]"
contacts:
  - "[[contacts/contact-${contact_ref}.md]]"
  - "[[contacts/contact-$(( (contact_ref % contact_count) + 1 )).md]]"
related:
  - "[[meetings/meeting-$(( (idx % (project_count * 2)) + 1 )).md]]"
  - "${unresolved}"
---
FM
}

write_project_note() {
  local root="$1"
  local idx="$2"
  local seed="$3"
  local project_count="$4"
  local contact_count="$5"
  local company_count="$6"
  local status_cycle=(active paused planning archived)
  local status="${status_cycle[$(( (idx + seed) % 4 ))]}"
  local priority=$(( (idx + seed) % 5 + 1 ))
  local company_ref=$(( (idx + seed) % company_count + 1 ))
  local next=$(( (idx % project_count) + 1 ))

  {
    make_note_frontmatter "project" "$idx" "$seed" "$project_count" "$contact_count" "$company_count"
    cat <<MD
# Project ${idx}

status: ${status}
priority: ${priority}
company: [[companies/company-${company_ref}.md]]

Depends on [[projects/project-${next}.md]]

- [ ] Align project ${idx} milestones
- [x] Confirm project ${idx} kickoff
MD
  } > "$root/notes/projects/project-${idx}.md"
}

write_contact_note() {
  local root="$1"
  local idx="$2"
  local seed="$3"
  local project_count="$4"
  local contact_count="$5"
  local company_count="$6"
  local company_ref=$(( (idx * 7 + seed) % company_count + 1 ))
  local project_ref=$(( (idx * 11 + seed) % project_count + 1 ))

  {
    make_note_frontmatter "contact" "$idx" "$seed" "$project_count" "$contact_count" "$company_count"
    cat <<MD
# Contact ${idx}

role: stakeholder
company: [[companies/company-${company_ref}.md]]
project: [[projects/project-${project_ref}.md]]

Tags: #contact #network

- [ ] Follow up with contact ${idx}
MD
  } > "$root/notes/contacts/contact-${idx}.md"
}

write_company_note() {
  local root="$1"
  local idx="$2"
  local seed="$3"
  local project_count="$4"
  local contact_count="$5"
  local company_count="$6"
  local project_ref=$(( (idx * 13 + seed) % project_count + 1 ))

  {
    make_note_frontmatter "company" "$idx" "$seed" "$project_count" "$contact_count" "$company_count"
    cat <<MD
# Company ${idx}

sector: software
stage: growth

Primary project: [[projects/project-${project_ref}.md]]

Aliases: company-${idx}, c-${idx}
MD
  } > "$root/notes/companies/company-${idx}.md"
}

write_meeting_note() {
  local root="$1"
  local idx="$2"
  local seed="$3"
  local project_count="$4"
  local contact_count="$5"
  local company_count="$6"
  local project_ref=$(( (idx + seed) % project_count + 1 ))
  local contact_ref=$(( (idx * 2 + seed) % contact_count + 1 ))

  {
    make_note_frontmatter "meeting" "$idx" "$seed" "$project_count" "$contact_count" "$company_count"
    cat <<MD
# Meeting ${idx}

date: 2026-01-$(( (idx % 28) + 1 ))
project: [[projects/project-${project_ref}.md]]
attendees:
  - [[contacts/contact-${contact_ref}.md]]

- [ ] Send meeting ${idx} notes
- [-] Cancelled optional follow-up ${idx}
MD
  } > "$root/notes/meetings/meeting-${idx}.md"
}

write_daily_note() {
  local root="$1"
  local idx="$2"
  local seed="$3"
  local project_count="$4"
  local contact_count="$5"
  local company_count="$6"
  local project_ref=$(( (idx + seed) % project_count + 1 ))
  local contact_ref=$(( (idx * 3 + seed) % contact_count + 1 ))

  {
    make_note_frontmatter "daily" "$idx" "$seed" "$project_count" "$contact_count" "$company_count"
    cat <<MD
# Daily ${idx}

Reviewed [[projects/project-${project_ref}.md]] and [[contacts/contact-${contact_ref}.md]].

- [ ] Daily action ${idx}
MD
  } > "$root/daily/2026-02-$(( (idx % 28) + 1 ))-${idx}.md"
}

generate_profile() {
  local name="$1"
  local notes_total="$2"
  local seed="$3"
  local root="$OUTPUT_ROOT/$name"

  assert_safe_path "$root" "generated fixture root"
  rm -rf "$root"
  mkdir -p "$root/notes/projects" "$root/notes/contacts" "$root/notes/companies" "$root/notes/meetings" "$root/daily" "$root/templates"

  # Deterministic category distribution tuned for realistic graph density.
  local projects=$(( notes_total / 6 ))
  local contacts=$(( notes_total / 7 ))
  local companies=$(( notes_total / 18 ))
  local meetings=$(( notes_total / 4 ))
  local dailies=$(( notes_total - projects - contacts - companies - meetings ))

  if (( projects < 1 )); then projects=1; fi
  if (( contacts < 1 )); then contacts=1; fi
  if (( companies < 1 )); then companies=1; fi
  if (( meetings < 1 )); then meetings=1; fi
  if (( dailies < 1 )); then dailies=1; fi

  local i
  for ((i=1; i<=projects; i++)); do
    write_project_note "$root" "$i" "$seed" "$projects" "$contacts" "$companies"
  done
  for ((i=1; i<=contacts; i++)); do
    write_contact_note "$root" "$i" "$seed" "$projects" "$contacts" "$companies"
  done
  for ((i=1; i<=companies; i++)); do
    write_company_note "$root" "$i" "$seed" "$projects" "$contacts" "$companies"
  done
  for ((i=1; i<=meetings; i++)); do
    write_meeting_note "$root" "$i" "$seed" "$projects" "$contacts" "$companies"
  done
  for ((i=1; i<=dailies; i++)); do
    write_daily_note "$root" "$i" "$seed" "$projects" "$contacts" "$companies"
  done

  cat > "$root/templates/daily-template.md" <<'TPL'
---
template: daily
---

# Daily Template

- [ ] Plan day
- [ ] Review links
TPL

  write_base_files "$root"
  echo "generated ${name}: notes=${notes_total} seed=${seed} root=${root}"
}

generate_parity_fixtures() {
  local root="$1"
  local graph_root="$root/graph-parity"
  local base_root="$root/base-parity"

  assert_safe_path "$root" "parity fixture root"
  assert_safe_path "$graph_root" "graph parity fixture root"
  assert_safe_path "$base_root" "base parity fixture root"
  rm -rf "$graph_root" "$base_root"
  mkdir -p "$graph_root/notes" "$graph_root/expected"
  mkdir -p "$base_root/notes/projects" "$base_root/notes/meetings" "$base_root/views"

  cat > "$graph_root/notes/root.md" <<'MD'
---
related:
  - "[[alpha]]"
  - "[[missing-frontmatter]]"
---
# Root

[[alpha]]
[[beta#Target Heading]]
[[beta#^block-a]]
[[beta#Missing Heading]]
[[beta#^missing-block]]
[[missing-body]]
MD
  cat > "$graph_root/notes/alpha.md" <<'MD'
# Alpha

[[beta]]
MD
  cat > "$graph_root/notes/beta.md" <<'MD'
# Target Heading

Paragraph content ^block-a
MD
  cat > "$graph_root/notes/incoming.md" <<'MD'
# Incoming

[[deadend]]
MD
  cat > "$graph_root/notes/deadend.md" <<'MD'
# Deadend
MD
  cat > "$graph_root/notes/orphan.md" <<'MD'
# Orphan
MD

  cat > "$base_root/views/projects.base" <<'BASE'
views:
  - name: ActiveProjects
    type: table
    source: notes/projects
    filters:
      - key: status
        op: eq
        value: active
    sorts:
      - key: priority
        direction: desc
    columns:
      - title
      - status
      - priority
      - owner
BASE
  cat > "$base_root/notes/projects/project-a.md" <<'MD'
---
status: active
priority: 5
owner: han
meeting_refs:
  - "[[meetings/meeting-1.md]]"
---
# Project A

Primary project record.
MD
  cat > "$base_root/notes/projects/project-b.md" <<'MD'
---
status: paused
priority: 2
owner: alex
---
# Project B

Secondary project record.
MD
  cat > "$base_root/notes/meetings/meeting-1.md" <<'MD'
---
project: "[[projects/project-a.md]]"
duration_minutes: 45
---
# Meeting 1

Weekly sync.
MD

  echo "generated parity fixtures in ${root}"
}

validate_parity_root() {
  local root="$1"
  local graph_root="$root/graph-parity"
  local base_root="$root/base-parity"
  assert_safe_path "$root" "parity fixture root"
  [[ -f "$graph_root/notes/root.md" ]] || fail_validation "missing graph parity root note"
  [[ -f "$graph_root/notes/alpha.md" ]] || fail_validation "missing graph parity alpha note"
  [[ -f "$base_root/views/projects.base" ]] || fail_validation "missing base parity projects.base"
  [[ -f "$base_root/notes/projects/project-a.md" ]] || fail_validation "missing base parity project-a"
  local graph_links
  graph_links=$(rg -n '\[\[' "$graph_root" -g '*.md' | wc -l | tr -d ' ')
  (( graph_links > 0 )) || fail_validation "graph parity fixture has no wikilinks"
  echo "parity fixtures validated at ${root}"
}

if [[ -n "$NOTES" ]]; then
  generate_profile "vault-custom" "$NOTES" "$SEED"
else
  case "$PROFILE" in
    parity)
      generate_parity_fixtures "${OUTPUT_ROOT}"
      ;;
    all)
      generate_profile "vault-1k" 1000 "$((SEED + 1000))"
      generate_profile "vault-2k" 2000 "$((SEED + 2000))"
      generate_profile "vault-5k" 5000 "$((SEED + 5000))"
      generate_profile "vault-10k" 10000 "$((SEED + 10000))"
      generate_profile "vault-25k" 25000 "$((SEED + 25000))"
      ;;
    1k)
      generate_profile "vault-1k" 1000 "$SEED"
      ;;
    2k)
      generate_profile "vault-2k" 2000 "$SEED"
      ;;
    5k)
      generate_profile "vault-5k" 5000 "$SEED"
      ;;
    10k)
      generate_profile "vault-10k" 10000 "$SEED"
      ;;
    25k)
      generate_profile "vault-25k" 25000 "$SEED"
      ;;
    *)
      echo "unsupported profile: $PROFILE" >&2
      exit 1
      ;;
  esac
fi

echo "fixtures generated in ${OUTPUT_ROOT}"
if [[ "${VALIDATE}" -eq 1 ]]; then
  if [[ "${PROFILE}" == "parity" ]]; then
    validate_parity_root "${OUTPUT_ROOT}"
  else
    validate_generated_root "${OUTPUT_ROOT}"
  fi
fi
