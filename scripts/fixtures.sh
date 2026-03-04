#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Usage: scripts/fixtures.sh [--profile PROFILE] [--notes N] [--seed N] [--output DIR]

Profiles:
  all      Generate vault-1k, vault-5k, vault-10k, vault-25k (default)
  1k       Generate vault-1k
  5k       Generate vault-5k
  10k      Generate vault-10k
  25k      Generate vault-25k

Examples:
  scripts/fixtures.sh
  scripts/fixtures.sh --profile 10k --seed 7
  scripts/fixtures.sh --notes 2000 --seed 99 --output vault/generated
USAGE
}

PROFILE="all"
NOTES=""
SEED="42"
OUTPUT_ROOT="vault/generated"

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

mkdir -p "$OUTPUT_ROOT"

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

if [[ -n "$NOTES" ]]; then
  generate_profile "vault-custom" "$NOTES" "$SEED"
else
  case "$PROFILE" in
    all)
      generate_profile "vault-1k" 1000 "$((SEED + 1000))"
      generate_profile "vault-5k" 5000 "$((SEED + 5000))"
      generate_profile "vault-10k" 10000 "$((SEED + 10000))"
      generate_profile "vault-25k" 25000 "$((SEED + 25000))"
      ;;
    1k)
      generate_profile "vault-1k" 1000 "$SEED"
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
