#!/usr/bin/env bash
#
# Lint a GitHub release body against the canonical Open Privacy Suite
# release-notes structure (see .claude/skills/release/SKILL.md).
#
# Reads the body from the first argument (a file path, or "-" for stdin),
# or from stdin when no argument is given. Exits non-zero and names the
# missing section(s) if any required marker is absent. It checks header
# PRESENCE only, not content — so a release authored without the /release
# skill still passes as long as it keeps the standard section headers.
#
# Enforcement is independent of the skill: run in CI on release publish
# (.github/workflows/release-notes-lint.yml), and locally on a draft:
#     scripts/lint-release-notes.sh <notes-file>
#
set -euo pipefail

if [ "${1:-}" != "" ] && [ "${1:-}" != "-" ]; then
  body="$(cat -- "$1")"
else
  body="$(cat)"
fi

# Required sections, kept in sync with the skill's §3 format.
#
# Each header is matched against a real Markdown header LINE (`^## …`), not an
# arbitrary substring — so the phrase appearing in prose (e.g. inside a
# Highlights bullet) does not satisfy the check. The match is emoji-agnostic:
# `## ⚠️ Action required on upgrade` passes with or without the ⚠️, whatever
# its encoding. "Full changelog:" is a bold line rather than a header, so it is
# matched as a line.
labels=(
  "## Highlights"
  "## ⚠️ Action required on upgrade"
  "## Incompatibilities / breaking"
  "## Docker images"
  "## Verify after deploy"
  "Full changelog:"
)
patterns=(
  '^##[[:space:]]+Highlights([[:space:]]|$)'
  '^##[[:space:]].*Action required on upgrade'
  '^##[[:space:]].*Incompatibilities'
  '^##[[:space:]].*Docker images'
  '^##[[:space:]].*Verify after deploy'
  'Full changelog:'
)

missing=()
for i in "${!labels[@]}"; do
  if ! printf '%s\n' "$body" | grep -qE "${patterns[$i]}"; then
    missing+=("${labels[$i]}")
  fi
done

if [ "${#missing[@]}" -ne 0 ]; then
  echo "release-notes lint FAILED — missing required section(s):" >&2
  for marker in "${missing[@]}"; do
    echo "  - ${marker}" >&2
  done
  echo >&2
  echo "See .claude/skills/release/SKILL.md for the canonical release-notes format." >&2
  exit 1
fi

echo "release-notes lint OK — all required sections present."
