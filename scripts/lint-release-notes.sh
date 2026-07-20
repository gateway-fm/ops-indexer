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

# Required markers, kept in sync with the skill's §3 format. The
# "Action required" / "Verify after deploy" markers omit the emoji so the
# match is robust to emoji encoding in the release body.
required=(
  "## Highlights"
  "Action required on upgrade"
  "## Docker images"
  "Verify after deploy"
  "Full changelog:"
)

missing=()
for marker in "${required[@]}"; do
  case "$body" in
    *"$marker"*) : ;;
    *) missing+=("$marker") ;;
  esac
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
