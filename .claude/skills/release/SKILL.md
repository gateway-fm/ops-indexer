---
name: release
description: Draft and publish a GitHub release for ops-indexer (Chain Indexer) with a consistent, operator-focused notes format. Use when the user asks to "release", "cut a release", "draft release notes", or "publish vX.Y.Z".
---

# Release

Produce a GitHub Release for `gateway-fm/ops-indexer` whose notes are **operator-focused, precise, and short** — someone deploying the upgrade reads it in under two minutes and knows exactly what changed, what they MUST do, and how to verify it worked.

This is the shared Open Privacy Suite release format (kept in sync with the `open-privacy-suite` and `ops-explorer` repos). Never paste raw commit subjects or an auto-generated changelog dump; judge what actually matters (operator-visible behavior, security, breaking changes) and say it in plain language. Everything is derived from the **code/diff**, not from tickets.

## 0. Inputs

- **Target tag** (e.g. `v0.4.0`, or `v0.4.0-rc.1` for a pre-release). If not given, infer the next semver from the latest release + the size/nature of changes and **confirm with the user** first.
- **Range** = `<previous tag>..<target>`. Previous tag: `gh release view --repo gateway-fm/ops-indexer --json tagName -q .tagName`, or `git describe --tags --abbrev=0`. If the target tag doesn't exist yet, use `<previous tag>..HEAD` on `main`.

## 1. Gather the actual changes (read the evidence — don't guess)

- **Merged PRs in range:** `gh pr list --repo gateway-fm/ops-indexer --state merged --base main --limit 300 --json number,title,mergedAt,labels`; cross-check with `git log --oneline <range>`.
- **Migrations / schema** (if the repo has them): open each new migration; state WHAT it changes and whether it's expand-only/auto or needs an ordered/manual step.
- **Env & config surface:** `git diff <range> -- .env.example` — added / removed / renamed / newly-required keys and secrets. A secret-class key must be injected via the secret store, never committed. (Adding a key to `.env.example` for documentation is not itself a new *required* key — say so.)
- **Docker images** the release publishes (from `.github/workflows/docker-publish.yml`): `ghcr.io/gateway-fm/chain-indexer` and `gatewayfm/chain-indexer`; tag = the version **without** the leading `v` (`type=semver,pattern={{version}}`), also tagged `:sha-<short>`.
- **Breaking / behavior changes:** changed gRPC/API shapes, changed defaults, indexing/pagination semantics.

## 2. Classify — keep only what an operator or the business cares about

For each change decide: **operator-visible feature**, **behavior change**, **security fix**, **breaking / migration / new env**, or **internal noise**. Drop refactors, test-only, doc-only, and dependency bumps from Highlights (a dep bump only appears if it's a security CVE). A **security fix** or a **required env / migration** is ALWAYS surfaced, even when the code change is tiny.

## 3. Write the notes — this exact structure, terse

The release **title is the tag verbatim** (e.g. `v0.4.0` or `v0.4.0-rc.1`). For a **pre-release** (tag contains `-rc`/`-beta`/`-alpha`), the body's first line is a banner:

```
> **Release candidate (v0.4.0-rc.1)** — pre-release for validation, not a GA/production release.
```

Then, in this exact section order (keep the `##` headers verbatim — the CI lint in §5 checks for them):

```
## Highlights
- 3–7 bullets, plain operator/business language, value first (not the ticket id).
  Prefix security fixes with 🔒.

## ⚠️ Action required on upgrade
- New required env, migrations, or secrets — what breaks if unset / what to run.
(If truly nothing: "None — no migrations, no new required env. Drop-in.")

## Incompatibilities / breaking
- Changed gRPC/API shapes, changed defaults, indexing semantics. "None." if none.

## Deprecations
- Envs/configs still honored but going away, and what to move to. OMIT this section entirely if none.

## Docker images
- `ghcr.io/gateway-fm/chain-indexer:<version>`
- `gatewayfm/chain-indexer:<version>` (also tagged `:sha-<short>`)
(version = the tag without the leading `v`, e.g. `0.4.0-rc.1`)

## Verify after deploy
- 2–4 concrete checks precise enough for infra to run without asking.

**Full changelog:** https://github.com/gateway-fm/ops-indexer/compare/<previous tag>...<target tag>
```

Rules: no section longer than it needs to be; omit the optional empty section (Deprecations) rather than writing "None"; Highlights are operator-first, not a changelog dump.

## 4. Confirm, then publish

- Show the drafted notes to the user. **Do NOT publish unprompted.**
- On approval, create as a **draft** first:
  `gh release create <tag> --repo gateway-fm/ops-indexer --title "<tag>" --notes-file <file> --draft --target main`
  (add `--prerelease` for an `-rc`/`-beta`/`-alpha` tag). Share the draft URL.
- Publish only on an explicit "publish": `gh release edit <tag> --repo gateway-fm/ops-indexer --draft=false`.
- **Heads-up:** pushing the `v*` tag triggers `docker-publish.yml`, which builds and pushes the multi-arch images above to GHCR and Docker Hub. Images are immutable — a tag can never be re-cut, only superseded.

## 5. Enforcement — this skill is guidance; the CI lint is the gate

The format is enforced independently of this skill by **`.github/workflows/release-notes-lint.yml`**, which pipes the release body into **`scripts/lint-release-notes.sh`** on `release: published`/`edited` and fails the check when a required section header is missing (`## Highlights`, `## ⚠️ Action required on upgrade`, `## Incompatibilities / breaking`, `## Docker images`, `## Verify after deploy`, and a `Full changelog:` line). The lint matches real `##` header lines (emoji-agnostic — the ⚠️ is optional), so the phrase in prose does not satisfy it. Keep the headers in §3 verbatim so a release authored **without** this skill still passes. Check a draft locally before publishing: `scripts/lint-release-notes.sh <notes-file>`.

## Notes

- Keep this format in sync with the sibling `open-privacy-suite` and `ops-explorer` release skills.
- If the release is gated on manual acceptance that isn't signed off, say the release is **pending sign-off** and stop at the draft — don't cut the tag.
