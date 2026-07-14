#!/usr/bin/env bash
#
# Full release in one command.
#
# Orchestrates the whole Changesets flow with safety gates:
#   preflight → quality gates → version → commit → push → publish → push tags
#
# Usage:
#   pnpm release            # interactive: prints the plan, asks to confirm
#   pnpm release --yes      # non-interactive (CI / "I'm sure")
#   pnpm release --dry-run  # run every gate + show the version plan, but don't
#                           #   bump, commit, push, or publish
#
# Secrets are pulled from `passage` (same as the underlying changeset:* scripts),
# so nothing sensitive is written to disk except the transient .npmrc that
# changeset:publish creates and removes.

set -euo pipefail

cd "$(dirname "$0")/.."

# ---- args -------------------------------------------------------------------
ASSUME_YES=0
DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --yes|-y) ASSUME_YES=1 ;;
    --dry-run|-n) DRY_RUN=1 ;;
    -h|--help)
      sed -n '/^# Full release/,/^$/p' "$0" | sed 's/^#$//; s/^# //'
      exit 0
      ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

# ---- pretty output ----------------------------------------------------------
if [ -t 1 ]; then
  BOLD=$(printf '\033[1m'); DIM=$(printf '\033[2m'); RED=$(printf '\033[31m')
  GREEN=$(printf '\033[32m'); YELLOW=$(printf '\033[33m'); RESET=$(printf '\033[0m')
else
  BOLD=""; DIM=""; RED=""; GREEN=""; YELLOW=""; RESET=""
fi
step() { printf '\n%s==>%s %s%s%s\n' "$GREEN" "$RESET" "$BOLD" "$1" "$RESET"; }
info() { printf '    %s%s%s\n' "$DIM" "$1" "$RESET"; }
die()  { printf '\n%sError:%s %s\n' "$RED" "$RESET" "$1" >&2; exit 1; }

BASE_BRANCH="main"

# ---- 1. preflight -----------------------------------------------------------
step "Preflight checks"

command -v passage >/dev/null 2>&1 || die "passage not found (needed for GitHub + npm tokens)."

CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
[ "$CURRENT_BRANCH" = "$BASE_BRANCH" ] || \
  die "On '$CURRENT_BRANCH', but releases must run from '$BASE_BRANCH'."
info "branch: $CURRENT_BRANCH"

if [ -n "$(git status --porcelain)" ]; then
  git status --short
  die "Working tree is dirty. Commit or stash everything before releasing."
fi
info "working tree: clean"

info "fetching origin…"
git fetch --quiet origin "$BASE_BRANCH"
LOCAL=$(git rev-parse @)
REMOTE=$(git rev-parse "origin/$BASE_BRANCH")
[ "$LOCAL" = "$REMOTE" ] || \
  die "Local $BASE_BRANCH is not in sync with origin/$BASE_BRANCH. Pull/push first."
info "in sync with origin/$BASE_BRANCH"

# Are there any changesets to release? (.changeset/*.md minus README.md)
PENDING=$(find .changeset -maxdepth 1 -name '*.md' ! -name 'README.md' | wc -l | tr -d ' ')
[ "$PENDING" -gt 0 ] || \
  die "No changesets found. Run 'pnpm changeset' to describe your release first."
info "pending changesets: $PENDING"

step "Planned version bumps"
npx changeset status || true

# ---- 2. quality gates -------------------------------------------------------
step "Quality gates (typecheck · test · lint)"
pnpm typecheck
pnpm test
pnpm lint
info "all gates passed"

# ---- confirm ----------------------------------------------------------------
if [ "$DRY_RUN" -eq 1 ]; then
  step "Dry run complete"
  info "Gates passed and the plan above is valid. No versions bumped, nothing published."
  exit 0
fi

if [ "$ASSUME_YES" -ne 1 ]; then
  printf '\n%sProceed with version bump, publish to npm, and push?%s [y/N] ' "$YELLOW" "$RESET"
  read -r reply
  case "$reply" in
    y|Y|yes|YES) ;;
    *) die "Aborted by user." ;;
  esac
fi

# ---- 3. version -------------------------------------------------------------
step "Bumping versions + writing changelogs"
pnpm changeset:version

if [ -z "$(git status --porcelain)" ]; then
  die "changeset:version produced no changes — nothing to release."
fi

# ---- 4. commit --------------------------------------------------------------
step "Committing version bump"
git add -A
git commit --no-verify -m "chore: release packages"
info "committed $(git rev-parse --short HEAD)"

# ---- 5. push commit ---------------------------------------------------------
step "Pushing release commit"
git push origin "$BASE_BRANCH"

# ---- 6. publish -------------------------------------------------------------
step "Building + publishing to npm"
# changeset publish creates a git tag per published package.
pnpm changeset:publish

# ---- 7. push tags -----------------------------------------------------------
step "Pushing tags"
git push origin --tags

step "Release complete 🎉"
info "Published from $(git rev-parse --short HEAD). Tags pushed to origin."
