#!/usr/bin/env bash
# Regression tests for next-version.sh.
#
# Each case builds a throwaway repository, arranges tags and commits, and
# asserts the version the script computes. A wrong tag published to the module
# proxy is immutable, so every behaviour this script relies on is pinned here.
set -euo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/next-version.sh"
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

failures=0
total=0

# repo NAME [TAG] - start a fresh v0/v1 module, optionally tagged at its root.
repo() {
  local name="$1" tag="${2:-}"
  rm -rf "${workdir:?}/$name"
  mkdir -p "$workdir/$name"
  cd "$workdir/$name"
  git init -q .
  git config user.email t@example.com
  git config user.name Test
  printf 'module example.com/m\n' > go.mod
  git add go.mod
  git commit -q -m "chore: root"
  [ -n "$tag" ] && git tag "$tag"
  return 0
}

commit() { git commit -q --allow-empty -m "$1"; }

# module LINE - commit go.mod with LINE as its module directive.
module() {
  printf '%s\n' "$1" > go.mod
  git add go.mod
  git commit -q -m "chore: set the module path"
}

# expect DESCRIPTION WANT [TITLE] - run the script with TITLE as the pull
# request title (none when omitted) and compare its stdout.
expect() {
  local description="$1" want="$2" title="${3:-}" got
  total=$((total + 1))
  got="$(PR_TITLE="$title" "$script" 2>/dev/null)" || got="<script failed>"
  if [ "$got" = "$want" ]; then
    printf 'ok   %s\n' "$description"
  else
    printf 'FAIL %s\n       want %s\n       got  %s\n' "$description" "$want" "$got"
    failures=$((failures + 1))
  fi
}

# --- level from the pull request title --------------------------------------
repo a v0.1.0; commit "x"
expect "major in the title -> major" v1.0.0 "major: first stable API"

repo b v0.3.0; commit "x"
expect "major is case-insensitive" v1.0.0 "Remove Foo [MAJOR]"

repo c v1.2.3; commit "x"
expect "feat in the title -> minor" v1.3.0 "feat: add a query option"

repo d v1.2.3; commit "x"
expect "a scoped feat -> minor" v1.3.0 "feat(query): add an option"

repo f v1.2.3; commit "x"
expect "a breaking marker alone does not mean major" v1.3.0 "feat!: remove Foo"

repo g v0.4.0; commit "x"
expect "major beats feat" v1.0.0 "feat: major rework of the query API"

repo h v1.2.3; commit "x"
expect "anything else -> patch" v1.2.4 "fix: a bug"

repo i v0.1.0; commit "x"
expect "below v1.0.0 the levels are the same" v0.2.0 "feat: x"

# --- the level must not come from a word that merely contains it -----------
repo j v1.2.3; commit "x"
expect "majority is not major" v1.2.4 "fix: the majority of flaky tests"

repo k v1.2.3; commit "x"
expect "defeat is not feat" v1.2.4 "fix: defeat the race in Close"

repo k2 v1.2.3; commit "x"
expect "feature is not feat" v1.2.4 "fix: remove the stale feature flag"

repo k3 v1.2.3; commit "x"
expect "an identifier containing major is not major" v1.2.4 "fix: rename is_major_version"

repo k4 v1.2.3; commit "x"
expect "a hyphenated word containing major is not major" v1.2.4 "docs: explain major-version module paths"

repo k5 v1.2.3; commit "x"
expect "a hyphenated word containing feat is not feat" v1.2.4 "fix: feat-flag parsing"

repo k6 v0.1.0; commit "x"
expect "major in brackets or parentheses counts" v1.0.0 "Remove Foo (major)"

# --- reverts ----------------------------------------------------------------
# GitHub titles a revert 'Revert "<original title>"', which quotes the level.
repo r1 v1.2.3; commit "x"
expect "a revert of a major title -> patch" v1.2.4 'Revert "major: stable v1"'

repo r2 v1.2.3; commit "x"
expect "a revert of a feat title -> patch" v1.2.4 'Revert "feat: add an option"'

# --- without a pull request title -------------------------------------------
# A direct push, or a merge commit whose subject names a feature/ or major-
# branch, must not choose the level from the commit.
repo m v1.2.3; commit "feat: pushed straight to main"
expect "no title -> patch, whatever the commit says" v1.2.4

repo n v1.2.3; commit "Merge branch 'major-cleanup'"
expect "a merge subject naming a branch is not a title" v1.2.4

# --- the module path must match the major version ---------------------------
# A tag that disagrees with go.mod's /vN suffix cannot be installed, and a tag
# on the proxy cannot be withdrawn, so the script must refuse rather than tag.
repo x1 v1.2.3; commit "x"
expect "major to v2 without /v2 in go.mod fails" "<script failed>" "major: x"

repo x2 v1.2.3; module "module example.com/m/v2"
expect "major to v2 with /v2 in go.mod -> v2.0.0" v2.0.0 "major: x"

repo x3 v2.0.0; module "module example.com/m/v2"
expect "a /v2 path does not license v3" "<script failed>" "major: x"

repo x4 v1.2.3; module "module example.com/m/v2"
expect "a patch on v1 with a /v2 path fails" "<script failed>" "fix: x"

repo x5 v2.0.0; module "module example.com/m/v2"
expect "a patch on v2 with a /v2 path -> v2.0.1" v2.0.1 "fix: x"

repo x6 v1.2.3; module 'module "example.com/m/v2" // the v2 API'
expect "a quoted path with a comment is read" v2.0.0 "major: x"

repo x7 v1.2.3; git rm -q go.mod; git commit -q -m "chore: drop go.mod"
expect "no go.mod fails" "<script failed>" "fix: x"

repo x8 v1.2.3; printf 'module example.com/m/v2\n' > go.mod; commit "x"
expect "go.mod is read as committed, not from the work tree" v1.2.4 "fix: x"

# Commit trailers no longer choose a level; only the title does.
repo o v1.2.3; commit "docs: x

Release-As: major"
expect "Release-As: major is not a level any more" v1.2.4 "docs: x"

# --- Release-As: skip -------------------------------------------------------
# A change that reaches no consumer - a workflow, a README - should not
# publish a version identical to the last one.
repo p4 v0.1.0; commit "ci: reshuffle a workflow

Release-As: skip"
expect "Release-As: skip publishes nothing" skip "ci: reshuffle a workflow"

repo p5 v0.1.0; commit "ci: x

Release-As: skip"
expect "skip beats a major title" skip "major: x"

repo p1 v0.1.0; commit "ci: x

Release-As:   skip  "
expect "skip tolerates surrounding whitespace" skip

repo p6 v0.1.0; commit "ci: x

Release-As: skip"; commit "fix: a real change"
expect "a skip below the merged commits does not skip" v0.1.1

# Skipping creates no tag, so a skip read from anywhere in the range would
# stay in the range and disable every future release. It must defer, not
# suppress.
repo p6b v0.1.0
commit "ci: reshuffle a workflow

Release-As: skip"
expect "the CI push itself skips" skip
commit "fix: a genuine bug fix"
expect "the next push releases, carrying the skipped commit" v0.1.1 "fix: a genuine bug fix"

# An indented Release-As: is how a commit body documents the convention.
repo p7 v0.1.0; commit "docs: x

    Release-As: skip"
expect "an indented skip is not a trailer" v0.1.1

repo p7b v0.1.0; commit "docs: x

	Release-As: skip"
expect "a tab-indented skip is not a trailer" v0.1.1

repo p7c v0.1.0; commit "docs: mention a trailer

Put Release-As: skip in the message to skip one."
expect "Release-As inside a sentence is inert" v0.1.1

# A merge commit leaves the trailer on the branch commits, not on HEAD, so
# reading only HEAD would miss it.
repo p8a v0.1.0
git checkout -q -b feature
commit "ci: reshuffle a workflow

Release-As: skip"
git checkout -q -
git merge -q --no-ff -m "Merge pull request #1 from feature" feature
expect "skip survives a merge commit" skip

# A parentless HEAD has no HEAD^ to diff against; it must not die under set -e.
repo p8b
git commit -q --allow-empty --amend -m "ci: first commit

Release-As: skip"
expect "skip works on a parentless HEAD" skip

# A squash collapses the branch into one commit whose body carries the
# original messages, trailer included.
repo p8 v0.1.0
git checkout -q -b feature
commit "ci: reshuffle a workflow

Release-As: skip"
git checkout -q -
git merge -q --squash feature
git commit -q --allow-empty -m "ci: reshuffle a workflow (#10)

* ci: reshuffle a workflow

Release-As: skip"
expect "skip survives a squash merge" skip

repo p9 v0.1.0
printf 'ci: x\r\n\r\nRelease-As: skip\r\n' > "$workdir/crlf.txt"
git commit -q --allow-empty --cleanup=verbatim -F "$workdir/crlf.txt"
expect "Release-As: skip survives CRLF line endings" skip

# --- tags -------------------------------------------------------------------
repo t; commit "x"
expect "no tags at all starts from v0.0.0" v0.1.0 "feat: first"

repo u v0.0.5; git tag v0.0.6-rc1; commit "x"
expect "prerelease tags are skipped" v0.0.6 "fix: x"

repo v v0.0.5; git tag -a v0.0.6 -m annotated; commit "x"
expect "annotated tags are read" v0.0.7 "fix: x"

repo w v0.0.5
expect "no commits since the tag -> skip" skip "major: x"

# A re-run for an older commit after a later push released: the newest tag is
# not an ancestor of HEAD, and "no commits since" would pass silently.
repo y v1.0.0; commit "feat: the release that failed"
git checkout -q -b later; commit "fix: released later"; git tag v1.0.1
git checkout -q -
expect "a re-run behind a newer release fails loudly" "<script failed>" "feat: the release that failed"

# ----------------------------------------------------------------------------
cd /
printf '\n%d/%d passed\n' "$((total - failures))" "$total"
[ "$failures" -eq 0 ]
