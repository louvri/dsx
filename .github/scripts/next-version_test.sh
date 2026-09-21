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

# repo NAME [TAG] - start a fresh repository, optionally tagged at its root.
repo() {
  local name="$1" tag="${2:-}"
  rm -rf "${workdir:?}/$name"
  mkdir -p "$workdir/$name"
  cd "$workdir/$name"
  git init -q .
  git config user.email t@example.com
  git config user.name Test
  git commit -q --allow-empty -m "chore: root"
  [ -n "$tag" ] && git tag "$tag"
  return 0
}

commit() { git commit -q --allow-empty -m "$1"; }

# expect DESCRIPTION WANT - run the script and compare its stdout.
expect() {
  local description="$1" want="$2" got
  total=$((total + 1))
  got="$("$script" 2>/dev/null)" || got="<script failed>"
  if [ "$got" = "$want" ]; then
    printf 'ok   %s\n' "$description"
  else
    printf 'FAIL %s\n       want %s\n       got  %s\n' "$description" "$want" "$got"
    failures=$((failures + 1))
  fi
}

# --- level from the conventional-commit subject -----------------------------
repo a v0.0.5; commit "fix: a bug"
expect "fix below v1.0.0 -> patch" v0.0.6

repo b v0.0.5; commit "feat: a feature"
expect "feat below v1.0.0 -> patch" v0.0.6

repo c v0.0.5; commit "feat!: a breaking change"
expect "breaking below v1.0.0 -> minor" v0.1.0

repo d v0.0.5; commit "refactor(api)!: drop Foo"
expect "any type with ! -> breaking" v0.1.0

repo e v1.2.3; commit "feat: a feature"
expect "feat at v1.x -> minor" v1.3.0

repo f v1.2.3; commit "feat!: a breaking change"
expect "breaking at v1.x -> major" v2.0.0

repo g v1.2.3; commit "docs: a doc change"
expect "other types -> patch" v1.2.4

# --- BREAKING CHANGE footers, both spellings the spec allows ----------------
repo h v0.0.5; commit "fix: x

BREAKING CHANGE: removed the thing"
expect "BREAKING CHANGE footer" v0.1.0

repo i v0.0.5; commit "fix: x

BREAKING-CHANGE: removed the thing"
expect "BREAKING-CHANGE footer (hyphenated)" v0.1.0

# --- the level must not be selectable from prose ----------------------------
repo j v0.0.5; commit "docs: describe the old scheme

The level used to be read from [major]/[minor] markers."
expect "prose mentioning [major] is inert" v0.0.6

repo k v0.0.5; commit "docs: mention a trailer

Put Release-As: major in the message to force one."
expect "Release-As inside a sentence is inert" v0.0.6

repo l v0.0.5
commit "docs: document the convention

Subjects we accept:
* feat!: a breaking change
* fix: a bugfix"
commit "chore: a second commit"
expect "prose bullets over a multi-commit range are inert" v0.0.6

# --- explicit override ------------------------------------------------------
repo m v0.0.5; commit "docs: x

Release-As: major"
expect "Release-As trailer wins over the subject" v1.0.0

repo n v0.0.5; commit "feat!: breaking

Release-As: patch"
expect "Release-As can lower the level too" v0.0.6

repo o v0.0.5
commit "docs: x

Release-As: minor"
commit "chore: y"
expect "Release-As in a non-tip commit still counts" v0.1.0

repo p1 v0.0.5; commit "docs: x

Release-As: major   "
expect "Release-As tolerates trailing whitespace" v1.0.0

repo p2 v0.0.5; commit "docs: x

Release-As:   minor  "
expect "Release-As tolerates whitespace around the level" v0.1.0

# An indented Release-As: is how a commit body documents the convention. Git
# does not treat it as a trailer, and neither may we: matching it would let a
# docs commit cut a release.
repo p2b v0.0.5; commit "docs: document the release convention

To force a release level, add the trailer on its own line:

    Release-As: major

Otherwise the level comes from the commit subjects."
expect "an indented Release-As is not a trailer" v0.0.6

repo p2c v0.0.5; commit "docs: x

	Release-As: major"
expect "a tab-indented Release-As is not a trailer" v0.0.6

repo p3 v0.0.5; commit "feat!: breaking

Release-As: enormous"
expect "unreadable Release-As level falls back to the subject" v0.1.0

# A change that reaches no consumer - a workflow, a README - should not
# publish a version identical to the last one.
repo p4 v0.1.0; commit "ci: reshuffle a workflow

Release-As: skip"
expect "Release-As: skip publishes nothing" skip

repo p5 v0.1.0; commit "feat!: breaking

Release-As: skip"
expect "skip beats the subject it is attached to" skip

repo p6 v0.1.0; commit "ci: x

Release-As: skip"; commit "feat!: a real change"
expect "an explicit level elsewhere in the range still wins over skip" v0.2.0

# Skipping creates no tag, so a skip read from anywhere in the range would
# stay in the range and disable every future release. It must defer, not
# suppress.
repo p6b v0.1.0
commit "ci: reshuffle a workflow

Release-As: skip"
expect "the CI push itself skips" skip
commit "fix: a genuine bug fix"
expect "the next push releases, carrying the skipped commit" v0.1.1

repo p7 v0.1.0; commit "docs: x

    Release-As: skip"
expect "an indented skip is not a trailer" v0.1.1

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

repo p v0.0.5
printf 'docs: x\r\n\r\nRelease-As: major\r\n' > "$workdir/crlf.txt"
git commit -q --allow-empty --cleanup=verbatim -F "$workdir/crlf.txt"
expect "Release-As survives CRLF line endings" v1.0.0

# --- merge strategies -------------------------------------------------------
repo q v0.0.5
git checkout -q -b feature
commit "feat!: the breaking change"
commit "docs: follow-up"
git checkout -q -
git merge -q --no-ff -m "Merge pull request #1 from feature" feature
expect "merge commit reads the branch commits" v0.1.0

repo r v0.0.5
git checkout -q -b feature
commit "feat!: the breaking change"
commit "docs: follow-up"
git checkout -q -
git merge -q --squash feature
git commit -q --allow-empty -m "Generated title that is not conventional (#1)

* feat!: the breaking change

* docs: follow-up"
expect "squash with a non-conventional title reads its body bullets" v0.1.0

repo s v0.0.5
git checkout -q -b feature
commit "feat!: the breaking change"
git checkout -q -
git merge -q feature
expect "fast-forward reads the branch commits" v0.1.0

# --- tags -------------------------------------------------------------------
repo t; commit "feat!: first"
expect "no tags at all starts from v0.0.0" v0.1.0

repo u v0.0.5; git tag v0.0.6-rc1; commit "fix: x"
expect "prerelease tags are skipped" v0.0.6

repo v v0.0.5; git tag -a v0.0.6 -m annotated; commit "fix: x"
expect "annotated tags are read" v0.0.7

repo w v0.0.5
expect "no commits since the tag -> skip" skip

# ----------------------------------------------------------------------------
cd /
printf '\n%d/%d passed\n' "$((total - failures))" "$total"
[ "$failures" -eq 0 ]
