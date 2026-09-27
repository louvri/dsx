#!/usr/bin/env bash
# Prints the tag for the next release, or "skip" when there is nothing to
# release. Diagnostics go to stderr so stdout stays machine-readable.
#
# The release level comes from the title of the merged pull request, passed
# in as PR_TITLE (release.yml looks it up). Matching ignores case and counts
# whole words only:
#
#   the word "major"  -> major
#   the word "feat"   -> minor  (feat:, feat(api):, feat!:)
#   Revert "..."      -> patch, whatever the reverted title said
#   anything else     -> patch, including a push with no pull request
#
# "Release-As: skip" on the merged commits publishes nothing. It is read only
# from what the push introduced, because skipping creates no tag: a skip read
# from the whole range would still be in the range next time, disabling
# releases for good. See next-version_test.sh for the behaviour this must keep.
set -euo pipefail

# Read the tag list into a variable rather than piping to head: with
# `pipefail`, git being SIGPIPE'd once the list outgrows the pipe
# buffer would fail the step.
tags=$(git tag --sort=-v:refname --list 'v[0-9]*.[0-9]*.[0-9]*')

# Take the newest tag that is exactly vMAJOR.MINOR.PATCH; the glob
# above still admits things like v1.2.3-rc1. If no stable tag exists
# at all, start from v0.0.0.
tag=""
major=0
minor=0
patch=0
while IFS= read -r candidate; do
  if [[ "$candidate" =~ ^v([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
    tag="$candidate"
    major="${BASH_REMATCH[1]}"
    minor="${BASH_REMATCH[2]}"
    patch="${BASH_REMATCH[3]}"
    break
  fi
done <<< "$tags"

if [ -n "$tag" ]; then
  # The newest tag is not an ancestor when a later push has already released
  # and this is a re-run for an older commit. Tagging it would publish a
  # version above one that has changes this commit lacks, so stop instead.
  if ! git merge-base --is-ancestor "$tag" HEAD; then
    echo "${tag} is not an ancestor of HEAD: a later push has already released. Carry this change's level in the next pull request's title." >&2
    exit 1
  fi
  range="${tag}..HEAD"
else
  tag="v0.0.0"
  range="HEAD"
fi
if [ -z "$(git rev-list -n 1 "$range")" ]; then
  echo "No commits since ${tag}; nothing to release." >&2
  echo "skip"
  exit 0
fi

# HEAD^..HEAD is the squash commit, or a merge commit plus the branch it
# brought in. Strip CR: the merge UI submits textarea content without git's
# message cleanup, and a trailing CR would defeat the whole-line match below.
if git rev-parse -q --verify HEAD^ >/dev/null 2>&1; then
  merged=$(git log HEAD^..HEAD --pretty=%B | tr -d '\r')
else
  merged=$(git log -1 --pretty=%B | tr -d '\r')
fi
# The trailer must start the line, as git's own trailer parsing requires: an
# indented Release-As: is how a commit body *documents* the convention.
# Whitespace after the colon and at the end of the line is noise the merge UI
# adds, so it is tolerated.
if grep -qE '^Release-As:[[:space:]]*skip[[:space:]]*$' <<< "$merged"; then
  echo "Release-As: skip; nothing to release." >&2
  echo "skip"
  exit 0
elif grep -qE '^Release-As:' <<< "$merged"; then
  echo "Release-As: only accepts skip; the level comes from the pull request title." >&2
fi

title="${PR_TITLE:-}"
if [ -z "$title" ]; then
  echo "No pull request title; releasing a patch." >&2
fi
# Whole words, so "majority", "is_major", "major-version" or "feature" cannot
# choose the level. GitHub titles a revert 'Revert "<original title>"', which
# would otherwise re-apply the reverted change's level.
if grep -qiE '^Revert[[:space:]]+"' <<< "$title"; then
  level="patch"
elif grep -qiE '(^|[^[:alnum:]_-])major([^[:alnum:]_-]|$)' <<< "$title"; then
  level="major"
elif grep -qiE '(^|[^[:alnum:]_-])feat([^[:alnum:]_-]|$)' <<< "$title"; then
  level="minor"
else
  level="patch"
fi

case "$level" in
  major) major=$((major + 1)); minor=0; patch=0 ;;
  minor) minor=$((minor + 1)); patch=0 ;;
  patch) patch=$((patch + 1)) ;;
esac

# Go ties the major version to the module path: v2 and later need a path
# ending in /vN, v0 and v1 a path without one. A tag that breaks the rule
# cannot be installed, and cannot be withdrawn from the proxy either, so
# refuse to cut it. Read go.mod as committed, since that is what gets tagged.
module=""
module_re='^module[[:space:]]+"?([^"[:space:]]+)"?[[:space:]]*(//.*)?$'
while IFS= read -r line; do
  if [[ "${line%$'\r'}" =~ $module_re ]]; then
    module="${BASH_REMATCH[1]}"
    break
  fi
done <<< "$(git show HEAD:go.mod 2>/dev/null || true)"
if [ -z "$module" ]; then
  echo "No module path found in go.mod at HEAD; cannot check it against v${major}." >&2
  exit 1
fi
suffix=""
if [[ "$module" =~ /v([0-9]+)$ ]]; then
  suffix="${BASH_REMATCH[1]}"
fi
want=""
if [ "$major" -ge 2 ]; then
  want="$major"
fi
if [ "$suffix" != "$want" ]; then
  if [ -n "$want" ]; then
    echo "A v${major} release needs the module path to end in /v${major}; go.mod declares ${module}." >&2
  else
    echo "A v${major} release needs a module path without a /vN suffix; go.mod declares ${module}." >&2
  fi
  exit 1
fi

next="v${major}.${minor}.${patch}"
echo "Bumping ${tag} -> ${next} (${level})" >&2
echo "$next"
