#!/usr/bin/env bash
# Prints the tag for the next release, or "skip" when there is nothing to
# release. Diagnostics go to stderr so stdout stays machine-readable.
#
# The release level comes from every commit since the last release tag, so it
# does not depend on whether a pull request was squashed, merged or rebased.
#
# "Release-As: skip" is the exception: it is read only from what the push
# introduced, because skipping creates no tag and a skip read from the whole
# range would still be in the range next time, disabling releases for good.
# That covers a squash and a merge commit; a rebase that leaves the trailer on
# a commit below the tip releases normally and says so. See
# next-version_test.sh for the behaviour this must keep.
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

# Read every commit since that tag rather than only the tip, so the
# bump does not depend on whether the pull request was squashed,
# merged or rebased.
if [ -n "$tag" ]; then
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

# Strip CR: the merge UI submits textarea content without git's
# message cleanup, and a trailing CR would defeat the whole-line
# match on the Release-As trailer below.
subjects=$(git log "$range" --pretty=%s | tr -d '\r')
messages=$(git log "$range" --pretty=%B | tr -d '\r')
if git rev-parse -q --verify HEAD^ >/dev/null 2>&1; then
  merged=$(git log HEAD^..HEAD --pretty=%B | tr -d '\r')
else
  merged=$(git log -1 --pretty=%B | tr -d '\r')
fi

# A squash merge collapses the branch into one commit whose body
# lists the original subjects as "* subject", so read those as
# subjects too - otherwise a squash with a non-conventional title
# hides the breaking change that the commits themselves declared.
# Only when the range really is one commit: over a longer range, an
# ordinary markdown bullet in a commit body would otherwise get to
# choose the release level.
if [ "$(git rev-list --count "$range")" -eq 1 ]; then
  status=0
  bullets=$(grep -E '^\* ' <<< "$messages") || status=$?
  if [ "$status" -gt 1 ]; then
    echo "grep failed while reading the squash body" >&2
    exit 1
  fi
  if [ -n "$bullets" ]; then
    subjects=$(printf '%s\n%s\n' "$subjects" "$(sed -E 's/^\* //' <<< "$bullets")")
  fi
fi

# An explicit override has to be its own trailer line. Matching a bare
# marker anywhere in the prose would let a commit that merely
# *mentions* it cut the wrong release.
# The trailer must start the line, as git's own trailer parsing requires:
# an indented Release-As: is how a commit body *documents* the convention, and
# matching that would let a docs commit cut a release. Whitespace after the
# colon and at the end of the line is noise the merge UI adds, so it is
# tolerated.
level=""
if grep -qE '^Release-As:[[:space:]]*major[[:space:]]*$' <<< "$messages"; then
  level="major"
elif grep -qE '^Release-As:[[:space:]]*minor[[:space:]]*$' <<< "$messages"; then
  level="minor"
elif grep -qE '^Release-As:[[:space:]]*patch[[:space:]]*$' <<< "$messages"; then
  level="patch"
elif grep -qE '^Release-As:[[:space:]]*skip[[:space:]]*$' <<< "$merged"; then
  # Nothing here reaches a consumer - a workflow change, a README edit - so
  # publishing a version identical to the last one would be noise.
  #
  # Read from what this push introduced, unlike every other level, which reads
  # the whole range. Skipping creates no tag, so a skip found anywhere in the
  # range would still be in the range on the next push, and every release after
  # it would skip too - one skip would disable releases permanently. Reading
  # only the merged commits makes a skip defer rather than suppress: the next
  # push releases normally and carries the skipped commits with it.
  echo "Release-As: skip; nothing to release." >&2
  echo "skip"
  exit 0
elif grep -qE '^Release-As:[[:space:]]*skip[[:space:]]*$' <<< "$messages"; then
  # Spelled correctly, just not on what this push introduced. Usually this is
  # a previous push that deliberately skipped and is now being carried, which
  # is the common path in this repo - so describe the outcome rather than
  # implying the operator got something wrong. The range alone cannot tell
  # that apart from a rebase that left the trailer below the tip.
  echo "A Release-As: skip earlier in the range does not apply to this push; releasing normally and carrying those commits." >&2
elif grep -qE '^Release-As:' <<< "$messages"; then
  # Present but unreadable: say so rather than fall through to the commit
  # subject, which would silently produce a different version.
  echo "Release-As: trailer found but its level is not major, minor, patch or skip; ignoring it." >&2
fi

if [ -z "$level" ]; then
  if grep -qE '^BREAKING[ -]CHANGE: ' <<< "$messages" \
    || grep -qE '^[a-z]+(\([^)]*\))?!:' <<< "$subjects"; then
    intent="breaking"
  elif grep -qE '^feat(\([^)]*\))?:' <<< "$subjects"; then
    intent="feature"
  else
    intent="fix"
  fi

  # Below v1.0.0, semver keeps breaking changes in the minor position
  # and everything else in the patch position.
  if [ "$major" -eq 0 ]; then
    case "$intent" in
      breaking) level="minor" ;;
      *)        level="patch" ;;
    esac
  else
    case "$intent" in
      breaking) level="major" ;;
      feature)  level="minor" ;;
      *)        level="patch" ;;
    esac
  fi
fi

case "$level" in
  major) major=$((major + 1)); minor=0; patch=0 ;;
  minor) minor=$((minor + 1)); patch=0 ;;
  patch) patch=$((patch + 1)) ;;
esac

next="v${major}.${minor}.${patch}"
echo "Bumping ${tag} -> ${next} (${level})" >&2
echo "$next"
