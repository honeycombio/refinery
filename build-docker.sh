#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o xtrace

TAGS=""
# Check if CIRCLE_BRANCH is set and not empty
if [[ -n "${CIRCLE_BRANCH}" ]]; then
    BRANCH_TAG=${CIRCLE_BRANCH//\//-}
    TAGS="${CIRCLE_BUILD_NUM},branch-${BRANCH_TAG}"
fi

# We only want to tag main with latest on ECR
if [[ "${CIRCLE_BRANCH}" == "main" ]]; then
    TAGS+=",latest"
fi

# Determine a version number based on most recent version tag in git.
#   git describe - Return the most recent annotated tag that is reachable from a commit.
#     --tags - OK, any tag, not just the annotated ones. We don't always remember to annotate a version tag.
#     --match='v[0-9]*' - But of all those tags, only the version ones (starts with a v and a digit).
#     --always - … and if a tag can't be found, fallback to the commit ID.
# Ex: v2.1.1-45-ga1b2c3d
#   - The build was on git commit ID a1b2c3d.
#   - 2.1.1 is the most recent version tag in the history behind that commit
#   - That commit is 45 commits ahead of that version tag.
VERSION=$(git describe --tags --match='v[0-9]*' --always)

# If we are doing a dev build on circle, append the build number (job id) to the version
# Ex: v2.1.1-45-ga1b2c3d-ci8675309
#   - The git information gleaned above plus ...
#   - The build happened within CircleCI job 8675309.
if [[ -n "${CIRCLE_BUILD_NUM:-}" ]]; then
  VERSION="${VERSION}-ci${CIRCLE_BUILD_NUM}"
fi

# if we're running off a git tag, it is a release which we tag with the versions as well as latest
# caution: this means if we ever release an update to a previous version, it will be marked latest
# it is probably best if people just use the major or minor version tags

if [[ -n ${CIRCLE_TAG:-} ]]; then
  # trim 'v' prefix if present
  VERSION=${CIRCLE_TAG#"v"}

  # Extract major, major.minor, and major.minor.patch versions
  MAJOR_VERSION=${VERSION%%.*}
  MINOR_VERSION=${VERSION%.*}

  # Append versions to image tags
  # So 2.1.1 would be tagged with "2","2.1","2.1.1"
  TAGS="$MAJOR_VERSION,$MINOR_VERSION,$VERSION,latest"
fi

unset GOOS
unset GOARCH
export KO_DOCKER_REPO=${KO_DOCKER_REPO:-ko.local}
export GOFLAGS="-ldflags=-X=main.BuildID=$VERSION"
export SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(make latest_modification_time)}
# shellcheck disable=SC2086
./ko publish \
  --tags "${TAGS}" \
  --base-import-paths \
  --platform "linux/amd64,linux/arm64" \
  --image-label org.opencontainers.image.source=https://github.com/honeycombio/refinery \
  --image-label org.opencontainers.image.licenses=Apache-2.0 \
  --image-label org.opencontainers.image.revision=${CIRCLE_SHA1} \
  ./cmd/refinery
