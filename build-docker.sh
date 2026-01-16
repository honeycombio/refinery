#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o xtrace

### Versioning and image tagging ###
#
# Three build scenarios:
#   1. CI release build: triggered by git tag
#      - Stable (vX.Y.Z): tagged with major, minor, patch, and "latest"
#      - Pre-release (vX.Y.Z-suffix): tagged only with exact version
#   2. CI branch build: version + CI job ID, tagged with branch name (+ "latest" if main)
#   3. Local build: version from git describe, tagged with that version

# Get version info from git (used by branch and local builds)
#   --tags: use any tag, not just annotated ones
#   --match='v[0-9]*': only version tags (starts with v and a digit)
#   --always: fall back to commit ID if no tag found
# e.g., v2.1.1-45-ga1b2c3d means commit a1b2c3d, 45 commits ahead of tag v2.1.1
VERSION_FROM_GIT=$(git describe --tags --match='v[0-9]*' --always)

if [[ -n "${CIRCLE_TAG:-}" ]]; then
  # Release build (triggered by git tag)
  VERSION=${CIRCLE_TAG#"v"}

  if [[ "${CIRCLE_TAG}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    # Stable release: tag with major, minor, patch, and latest
    # e.g., v2.1.1 -> "2", "2.1", "2.1.1", "latest"
    MAJOR_VERSION=${VERSION%%.*}
    MINOR_VERSION=${VERSION%.*}
    TAGS="$MAJOR_VERSION,$MINOR_VERSION,$VERSION,latest"
  else
    # Pre-release: only the exact version tag
    # e.g., v3.0.0-rc1 -> "3.0.0-rc1"
    TAGS="$VERSION"
  fi

elif [[ -n "${CIRCLE_BRANCH:-}" ]]; then
  # CI branch build
  # Version from git describe + CI job ID
  # e.g., 2.1.1-45-ga1b2c3d-ci8675309
  VERSION="${VERSION_FROM_GIT#'v'}-ci${CIRCLE_BUILD_NUM}"
  BRANCH_TAG=${CIRCLE_BRANCH//\//-}
  TAGS="${VERSION},branch-${BRANCH_TAG}"

  # Main branch builds are tagged "latest" in the private registry
  if [[ "${CIRCLE_BRANCH}" == "main" ]]; then
    TAGS+=",latest"
  fi

else
  # Local build
  # Version from git describe only
  # e.g., 2.1.1-45-ga1b2c3d
  VERSION=${VERSION_FROM_GIT#'v'}
  TAGS="${VERSION}"
fi

GIT_COMMIT=${CIRCLE_SHA1:-$(git rev-parse HEAD)}

unset GOOS
unset GOARCH
export GOFLAGS="-ldflags=-X=main.BuildID=$VERSION"
export SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(make latest_modification_time)}

# Build the image once, either to a remote registry designated by PRIMARY_DOCKER_REPO
# or to the local repository as "ko.local/refinery:<tags>" if PRIMARY_DOCKER_REPO is not set.
export KO_DOCKER_REPO="${PRIMARY_DOCKER_REPO:-ko.local}"

echo "Building image locally with ko for multi-registry push..."
# shellcheck disable=SC2086
IMAGE_REF=$(./ko publish \
  --tags "${TAGS}" \
  --base-import-paths \
  --platform "linux/amd64,linux/arm64" \
  --image-label org.opencontainers.image.source=https://github.com/honeycombio/refinery \
  --image-label org.opencontainers.image.licenses=Apache-2.0 \
  --image-label org.opencontainers.image.revision=${GIT_COMMIT} \
  ./cmd/refinery)

echo "Built image: ${IMAGE_REF}"

# If COPY_DOCKER_REPOS is set, copy the built image to each of the listed registries.
# This is a comma-separated list of registry/repo names, e.g.
#   "public.ecr.aws/honeycombio,ghcr.io/honeycombio/refinery"
if [[ -n "${COPY_DOCKER_REPOS:-}" ]]; then
  echo "Pushing to multiple registries: ${COPY_DOCKER_REPOS}"

  IFS=',' read -ra REPOS <<< "$COPY_DOCKER_REPOS"
  for REPO in "${REPOS[@]}"; do
    REPO=$(echo "$REPO" | xargs) # trim whitespace
    echo "Tagging and pushing to: $REPO"

    # Tag for each tag in the TAGS list
    IFS=',' read -ra TAG_LIST <<< "$TAGS"
    for TAG in "${TAG_LIST[@]}"; do
      TAG=$(echo "$TAG" | xargs) # trim whitespace
      TARGET_IMAGE="$REPO/refinery:$TAG"
      echo "Copying $IMAGE_REF to $TARGET_IMAGE"
      ./crane copy "$IMAGE_REF" "$TARGET_IMAGE"
    done
  done
fi
