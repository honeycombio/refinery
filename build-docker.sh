#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o xtrace

# Determine a version number based on most recent version tag in git.
#   git describe - Return the most recent annotated tag that is reachable from a commit.
#     --tags - OK, any tag, not just the annotated ones. We don't always remember to annotate a version tag.
#     --match='v[0-9]*' - But of all those tags, only the version ones (starts with a v and a digit).
#     --always - … and if a tag can't be found, fallback to the commit ID.
# Ex: v2.1.1-45-ga1b2c3d
#   - The build was on git commit ID a1b2c3d.
#   - v2.1.1 is the most recent version tag in the history behind that commit
#   - That commit is 45 commits ahead of that version tag.
VERSION_FROM_GIT=$(git describe --tags --match='v[0-9]*' --always)
# trim the v prefix per Docker image version-tagging conventions
VERSION=${VERSION_FROM_GIT#'v'}

# If we are doing a dev build on circle, append the build number (job id) to the version
# Ex: v2.1.1-45-ga1b2c3d-ci8675309
#   - The git information gleaned above plus ...
#   - The build happened within CircleCI job 8675309.
if [[ -n "${CIRCLE_BUILD_NUM:-}" ]]; then
  VERSION="${VERSION}-ci${CIRCLE_BUILD_NUM}"
fi

### Image tagging ###
TAGS="${VERSION}"

## CI dev tagging: append the dev branch name to image tags
if [[ -n "${CIRCLE_BRANCH:-}" ]]; then
  BRANCH_TAG=${CIRCLE_BRANCH//\//-}
  TAGS+=",branch-${BRANCH_TAG}"

  # If the dev build is on main, we tag it as latest in ECR
  if [[ "${CIRCLE_BRANCH}" == "main" ]]; then
      TAGS+=",latest"
  fi
fi

## CI release tagging: apply major, major.minor, major.minor.patch, and latest tags

# If we're running off a git tag, it's a release build.
# Stable releases (vX.Y.Z) get major, minor, patch, and latest tags.
# Pre-releases (vX.Y.Z-suffix) only get the exact version tag.

if [[ -n ${CIRCLE_TAG:-} ]]; then
  VERSION=${CIRCLE_TAG#"v"} # trim the v prefix per version-tagging convention

  # Reset tag list based on release type
  if [[ "${CIRCLE_TAG}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    # Stable release: tag with major, minor, patch, and latest
    # So v2.1.1 would be tagged with "2", "2.1", "2.1.1", and "latest".
    MAJOR_VERSION=${VERSION%%.*}
    MINOR_VERSION=${VERSION%.*}
    TAGS="$MAJOR_VERSION,$MINOR_VERSION,$VERSION,latest"
  else
    # Pre-release: only the exact version tag
    # So v3.0.0-rc1 would be tagged only with "3.0.0-rc1".
    TAGS="$VERSION"
  fi
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
