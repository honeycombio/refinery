set -o nounset
set -o pipefail
set -o xtrace

TAGS="latest"
VERSION="dev"
if [[ -n ${CIRCLE_TAG:-} ]]; then
    # trim 'v' prefix if present
    VERSION=${CIRCLE_TAG#"v"}
    # append version to image tags
    TAGS+=",$VERSION"
fi

unset GOOS
unset GOARCH
export KO_DOCKER_REPO=${KO_DOCKER_REPO:-ko.local}
export GOFLAGS="-ldflags=-X=main.BuildID=$VERSION"
export SOURCE_DATE_EPOCH=$(date +%s)
# shellcheck disable=SC2086
ko publish \
  --tags "${TAGS}" \
  --base-import-paths \
  --platform "linux/amd64,linux/arm64" \
  --image-label org.opencontainers.image.source=https://github.com/honeycombio/refinery \
  --image-label org.opencontainers.image.licenses=Apache-2.0 \
  --image-label org.opencontainers.image.revision=${CIRCLE_SHA1} \
  ./cmd/refinery
