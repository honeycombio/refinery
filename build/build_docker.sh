set -o nounset
set -o pipefail
set -o xtrace

TAGS="latest"
VERSION=${CIRCLE_TAG:-dev}
REPO=${KO_DOCKER_REPO:-ko.local}
if [[ $VERSION != "dev" ]]; then
    # set docker username and add version tag, trimming 'v' prefix if present
    VERSION=${VERSION#"v"}
    REPO="honeycombio"
    TAGS+=",$VERSION"
fi

unset GOOS
unset GOARCH
export KO_DOCKER_REPO=$REPO
export GOFLAGS="-ldflags=-X=main.BuildID=$VERSION"
export SOURCE_DATE_EPOCH=$(date +%s)
# shellcheck disable=SC2086
ko publish \
  --tags "${TAGS}" \
  --base-import-paths \
  --platform "linux/amd64,linux/arm64" \
  ./cmd/refinery
