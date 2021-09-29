set -o nounset
set -o pipefail

VERSION="${VERSION:-dev}"
TAGS="latest"
if [[ $VERSION != "dev" ]]; then
    TAGS+=",$VERSION"
fi

unset GOOS
unset GOARCH
export KO_DOCKER_REPO=${KO_DOCKER_REPO:-ko.local}
export GOFLAGS="-ldflags=-X=main.version=$VERSION"
export SOURCE_DATE_EPOCH=$(date +%s)
# shellcheck disable=SC2086
ko publish \
  --tags "${TAGS}" \
  --base-import-paths \
  --platform "linux/amd64,linux/arm64,darwin/amd64" \
  ./cmd/refinery
