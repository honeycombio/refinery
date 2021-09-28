set -o nounset
set -o pipefail

VERSION=${VERSION:-dev}
PLATFORM="${PLATFORM:-linux/amd64,linux/arm64,darwin/amd64}"

unset GOOS
unset GOARCH
export KO_DOCKER_REPO=${KO_DOCKER_REPO:-ko.local}
export GOFLAGS="-ldflags=-X=main.version=${VERSION}"
export SOURCE_DATE_EPOCH=$(date +%s)
# shellcheck disable=SC2086
ko publish \
  --tags "head,${VERSION}" \
  --base-import-paths \
  --platform "${PLATFORM}" \
  ${PUBLISH_ARGS-} \
  ./cmd/refinery
