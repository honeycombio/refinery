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
    # set ko target repo as docker org name so it creates image named '{org}}/{project}'
    # and will attempt to publish to docker hub
    export KO_DOCKER_REPO="honeycombio"
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
  ./cmd/refinery

# retag to correct org/repo
docker image tag ko.local/refinery honeycombio/refinery:latest
