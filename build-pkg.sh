#!/bin/bash

# Build deb or rpm packages for samproxy.
set -e

function usage() {
    echo "Usage: build-pkg.sh -v <version> -t <package_type>"
    exit 2
}

while getopts "v:t:" opt; do
    case "$opt" in
    v)
        version=$OPTARG
        ;;
    t)
        pkg_type=$OPTARG
        ;;
    esac
done

if [ -z "$version" ] || [ -z "$pkg_type" ]; then
    usage
fi

fpm -s dir -n samproxy \
    -m "Honeycomb <support@honeycomb.io>" \
    -p $GOPATH/bin \
    -v $version \
    -t $pkg_type \
    --pre-install=./preinstall \
    $GOPATH/bin/samproxy=/usr/bin/samproxy \
    ./samproxy.upstart=/etc/init/samproxy.conf \
    ./samproxy.service=/lib/systemd/system/samproxy.service \
    ./config.toml=/etc/samproxy/samproxy.toml
