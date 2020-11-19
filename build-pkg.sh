#!/bin/bash

# Build deb or rpm packages for Refinery.
set -e

function usage() {
    echo "Usage: build-pkg.sh -m <arch> -v <version> -t <package_type>"
    exit 2
}

while getopts "v:t:m:" opt; do
    case "$opt" in
    v)
        version=$OPTARG
        ;;
    t)
        pkg_type=$OPTARG
        ;;
    m)
        arch=$OPTARG
        ;;
    esac
done

if [ -z "$pkg_type" ] || [ -z "$arch" ]; then
    usage
fi

if [ -z "$version" ]; then
    version=v0.0.0-dev
fi

fpm -s dir -n refinery \
    -m "Honeycomb <team@honeycomb.io>" \
    -v ${version#v} \
    -t $pkg_type \
    -a $arch \
    --pre-install=./preinstall \
    $GOPATH/bin/refinery-linux-${arch}=/usr/bin/refinery \
    ./refinery.upstart=/etc/init/refinery.conf \
    ./refinery.service=/lib/systemd/system/refinery.service \
    ./config.toml=/etc/refinery/refinery.toml \
    ./rules.toml=/etc/refinery/rules.toml
