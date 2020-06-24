#!/bin/bash

# Build deb or rpm packages for samproxy.
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

fpm -s dir -n samproxy \
    -m "Honeycomb <team@honeycomb.io>" \
    -v ${version#v} \
    -t $pkg_type \
    -a $arch \
    --pre-install=./preinstall \
    $GOPATH/bin/samproxy-linux-${arch}=/usr/bin/samproxy \
    ./samproxy.upstart=/etc/init/samproxy.conf \
    ./samproxy.service=/lib/systemd/system/samproxy.service \
    ./config.toml=/etc/samproxy/samproxy.toml
