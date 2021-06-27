#!/bin/bash

# Configure the libraries needed to build wheel packages on linux.
# This script is designed to be used by cibuildwheel as CIBW_BEFORE_ALL_LINUX

set -euo pipefail
set -x

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Install libpq
# Note that the pgdg doesn't have an aarch64 repository so wheels are build
# with the libpq packaged with Debian 9, which is 9.6.
if [[ ! "$AUDITWHEEL_ARCH" = "aarch64" ]]; then
    source /etc/os-release
    echo "deb http://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list
    curl --silent https://www.postgresql.org/media/keys/ACCC4CF8.asc \
        | apt-key add -
fi
apt-get update
apt-get -y install libpq-dev
