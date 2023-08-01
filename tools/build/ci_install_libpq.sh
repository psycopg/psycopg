#!/bin/bash

# Install the desired libpq in github action (Linux runner)
#
# Specify `oldest` or `newest` as first argument in order to choose the oldest
# available to the debian distro or the newest available from the pgdg ppa.

set -euo pipefail
set -x

libpq=${1:-}
rel=$(lsb_release -c -s)

setup_repo () {
    version=${1:-}
    repo_suffix=${2:-pgdg}
    curl -sL -o /etc/apt/trusted.gpg.d/apt.postgresql.org.asc \
        https://www.postgresql.org/media/keys/ACCC4CF8.asc
    echo "deb http://apt.postgresql.org/pub/repos/apt ${rel}-${repo_suffix} main ${version}" \
        >> /etc/apt/sources.list.d/pgdg.list
    apt-get -qq update
}

case "$libpq" in
    "")
        # Assume a libpq is already installed in the system. We don't care about
        # the version.
        exit 0
        ;;

    oldest)
        setup_repo 10
        pqver=$(apt-cache show libpq5 | grep ^Version: | tail -1 | awk '{print $2}')
        apt-get -qq -y --allow-downgrades install "libpq-dev=${pqver}" "libpq5=${pqver}"
        ;;

    newest)
        setup_repo
        pqver=$(apt-cache show libpq5 | grep ^Version: | head -1 | awk '{print $2}')
        apt-get -qq -y install "libpq-dev=${pqver}" "libpq5=${pqver}"
        ;;

    master)
        setup_repo 17 pgdg-snapshot
        pqver=$(apt-cache show libpq5 | grep ^Version: | head -1 | awk '{print $2}')
        apt-get -qq -y install "libpq-dev=${pqver}" "libpq5=${pqver}"
        ;;

    *)
        echo "Unexpected wanted libpq: '${libpq}'" >&2
        exit 1
        ;;

esac
