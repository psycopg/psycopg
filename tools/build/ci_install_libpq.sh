#!/bin/bash

# Install the desired libpw in github action (Linux runner)
#
# Specify `oldest` or `newest` as first argument in order to choose the oldest
# available to the debian distro or the newest available from the pgdg ppa.

set -euo pipefail
set -x

libpq=${1:-}

case "$libpq" in
    "")
        # Assume a libpq is already installed in the system. We don't care about
        # the version.
        exit 0
        ;;

    oldest)
        curl -sL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
            | gpg --dearmor \
            | sudo tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg > /dev/null

        rel=$(lsb_release -c -s)
        echo "deb http://apt.postgresql.org/pub/repos/apt ${rel}-pgdg main 10" \
            | sudo tee -a /etc/apt/sources.list.d/pgdg.list > /dev/null
        sudo apt-get -qq update
        pqver=$(apt-cache show libpq5 | grep ^Version: | tail -1 | awk '{print $2}')
        sudo apt-get -qq -y --allow-downgrades install "libpq-dev=${pqver}" "libpq5=${pqver}"
        ;;

    newest)
        curl -sL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
            | gpg --dearmor \
            | sudo tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg > /dev/null

        # NOTE: in order to test with a preview release, add its number to
        # the deb entry. For instance, to test on preview Postgres 16, use:
        # "deb http://apt.postgresql.org/pub/repos/apt ${rel}-pgdg main 16"
        rel=$(lsb_release -c -s)
        echo "deb http://apt.postgresql.org/pub/repos/apt ${rel}-pgdg main" \
            | sudo tee -a /etc/apt/sources.list.d/pgdg.list > /dev/null
        sudo apt-get -qq update

        pqver=$(apt-cache show libpq5 | grep ^Version: | head -1 | awk '{print $2}')
        sudo apt-get -qq -y install "libpq-dev=${pqver}" "libpq5=${pqver}"
        ;;

    *)
        echo "Unexpected wanted libpq: '${libpq}'" >&2
        exit 1
        ;;

esac
