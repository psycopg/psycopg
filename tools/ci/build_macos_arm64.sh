#!/bin/bash

# Build psycopg-binary wheel packages for Apple M1 (cpNNN-macosx_arm64)
#
# This script is designed to run on Scaleway Apple Silicon machines.
#
# The script cannot be run as sudo (installing brew fails), but requires sudo,
# so it can pretty much only be executed by a sudo user as it is.

set -euo pipefail

python_versions="3.9.19 3.10.14 3.11.9 3.12.5 3.13.0"
pg_version=17

function log {
    echo "$@" >&2
}

# Move to the root of the project
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${dir}/../../"

# Add /usr/local/bin to the path. It seems it's not, in non-interactive sessions
if ! (echo $PATH | grep -q '/usr/local/bin'); then
    export PATH=/usr/local/bin:$PATH
fi

# Install brew, if necessary. Otherwise just make sure it's in the path
if [[ -x /opt/homebrew/bin/brew ]]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
else
    log "installing brew"
    command -v brew > /dev/null || (
        # Not necessary: already installed
        # xcode-select --install
        NONINTERACTIVE=1 /bin/bash -c "$(curl -fsSL \
            https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
    )
    eval "$(/opt/homebrew/bin/brew shellenv)"
fi

export PGDATA=/opt/homebrew/var/postgresql@${pg_version}

# Install PostgreSQL, if necessary
command -v pg_config > /dev/null || (
    log "installing postgres"
    brew install postgresql@${pg_version}
)

# Starting from PostgreSQL 15, the bin path is not in the path.
export PATH="$(ls -d1 /opt/homebrew/Cellar/postgresql@${pg_version}/*/bin):$PATH"

# Make sure the server is running

# Currently not working
# brew services start postgresql@${pg_version}

if ! pg_ctl status; then
    log "starting the server"
    pg_ctl -l "/opt/homebrew/var/log/postgresql@${pg_version}.log" start
fi


# Install the Python versions we want to build
for ver_full in $python_versions; do
    # Get the major.minor.patch version, without pre-release markers
    ver3=$(echo $ver_full | sed 's/\([0-9]*\.[0-9]*\.[0-9]*\).*/\1/')

    # Get the major.minor version
    ver2=$(echo $ver3 | sed 's/\([^\.]*\)\(\.[^\.]*\)\(.*\)/\1\2/')

    command -v python${ver2} > /dev/null || (
        log "installing Python $ver_full"
        (cd /tmp &&
            curl -fsSl -O \
                https://www.python.org/ftp/python/${ver3}/python-${ver_full}-macos11.pkg)
        sudo installer -pkg /tmp/python-${ver_full}-macos11.pkg -target /
    )
done

# Create a virtualenv where to work
if [[ ! -x .venv/bin/python ]]; then
    log "creating a virtualenv"
    python3 -m venv .venv
fi

log "installing cibuildwheel"
source .venv/bin/activate
pip install cibuildwheel

log "building wheels"

# Create the psycopg_binary source package
rm -rf psycopg_binary
python tools/ci/copy_to_binary.py

# Build the binary packages
export CIBW_PLATFORM=macos
export CIBW_ARCHS=arm64
export CIBW_BUILD='cp{38,39,310,311,312,313}-*'
export CIBW_TEST_REQUIRES="./psycopg[test] ./psycopg_pool"
export CIBW_TEST_COMMAND="pytest {project}/tests -m 'not slow and not flakey' --color yes"

export PSYCOPG_IMPL=binary
export PSYCOPG_TEST_DSN="dbname=postgres"
export PSYCOPG_TEST_WANT_LIBPQ_BUILD=">= ${pg_version}"
export PSYCOPG_TEST_WANT_LIBPQ_IMPORT=">= ${pg_version}"

cibuildwheel psycopg_binary
