#!/bin/bash

# Configure the environment needed to build wheel packages on Mac OS.
# This script is designed to be used by cibuildwheel as CIBW_BEFORE_ALL_MACOS
#
# The PG_VERSION env var must be set to a Postgres major version (e.g. 16).

set -euo pipefail

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
prjdir="$( cd "${dir}/../.." && pwd )"

# Build dependency libraries
"${prjdir}/tools/ci/build_libpq.sh"

# Show dependency tree
otool -L /tmp/libpq.build/lib/*.dylib

brew install gnu-sed postgresql@${PG_VERSION}
brew link --overwrite postgresql@${PG_VERSION}

# Start the database for testing
brew services start postgresql@${PG_VERSION}

# Wait for postgres to come up
for i in $(seq 10 -1 0); do
  eval pg_isready && break
  if [ $i == 0 ]; then
      echo "PostgreSQL service not ready, giving up"
      exit 1
  fi
  echo "PostgreSQL service not ready, waiting a bit, attempts left: $i"
  sleep 5
done
