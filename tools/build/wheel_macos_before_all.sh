#!/bin/bash

# Configure the environment needed to build wheel packages on Mac OS.
# This script is designed to be used by cibuildwheel as CIBW_BEFORE_ALL_MACOS

set -euo pipefail
set -x

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

brew update
brew install gnu-sed postgresql@14
# Fetch 14.1 if 14.0 is still the default version
brew reinstall postgresql

# Start the database for testing
brew services start postgresql

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
