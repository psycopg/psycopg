#!/bin/bash

# Script to build binary psycopg-c package.
# Built packages will be available in the `dist` directory.

# Copyright (C) 2020-2021 The Psycopg Team

set -euo pipefail
set -x

# work in progress: currently running with:
#
# docker run --rm \
#   -e PLAT=manylinux2014_x86_64 \
#   -e PSYCOPG_TEST_DSN="host=172.17.0.1 user=piro dbname=psycopg_test" \
#   -v `pwd`:/psycopg \
#   quay.io/pypa/manylinux2014_x86_64 /psycopg/tools/build_wheels.sh

# The version of psycopg we are building
version=$( \
    (cat psycopg/psycopg/psycopg/version.py && echo "print(__version__)") \
    | python)

function repair_wheel {
    wheel="$1"
    if ! auditwheel show "$wheel"; then
        echo "Skipping non-platform wheel $wheel"
    else
        auditwheel repair "$wheel" --plat "$PLAT" -w /psycopg/dist/
    fi
}

# Install system packages required to build the library
yum_url=https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
yum install -y $yum_url
yum install -y postgresql13-devel

# Make pg_config available
export PATH="/usr/pgsql-13/bin/:$PATH"

# Using --global-option="-L/usr/pgsql-13/lib/" disables wheels, so no-go.
cp -avr /usr/pgsql-13/lib/* /usr/lib/

# Patch a copy of the c package to name it -binary
cp -r /psycopg/psycopg_c /psycopg_binary
mv /psycopg_binary/{psycopg_c,psycopg_binary}/
sed -i 's/psycopg-c/psycopg-binary/' /psycopg_binary/setup.cfg
sed -i "s/__impl__[[:space:]]*=.*/__impl__ = 'binary'/" \
    /psycopg_binary/psycopg_binary/pq.pyx
find /psycopg_binary/ -name \*.pyx -or -name \*.pxd -or -name \*.py \
    | xargs sed -i 's/\bpsycopg_c\b/psycopg_binary/'

# Compile wheels
for PYBIN in /opt/python/*/bin; do
    if [[ $PYBIN =~ "cp35" ]]; then continue; fi
    "${PYBIN}/pip" wheel /psycopg_binary/ --no-deps -w /tmpwheels/
done

# Bundle external shared libraries into the wheels
for whl in /tmpwheels/*.whl; do
    repair_wheel "$whl"
done

# Create a sdist package with the basic psycopg package in order to install
# psycopg[binary] with packages from a single dir.
# While you are there, build the sdist for psycopg-c too.
"/opt/python/cp38-cp38/bin/python" /psycopg/psycopg/setup.py sdist --dist-dir /psycopg/dist/
"/opt/python/cp38-cp38/bin/python" /psycopg/psycopg_c/setup.py sdist --dist-dir /psycopg/dist/

# Delete the libpq to make sure the package is independent.
rm -v /usr/lib/libpq.*
rm -v /usr/pgsql-13/lib/libpq.*

# Install packages and test
for PYBIN in /opt/python/*/bin/; do
    if [[ $PYBIN =~ "cp35" ]]; then continue; fi
    "${PYBIN}/pip" install psycopg[binary,test]==$version -f /psycopg/dist
    PSYCOPG_IMPL=binary "${PYBIN}/pytest" /psycopg/tests -m "not slow"
done
