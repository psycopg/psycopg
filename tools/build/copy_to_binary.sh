#!/bin/bash

# Create the psycopg-binary package by renaming and patching psycopg-c
# This script is designed to run

set -euo pipefail
set -x

dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pdir="$( cd "${dir}/../.." && pwd )"
target="${pdir}/psycopg_binary"

cp -r "${pdir}/psycopg_c" "${target}"
mv "${target}"/{psycopg_c,psycopg_binary}/
sed -i 's/psycopg-c/psycopg-binary/' "${target}"/setup.cfg
sed -i "s/__impl__[[:space:]]*=.*/__impl__ = 'binary'/" \
    "${target}"/psycopg_binary/pq.pyx
find "${target}" -name \*.pyx -or -name \*.pxd -or -name \*.py \
    | xargs sed -i 's/\bpsycopg_c\b/psycopg_binary/'
