#!/bin/bash

# Convert all the auto-generated sync files from their async counterparts.

set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${dir}/.."

check=

# If --check is used, give an error if files are changed
# (note: it's not a --dry-run)
if [[ ${1:-} == '--check' ]]; then
    check=1
fi

outputs=""

for async in \
    psycopg/psycopg/connection_async.py \
    psycopg/psycopg/cursor_async.py \
    psycopg_pool/psycopg_pool/sched_async.py \
    tests/pool/test_pool_async.py \
    tests/pool/test_sched_async.py \
    tests/test_client_cursor_async.py \
    tests/test_connection_async.py \
    tests/test_copy_async.py \
    tests/test_cursor_common_async.py \
    tests/test_default_cursor_async.py \
    tests/test_pipeline_async.py \
    tests/test_prepared_async.py \
    tests/test_raw_cursor_async.py \
    tests/test_server_cursor_async.py \
    tests/test_tpc_async.py \
    tests/test_transaction_async.py
do
    sync=${async/_async/}
    echo "converting '${async}' -> '${sync}'" >&2
    python "${dir}/async_to_sync.py" ${async} > ${sync}
    black -q ${sync}
    outputs="$outputs ${sync}"
done

if [[ $check ]]; then
    if ! git diff --exit-code $outputs; then
        echo "
ERROR: sync and async files out of sync!" >&2
        exit 1
    fi
fi
