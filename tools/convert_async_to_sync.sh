#!/bin/bash

# Convert all the auto-generated sync files from their async counterparts.

set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${dir}/.."

function log {
    echo "$@" >&2
}
function error {
    # Print an error message and exit.
    log "
ERROR: $@"
    exit 1
}

check=

# If --check is used, give an error if files are changed
# (note: it's not a --dry-run)
if [[ ${1:-} == '--check' ]]; then
    check=1
    shift
fi

all_inputs="
    psycopg/psycopg/connection_async.py
    psycopg/psycopg/cursor_async.py
    psycopg_pool/psycopg_pool/null_pool_async.py
    psycopg_pool/psycopg_pool/pool_async.py
    psycopg_pool/psycopg_pool/sched_async.py
    tests/pool/test_pool_async.py
    tests/pool/test_pool_common_async.py
    tests/pool/test_pool_null_async.py
    tests/pool/test_sched_async.py
    tests/test_connection_async.py
    tests/test_copy_async.py
    tests/test_cursor_async.py
    tests/test_cursor_client_async.py
    tests/test_cursor_common_async.py
    tests/test_cursor_raw_async.py
    tests/test_cursor_server_async.py
    tests/test_pipeline_async.py
    tests/test_prepared_async.py
    tests/test_tpc_async.py
    tests/test_transaction_async.py
"

# Take other arguments as file names if specified
if [[ ${1:-} ]]; then
    inputs="$@"
else
    inputs="$all_inputs"
fi


outputs=""

for async in $inputs; do
    test -f "${async}" || error "file not found: '${async}'"
    sync=${async/_async/}
    log "converting '${async}' -> '${sync}'"
    python "${dir}/async_to_sync.py" ${async} > ${sync}
    black -q ${sync}
    outputs="$outputs ${sync}"
done

if [[ $check ]]; then
    if ! git diff --exit-code $outputs; then
        error "sync and async files... out of sync!"
    fi
fi
