#!/bin/bash

# Convert all the auto-generated sync files from their async counterparts.

set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${dir}/.."

for async in \
    tests/test_client_cursor_async.py \
    tests/test_connection_async.py \
    tests/test_copy_async.py \
    tests/test_cursor_async.py \
    tests/test_default_cursor_async.py \
    tests/test_pipeline_async.py \
    tests/test_raw_cursor_async.py \
    tests/test_server_cursor_async.py
do
    sync=${async/_async/}
    echo "converting '${async}' -> '${sync}'" >&2
    python "${dir}/async_to_sync.py" ${async} > ${sync}
    black -q ${sync}
done
