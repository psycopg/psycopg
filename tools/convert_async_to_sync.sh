#!/bin/bash

# Convert all the auto-generated sync files from their async counterparts.

set -euo pipefail

dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${dir}/.."

python "${dir}/async_to_sync.py" tests/test_connection_async.py > tests/test_connection.py
black -q tests/test_connection.py
python "${dir}/async_to_sync.py" tests/test_cursor_async.py > tests/test_cursor.py
black -q tests/test_cursor.py
