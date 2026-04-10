#!/bin/bash
set -euo pipefail

cd /workspace

pytest_args=(-q)
if [ -n "${TEST_DSN:-}" ]; then
    pytest_args+=(--test-dsn="$TEST_DSN")
fi

if [ $# -eq 0 ]; then
    python -m pytest "${pytest_args[@]}"
else
    IFS=',' read -ra tests <<< "$1"
    python -m pytest "${pytest_args[@]}" "${tests[@]}"
fi
