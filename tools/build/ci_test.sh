#!/bin/bash

# Run the tests in Github Action
#
# Failed tests run up to three times, to take into account flakey tests.
# Of course the random generator is not re-seeded between runs, in order to
# repeat the same result.

set -euo pipefail
set -x

# Assemble a markers expression from the MARKERS and NOT_MARKERS env vars
markers=""
for m in ${MARKERS:-}; do
    [[ "$markers" != "" ]] && markers="$markers and"
    markers="$markers $m"
done
for m in ${NOT_MARKERS:-}; do
    [[ "$markers" != "" ]] && markers="$markers and"
    markers="$markers not $m"
done

pytest="python -bb -m pytest --color=yes"

$pytest -m "$markers" "$@" && exit 0

$pytest -m "$markers" --lf --randomly-seed=last "$@" && exit 0

$pytest -m "$markers" --lf --randomly-seed=last "$@"
