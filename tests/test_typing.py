import os
import sys
import subprocess as sp

import pytest


@pytest.mark.slow
@pytest.mark.skipif(sys.version_info < (3, 7), reason="no future annotations")
def test_typing_example():
    cmdline = f"""
        mypy
        --strict
        --show-error-codes --no-color-output --no-error-summary
        --config-file= --no-incremental --cache-dir={os.devnull}
        tests/typing_example.py
        """.split()
    cp = sp.run(cmdline, stdout=sp.PIPE, stderr=sp.STDOUT)
    errors = cp.stdout.decode("utf8", "replace").splitlines()
    assert not errors
    assert cp.returncode == 0
