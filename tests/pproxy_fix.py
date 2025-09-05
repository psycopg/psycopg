#!/usr/bin/env python
"""Wrapper for pproxy to fix Python 3.14 compatibility

Work around https://github.com/qwj/python-proxy/issues/200
"""

import sys
import asyncio
from typing import Any

from pproxy.server import main as main_  # type: ignore[import-untyped]


def main() -> Any:
    # Before Python 3.14 `get_event_loop()` used to create a new loop.
    # From Python 3.14 it raises a `RuntimeError`.
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())

    return main_()


if __name__ == "__main__":
    sys.exit(main())
