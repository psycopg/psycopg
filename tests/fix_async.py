import asyncio

import pytest


@pytest.fixture
def loop():
    """Return the async loop to test coroutines."""
    return asyncio.get_event_loop()
