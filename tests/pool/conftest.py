import pytest

from ..conftest import asyncio_options


@pytest.fixture(scope="session")
def anyio_backend(request):
    options = asyncio_options.copy()
    if request.config.option.loop == "uvloop":
        options["use_uvloop"] = True
    return "asyncio", options
