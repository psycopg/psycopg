import pytest

from ..conftest import asyncio_options


@pytest.fixture(
    params=[pytest.param(("asyncio", asyncio_options.copy()), id="asyncio")],
    scope="session",
)
def anyio_backend(request):
    backend, options = request.param
    if request.config.option.loop == "uvloop":
        options["use_uvloop"] = True
    return backend, options
