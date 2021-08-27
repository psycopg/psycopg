from copy import deepcopy

import pytest


@pytest.fixture
def global_adapters():
    """Restore the global adapters after a test has changed them."""
    from psycopg import adapters

    dumpers = deepcopy(adapters._dumpers)
    dumpers_by_oid = deepcopy(adapters._dumpers_by_oid)
    loaders = deepcopy(adapters._loaders)
    types = list(adapters.types)

    yield None

    adapters._dumpers = dumpers
    adapters._dumpers_by_oid = dumpers_by_oid
    adapters._loaders = loaders
    adapters.types.clear()
    for t in types:
        adapters.types.add(t)
