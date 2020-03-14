import os
import pytest


class TempEnv:
    def __init__(self):
        self._prev = {}

    def get(self, item, default):
        return os.environ.get(self, item, default)

    def __getitem__(self, item):
        return os.environ[item]

    def __setitem__(self, item, value):
        self._prev.setdefault(item, os.environ.get(item))
        os.environ[item] = value

    def __delitem__(self, item):
        self._prev.setdefault(item, os.environ.get(item))
        del os.environ[item]

    def restore(self):
        for k, v in self._prev.items():
            if v is not None:
                os.environ[k] = v
            else:
                if k in os.environ:
                    del os.environ[k]


@pytest.fixture
def tempenv():
    """Allow to change the env vars temporarily."""
    env = TempEnv()
    yield env
    env.restore()
