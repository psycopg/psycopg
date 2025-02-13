import socket
import asyncio

import pytest


@pytest.fixture
def fake_resolve(monkeypatch):
    """
    Fixture to return known name from name resolution.
    """
    fake_hosts = {
        "localhost": ["127.0.0.1"],
        "foo.com": ["1.1.1.1"],
        "qux.com": ["2.2.2.2"],
        "dup.com": ["3.3.3.3", "3.3.3.4"],
        "alot.com": [f"4.4.4.{n}" for n in range(10, 30)],
    }

    def family(host):
        return socket.AF_INET6 if ":" in host else socket.AF_INET

    def fake_getaddrinfo(host, port, *args, **kwargs):
        assert isinstance(port, int) or (isinstance(port, str) and port.isdigit())
        try:
            addrs = fake_hosts[host]
        except KeyError:
            raise OSError(f"unknown test host: {host}")
        else:
            return [
                (family(addr), socket.SOCK_STREAM, 6, "", (addr, port))
                for addr in addrs
            ]

    _patch_gai(monkeypatch, fake_getaddrinfo)


@pytest.fixture
def fail_resolve(monkeypatch):
    """
    Fixture to fail any name resolution.
    """

    def fail_getaddrinfo(host, port, **kwargs):
        pytest.fail(f"shouldn't try to resolve {host}")

    _patch_gai(monkeypatch, fail_getaddrinfo)


def _patch_gai(monkeypatch, f):
    monkeypatch.setattr(socket, "getaddrinfo", f)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:

        async def af(*args, **kwargs):
            return f(*args, **kwargs)

        monkeypatch.setattr(loop, "getaddrinfo", af)
