import os
import time
import socket
import logging
import subprocess as sp
from shutil import which

import pytest

import psycopg
from psycopg import conninfo


def pytest_collection_modifyitems(items):
    for item in items:
        # TODO: there is a race condition on macOS and Windows in the CI:
        # listen returns before really listening and tests based on 'deaf_port'
        # fail 50% of the times. Just add the 'proxy' mark on these tests
        # because they are already skipped in the CI.
        if "proxy" in item.fixturenames or "deaf_port" in item.fixturenames:
            item.add_marker(pytest.mark.proxy)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "proxy: the test uses pproxy (the marker is set automatically"
        " on tests using the fixture)",
    )


@pytest.fixture
def proxy(dsn):
    """Return a proxy to the --test-dsn database"""
    p = Proxy(dsn)
    yield p
    p.stop()


@pytest.fixture
def deaf_port(dsn):
    """Return a port number with a socket open but not answering"""
    with socket.socket(socket.AF_INET) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
        s.listen(0)
        yield port


class Proxy:
    """
    Proxy a Postgres service for testing purpose.

    Allow to lose connectivity and restart it using stop/start.
    """

    def __init__(self, server_dsn):
        cdict = conninfo.conninfo_to_dict(server_dsn)

        # Get server params
        host = cdict.get("host") or os.environ.get("PGHOST")
        self.server_host = host if host and not host.startswith("/") else "localhost"
        self.server_port = cdict.get("port", "5432")

        # Get client params
        self.client_host = "localhost"
        self.client_port = self._get_random_port()

        # Make a connection string to the proxy
        cdict["host"] = self.client_host
        cdict["port"] = self.client_port
        cdict["sslmode"] = "disable"  # not supported by the proxy
        self.client_dsn = conninfo.make_conninfo(**cdict)

        # The running proxy process
        self.proc = None

    def start(self):
        if self.proc:
            logging.info("proxy already started")
            return

        logging.info("starting proxy")
        pproxy = which("pproxy")
        if not pproxy:
            raise ValueError("pproxy program not found")
        cmdline = [pproxy, "--reuse"]
        cmdline.extend(["-l", f"tunnel://:{self.client_port}"])
        cmdline.extend(["-r", f"tunnel://{self.server_host}:{self.server_port}"])

        self.proc = sp.Popen(cmdline, stdout=sp.DEVNULL)
        logging.info("proxy started")
        self._wait_listen()

        # verify that the proxy works
        try:
            with psycopg.connect(self.client_dsn):
                pass
        except Exception as e:
            pytest.fail(f"failed to create a working proxy: {e}")

    def stop(self):
        if not self.proc:
            return

        logging.info("stopping proxy")
        self.proc.terminate()
        self.proc.wait()
        logging.info("proxy stopped")
        self.proc = None

    @classmethod
    def _get_random_port(cls):
        with socket.socket() as s:
            s.bind(("", 0))
            return s.getsockname()[1]

    def _wait_listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            for i in range(20):
                if 0 == sock.connect_ex((self.client_host, self.client_port)):
                    break
                time.sleep(0.1)
            else:
                raise ValueError("the proxy didn't start listening in time")

        logging.info("proxy listening")
