from __future__ import annotations

import os
import shutil
import socket
import logging
import tempfile
import subprocess as sp
from pathlib import Path
from functools import cache
from contextlib import contextmanager

import pytest

logger = logging.getLogger(__name__)


@cache
def _get_pg_bindir() -> str | None:
    """Get the PostgreSQL binary directory via pg_config --bindir."""
    pg_config = os.environ.get("PG_CONFIG", "pg_config")
    try:
        out = sp.run(
            [pg_config, "--bindir"], capture_output=True, text=True, check=True
        )
        return out.stdout.strip()
    except (OSError, sp.CalledProcessError):
        return None


def find_pg_binary(name: str) -> str | None:
    """Find a PostgreSQL binary (pg_ctl, initdb) via pg_config or PATH."""
    bindir = _get_pg_bindir()
    if bindir:
        candidate = Path(bindir) / name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)

    return shutil.which(name)


class PGInstance:
    """Manages a temporary PostgreSQL instance for testing."""

    def __init__(
        self,
        pg_config: dict[str, str] | None = None,
        initdb_args: list[str] | None = None,
        pg_hba_entries: list[str] | None = None,
        post_start_sql: list[str] | None = None,
    ):
        self.pg_config = pg_config or {}
        self.initdb_args = initdb_args or []
        self.pg_hba_entries = pg_hba_entries
        self.post_start_sql = post_start_sql
        self.dsn: str | None = None
        self._tmpdir: str | None = None
        self._datadir: str | None = None
        self._pg_ctl = find_pg_binary("pg_ctl")
        self._initdb = find_pg_binary("initdb")
        self._psql = find_pg_binary("psql")

    @property
    def available(self) -> bool:
        """Return True if pg_ctl and initdb are found."""
        return self._pg_ctl is not None and self._initdb is not None

    def start(self) -> str:
        """Initialize a temporary PostgreSQL cluster, start it, and return a DSN."""
        if not self.available:
            raise RuntimeError("pg_ctl or initdb not found")

        assert self._pg_ctl is not None
        assert self._initdb is not None

        self._tmpdir = tempfile.mkdtemp(prefix="psycopg_test_")
        self._datadir = os.path.join(self._tmpdir, "data")

        # Run initdb
        initdb_cmd = [
            self._initdb,
            "--nosync",
            "-D",
            self._datadir,
            *self.initdb_args,
        ]
        logger.info("running initdb: %s", initdb_cmd)
        result = sp.run(initdb_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"initdb failed:\n{result.stderr}")

        # Build postgresql.conf settings
        config: dict[str, str] = {
            "max_prepared_transactions": "10",
            "fsync": "off",
        }

        port = self._get_random_port()
        config["listen_addresses"] = "'127.0.0.1'"
        config["port"] = str(port)
        config["unix_socket_directories"] = "''"
        self.dsn = f"host=127.0.0.1 port={port} dbname=postgres"

        config.update(
            {k: self._quote_config_value(v) for k, v in self.pg_config.items()}
        )

        # Append settings to postgresql.conf
        conf_path = os.path.join(self._datadir, "postgresql.conf")
        with open(conf_path, "a") as f:
            f.write("\n# psycopg test settings\n")
            for key, value in config.items():
                f.write(f"{key} = {value}\n")

        # Prepend custom pg_hba entries if provided
        if self.pg_hba_entries:
            hba_path = os.path.join(self._datadir, "pg_hba.conf")
            with open(hba_path) as f:
                existing_hba = f.read()
            with open(hba_path, "w") as f:
                for entry in self.pg_hba_entries:
                    f.write(entry + "\n")
                f.write(existing_hba)

        # Start PostgreSQL
        # Use -l to redirect server output to a log file. Without this,
        # pg_ctl start forks postgres in the background and the child inherits
        # captured pipes, causing sp.run to block forever.
        logfile = os.path.join(self._tmpdir, "pg.log")
        start_cmd = [
            self._pg_ctl,
            "start",
            "-D",
            self._datadir,
            "-w",
            "-l",
            logfile,
            "-o",
            "-F",
        ]
        logger.info("starting PostgreSQL: %s", start_cmd)
        result = sp.run(start_cmd, capture_output=True, text=True)
        if result.returncode != 0:
            # Include the server log for diagnosis
            try:
                with open(logfile) as f:
                    log_contents = f.read()
            except OSError:
                log_contents = "(no log file)"
            raise RuntimeError(
                f"pg_ctl start failed:\n{result.stderr}\nServer log:\n{log_contents}"
            )

        # Run post-start SQL commands if provided
        if self.post_start_sql and self._psql:
            for sql in self.post_start_sql:
                logger.info("running post-start SQL: %s", sql)
                result = sp.run(
                    [self._psql, "-d", self.dsn, "-c", sql],
                    capture_output=True,
                    text=True,
                )
                if result.returncode != 0:
                    raise RuntimeError(f"post-start SQL failed: {sql}\n{result.stderr}")

        logger.info("managed PostgreSQL started, dsn: %s", self.dsn)
        return self.dsn

    def stop(self) -> None:
        """Stop the managed PostgreSQL instance and clean up."""
        if self._pg_ctl and self._datadir and os.path.isdir(self._datadir):
            logger.info("stopping managed PostgreSQL")
            sp.run(
                [self._pg_ctl, "stop", "-D", self._datadir, "-m", "immediate", "-w"],
                capture_output=True,
                text=True,
            )

        if self._tmpdir and os.path.isdir(self._tmpdir):
            shutil.rmtree(self._tmpdir, ignore_errors=True)
            self._tmpdir = None

    @staticmethod
    def _get_random_port() -> int:
        with socket.socket() as s:
            s.bind(("", 0))
            port: int = s.getsockname()[1]
            return port

    @staticmethod
    def _quote_config_value(value: str) -> str:
        """Quote a postgresql.conf value if it isn't already quoted."""
        if value.startswith("'") and value.endswith("'"):
            return value
        # Numbers and booleans don't need quoting
        if value.lower() in ("on", "off", "true", "false"):
            return value
        try:
            float(value)
            return value
        except ValueError:
            return f"'{value}'"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "managed_pg: the test requires starting a managed PostgreSQL instance"
        " (skipped if pg_ctl/initdb not available)",
    )


def pytest_runtest_setup(item):
    if list(item.iter_markers(name="managed_pg")):
        if not find_pg_binary("pg_ctl") or not find_pg_binary("initdb"):
            pytest.skip("pg_ctl/initdb not available for managed_pg test")


@pytest.fixture(scope="session")
def managed_pg_instance(request):
    """Start a managed PostgreSQL instance if no --test-dsn is provided.

    Yields the PGInstance if started, or None if an explicit DSN is set
    or if pg_ctl/initdb are not available.
    """
    dsn = request.config.getoption("--test-dsn")
    if dsn is not None:
        yield None
        return

    instance = PGInstance()
    if not instance.available:
        yield None
        return

    try:
        instance.start()
    except Exception as exc:
        logger.warning("failed to start managed PostgreSQL: %s", exc)
        yield None
        return

    yield instance
    instance.stop()


@pytest.fixture(scope="session")
def pg_factory():
    """Factory to create managed PostgreSQL instances with custom config.

    Usage in tests::

        @pytest.mark.managed_pg
        def test_something(pg_factory):
            with pg_factory(pg_config={"wal_level": "logical"}) as instance:
                with psycopg.connect(instance.dsn) as conn:
                    ...
    """
    instances: list[PGInstance] = []

    @contextmanager
    def create(**kwargs):
        instance = PGInstance(**kwargs)
        if not instance.available:
            pytest.skip("pg_ctl/initdb not available")
        instance.start()
        instances.append(instance)
        try:
            yield instance
        finally:
            instance.stop()

    yield create
