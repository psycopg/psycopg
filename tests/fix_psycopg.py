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


@pytest.fixture
@pytest.mark.crdb_skip("2-phase commit")
def tpc(svcconn):
    tpc = Tpc(svcconn)
    tpc.check_tpc()
    tpc.clear_test_xacts()
    tpc.make_test_table()
    yield tpc
    tpc.clear_test_xacts()


class Tpc:
    """Helper object to test two-phase transactions"""

    def __init__(self, conn):
        assert conn.autocommit
        self.conn = conn

    def check_tpc(self):
        from .fix_crdb import is_crdb, crdb_skip_message

        if is_crdb(self.conn):
            pytest.skip(crdb_skip_message("2-phase commit"))

        val = int(self.conn.execute("show max_prepared_transactions").fetchone()[0])
        if not val:
            pytest.skip("prepared transactions disabled in the database")

    def clear_test_xacts(self):
        """Rollback all the prepared transaction in the testing db."""
        from psycopg import sql

        cur = self.conn.execute(
            "select gid from pg_prepared_xacts where database = %s",
            (self.conn.info.dbname,),
        )
        gids = [r[0] for r in cur]
        for gid in gids:
            self.conn.execute(sql.SQL("rollback prepared {}").format(gid))

    def make_test_table(self):
        self.conn.execute("CREATE TABLE IF NOT EXISTS test_tpc (data text)")
        self.conn.execute("TRUNCATE test_tpc")

    def count_xacts(self):
        """Return the number of prepared xacts currently in the test db."""
        cur = self.conn.execute(
            """
            select count(*) from pg_prepared_xacts
            where database = %s""",
            (self.conn.info.dbname,),
        )
        return cur.fetchone()[0]

    def count_test_records(self):
        """Return the number of records in the test table."""
        cur = self.conn.execute("select count(*) from test_tpc")
        return cur.fetchone()[0]


@pytest.fixture(scope="module")
def generators():
    """Return the 'generators' module for selected psycopg implementation."""
    from psycopg import pq

    if pq.__impl__ == "c":
        from psycopg._cmodule import _psycopg

        return _psycopg
    else:
        import psycopg.generators

        return psycopg.generators
