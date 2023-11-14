"""
Support module for test_connection[_async].py
"""

from typing import Any, List
from dataclasses import dataclass

import pytest
import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg._connection_base import BaseConnection

DEFAULT_TIMEOUT = BaseConnection._DEFAULT_CONNECT_TIMEOUT


@pytest.fixture
def testctx(svcconn):
    svcconn.execute("create table if not exists testctx (id int primary key)")
    svcconn.execute("delete from testctx")
    return None


@dataclass
class ParamDef:
    name: str
    guc: str
    values: List[Any]
    non_default: str


param_isolation = ParamDef(
    name="isolation_level",
    guc="isolation",
    values=list(psycopg.IsolationLevel),
    non_default="serializable",
)
param_read_only = ParamDef(
    name="read_only",
    guc="read_only",
    values=[True, False],
    non_default="on",
)
param_deferrable = ParamDef(
    name="deferrable",
    guc="deferrable",
    values=[True, False],
    non_default="on",
)

# Map Python values to Postgres values for the tx_params possible values
tx_values_map = {
    v.name.lower().replace("_", " "): v.value for v in psycopg.IsolationLevel
}
tx_values_map["on"] = True
tx_values_map["off"] = False


tx_params = [
    param_isolation,
    param_read_only,
    pytest.param(param_deferrable, marks=pytest.mark.crdb_skip("deferrable")),
]
tx_params_isolation = [
    pytest.param(
        param_isolation,
        id="isolation_level",
        marks=pytest.mark.crdb("skip", reason="transaction isolation"),
    ),
    pytest.param(
        param_read_only, id="read_only", marks=pytest.mark.crdb_skip("begin_read_only")
    ),
    pytest.param(
        param_deferrable, id="deferrable", marks=pytest.mark.crdb_skip("deferrable")
    ),
]


conninfo_params_timeout = [
    (
        "",
        {"dbname": "mydb", "connect_timeout": None},
        ({"dbname": "mydb"}, DEFAULT_TIMEOUT),
    ),
    (
        "",
        {"dbname": "mydb", "connect_timeout": 1},
        ({"dbname": "mydb", "connect_timeout": "1"}, 1),
    ),
    (
        "dbname=postgres",
        {},
        ({"dbname": "postgres"}, DEFAULT_TIMEOUT),
    ),
    (
        "dbname=postgres connect_timeout=2",
        {},
        ({"dbname": "postgres", "connect_timeout": "2"}, 2),
    ),
    (
        "postgresql:///postgres?connect_timeout=2",
        {"connect_timeout": 10},
        ({"dbname": "postgres", "connect_timeout": "10"}, 10),
    ),
]


def drop_default_args_from_conninfo(conninfo):
    if isinstance(conninfo, str):
        params = conninfo_to_dict(conninfo)
    else:
        params = conninfo.copy()

    def removeif(key, value):
        if params.get(key) == value:
            params.pop(key)

    removeif("host", "")
    removeif("hostaddr", "")
    removeif("port", "5432")
    if "," in params.get("host", ""):
        nhosts = len(params["host"].split(","))
        removeif("port", ",".join(["5432"] * nhosts))
        removeif("hostaddr", "," * (nhosts - 1))
    removeif("connect_timeout", str(DEFAULT_TIMEOUT))

    return params
