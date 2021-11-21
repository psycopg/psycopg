import pytest


@pytest.mark.slow
def test_copy_to_large(conn):
    cur = conn.cursor()

    cur.execute(
        """
CREATE TEMP TABLE dump_test_table AS
SELECT 
    md5('00' || x::text)::text AS col_00,
    md5('01' || x::text)::text AS col_01,
    md5('02' || x::text)::text AS col_02,
    md5('03' || x::text)::text AS col_03,
    md5('04' || x::text)::text AS col_04,
    md5('05' || x::text)::text AS col_05,
    md5('06' || x::text)::text AS col_06,
    md5('07' || x::text)::text AS col_07,
    md5('08' || x::text)::text AS col_08,
    md5('09' || x::text)::text AS col_09,
    md5('10' || x::text)::text AS col_10,
    md5('11' || x::text)::text AS col_11,
    md5('12' || x::text)::text AS col_12,
    md5('13' || x::text)::text AS col_13,
    md5('14' || x::text)::text AS col_14,
    md5('15' || x::text)::text AS col_15
FROM generate_series(1, 3000000) AS x;"""
    )

    with cur.copy("""COPY pg_temp.dump_test_table TO STDOUT;""") as copy:
        while copy.read():
            pass

    cur.execute("""DISCARD TEMP;""")

    assert conn.pgconn.transaction_status == conn.TransactionStatus.INTRANS
