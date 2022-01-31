"""
A quick and rough performance comparison of text vs. binary Decimal adaptation
"""
from random import randrange
from decimal import Decimal
import psycopg
from psycopg import sql

ncols = 10
nrows = 500000
format = psycopg.pq.Format.BINARY
test = "copy"


def main() -> None:
    cnn = psycopg.connect()

    cnn.execute(
        sql.SQL("create table testdec ({})").format(
            sql.SQL(", ").join(
                [
                    sql.SQL("{} numeric(10,2)").format(sql.Identifier(f"t{i}"))
                    for i in range(ncols)
                ]
            )
        )
    )
    cur = cnn.cursor()

    if test == "copy":
        with cur.copy(f"copy testdec from stdin (format {format.name})") as copy:
            for j in range(nrows):
                copy.write_row(
                    [Decimal(randrange(10000000000)) / 100 for i in range(ncols)]
                )

    elif test == "insert":
        ph = ["%t", "%b"][format]
        cur.executemany(
            "insert into testdec values (%s)" % ", ".join([ph] * ncols),
            (
                [Decimal(randrange(10000000000)) / 100 for i in range(ncols)]
                for j in range(nrows)
            ),
        )
    else:
        raise Exception(f"bad test: {test}")


if __name__ == "__main__":
    main()
