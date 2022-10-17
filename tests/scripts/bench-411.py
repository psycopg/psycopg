import os
import sys
import time
import random
import asyncio
import logging
from enum import Enum
from typing import Any, Dict, List, Generator
from argparse import ArgumentParser, Namespace
from contextlib import contextmanager

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


class Driver(str, Enum):
    psycopg2 = "psycopg2"
    psycopg = "psycopg"
    psycopg_async = "psycopg_async"
    asyncpg = "asyncpg"


ids: List[int] = []
data: List[Dict[str, Any]] = []


def main() -> None:

    args = parse_cmdline()

    ids[:] = range(args.ntests)
    data[:] = [
        dict(
            id=i,
            name="c%d" % i,
            description="c%d" % i,
            q=i * 10,
            p=i * 20,
            x=i * 30,
            y=i * 40,
        )
        for i in ids
    ]

    # Must be done just on end
    drop_at_the_end = args.drop
    args.drop = False

    for i, name in enumerate(args.drivers):
        if i == len(args.drivers) - 1:
            args.drop = drop_at_the_end

        if name == Driver.psycopg2:
            import psycopg2  # type: ignore

            run_psycopg2(psycopg2, args)

        elif name == Driver.psycopg:
            import psycopg

            run_psycopg(psycopg, args)

        elif name == Driver.psycopg_async:
            import psycopg

            if sys.platform == "win32":
                if hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
                    asyncio.set_event_loop_policy(
                        asyncio.WindowsSelectorEventLoopPolicy()
                    )

            asyncio.run(run_psycopg_async(psycopg, args))

        elif name == Driver.asyncpg:
            import asyncpg  # type: ignore

            asyncio.run(run_asyncpg(asyncpg, args))

        else:
            raise AssertionError(f"unknown driver: {name!r}")

        # Must be done just on start
        args.create = False


table = """
CREATE TABLE customer (
        id SERIAL NOT NULL,
        name VARCHAR(255),
        description VARCHAR(255),
        q INTEGER,
        p INTEGER,
        x INTEGER,
        y INTEGER,
        z INTEGER,
        PRIMARY KEY (id)
)
"""
drop = "DROP TABLE IF EXISTS customer"

insert = """
INSERT INTO customer (id, name, description, q, p, x, y) VALUES
(%(id)s, %(name)s, %(description)s, %(q)s, %(p)s, %(x)s, %(y)s)
"""

select = """
SELECT customer.id, customer.name, customer.description, customer.q,
    customer.p, customer.x, customer.y, customer.z
FROM customer
WHERE customer.id = %(id)s
"""


@contextmanager
def time_log(message: str) -> Generator[None, None, None]:
    start = time.monotonic()
    yield
    end = time.monotonic()
    logger.info(f"Run {message} in {end-start} s")


def run_psycopg2(psycopg2: Any, args: Namespace) -> None:
    logger.info("Running psycopg2")

    if args.create:
        logger.info(f"inserting {args.ntests} test records")
        with psycopg2.connect(args.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(drop)
                cursor.execute(table)
                cursor.executemany(insert, data)
            conn.commit()

    logger.info(f"running {args.ntests} queries")
    to_query = random.choices(ids, k=args.ntests)
    with psycopg2.connect(args.dsn) as conn:
        with time_log("psycopg2"):
            for id_ in to_query:
                with conn.cursor() as cursor:
                    cursor.execute(select, {"id": id_})
                    cursor.fetchall()
                # conn.rollback()

    if args.drop:
        logger.info("dropping test records")
        with psycopg2.connect(args.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(drop)
            conn.commit()


def run_psycopg(psycopg: Any, args: Namespace) -> None:
    logger.info("Running psycopg sync")

    if args.create:
        logger.info(f"inserting {args.ntests} test records")
        with psycopg.connect(args.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(drop)
                cursor.execute(table)
                cursor.executemany(insert, data)
            conn.commit()

    logger.info(f"running {args.ntests} queries")
    to_query = random.choices(ids, k=args.ntests)
    with psycopg.connect(args.dsn) as conn:
        with time_log("psycopg"):
            for id_ in to_query:
                with conn.cursor() as cursor:
                    cursor.execute(select, {"id": id_})
                    cursor.fetchall()
                # conn.rollback()

    if args.drop:
        logger.info("dropping test records")
        with psycopg.connect(args.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(drop)
            conn.commit()


async def run_psycopg_async(psycopg: Any, args: Namespace) -> None:
    logger.info("Running psycopg async")

    conn: Any

    if args.create:
        logger.info(f"inserting {args.ntests} test records")
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(drop)
                await cursor.execute(table)
                await cursor.executemany(insert, data)
            await conn.commit()

    logger.info(f"running {args.ntests} queries")
    to_query = random.choices(ids, k=args.ntests)
    async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
        with time_log("psycopg_async"):
            for id_ in to_query:
                cursor = await conn.execute(select, {"id": id_})
                await cursor.fetchall()
                await cursor.close()
                # await conn.rollback()

    if args.drop:
        logger.info("dropping test records")
        async with await psycopg.AsyncConnection.connect(args.dsn) as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(drop)
            await conn.commit()


async def run_asyncpg(asyncpg: Any, args: Namespace) -> None:
    logger.info("Running asyncpg")

    places = dict(id="$1", name="$2", description="$3", q="$4", p="$5", x="$6", y="$7")
    a_insert = insert % places
    a_select = select % {"id": "$1"}

    conn: Any

    if args.create:
        logger.info(f"inserting {args.ntests} test records")
        conn = await asyncpg.connect(args.dsn)
        async with conn.transaction():
            await conn.execute(drop)
            await conn.execute(table)
            await conn.executemany(a_insert, [tuple(d.values()) for d in data])
        await conn.close()

    logger.info(f"running {args.ntests} queries")
    to_query = random.choices(ids, k=args.ntests)
    conn = await asyncpg.connect(args.dsn)
    with time_log("asyncpg"):
        for id_ in to_query:
            tr = conn.transaction()
            await tr.start()
            await conn.fetch(a_select, id_)
            # await tr.rollback()
    await conn.close()

    if args.drop:
        logger.info("dropping test records")
        conn = await asyncpg.connect(args.dsn)
        async with conn.transaction():
            await conn.execute(drop)
        await conn.close()


def parse_cmdline() -> Namespace:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "drivers",
        nargs="+",
        metavar="DRIVER",
        type=Driver,
        help=f"the drivers to test [choices: {', '.join(d.value for d in Driver)}]",
    )

    parser.add_argument(
        "--ntests",
        type=int,
        default=10_000,
        help="number of tests to perform [default: %(default)s]",
    )

    parser.add_argument(
        "--dsn",
        default=os.environ.get("PSYCOPG_TEST_DSN", ""),
        help="database connection string"
        " [default: %(default)r (from PSYCOPG_TEST_DSN env var)]",
    )

    parser.add_argument(
        "--no-create",
        dest="create",
        action="store_false",
        default="True",
        help="skip data creation before tests (it must exist already)",
    )

    parser.add_argument(
        "--no-drop",
        dest="drop",
        action="store_false",
        default="True",
        help="skip data drop after tests",
    )

    opt = parser.parse_args()

    return opt


if __name__ == "__main__":
    main()
