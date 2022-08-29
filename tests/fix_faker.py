import datetime as dt
import importlib
import ipaddress
from math import isnan
from uuid import UUID
from random import choice, random, randrange
from typing import Any, List, Set, Tuple, Union
from decimal import Decimal
from contextlib import contextmanager, asynccontextmanager

import pytest

import psycopg
from psycopg import sql
from psycopg.adapt import PyFormat
from psycopg._compat import Deque
from psycopg.types.range import Range
from psycopg.types.json import Json, Jsonb
from psycopg.types.numeric import Int4, Int8
from psycopg.types.multirange import Multirange


@pytest.fixture
def faker(conn):
    return Faker(conn)


class Faker:
    """
    An object to generate random records.
    """

    json_max_level = 3
    json_max_length = 10
    str_max_length = 100
    list_max_length = 20
    tuple_max_length = 15

    def __init__(self, connection):
        self.conn = connection
        self.format = PyFormat.BINARY
        self.records = []

        self._schema = None
        self._types = None
        self._types_names = None
        self._makers = {}
        self.table_name = sql.Identifier("fake_table")

    @property
    def schema(self):
        if not self._schema:
            self.schema = self.choose_schema()
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema
        self._types_names = None

    @property
    def fields_names(self):
        return [sql.Identifier(f"fld_{i}") for i in range(len(self.schema))]

    @property
    def types(self):
        if not self._types:

            def key(cls: type) -> str:
                return cls.__name__

            self._types = sorted(self.get_supported_types(), key=key)
        return self._types

    @property
    def types_names_sql(self):
        if self._types_names:
            return self._types_names

        record = self.make_record(nulls=0)
        tx = psycopg.adapt.Transformer(self.conn)
        types = [
            self._get_type_name(tx, schema, value)
            for schema, value in zip(self.schema, record)
        ]
        self._types_names = types
        return types

    @property
    def types_names(self):
        types = [t.as_string(self.conn).replace('"', "") for t in self.types_names_sql]
        return types

    def _get_type_name(self, tx, schema, value):
        # Special case it as it is passed as unknown so is returned as text
        if schema == (list, str):
            return sql.SQL("text[]")

        registry = self.conn.adapters.types
        dumper = tx.get_dumper(value, self.format)
        dumper.dump(value)  # load the oid if it's dynamic (e.g. array)
        info = registry.get(dumper.oid) or registry.get("text")
        if dumper.oid == info.array_oid:
            return sql.SQL("{}[]").format(sql.Identifier(info.name))
        else:
            return sql.Identifier(info.name)

    @property
    def drop_stmt(self):
        return sql.SQL("drop table if exists {}").format(self.table_name)

    @property
    def create_stmt(self):
        field_values = []
        for name, type in zip(self.fields_names, self.types_names_sql):
            field_values.append(sql.SQL("{} {}").format(name, type))

        fields = sql.SQL(", ").join(field_values)
        return sql.SQL("create table {table} (id serial primary key, {fields})").format(
            table=self.table_name, fields=fields
        )

    @property
    def insert_stmt(self):
        phs = [sql.Placeholder(format=self.format) for i in range(len(self.schema))]
        return sql.SQL("insert into {} ({}) values ({})").format(
            self.table_name,
            sql.SQL(", ").join(self.fields_names),
            sql.SQL(", ").join(phs),
        )

    @property
    def select_stmt(self):
        fields = sql.SQL(", ").join(self.fields_names)
        return sql.SQL("select {} from {} order by id").format(fields, self.table_name)

    @contextmanager
    def find_insert_problem(self, conn):
        """Context manager to help finding a problematic value."""
        try:
            with conn.transaction():
                yield
        except psycopg.DatabaseError:
            cur = conn.cursor()
            # Repeat insert one field at time, until finding the wrong one
            cur.execute(self.drop_stmt)
            cur.execute(self.create_stmt)
            for i, rec in enumerate(self.records):
                for j, val in enumerate(rec):
                    try:
                        cur.execute(self._insert_field_stmt(j), (val,))
                    except psycopg.DatabaseError as e:
                        r = repr(val)
                        if len(r) > 200:
                            r = f"{r[:200]}... ({len(r)} chars)"
                        raise Exception(
                            f"value {r!r} at record {i} column0 {j} failed insert: {e}"
                        ) from None

            # just in case, but hopefully we should have triggered the problem
            raise

    @asynccontextmanager
    async def find_insert_problem_async(self, aconn):
        try:
            async with aconn.transaction():
                yield
        except psycopg.DatabaseError:
            acur = aconn.cursor()
            # Repeat insert one field at time, until finding the wrong one
            await acur.execute(self.drop_stmt)
            await acur.execute(self.create_stmt)
            for i, rec in enumerate(self.records):
                for j, val in enumerate(rec):
                    try:
                        await acur.execute(self._insert_field_stmt(j), (val,))
                    except psycopg.DatabaseError as e:
                        r = repr(val)
                        if len(r) > 200:
                            r = f"{r[:200]}... ({len(r)} chars)"
                        raise Exception(
                            f"value {r!r} at record {i} column0 {j} failed insert: {e}"
                        ) from None

            # just in case, but hopefully we should have triggered the problem
            raise

    def _insert_field_stmt(self, i):
        ph = sql.Placeholder(format=self.format)
        return sql.SQL("insert into {} ({}) values ({})").format(
            self.table_name, self.fields_names[i], ph
        )

    def choose_schema(self, ncols=20):
        schema: List[Union[Tuple[type, ...], type]] = []
        while len(schema) < ncols:
            s = self.make_schema(choice(self.types))
            if s is not None:
                schema.append(s)
        self.schema = schema
        return schema

    def make_records(self, nrecords):
        self.records = [self.make_record(nulls=0.05) for i in range(nrecords)]

    def make_record(self, nulls=0):
        if not nulls:
            return tuple(self.example(spec) for spec in self.schema)
        else:
            return tuple(
                self.make(spec) if random() > nulls else None for spec in self.schema
            )

    def assert_record(self, got, want):
        for spec, g, w in zip(self.schema, got, want):
            if g is None and w is None:
                continue
            m = self.get_matcher(spec)
            m(spec, g, w)

    def get_supported_types(self) -> Set[type]:
        dumpers = self.conn.adapters._dumpers[self.format]
        rv = set()
        for cls in dumpers.keys():
            if isinstance(cls, str):
                cls = deep_import(cls)
            if issubclass(cls, Multirange) and self.conn.info.server_version < 140000:
                continue

            rv.add(cls)

        # check all the types are handled
        for cls in rv:
            self.get_maker(cls)

        return rv

    def make_schema(self, cls: type) -> Union[Tuple[type, ...], type, None]:
        """Create a schema spec from a Python type.

        A schema specifies what Postgres type to generate when a Python type
        maps to more than one (e.g. tuple -> composite, list -> array[],
        datetime -> timestamp[tz]).

        A schema for a type is represented by a tuple (type, ...) which the
        matching make_*() method can interpret, or just type if the type
        doesn't require further specification.

        A `None` means that the type is not supported.
        """
        meth = self._get_method("schema", cls)
        return meth(cls) if meth else cls

    def get_maker(self, spec):
        cls = spec if isinstance(spec, type) else spec[0]

        try:
            return self._makers[cls]
        except KeyError:
            pass

        meth = self._get_method("make", cls)
        if meth:
            self._makers[cls] = meth
            return meth
        else:
            raise NotImplementedError(f"cannot make fake objects of class {cls}")

    def get_matcher(self, spec):
        cls = spec if isinstance(spec, type) else spec[0]
        meth = self._get_method("match", cls)
        return meth if meth else self.match_any

    def _get_method(self, prefix, cls):
        name = cls.__name__
        if cls.__module__ != "builtins":
            name = f"{cls.__module__}.{name}"

        parts = name.split(".")
        for i in range(len(parts)):
            mname = f"{prefix}_{'_'.join(parts[-(i + 1) :])}"
            meth = getattr(self, mname, None)
            if meth:
                return meth

        return None

    def make(self, spec):
        # spec can be a type or a tuple (type, options)
        return self.get_maker(spec)(spec)

    def example(self, spec):
        # A good representative of the object - no degenerate case
        cls = spec if isinstance(spec, type) else spec[0]
        meth = self._get_method("example", cls)
        if meth:
            return meth(spec)
        else:
            return self.make(spec)

    def match_any(self, spec, got, want):
        assert got == want

    # methods to generate samples of specific types

    def make_Binary(self, spec):
        return self.make_bytes(spec)

    def match_Binary(self, spec, got, want):
        return want.obj == got

    def make_bool(self, spec):
        return choice((True, False))

    def make_bytearray(self, spec):
        return self.make_bytes(spec)

    def make_bytes(self, spec):
        length = randrange(self.str_max_length)
        return spec(bytes([randrange(256) for i in range(length)]))

    def make_date(self, spec):
        day = randrange(dt.date.max.toordinal())
        return dt.date.fromordinal(day + 1)

    def schema_datetime(self, cls):
        return self.schema_time(cls)

    def make_datetime(self, spec):
        # Add a day because with timezone we might go BC
        dtmin = dt.datetime.min + dt.timedelta(days=1)
        delta = dt.datetime.max - dtmin
        micros = randrange((delta.days + 1) * 24 * 60 * 60 * 1_000_000)
        rv = dtmin + dt.timedelta(microseconds=micros)
        if spec[1]:
            rv = rv.replace(tzinfo=self._make_tz(spec))
        return rv

    def match_datetime(self, spec, got, want):
        # Comparisons with different timezones is unreliable: certain pairs
        # are reported different but their delta is 0
        # https://bugs.python.org/issue45347
        assert not (got - want)

    def make_Decimal(self, spec):
        if random() >= 0.99:
            return Decimal(choice(self._decimal_special_values()))

        sign = choice("+-")
        num = choice(["0.zd", "d", "d.d"])
        while "z" in num:
            ndigits = randrange(1, 20)
            num = num.replace("z", "0" * ndigits, 1)
        while "d" in num:
            ndigits = randrange(1, 20)
            num = num.replace(
                "d", "".join([str(randrange(10)) for i in range(ndigits)]), 1
            )
        expsign = choice(["e+", "e-", ""])
        exp = randrange(20) if expsign else ""
        rv = Decimal(f"{sign}{num}{expsign}{exp}")
        return rv

    def match_Decimal(self, spec, got, want):
        if got is not None and got.is_nan():
            assert want.is_nan()
        else:
            assert got == want

    def _decimal_special_values(self):
        values = ["NaN", "sNaN"]

        if self.conn.info.vendor == "PostgreSQL":
            if self.conn.info.server_version >= 140000:
                values.extend(["Inf", "-Inf"])
        elif self.conn.info.vendor == "CockroachDB":
            if self.conn.info.server_version >= 220100:
                values.extend(["Inf", "-Inf"])
        else:
            pytest.fail(f"unexpected vendor: {self.conn.info.vendor}")

        return values

    def schema_Enum(self, cls):
        # TODO: can't fake those as we would need to create temporary types
        return None

    def make_Enum(self, spec):
        return None

    def make_float(self, spec, double=True):
        if random() <= 0.99:
            # These exponents should generate no inf
            return float(
                f"{choice('-+')}0.{randrange(1 << 53)}e{randrange(-310,309)}"
                if double
                else f"{choice('-+')}0.{randrange(1 << 22)}e{randrange(-37,38)}"
            )
        else:
            return choice((0.0, -0.0, float("-inf"), float("inf"), float("nan")))

    def match_float(self, spec, got, want, approx=False, rel=None):
        if got is not None and isnan(got):
            assert isnan(want)
        else:
            if approx or self._server_rounds():
                assert got == pytest.approx(want, rel=rel)
            else:
                assert got == want

    def _server_rounds(self):
        """Return True if the connected server perform float rounding"""
        if self.conn.info.vendor == "CockroachDB":
            return True
        else:
            # Versions older than 12 make some rounding. e.g. in Postgres 10.4
            # select '-1.409006204063909e+112'::float8
            #      -> -1.40900620406391e+112
            return self.conn.info.server_version < 120000

    def make_Float4(self, spec):
        return spec(self.make_float(spec, double=False))

    def match_Float4(self, spec, got, want):
        self.match_float(spec, got, want, approx=True, rel=1e-5)

    def make_Float8(self, spec):
        return spec(self.make_float(spec))

    match_Float8 = match_float

    def make_int(self, spec):
        return randrange(-(1 << 90), 1 << 90)

    def make_Int2(self, spec):
        return spec(randrange(-(1 << 15), 1 << 15))

    def make_Int4(self, spec):
        return spec(randrange(-(1 << 31), 1 << 31))

    def make_Int8(self, spec):
        return spec(randrange(-(1 << 63), 1 << 63))

    def make_IntNumeric(self, spec):
        return spec(randrange(-(1 << 100), 1 << 100))

    def make_IPv4Address(self, spec):
        return ipaddress.IPv4Address(bytes(randrange(256) for _ in range(4)))

    def make_IPv4Interface(self, spec):
        prefix = randrange(32)
        return ipaddress.IPv4Interface(
            (bytes(randrange(256) for _ in range(4)), prefix)
        )

    def make_IPv4Network(self, spec):
        return self.make_IPv4Interface(spec).network

    def make_IPv6Address(self, spec):
        return ipaddress.IPv6Address(bytes(randrange(256) for _ in range(16)))

    def make_IPv6Interface(self, spec):
        prefix = randrange(128)
        return ipaddress.IPv6Interface(
            (bytes(randrange(256) for _ in range(16)), prefix)
        )

    def make_IPv6Network(self, spec):
        return self.make_IPv6Interface(spec).network

    def make_Json(self, spec):
        return spec(self._make_json())

    def match_Json(self, spec, got, want):
        if want is not None:
            want = want.obj
        assert got == want

    def make_Jsonb(self, spec):
        return spec(self._make_json())

    def match_Jsonb(self, spec, got, want):
        self.match_Json(spec, got, want)

    def make_JsonFloat(self, spec):
        # A float limited to what json accepts
        # this exponent should generate no inf
        return float(f"{choice('-+')}0.{randrange(1 << 20)}e{randrange(-15,15)}")

    def schema_list(self, cls):
        while True:
            scls = choice(self.types)
            if scls is cls:
                continue
            if scls is float:
                # TODO: float lists are currently adapted as decimal.
                # There may be rounding errors or problems with inf.
                continue

            # CRDB doesn't support arrays of json
            # https://github.com/cockroachdb/cockroach/issues/23468
            if self.conn.info.vendor == "CockroachDB" and scls in (Json, Jsonb):
                continue

            schema = self.make_schema(scls)
            if schema is not None:
                break

        return (cls, schema)

    def make_list(self, spec):
        # don't make empty lists because they regularly fail cast
        length = randrange(1, self.list_max_length)
        spec = spec[1]
        while True:
            rv = [self.make(spec) for i in range(length)]

            # TODO multirange lists fail binary dump if the last element is
            # empty and there is no type annotation. See xfail in
            # test_multirange::test_dump_builtin_array
            if rv and isinstance(rv[-1], Multirange) and not rv[-1]:
                continue

            return rv

    def example_list(self, spec):
        return [self.example(spec[1])]

    def match_list(self, spec, got, want):
        assert len(got) == len(want)
        m = self.get_matcher(spec[1])
        for g, w in zip(got, want):
            m(spec[1], g, w)

    def make_memoryview(self, spec):
        return self.make_bytes(spec)

    def schema_Multirange(self, cls):
        return self.schema_Range(cls)

    def make_Multirange(self, spec, length=None, **kwargs):
        if length is None:
            length = randrange(0, self.list_max_length)

        def overlap(r1, r2):
            l1, u1 = r1.lower, r1.upper
            l2, u2 = r2.lower, r2.upper
            if l1 is None and l2 is None:
                return True
            elif l1 is None:
                l1 = l2
            elif l2 is None:
                l2 = l1

            if u1 is None and u2 is None:
                return True
            elif u1 is None:
                u1 = u2
            elif u2 is None:
                u2 = u1

            return l1 <= u2 and l2 <= u1

        out: List[Range[Any]] = []
        for i in range(length):
            r = self.make_Range((Range, spec[1]), **kwargs)
            if r.isempty:
                continue
            for r2 in out:
                if overlap(r, r2):
                    insert = False
                    break
            else:
                insert = True
            if insert:
                out.append(r)  # alternatively, we could merge

        return spec[0](sorted(out))

    def example_Multirange(self, spec):
        return self.make_Multirange(spec, length=1, empty_chance=0, no_bound_chance=0)

    def make_Int4Multirange(self, spec):
        return self.make_Multirange((spec, Int4))

    def make_Int8Multirange(self, spec):
        return self.make_Multirange((spec, Int8))

    def make_NumericMultirange(self, spec):
        return self.make_Multirange((spec, Decimal))

    def make_DateMultirange(self, spec):
        return self.make_Multirange((spec, dt.date))

    def make_TimestampMultirange(self, spec):
        return self.make_Multirange((spec, (dt.datetime, False)))

    def make_TimestamptzMultirange(self, spec):
        return self.make_Multirange((spec, (dt.datetime, True)))

    def match_Multirange(self, spec, got, want):
        assert len(got) == len(want)
        for ig, iw in zip(got, want):
            self.match_Range(spec, ig, iw)

    def match_Int4Multirange(self, spec, got, want):
        return self.match_Multirange((spec, Int4), got, want)

    def match_Int8Multirange(self, spec, got, want):
        return self.match_Multirange((spec, Int8), got, want)

    def match_NumericMultirange(self, spec, got, want):
        return self.match_Multirange((spec, Decimal), got, want)

    def match_DateMultirange(self, spec, got, want):
        return self.match_Multirange((spec, dt.date), got, want)

    def match_TimestampMultirange(self, spec, got, want):
        return self.match_Multirange((spec, (dt.datetime, False)), got, want)

    def match_TimestamptzMultirange(self, spec, got, want):
        return self.match_Multirange((spec, (dt.datetime, True)), got, want)

    def schema_NoneType(self, cls):
        return None

    def make_NoneType(self, spec):
        return None

    def make_Oid(self, spec):
        return spec(randrange(1 << 32))

    def schema_Range(self, cls):
        subtypes = [
            Decimal,
            Int4,
            Int8,
            dt.date,
            (dt.datetime, True),
            (dt.datetime, False),
        ]

        return (cls, choice(subtypes))

    def make_Range(self, spec, empty_chance=0.02, no_bound_chance=0.05):
        # TODO: drop format check after fixing binary dumping of empty ranges
        # (an array starting with an empty range will get the wrong type currently)
        if (
            random() < empty_chance
            and spec[0] is Range
            and self.format == PyFormat.TEXT
        ):
            return spec[0](empty=True)

        while True:
            bounds: List[Union[Any, None]] = []
            while len(bounds) < 2:
                if random() < no_bound_chance:
                    bounds.append(None)
                    continue

                val = self.make(spec[1])
                # NaN are allowed in a range, but comparison in Python get tricky.
                if spec[1] is Decimal and val.is_nan():
                    continue

                bounds.append(val)

            if bounds[0] is not None and bounds[1] is not None:
                if bounds[0] == bounds[1]:
                    # It would come out empty
                    continue

                if bounds[0] > bounds[1]:
                    bounds.reverse()

            # avoid generating ranges with no type info if dumping in binary
            # TODO: lift this limitation after test_copy_in_empty xfail is fixed
            if spec[0] is Range and self.format == PyFormat.BINARY:
                if bounds[0] is bounds[1] is None:
                    continue

            break

        r = spec[0](bounds[0], bounds[1], choice("[(") + choice("])"))
        return r

    def example_Range(self, spec):
        return self.make_Range(spec, empty_chance=0, no_bound_chance=0)

    def make_Int4Range(self, spec):
        return self.make_Range((spec, Int4))

    def make_Int8Range(self, spec):
        return self.make_Range((spec, Int8))

    def make_NumericRange(self, spec):
        return self.make_Range((spec, Decimal))

    def make_DateRange(self, spec):
        return self.make_Range((spec, dt.date))

    def make_TimestampRange(self, spec):
        return self.make_Range((spec, (dt.datetime, False)))

    def make_TimestamptzRange(self, spec):
        return self.make_Range((spec, (dt.datetime, True)))

    def match_Range(self, spec, got, want):
        # normalise the bounds of unbounded ranges
        if want.lower is None and want.lower_inc:
            want = type(want)(want.lower, want.upper, "(" + want.bounds[1])
        if want.upper is None and want.upper_inc:
            want = type(want)(want.lower, want.upper, want.bounds[0] + ")")

        # Normalise discrete ranges
        unit: Union[dt.timedelta, int, None]
        if spec[1] is dt.date:
            unit = dt.timedelta(days=1)
        elif type(spec[1]) is type and issubclass(spec[1], int):
            unit = 1
        else:
            unit = None

        if unit is not None:
            if want.lower is not None and not want.lower_inc:
                want = type(want)(want.lower + unit, want.upper, "[" + want.bounds[1])
            if want.upper_inc:
                want = type(want)(want.lower, want.upper + unit, want.bounds[0] + ")")

        if spec[1] == (dt.datetime, True) and not want.isempty:
            # work around https://bugs.python.org/issue45347
            def fix_dt(x):
                return x.astimezone(dt.timezone.utc) if x is not None else None

            def fix_range(r):
                return type(r)(fix_dt(r.lower), fix_dt(r.upper), r.bounds)

            want = fix_range(want)
            got = fix_range(got)

        assert got == want

    def match_Int4Range(self, spec, got, want):
        return self.match_Range((spec, Int4), got, want)

    def match_Int8Range(self, spec, got, want):
        return self.match_Range((spec, Int8), got, want)

    def match_NumericRange(self, spec, got, want):
        return self.match_Range((spec, Decimal), got, want)

    def match_DateRange(self, spec, got, want):
        return self.match_Range((spec, dt.date), got, want)

    def match_TimestampRange(self, spec, got, want):
        return self.match_Range((spec, (dt.datetime, False)), got, want)

    def match_TimestamptzRange(self, spec, got, want):
        return self.match_Range((spec, (dt.datetime, True)), got, want)

    def make_str(self, spec, length=0):
        if not length:
            length = randrange(self.str_max_length)

        rv: List[int] = []
        while len(rv) < length:
            c = randrange(1, 128) if random() < 0.5 else randrange(1, 0x110000)
            if not (0xD800 <= c <= 0xDBFF or 0xDC00 <= c <= 0xDFFF):
                rv.append(c)

        return "".join(map(chr, rv))

    def schema_time(self, cls):
        # Choose timezone yes/no
        return (cls, choice([True, False]))

    def make_time(self, spec):
        val = randrange(24 * 60 * 60 * 1_000_000)
        val, ms = divmod(val, 1_000_000)
        val, s = divmod(val, 60)
        h, m = divmod(val, 60)
        tz = self._make_tz(spec) if spec[1] else None
        return dt.time(h, m, s, ms, tz)

    CRDB_TIMEDELTA_MAX = dt.timedelta(days=1281239)

    def make_timedelta(self, spec):
        if self.conn.info.vendor == "CockroachDB":
            rng = [-self.CRDB_TIMEDELTA_MAX, self.CRDB_TIMEDELTA_MAX]
        else:
            rng = [dt.timedelta.min, dt.timedelta.max]

        return choice(rng) * random()

    def schema_tuple(self, cls):
        # TODO: this is a complicated matter as it would involve creating
        # temporary composite types.
        # length = randrange(1, self.tuple_max_length)
        # return (cls, self.make_random_schema(ncols=length))
        return None

    def make_tuple(self, spec):
        return tuple(self.make(s) for s in spec[1])

    def match_tuple(self, spec, got, want):
        assert len(got) == len(want) == len(spec[1])
        for g, w, s in zip(got, want, spec):
            if g is None or w is None:
                assert g is w
            else:
                m = self.get_matcher(s)
                m(s, g, w)

    def make_UUID(self, spec):
        return UUID(bytes=bytes([randrange(256) for i in range(16)]))

    def _make_json(self, container_chance=0.66):
        rec_types = [list, dict]
        scal_types = [type(None), int, JsonFloat, bool, str]
        if random() < container_chance:
            cls = choice(rec_types)
            if cls is list:
                return [
                    self._make_json(container_chance=container_chance / 2.0)
                    for i in range(randrange(self.json_max_length))
                ]
            elif cls is dict:
                return {
                    self.make_str(str, 15): self._make_json(
                        container_chance=container_chance / 2.0
                    )
                    for i in range(randrange(self.json_max_length))
                }
            else:
                assert False, f"unknown rec type: {cls}"

        else:
            cls = choice(scal_types)  # type: ignore[assignment]
            return self.make(cls)

    def _make_tz(self, spec):
        minutes = randrange(-12 * 60, 12 * 60 + 1)
        return dt.timezone(dt.timedelta(minutes=minutes))


class JsonFloat:
    pass


def deep_import(name):
    parts = Deque(name.split("."))
    seen = []
    if not parts:
        raise ValueError("name must be a dot-separated name")

    seen.append(parts.popleft())
    thing = importlib.import_module(seen[-1])
    while parts:
        attr = parts.popleft()
        seen.append(attr)

        if hasattr(thing, attr):
            thing = getattr(thing, attr)
        else:
            thing = importlib.import_module(".".join(seen))

    return thing
