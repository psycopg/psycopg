import datetime as dt

import pytest

from psycopg import DataError, pq, sql
from psycopg.adapt import PyFormat

crdb_skip_datestyle = pytest.mark.crdb("skip", reason="set datestyle/intervalstyle")
crdb_skip_negative_interval = pytest.mark.crdb("skip", reason="negative interval")
crdb_skip_invalid_tz = pytest.mark.crdb(
    "skip", reason="crdb doesn't allow invalid timezones"
)

datestyles_in = [
    pytest.param(datestyle, marks=crdb_skip_datestyle)
    for datestyle in ["DMY", "MDY", "YMD"]
]
datestyles_out = [
    pytest.param(datestyle, marks=crdb_skip_datestyle)
    for datestyle in ["ISO", "Postgres", "SQL", "German"]
]

intervalstyles = [
    pytest.param(datestyle, marks=crdb_skip_datestyle)
    for datestyle in ["sql_standard", "postgres", "postgres_verbose", "iso_8601"]
]


class TestDate:
    @pytest.mark.parametrize(
        "val, expr",
        [
            ("min", "0001-01-01"),
            ("1000,1,1", "1000-01-01"),
            ("2000,1,1", "2000-01-01"),
            ("2000,12,31", "2000-12-31"),
            ("3000,1,1", "3000-01-01"),
            ("max", "9999-12-31"),
        ],
    )
    @pytest.mark.parametrize("fmt_in", PyFormat)
    def test_dump_date(self, conn, val, expr, fmt_in):
        val = as_date(val)
        cur = conn.cursor()
        cur.execute(f"select '{expr}'::date = %{fmt_in.value}", (val,))
        assert cur.fetchone()[0] is True

        cur.execute(
            sql.SQL("select {}::date = {}").format(
                sql.Literal(val), sql.Placeholder(format=fmt_in)
            ),
            (val,),
        )
        assert cur.fetchone()[0] is True

    @pytest.mark.parametrize("datestyle_in", datestyles_in)
    def test_dump_date_datestyle(self, conn, datestyle_in):
        cur = conn.cursor(binary=False)
        cur.execute(f"set datestyle = ISO,{datestyle_in}")
        cur.execute("select 'epoch'::date + 1 = %t", (dt.date(1970, 1, 2),))
        assert cur.fetchone()[0] is True

    @pytest.mark.parametrize(
        "val, expr",
        [
            ("min", "0001-01-01"),
            ("1000,1,1", "1000-01-01"),
            ("2000,1,1", "2000-01-01"),
            ("2000,12,31", "2000-12-31"),
            ("3000,1,1", "3000-01-01"),
            ("max", "9999-12-31"),
        ],
    )
    @pytest.mark.parametrize("fmt_out", pq.Format)
    def test_load_date(self, conn, val, expr, fmt_out):
        cur = conn.cursor(binary=fmt_out)
        cur.execute(f"select '{expr}'::date")
        assert cur.fetchone()[0] == as_date(val)

    @pytest.mark.parametrize("datestyle_out", datestyles_out)
    def test_load_date_datestyle(self, conn, datestyle_out):
        cur = conn.cursor(binary=False)
        cur.execute(f"set datestyle = {datestyle_out}, YMD")
        cur.execute("select '2000-01-02'::date")
        assert cur.fetchone()[0] == dt.date(2000, 1, 2)

    @pytest.mark.parametrize("val", ["min", "max"])
    @pytest.mark.parametrize("datestyle_out", datestyles_out)
    def test_load_date_overflow(self, conn, val, datestyle_out):
        cur = conn.cursor(binary=False)
        cur.execute(f"set datestyle = {datestyle_out}, YMD")
        cur.execute("select %t + %s::int", (as_date(val), -1 if val == "min" else 1))
        with pytest.raises(DataError):
            cur.fetchone()[0]

    @pytest.mark.parametrize("val", ["min", "max"])
    def test_load_date_overflow_binary(self, conn, val):
        cur = conn.cursor(binary=True)
        cur.execute("select %s + %s::int", (as_date(val), -1 if val == "min" else 1))
        with pytest.raises(DataError):
            cur.fetchone()[0]

    overflow_samples = [
        ("-infinity", "date too small"),
        ("1000-01-01 BC", "date too small"),
        ("10000-01-01", "date too large"),
        ("infinity", "date too large"),
    ]

    @pytest.mark.parametrize("datestyle_out", datestyles_out)
    @pytest.mark.parametrize("val, msg", overflow_samples)
    def test_load_overflow_message(self, conn, datestyle_out, val, msg):
        cur = conn.cursor()
        cur.execute(f"set datestyle = {datestyle_out}, YMD")
        cur.execute("select %s::date", (val,))
        with pytest.raises(DataError) as excinfo:
            cur.fetchone()[0]
        assert msg in str(excinfo.value)

    @pytest.mark.parametrize("val, msg", overflow_samples)
    def test_load_overflow_message_binary(self, conn, val, msg):
        cur = conn.cursor(binary=True)
        cur.execute("select %s::date", (val,))
        with pytest.raises(DataError) as excinfo:
            cur.fetchone()[0]
        assert msg in str(excinfo.value)

    def test_infinity_date_example(self, conn):
        # NOTE: this is an example in the docs. Make sure it doesn't regress when
        # adding binary datetime adapters
        from datetime import date
        from psycopg.types.datetime import DateLoader, DateDumper

        class InfDateDumper(DateDumper):
            def dump(self, obj):
                if obj == date.max:
                    return b"infinity"
                else:
                    return super().dump(obj)

        class InfDateLoader(DateLoader):
            def load(self, data):
                if data == b"infinity":
                    return date.max
                else:
                    return super().load(data)

        cur = conn.cursor()
        cur.adapters.register_dumper(date, InfDateDumper)
        cur.adapters.register_loader("date", InfDateLoader)

        rec = cur.execute(
            "SELECT %s::text, %s::text", [date(2020, 12, 31), date.max]
        ).fetchone()
        assert rec == ("2020-12-31", "infinity")
        rec = cur.execute("select '2020-12-31'::date, 'infinity'::date").fetchone()
        assert rec == (date(2020, 12, 31), date(9999, 12, 31))


class TestDatetime:
    @pytest.mark.parametrize(
        "val, expr",
        [
            ("min", "0001-01-01 00:00"),
            ("258,1,8,1,12,32,358261", "0258-1-8 1:12:32.358261"),
            ("1000,1,1,0,0", "1000-01-01 00:00"),
            ("2000,1,1,0,0", "2000-01-01 00:00"),
            ("2000,1,2,3,4,5,6", "2000-01-02 03:04:05.000006"),
            ("2000,1,2,3,4,5,678", "2000-01-02 03:04:05.000678"),
            ("2000,1,2,3,0,0,456789", "2000-01-02 03:00:00.456789"),
            ("2000,1,1,0,0,0,1", "2000-01-01 00:00:00.000001"),
            ("2034,02,03,23,34,27,951357", "2034-02-03 23:34:27.951357"),
            ("2200,1,1,0,0,0,1", "2200-01-01 00:00:00.000001"),
            ("2300,1,1,0,0,0,1", "2300-01-01 00:00:00.000001"),
            ("7000,1,1,0,0,0,1", "7000-01-01 00:00:00.000001"),
            ("max", "9999-12-31 23:59:59.999999"),
        ],
    )
    @pytest.mark.parametrize("fmt_in", PyFormat)
    def test_dump_datetime(self, conn, val, expr, fmt_in):
        cur = conn.cursor()
        cur.execute("set timezone to '+02:00'")
        cur.execute(f"select %{fmt_in.value}", (as_dt(val),))
        cur.execute(f"select '{expr}'::timestamp = %{fmt_in.value}", (as_dt(val),))
        cur.execute(
            f"""
            select '{expr}'::timestamp = %(val){fmt_in.value},
            '{expr}', %(val){fmt_in.value}::text
            """,
            {"val": as_dt(val)},
        )
        ok, want, got = cur.fetchone()
        assert ok, (want, got)

    @pytest.mark.parametrize("datestyle_in", datestyles_in)
    def test_dump_datetime_datestyle(self, conn, datestyle_in):
        cur = conn.cursor(binary=False)
        cur.execute(f"set datestyle = ISO, {datestyle_in}")
        cur.execute(
            "select 'epoch'::timestamp + '1d 3h 4m 5s'::interval = %t",
            (dt.datetime(1970, 1, 2, 3, 4, 5),),
        )
        assert cur.fetchone()[0] is True

    load_datetime_samples = [
        ("min", "0001-01-01"),
        ("1000,1,1", "1000-01-01"),
        ("2000,1,1", "2000-01-01"),
        ("2000,1,2,3,4,5,6", "2000-01-02 03:04:05.000006"),
        ("2000,1,2,3,4,5,678", "2000-01-02 03:04:05.000678"),
        ("2000,1,2,3,0,0,456789", "2000-01-02 03:00:00.456789"),
        ("2000,12,31", "2000-12-31"),
        ("3000,1,1", "3000-01-01"),
        ("max", "9999-12-31 23:59:59.999999"),
    ]

    @pytest.mark.parametrize("val, expr", load_datetime_samples)
    @pytest.mark.parametrize("datestyle_out", datestyles_out)
    @pytest.mark.parametrize("datestyle_in", datestyles_in)
    def test_load_datetime(self, conn, val, expr, datestyle_in, datestyle_out):
        cur = conn.cursor(binary=False)
        cur.execute(f"set datestyle = {datestyle_out}, {datestyle_in}")
        cur.execute("set timezone to '+02:00'")
        cur.execute(f"select '{expr}'::timestamp")
        assert cur.fetchone()[0] == as_dt(val)

    @pytest.mark.parametrize("val, expr", load_datetime_samples)
    def test_load_datetime_binary(self, conn, val, expr):
        cur = conn.cursor(binary=True)
        cur.execute("set timezone to '+02:00'")
        cur.execute(f"select '{expr}'::timestamp")
        assert cur.fetchone()[0] == as_dt(val)

    @pytest.mark.parametrize("val", ["min", "max"])
    @pytest.mark.parametrize("datestyle_out", datestyles_out)
    def test_load_datetime_overflow(self, conn, val, datestyle_out):
        cur = conn.cursor(binary=False)
        cur.execute(f"set datestyle = {datestyle_out}, YMD")
        cur.execute(
            "select %t::timestamp + %s * '1s'::interval",
            (as_dt(val), -1 if val == "min" else 1),
        )
        with pytest.raises(DataError):
            cur.fetchone()[0]

    @pytest.mark.parametrize("val", ["min", "max"])
    def test_load_datetime_overflow_binary(self, conn, val):
        cur = conn.cursor(binary=True)
        cur.execute(
            "select %t::timestamp + %s * '1s'::interval",
            (as_dt(val), -1 if val == "min" else 1),
        )
        with pytest.raises(DataError):
            cur.fetchone()[0]

    overflow_samples = [
        ("-infinity", "timestamp too small"),
        ("1000-01-01 12:00 BC", "timestamp too small"),
        ("10000-01-01 12:00", "timestamp too large"),
        ("infinity", "timestamp too large"),
    ]

    @pytest.mark.parametrize("datestyle_out", datestyles_out)
    @pytest.mark.parametrize("val, msg", overflow_samples)
    def test_overflow_message(self, conn, datestyle_out, val, msg):
        cur = conn.cursor()
        cur.execute(f"set datestyle = {datestyle_out}, YMD")
        cur.execute("select %s::timestamp", (val,))
        with pytest.raises(DataError) as excinfo:
            cur.fetchone()[0]
        assert msg in str(excinfo.value)

    @pytest.mark.parametrize("val, msg", overflow_samples)
    def test_overflow_message_binary(self, conn, val, msg):
        cur = conn.cursor(binary=True)
        cur.execute("select %s::timestamp", (val,))
        with pytest.raises(DataError) as excinfo:
            cur.fetchone()[0]
        assert msg in str(excinfo.value)

    @crdb_skip_datestyle
    def test_load_all_month_names(self, conn):
        cur = conn.cursor(binary=False)
        cur.execute("set datestyle = 'Postgres'")
        for i in range(12):
            d = dt.datetime(2000, i + 1, 15)
            cur.execute("select %s", [d])
            assert cur.fetchone()[0] == d


class TestDateTimeTz:
    @pytest.mark.parametrize(
        "val, expr",
        [
            ("min~-2", "0001-01-01 00:00-02:00"),
            ("min~-12", "0001-01-01 00:00-12:00"),
            (
                "258,1,8,1,12,32,358261~1:2:3",
                "0258-1-8 1:12:32.358261+01:02:03",
            ),
            ("1000,1,1,0,0~2", "1000-01-01 00:00+2"),
            ("2000,1,1,0,0~2", "2000-01-01 00:00+2"),
            ("2000,1,1,0,0~12", "2000-01-01 00:00+12"),
            ("2000,1,1,0,0~-12", "2000-01-01 00:00-12"),
            ("2000,1,1,0,0~01:02:03", "2000-01-01 00:00+01:02:03"),
            ("2000,1,1,0,0~-01:02:03", "2000-01-01 00:00-01:02:03"),
            ("2000,12,31,23,59,59,999999~2", "2000-12-31 23:59:59.999999+2"),
            (
                "2034,02,03,23,34,27,951357~-4:27",
                "2034-02-03 23:34:27.951357-04:27",
            ),
            ("2300,1,1,0,0,0,1~1", "2300-01-01 00:00:00.000001+1"),
            ("3000,1,1,0,0~2", "3000-01-01 00:00+2"),
            ("7000,1,1,0,0,0,1~-1:2:3", "7000-01-01 00:00:00.000001-01:02:03"),
            ("max~2", "9999-12-31 23:59:59.999999"),
        ],
    )
    @pytest.mark.parametrize("fmt_in", PyFormat)
    def test_dump_datetimetz(self, conn, val, expr, fmt_in):
        cur = conn.cursor()
        cur.execute("set timezone to '-02:00'")
        cur.execute(
            f"""
            select '{expr}'::timestamptz = %(val){fmt_in.value},
            '{expr}', %(val){fmt_in.value}::text
            """,
            {"val": as_dt(val)},
        )
        ok, want, got = cur.fetchone()
        assert ok, (want, got)

    @pytest.mark.parametrize("datestyle_in", datestyles_in)
    def test_dump_datetimetz_datestyle(self, conn, datestyle_in):
        tzinfo = dt.timezone(dt.timedelta(hours=2))
        cur = conn.cursor(binary=False)
        cur.execute(f"set datestyle = ISO, {datestyle_in}")
        cur.execute("set timezone to '-02:00'")
        cur.execute(
            "select 'epoch'::timestamptz + '1d 3h 4m 5.678s'::interval = %t",
            (dt.datetime(1970, 1, 2, 5, 4, 5, 678000, tzinfo=tzinfo),),
        )
        assert cur.fetchone()[0] is True

    load_datetimetz_samples = [
        ("2000,1,1~2", "2000-01-01", "-02:00"),
        ("2000,1,2,3,4,5,6~2", "2000-01-02 03:04:05.000006", "-02:00"),
        ("2000,1,2,3,4,5,678~1", "2000-01-02 03:04:05.000678", "Europe/Rome"),
        ("2000,7,2,3,4,5,678~2", "2000-07-02 03:04:05.000678", "Europe/Rome"),
        ("2000,1,2,3,0,0,456789~2", "2000-01-02 03:00:00.456789", "-02:00"),
        ("2000,1,2,3,0,0,456789~-2", "2000-01-02 03:00:00.456789", "+02:00"),
        ("2000,12,31~2", "2000-12-31", "-02:00"),
        ("1900,1,1~05:21:10", "1900-01-01", "Asia/Calcutta"),
    ]

    @crdb_skip_datestyle
    @pytest.mark.parametrize("val, expr, timezone", load_datetimetz_samples)
    @pytest.mark.parametrize("datestyle_out", ["ISO"])
    def test_load_datetimetz(self, conn, val, expr, timezone, datestyle_out):
        cur = conn.cursor(binary=False)
        cur.execute(f"set datestyle = {datestyle_out}, DMY")
        cur.execute(f"set timezone to '{timezone}'")
        got = cur.execute(f"select '{expr}'::timestamptz").fetchone()[0]
        assert got == as_dt(val)

    @pytest.mark.parametrize("val, expr, timezone", load_datetimetz_samples)
    def test_load_datetimetz_binary(self, conn, val, expr, timezone):
        cur = conn.cursor(binary=True)
        cur.execute(f"set timezone to '{timezone}'")
        got = cur.execute(f"select '{expr}'::timestamptz").fetchone()[0]
        assert got == as_dt(val)

    @pytest.mark.xfail  # parse timezone names
    @crdb_skip_datestyle
    @pytest.mark.parametrize("val, expr", [("2000,1,1~2", "2000-01-01")])
    @pytest.mark.parametrize("datestyle_out", ["SQL", "Postgres", "German"])
    @pytest.mark.parametrize("datestyle_in", datestyles_in)
    def test_load_datetimetz_tzname(self, conn, val, expr, datestyle_in, datestyle_out):
        cur = conn.cursor(binary=False)
        cur.execute(f"set datestyle = {datestyle_out}, {datestyle_in}")
        cur.execute("set timezone to '-02:00'")
        cur.execute(f"select '{expr}'::timestamptz")
        assert cur.fetchone()[0] == as_dt(val)

    @pytest.mark.parametrize(
        "tzname, expr, tzoff",
        [
            ("UTC", "2000-1-1", 0),
            ("UTC", "2000-7-1", 0),
            ("Europe/Rome", "2000-1-1", 3600),
            ("Europe/Rome", "2000-7-1", 7200),
            ("Europe/Rome", "1000-1-1", 2996),
            pytest.param("NOSUCH0", "2000-1-1", 0, marks=crdb_skip_invalid_tz),
            pytest.param("0", "2000-1-1", 0, marks=crdb_skip_invalid_tz),
        ],
    )
    @pytest.mark.parametrize("fmt_out", pq.Format)
    def test_load_datetimetz_tz(self, conn, fmt_out, tzname, expr, tzoff):
        conn.execute("select set_config('TimeZone', %s, true)", [tzname])
        cur = conn.cursor(binary=fmt_out)
        ts = cur.execute("select %s::timestamptz", [expr]).fetchone()[0]
        assert ts.utcoffset().total_seconds() == tzoff

    @pytest.mark.parametrize(
        "val, type",
        [
            ("2000,1,2,3,4,5,6", "timestamp"),
            ("2000,1,2,3,4,5,6~0", "timestamptz"),
            ("2000,1,2,3,4,5,6~2", "timestamptz"),
        ],
    )
    @pytest.mark.parametrize("fmt_in", PyFormat)
    def test_dump_datetime_tz_or_not_tz(self, conn, val, type, fmt_in):
        val = as_dt(val)
        cur = conn.cursor()
        cur.execute(
            f"""
            select pg_typeof(%{fmt_in.value})::regtype = %s::regtype, %{fmt_in.value}
            """,
            [val, type, val],
        )
        rec = cur.fetchone()
        assert rec[0] is True, type
        assert rec[1] == val

    @pytest.mark.crdb_skip("copy")
    def test_load_copy(self, conn):
        cur = conn.cursor(binary=False)
        with cur.copy(
            """
            copy (
                select
                    '2000-01-01 01:02:03.123456-10:20'::timestamptz,
                    '11111111'::int4
            ) to stdout
            """
        ) as copy:
            copy.set_types(["timestamptz", "int4"])
            rec = copy.read_row()

        tz = dt.timezone(-dt.timedelta(hours=10, minutes=20))
        want = dt.datetime(2000, 1, 1, 1, 2, 3, 123456, tzinfo=tz)
        assert rec[0] == want
        assert rec[1] == 11111111

    overflow_samples = [
        ("-infinity", "timestamp too small"),
        ("1000-01-01 12:00+00 BC", "timestamp too small"),
        ("10000-01-01 12:00+00", "timestamp too large"),
        ("infinity", "timestamp too large"),
    ]

    @pytest.mark.parametrize("datestyle_out", datestyles_out)
    @pytest.mark.parametrize("val, msg", overflow_samples)
    def test_overflow_message(self, conn, datestyle_out, val, msg):
        cur = conn.cursor()
        cur.execute(f"set datestyle = {datestyle_out}, YMD")
        cur.execute("select %s::timestamptz", (val,))
        if datestyle_out == "ISO":
            with pytest.raises(DataError) as excinfo:
                cur.fetchone()[0]
            assert msg in str(excinfo.value)
        else:
            with pytest.raises(NotImplementedError):
                cur.fetchone()[0]

    @pytest.mark.parametrize("val, msg", overflow_samples)
    def test_overflow_message_binary(self, conn, val, msg):
        cur = conn.cursor(binary=True)
        cur.execute("select %s::timestamptz", (val,))
        with pytest.raises(DataError) as excinfo:
            cur.fetchone()[0]
        assert msg in str(excinfo.value)

    @pytest.mark.parametrize(
        "valname, tzval, tzname",
        [
            ("max", "-06", "America/Chicago"),
            ("min", "+09:18:59", "Asia/Tokyo"),
        ],
    )
    @pytest.mark.parametrize("fmt_out", pq.Format)
    def test_max_with_timezone(self, conn, fmt_out, valname, tzval, tzname):
        # This happens e.g. in Django when it caches forever.
        # e.g. see Django test cache.tests.DBCacheTests.test_forever_timeout
        val = getattr(dt.datetime, valname).replace(microsecond=0)
        tz = dt.timezone(as_tzoffset(tzval))
        want = val.replace(tzinfo=tz)

        conn.execute("set timezone to '%s'" % tzname)
        cur = conn.cursor(binary=fmt_out)
        cur.execute("select %s::timestamptz", [str(val) + tzval])
        got = cur.fetchone()[0]

        assert got == want

        extra = "1 day" if valname == "max" else "-1 day"
        with pytest.raises(DataError):
            cur.execute(
                "select %s::timestamptz + %s::interval",
                [str(val) + tzval, extra],
            )
            got = cur.fetchone()[0]


class TestTime:
    @pytest.mark.parametrize(
        "val, expr",
        [
            ("min", "00:00"),
            ("10,20,30,40", "10:20:30.000040"),
            ("max", "23:59:59.999999"),
        ],
    )
    @pytest.mark.parametrize("fmt_in", PyFormat)
    def test_dump_time(self, conn, val, expr, fmt_in):
        cur = conn.cursor()
        cur.execute(
            f"""
            select '{expr}'::time = %(val){fmt_in.value},
                '{expr}'::time::text, %(val){fmt_in.value}::text
            """,
            {"val": as_time(val)},
        )
        ok, want, got = cur.fetchone()
        assert ok, (got, want)

    @pytest.mark.parametrize(
        "val, expr",
        [
            ("min", "00:00"),
            ("1,2", "01:02"),
            ("10,20", "10:20"),
            ("10,20,30", "10:20:30"),
            ("10,20,30,40", "10:20:30.000040"),
            ("max", "23:59:59.999999"),
        ],
    )
    @pytest.mark.parametrize("fmt_out", pq.Format)
    def test_load_time(self, conn, val, expr, fmt_out):
        cur = conn.cursor(binary=fmt_out)
        cur.execute(f"select '{expr}'::time")
        assert cur.fetchone()[0] == as_time(val)

    @pytest.mark.parametrize("fmt_out", pq.Format)
    def test_load_time_24(self, conn, fmt_out):
        cur = conn.cursor(binary=fmt_out)
        cur.execute("select '24:00'::time")
        with pytest.raises(DataError):
            cur.fetchone()[0]


class TestTimeTz:
    @pytest.mark.parametrize(
        "val, expr",
        [
            ("min~-10", "00:00-10:00"),
            ("min~+12", "00:00+12:00"),
            ("10,20,30,40~-2", "10:20:30.000040-02:00"),
            ("10,20,30,40~0", "10:20:30.000040Z"),
            ("10,20,30,40~+2:30", "10:20:30.000040+02:30"),
            ("max~-12", "23:59:59.999999-12:00"),
            ("max~+12", "23:59:59.999999+12:00"),
        ],
    )
    @pytest.mark.parametrize("fmt_in", PyFormat)
    def test_dump_timetz(self, conn, val, expr, fmt_in):
        cur = conn.cursor()
        cur.execute("set timezone to '-02:00'")
        cur.execute(f"select '{expr}'::timetz = %{fmt_in.value}", (as_time(val),))
        assert cur.fetchone()[0] is True

    @pytest.mark.parametrize(
        "val, expr, timezone",
        [
            ("0,0~-12", "00:00", "12:00"),
            ("0,0~12", "00:00", "-12:00"),
            ("3,4,5,6~2", "03:04:05.000006", "-02:00"),
            ("3,4,5,6~7:8", "03:04:05.000006", "-07:08"),
            ("3,0,0,456789~2", "03:00:00.456789", "-02:00"),
            ("3,0,0,456789~-2", "03:00:00.456789", "+02:00"),
        ],
    )
    @pytest.mark.parametrize("fmt_out", pq.Format)
    def test_load_timetz(self, conn, val, timezone, expr, fmt_out):
        cur = conn.cursor(binary=fmt_out)
        cur.execute(f"set timezone to '{timezone}'")
        cur.execute(f"select '{expr}'::timetz")
        assert cur.fetchone()[0] == as_time(val)

    @pytest.mark.parametrize("fmt_out", pq.Format)
    def test_load_timetz_24(self, conn, fmt_out):
        cur = conn.cursor()
        cur.execute("select '24:00'::timetz")
        with pytest.raises(DataError):
            cur.fetchone()[0]

    @pytest.mark.parametrize(
        "val, type",
        [
            ("3,4,5,6", "time"),
            ("3,4,5,6~0", "timetz"),
            ("3,4,5,6~2", "timetz"),
        ],
    )
    @pytest.mark.parametrize("fmt_in", PyFormat)
    def test_dump_time_tz_or_not_tz(self, conn, val, type, fmt_in):
        val = as_time(val)
        cur = conn.cursor()
        cur.execute(
            f"""
            select pg_typeof(%{fmt_in.value})::regtype = %s::regtype, %{fmt_in.value}
            """,
            [val, type, val],
        )
        rec = cur.fetchone()
        assert rec[0] is True, type
        assert rec[1] == val

    @pytest.mark.crdb_skip("copy")
    def test_load_copy(self, conn):
        cur = conn.cursor(binary=False)
        with cur.copy(
            """
            copy (
                select
                    '01:02:03.123456-10:20'::timetz,
                    '11111111'::int4
            ) to stdout
            """
        ) as copy:
            copy.set_types(["timetz", "int4"])
            rec = copy.read_row()

        tz = dt.timezone(-dt.timedelta(hours=10, minutes=20))
        want = dt.time(1, 2, 3, 123456, tzinfo=tz)
        assert rec[0] == want
        assert rec[1] == 11111111


class TestInterval:
    dump_timedelta_samples = [
        ("min", "-999999999 days"),
        ("1d", "1 day"),
        pytest.param("-1d", "-1 day", marks=crdb_skip_negative_interval),
        ("1s", "1 s"),
        pytest.param("-1s", "-1 s", marks=crdb_skip_negative_interval),
        pytest.param("-1m", "-0.000001 s", marks=crdb_skip_negative_interval),
        ("1m", "0.000001 s"),
        ("max", "999999999 days 23:59:59.999999"),
    ]

    @pytest.mark.parametrize("val, expr", dump_timedelta_samples)
    @pytest.mark.parametrize("intervalstyle", intervalstyles)
    def test_dump_interval(self, conn, val, expr, intervalstyle):
        cur = conn.cursor()
        cur.execute(f"set IntervalStyle to '{intervalstyle}'")
        cur.execute(f"select '{expr}'::interval = %t", (as_td(val),))
        assert cur.fetchone()[0] is True

    @pytest.mark.parametrize("val, expr", dump_timedelta_samples)
    def test_dump_interval_binary(self, conn, val, expr):
        cur = conn.cursor()
        cur.execute(f"select '{expr}'::interval = %b", (as_td(val),))
        assert cur.fetchone()[0] is True

    @pytest.mark.parametrize(
        "val, expr",
        [
            ("1s", "1 sec"),
            ("-1s", "-1 sec"),
            ("60s", "1 min"),
            ("3600s", "1 hour"),
            ("1s,1000m", "1.001 sec"),
            ("1s,1m", "1.000001 sec"),
            ("1d", "1 day"),
            ("-10d", "-10 day"),
            ("1d,1s,1m", "1 day 1.000001 sec"),
            ("-86399s,-999999m", "-23:59:59.999999"),
            ("-3723s,-400000m", "-1:2:3.4"),
            ("3723s,400000m", "1:2:3.4"),
            ("86399s,999999m", "23:59:59.999999"),
            ("30d", "30 day"),
            ("365d", "1 year"),
            ("-365d", "-1 year"),
            ("-730d", "-2 years"),
            ("1460d", "4 year"),
            ("30d", "1 month"),
            ("-30d", "-1 month"),
            ("60d", "2 month"),
            ("-90d", "-3 month"),
        ],
    )
    @pytest.mark.parametrize("fmt_out", pq.Format)
    def test_load_interval(self, conn, val, expr, fmt_out):
        cur = conn.cursor(binary=fmt_out)
        cur.execute(f"select '{expr}'::interval")
        assert cur.fetchone()[0] == as_td(val)

    @crdb_skip_datestyle
    @pytest.mark.xfail  # weird interval outputs
    @pytest.mark.parametrize("val, expr", [("1d,1s", "1 day 1 sec")])
    @pytest.mark.parametrize(
        "intervalstyle",
        ["sql_standard", "postgres_verbose", "iso_8601"],
    )
    def test_load_interval_intervalstyle(self, conn, val, expr, intervalstyle):
        cur = conn.cursor(binary=False)
        cur.execute(f"set IntervalStyle to '{intervalstyle}'")
        cur.execute(f"select '{expr}'::interval")
        assert cur.fetchone()[0] == as_td(val)

    @pytest.mark.parametrize("fmt_out", pq.Format)
    @pytest.mark.parametrize("val", ["min", "max"])
    def test_load_interval_overflow(self, conn, val, fmt_out):
        cur = conn.cursor(binary=fmt_out)
        cur.execute(
            "select %s + %s * '1s'::interval",
            (as_td(val), -1 if val == "min" else 1),
        )
        with pytest.raises(DataError):
            cur.fetchone()[0]

    @pytest.mark.crdb_skip("copy")
    def test_load_copy(self, conn):
        cur = conn.cursor(binary=False)
        with cur.copy(
            """
            copy (
                select
                    '1 days +00:00:01.000001'::interval,
                    'foo bar'::text
            ) to stdout
            """
        ) as copy:
            copy.set_types(["interval", "text"])
            rec = copy.read_row()

        want = dt.timedelta(days=1, seconds=1, microseconds=1)
        assert rec[0] == want
        assert rec[1] == "foo bar"


#
# Support
#


def as_date(s):
    return dt.date(*map(int, s.split(","))) if "," in s else getattr(dt.date, s)


def as_time(s):
    if "~" in s:
        s, off = s.split("~")
    else:
        off = None

    if "," in s:
        rv = dt.time(*map(int, s.split(",")))  # type: ignore[arg-type]
    else:
        rv = getattr(dt.time, s)
    if off:
        rv = rv.replace(tzinfo=as_tzinfo(off))

    return rv


def as_dt(s):
    if "~" not in s:
        return as_naive_dt(s)

    s, off = s.split("~")
    rv = as_naive_dt(s)
    off = as_tzoffset(off)
    rv = (rv - off).replace(tzinfo=dt.timezone.utc)
    return rv


def as_naive_dt(s):
    if "," in s:
        rv = dt.datetime(*map(int, s.split(",")))  # type: ignore[arg-type]
    else:
        rv = getattr(dt.datetime, s)

    return rv


def as_tzoffset(s):
    if s.startswith("-"):
        mul = -1
        s = s[1:]
    else:
        mul = 1

    fields = ("hours", "minutes", "seconds")
    return mul * dt.timedelta(**dict(zip(fields, map(int, s.split(":")))))


def as_tzinfo(s):
    off = as_tzoffset(s)
    return dt.timezone(off)


def as_td(s):
    if s in ("min", "max"):
        return getattr(dt.timedelta, s)

    suffixes = {"d": "days", "s": "seconds", "m": "microseconds"}
    kwargs = {}
    for part in s.split(","):
        kwargs[suffixes[part[-1]]] = int(part[:-1])

    return dt.timedelta(**kwargs)
