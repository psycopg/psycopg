"""
Cython adapters for date/time types.
"""

# Copyright (C) 2021 The Psycopg Team

from libc.string cimport memset, strchr
from cpython cimport datetime as cdt
from cpython.dict cimport PyDict_GetItem
from cpython.object cimport PyObject, PyObject_CallFunctionObjArgs

cdef extern from "Python.h":
    const char *PyUnicode_AsUTF8AndSize(unicode obj, Py_ssize_t *size) except NULL
    object PyTimeZone_FromOffset(object offset)

cdef extern from *:
    """
/* Multipliers from fraction of seconds to microseconds */
static int _uspad[] = {0, 100000, 10000, 1000, 100, 10, 1};
    """
    cdef int *_uspad

from datetime import date, time, timedelta, datetime, timezone

from psycopg_c._psycopg cimport endian

from psycopg import errors as e
from psycopg._compat import ZoneInfo


# Initialise the datetime C API
cdt.import_datetime()

cdef enum:
    ORDER_YMD = 0
    ORDER_DMY = 1
    ORDER_MDY = 2
    ORDER_PGDM = 3
    ORDER_PGMD = 4

cdef enum:
    INTERVALSTYLE_OTHERS = 0
    INTERVALSTYLE_SQL_STANDARD = 1
    INTERVALSTYLE_POSTGRES = 2

cdef enum:
    PG_DATE_EPOCH_DAYS = 730120  # date(2000, 1, 1).toordinal()
    PY_DATE_MIN_DAYS = 1  # date.min.toordinal()

cdef object date_toordinal = date.toordinal
cdef object date_fromordinal = date.fromordinal
cdef object datetime_astimezone = datetime.astimezone
cdef object time_utcoffset = time.utcoffset
cdef object timedelta_total_seconds = timedelta.total_seconds
cdef object timezone_utc = timezone.utc
cdef object pg_datetime_epoch = datetime(2000, 1, 1)
cdef object pg_datetimetz_epoch = datetime(2000, 1, 1, tzinfo=timezone.utc)

cdef object _month_abbr = {
    n: i
    for i, n in enumerate(
        b"Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1
    )
}


@cython.final
cdef class DateDumper(CDumper):

    format = PQ_TEXT
    oid = oids.DATE_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef Py_ssize_t size;
        cdef const char *src

        # NOTE: whatever the PostgreSQL DateStyle input format (DMY, MDY, YMD)
        # the YYYY-MM-DD is always understood correctly.
        cdef str s = str(obj)
        src = PyUnicode_AsUTF8AndSize(s, &size)

        cdef char *buf = CDumper.ensure_size(rv, offset, size)
        memcpy(buf, src, size)
        return size


@cython.final
cdef class DateBinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.DATE_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef int32_t days = PyObject_CallFunctionObjArgs(
            date_toordinal, <PyObject *>obj, NULL)
        days -= PG_DATE_EPOCH_DAYS
        cdef int32_t *buf = <int32_t *>CDumper.ensure_size(
            rv, offset, sizeof(int32_t))
        buf[0] = endian.htobe32(days)
        return sizeof(int32_t)


cdef class _BaseTimeDumper(CDumper):

    cpdef get_key(self, obj, format):
        # Use (cls,) to report the need to upgrade to a dumper for timetz (the
        # Frankenstein of the data types).
        if not obj.tzinfo:
            return self.cls
        else:
            return (self.cls,)

    cpdef upgrade(self, obj: time, format):
        raise NotImplementedError


cdef class _BaseTimeTextDumper(_BaseTimeDumper):

    format = PQ_TEXT

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef Py_ssize_t size;
        cdef const char *src

        cdef str s = str(obj)
        src = PyUnicode_AsUTF8AndSize(s, &size)

        cdef char *buf = CDumper.ensure_size(rv, offset, size)
        memcpy(buf, src, size)
        return size


@cython.final
cdef class TimeDumper(_BaseTimeTextDumper):

    oid = oids.TIME_OID

    cpdef upgrade(self, obj, format):
        if not obj.tzinfo:
            return self
        else:
            return TimeTzDumper(self.cls)


@cython.final
cdef class TimeTzDumper(_BaseTimeTextDumper):

    oid = oids.TIMETZ_OID


@cython.final
cdef class TimeBinaryDumper(_BaseTimeDumper):

    format = PQ_BINARY
    oid = oids.TIME_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef int64_t micros = cdt.time_microsecond(obj) + 1000000 * (
            cdt.time_second(obj)
            + 60 * (cdt.time_minute(obj) + 60 * <int64_t>cdt.time_hour(obj))
        )

        cdef int64_t *buf = <int64_t *>CDumper.ensure_size(
            rv, offset, sizeof(int64_t))
        buf[0] = endian.htobe64(micros)
        return sizeof(int64_t)

    cpdef upgrade(self, obj, format):
        if not obj.tzinfo:
            return self
        else:
            return TimeTzBinaryDumper(self.cls)


@cython.final
cdef class TimeTzBinaryDumper(_BaseTimeDumper):

    format = PQ_BINARY
    oid = oids.TIMETZ_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef int64_t micros = cdt.time_microsecond(obj) + 1_000_000 * (
            cdt.time_second(obj)
            + 60 * (cdt.time_minute(obj) + 60 * <int64_t>cdt.time_hour(obj))
        )

        off = PyObject_CallFunctionObjArgs(time_utcoffset, <PyObject *>obj, NULL)
        cdef int32_t offsec = int(PyObject_CallFunctionObjArgs(
            timedelta_total_seconds, <PyObject *>off, NULL))

        cdef char *buf = CDumper.ensure_size(
            rv, offset, sizeof(int64_t) + sizeof(int32_t))
        (<int64_t *>buf)[0] = endian.htobe64(micros)
        (<int32_t *>(buf + sizeof(int64_t)))[0] = endian.htobe32(-offsec)

        return sizeof(int64_t) + sizeof(int32_t)


cdef class _BaseDatetimeDumper(CDumper):

    cpdef get_key(self, obj, format):
        # Use (cls,) to report the need to upgrade (downgrade, actually) to a
        # dumper for naive timestamp.
        if obj.tzinfo:
            return self.cls
        else:
            return (self.cls,)

    cpdef upgrade(self, obj: time, format):
        raise NotImplementedError


cdef class _BaseDatetimeTextDumper(_BaseDatetimeDumper):

    format = PQ_TEXT

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef Py_ssize_t size;
        cdef const char *src

        # NOTE: whatever the PostgreSQL DateStyle input format (DMY, MDY, YMD)
        # the YYYY-MM-DD is always understood correctly.
        cdef str s = str(obj)
        src = PyUnicode_AsUTF8AndSize(s, &size)

        cdef char *buf = CDumper.ensure_size(rv, offset, size)
        memcpy(buf, src, size)
        return size


@cython.final
cdef class DatetimeDumper(_BaseDatetimeTextDumper):

    oid = oids.TIMESTAMPTZ_OID

    cpdef upgrade(self, obj, format):
        if obj.tzinfo:
            return self
        else:
            return DatetimeNoTzDumper(self.cls)


@cython.final
cdef class DatetimeNoTzDumper(_BaseDatetimeTextDumper):

    oid = oids.TIMESTAMP_OID


@cython.final
cdef class DatetimeBinaryDumper(_BaseDatetimeDumper):

    format = PQ_BINARY
    oid = oids.TIMESTAMPTZ_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        delta = obj - pg_datetimetz_epoch

        cdef int64_t micros = cdt.timedelta_microseconds(delta) + 1_000_000 * (
            86_400 * <int64_t>cdt.timedelta_days(delta)
                + <int64_t>cdt.timedelta_seconds(delta))

        cdef char *buf = CDumper.ensure_size(rv, offset, sizeof(int64_t))
        (<int64_t *>buf)[0] = endian.htobe64(micros)
        return sizeof(int64_t)

    cpdef upgrade(self, obj, format):
        if obj.tzinfo:
            return self
        else:
            return DatetimeNoTzBinaryDumper(self.cls)


@cython.final
cdef class DatetimeNoTzBinaryDumper(_BaseDatetimeDumper):

    format = PQ_BINARY
    oid = oids.TIMESTAMP_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        delta = obj - pg_datetime_epoch

        cdef int64_t micros = cdt.timedelta_microseconds(delta) + 1_000_000 * (
            86_400 * <int64_t>cdt.timedelta_days(delta)
                + <int64_t>cdt.timedelta_seconds(delta))

        cdef char *buf = CDumper.ensure_size(rv, offset, sizeof(int64_t))
        (<int64_t *>buf)[0] = endian.htobe64(micros)
        return sizeof(int64_t)


@cython.final
cdef class TimedeltaDumper(CDumper):

    format = PQ_TEXT
    oid = oids.INTERVAL_OID
    cdef int _style

    def __cinit__(self, cls, context: Optional[AdaptContext] = None):

        cdef const char *ds = _get_intervalstyle(self._pgconn)
        if ds[0] == b's':  # sql_standard
            self._style = INTERVALSTYLE_SQL_STANDARD
        else:  # iso_8601, postgres, postgres_verbose
            self._style = INTERVALSTYLE_OTHERS

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef Py_ssize_t size;
        cdef const char *src

        cdef str s
        if self._style == INTERVALSTYLE_OTHERS:
            # The comma is parsed ok by PostgreSQL but it's not documented
            # and it seems brittle to rely on it. CRDB doesn't consume it well.
            s = str(obj).replace(",", "")
        else:
            # sql_standard format needs explicit signs
            # otherwise -1 day 1 sec will mean -1 sec
            s = "%+d day %+d second %+d microsecond" % (
                obj.days, obj.seconds, obj.microseconds)

        src = PyUnicode_AsUTF8AndSize(s, &size)

        cdef char *buf = CDumper.ensure_size(rv, offset, size)
        memcpy(buf, src, size)
        return size


@cython.final
cdef class TimedeltaBinaryDumper(CDumper):

    format = PQ_BINARY
    oid = oids.INTERVAL_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        cdef int64_t micros = (
            1_000_000 * <int64_t>cdt.timedelta_seconds(obj)
            + cdt.timedelta_microseconds(obj))
        cdef int32_t days = cdt.timedelta_days(obj)

        cdef char *buf = CDumper.ensure_size(
            rv, offset, sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t))
        (<int64_t *>buf)[0] = endian.htobe64(micros)
        (<int32_t *>(buf + sizeof(int64_t)))[0] = endian.htobe32(days)
        (<int32_t *>(buf + sizeof(int64_t) + sizeof(int32_t)))[0] = 0

        return sizeof(int64_t) + sizeof(int32_t) + sizeof(int32_t)


@cython.final
cdef class DateLoader(CLoader):

    format = PQ_TEXT
    cdef int _order

    def __cinit__(self, oid: int, context: Optional[AdaptContext] = None):

        cdef const char *ds = _get_datestyle(self._pgconn)
        if ds[0] == b'I':  # ISO
            self._order = ORDER_YMD
        elif ds[0] == b'G':  # German
            self._order = ORDER_DMY
        elif ds[0] == b'S':  # SQL, DMY / MDY
            self._order = ORDER_DMY if ds[5] == b'D' else ORDER_MDY
        elif ds[0] == b'P':  # Postgres, DMY / MDY
            self._order = ORDER_DMY if ds[10] == b'D' else ORDER_MDY
        else:
            raise e.InterfaceError(f"unexpected DateStyle: {ds.decode('ascii')}")

    cdef object _error_date(self, const char *data, str msg):
        s = bytes(data).decode("utf8", "replace")
        if s == "infinity" or len(s.split()[0]) > 10:
            raise e.DataError(f"date too large (after year 10K): {s!r}") from None
        elif s == "-infinity" or "BC" in s:
            raise e.DataError(f"date too small (before year 1): {s!r}") from None
        else:
            raise e.DataError(f"can't parse date {s!r}: {msg}") from None

    cdef object cload(self, const char *data, size_t length):
        if length != 10:
            self._error_date(data, "unexpected length")

        cdef int vals[3]
        memset(vals, 0, sizeof(vals))

        cdef const char *ptr
        cdef const char *end = data + length
        ptr = _parse_date_values(data, end, vals, ARRAYSIZE(vals))
        if ptr == NULL:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse date {s!r}")

        try:
            if self._order == ORDER_YMD:
                return cdt.date_new(vals[0], vals[1], vals[2])
            elif self._order == ORDER_DMY:
                return cdt.date_new(vals[2], vals[1], vals[0])
            else:
                return cdt.date_new(vals[2], vals[0], vals[1])
        except ValueError as ex:
            self._error_date(data, str(ex))


@cython.final
cdef class DateBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int days = endian.be32toh((<uint32_t *>data)[0])
        cdef object pydays = days + PG_DATE_EPOCH_DAYS
        try:
            return PyObject_CallFunctionObjArgs(
                date_fromordinal, <PyObject *>pydays, NULL)
        except ValueError:
            if days < PY_DATE_MIN_DAYS:
                raise e.DataError("date too small (before year 1)") from None
            else:
                raise e.DataError("date too large (after year 10K)") from None


@cython.final
cdef class TimeLoader(CLoader):

    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):

        cdef int vals[3]
        memset(vals, 0, sizeof(vals))
        cdef const char *ptr
        cdef const char *end = data + length

        # Parse the first 3 groups of digits
        ptr = _parse_date_values(data, end, vals, ARRAYSIZE(vals))
        if ptr == NULL:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse time {s!r}")

        # Parse the microseconds
        cdef int us = 0
        if ptr[0] == b".":
            ptr = _parse_micros(ptr + 1, &us)

        try:
            return cdt.time_new(vals[0], vals[1], vals[2], us, None)
        except ValueError as ex:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse time {s!r}: {ex}") from None


@cython.final
cdef class TimeBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int64_t val = endian.be64toh((<uint64_t *>data)[0])
        cdef int h, m, s, us

        with cython.cdivision(True):
            us = val % 1_000_000
            val //= 1_000_000

            s = val % 60
            val //= 60

            m = val % 60
            h = <int>(val // 60)

        try:
            return cdt.time_new(h, m, s, us, None)
        except ValueError:
            raise e.DataError(
                f"time not supported by Python: hour={h}"
            ) from None


@cython.final
cdef class TimetzLoader(CLoader):

    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):

        cdef int vals[3]
        memset(vals, 0, sizeof(vals))
        cdef const char *ptr
        cdef const char *end = data + length

        # Parse the first 3 groups of digits (time)
        ptr = _parse_date_values(data, end, vals, ARRAYSIZE(vals))
        if ptr == NULL:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse timetz {s!r}")

        # Parse the microseconds
        cdef int us = 0
        if ptr[0] == b".":
            ptr = _parse_micros(ptr + 1, &us)

        # Parse the timezone
        cdef int offsecs = _parse_timezone_to_seconds(&ptr, end)
        if ptr == NULL:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse timetz {s!r}")

        tz = _timezone_from_seconds(offsecs)
        try:
            return cdt.time_new(vals[0], vals[1], vals[2], us, tz)
        except ValueError as ex:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse timetz {s!r}: {ex}") from None


@cython.final
cdef class TimetzBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int64_t val = endian.be64toh((<uint64_t *>data)[0])
        cdef int32_t off = endian.be32toh((<uint32_t *>(data + sizeof(int64_t)))[0])
        cdef int h, m, s, us

        with cython.cdivision(True):
            us = val % 1_000_000
            val //= 1_000_000

            s = val % 60
            val //= 60

            m = val % 60
            h = <int>(val // 60)

        tz = _timezone_from_seconds(-off)
        try:
            return cdt.time_new(h, m, s, us, tz)
        except ValueError:
            raise e.DataError(
                f"time not supported by Python: hour={h}"
            ) from None


@cython.final
cdef class TimestampLoader(CLoader):

    format = PQ_TEXT
    cdef int _order

    def __cinit__(self, oid: int, context: Optional[AdaptContext] = None):

        cdef const char *ds = _get_datestyle(self._pgconn)
        if ds[0] == b'I':  # ISO
            self._order = ORDER_YMD
        elif ds[0] == b'G':  # German
            self._order = ORDER_DMY
        elif ds[0] == b'S':  # SQL, DMY / MDY
            self._order = ORDER_DMY if ds[5] == b'D' else ORDER_MDY
        elif ds[0] == b'P':  # Postgres, DMY / MDY
            self._order = ORDER_PGDM if ds[10] == b'D' else ORDER_PGMD
        else:
            raise e.InterfaceError(f"unexpected DateStyle: {ds.decode('ascii')}")

    cdef object cload(self, const char *data, size_t length):
        cdef const char *end = data + length
        if end[-1] == b'C':  # ends with BC
            raise _get_timestamp_load_error(self._pgconn, data) from None

        if self._order == ORDER_PGDM or self._order == ORDER_PGMD:
            return self._cload_pg(data, end)

        cdef int vals[6]
        memset(vals, 0, sizeof(vals))
        cdef const char *ptr

        # Parse the first 6 groups of digits (date and time)
        ptr = _parse_date_values(data, end, vals, ARRAYSIZE(vals))
        if ptr == NULL:
            raise _get_timestamp_load_error(self._pgconn, data) from None

        # Parse the microseconds
        cdef int us = 0
        if ptr[0] == b".":
            ptr = _parse_micros(ptr + 1, &us)

        # Resolve the YMD order
        cdef int y, m, d
        if self._order == ORDER_YMD:
            y, m, d = vals[0], vals[1], vals[2]
        elif self._order == ORDER_DMY:
            d, m, y = vals[0], vals[1], vals[2]
        else: # self._order == ORDER_MDY
            m, d, y = vals[0], vals[1], vals[2]

        try:
            return cdt.datetime_new(
                y, m, d, vals[3], vals[4], vals[5], us, None)
        except ValueError as ex:
            raise _get_timestamp_load_error(self._pgconn, data, ex) from None

    cdef object _cload_pg(self, const char *data, const char *end):
        cdef int vals[4]
        memset(vals, 0, sizeof(vals))
        cdef const char *ptr

        # Find Wed Jun 02 or Wed 02 Jun
        cdef char *seps[3]
        seps[0] = strchr(data, b' ')
        seps[1] = strchr(seps[0] + 1, b' ') if seps[0] != NULL else NULL
        seps[2] = strchr(seps[1] + 1, b' ') if seps[1] != NULL else NULL
        if seps[2] == NULL:
            raise _get_timestamp_load_error(self._pgconn, data) from None

        # Parse the following 3 groups of digits (time)
        ptr = _parse_date_values(seps[2] + 1, end, vals, 3)
        if ptr == NULL:
            raise _get_timestamp_load_error(self._pgconn, data) from None

        # Parse the microseconds
        cdef int us = 0
        if ptr[0] == b".":
            ptr = _parse_micros(ptr + 1, &us)

        # Parse the year
        ptr = _parse_date_values(ptr + 1, end, vals + 3, 1)
        if ptr == NULL:
            raise _get_timestamp_load_error(self._pgconn, data) from None

        # Resolve the MD order
        cdef int m, d
        try:
            if self._order == ORDER_PGDM:
                d = int(seps[0][1 : seps[1] - seps[0]])
                m = _month_abbr[seps[1][1 : seps[2] - seps[1]]]
            else: # self._order == ORDER_PGMD
                m = _month_abbr[seps[0][1 : seps[1] - seps[0]]]
                d = int(seps[1][1 : seps[2] - seps[1]])
        except (KeyError, ValueError) as ex:
            raise _get_timestamp_load_error(self._pgconn, data, ex) from None

        try:
            return cdt.datetime_new(
                vals[3], m, d, vals[0], vals[1], vals[2], us, None)
        except ValueError as ex:
            raise _get_timestamp_load_error(self._pgconn, data, ex) from None


@cython.final
cdef class TimestampBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int64_t val = endian.be64toh((<uint64_t *>data)[0])
        cdef int64_t micros, secs, days

        # Work only with positive values as the cdivision behaves differently
        # with negative values, and cdivision=False adds overhead.
        cdef int64_t aval = val if val >= 0 else -val

        # Group the micros in biggers stuff or timedelta_new might overflow
        with cython.cdivision(True):
            secs = aval // 1_000_000
            micros = aval % 1_000_000

            days = secs // 86_400
            secs %= 86_400

        try:
            delta = cdt.timedelta_new(<int>days, <int>secs, <int>micros)
            if val > 0:
                return pg_datetime_epoch + delta
            else:
                return pg_datetime_epoch - delta

        except OverflowError:
            if val <= 0:
                raise e.DataError("timestamp too small (before year 1)") from None
            else:
                raise e.DataError("timestamp too large (after year 10K)") from None


cdef class _BaseTimestamptzLoader(CLoader):
    cdef object _time_zone

    def __cinit__(self, oid: int, context: Optional[AdaptContext] = None):
        self._time_zone = _timezone_from_connection(self._pgconn)


@cython.final
cdef class TimestamptzLoader(_BaseTimestamptzLoader):

    format = PQ_TEXT
    cdef int _order

    def __cinit__(self, oid: int, context: Optional[AdaptContext] = None):

        cdef const char *ds = _get_datestyle(self._pgconn)
        if ds[0] == b'I':  # ISO
            self._order = ORDER_YMD
        else:  # Not true, but any non-YMD will do.
            self._order = ORDER_DMY

    cdef object cload(self, const char *data, size_t length):
        if self._order != ORDER_YMD:
            return self._cload_notimpl(data, length)

        cdef const char *end = data + length
        if end[-1] == b'C':  # ends with BC
            raise _get_timestamp_load_error(self._pgconn, data) from None

        cdef int vals[6]
        memset(vals, 0, sizeof(vals))

        # Parse the first 6 groups of digits (date and time)
        cdef const char *ptr
        ptr = _parse_date_values(data, end, vals, ARRAYSIZE(vals))
        if ptr == NULL:
            raise _get_timestamp_load_error(self._pgconn, data) from None

        # Parse the microseconds
        cdef int us = 0
        if ptr[0] == b".":
            ptr = _parse_micros(ptr + 1, &us)

        # Resolve the YMD order
        cdef int y, m, d
        if self._order == ORDER_YMD:
            y, m, d = vals[0], vals[1], vals[2]
        elif self._order == ORDER_DMY:
            d, m, y = vals[0], vals[1], vals[2]
        else: # self._order == ORDER_MDY
            m, d, y = vals[0], vals[1], vals[2]

        # Parse the timezone
        cdef int offsecs = _parse_timezone_to_seconds(&ptr, end)
        if ptr == NULL:
            raise _get_timestamp_load_error(self._pgconn, data) from None

        tzoff = cdt.timedelta_new(0, offsecs, 0)

        # The return value is a datetime with the timezone of the connection
        # (in order to be consistent with the binary loader, which is the only
        # thing it can return). So create a temporary datetime object, in utc,
        # shift it by the offset parsed from the timestamp, and then move it to
        # the connection timezone.
        dt = None
        try:
            dt = cdt.datetime_new(
                y, m, d, vals[3], vals[4], vals[5], us, timezone_utc)
            dt -= tzoff
            return PyObject_CallFunctionObjArgs(datetime_astimezone,
                <PyObject *>dt, <PyObject *>self._time_zone, NULL)
        except OverflowError as ex:
            # If we have created the temporary 'dt' it means that we have a
            # datetime close to max, the shift pushed it past max, overflowing.
            # In this case return the datetime in a fixed offset timezone.
            if dt is not None:
                return dt.replace(tzinfo=timezone(tzoff))
            else:
                ex1 = ex
        except ValueError as ex:
            ex1 = ex

        raise _get_timestamp_load_error(self._pgconn, data, ex1) from None

    cdef object _cload_notimpl(self, const char *data, size_t length):
        s = bytes(data)[:length].decode("utf8", "replace")
        ds = _get_datestyle(self._pgconn).decode()
        raise NotImplementedError(
            f"can't parse timestamptz with DateStyle {ds!r}: {s!r}"
        )


@cython.final
cdef class TimestamptzBinaryLoader(_BaseTimestamptzLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int64_t val = endian.be64toh((<uint64_t *>data)[0])
        cdef int64_t micros, secs, days

        # Work only with positive values as the cdivision behaves differently
        # with negative values, and cdivision=False adds overhead.
        cdef int64_t aval = val if val >= 0 else -val

        # Group the micros in biggers stuff or timedelta_new might overflow
        with cython.cdivision(True):
            secs = aval // 1_000_000
            micros = aval % 1_000_000

            days = secs // 86_400
            secs %= 86_400

        try:
            delta = cdt.timedelta_new(<int>days, <int>secs, <int>micros)
            if val > 0:
                dt = pg_datetimetz_epoch + delta
            else:
                dt = pg_datetimetz_epoch - delta
            return PyObject_CallFunctionObjArgs(datetime_astimezone,
                <PyObject *>dt, <PyObject *>self._time_zone, NULL)

        except OverflowError:
            # If we were asked about a timestamp which would overflow in UTC,
            # but not in the desired timezone (e.g. datetime.max at Chicago
            # timezone) we can still save the day by shifting the value by the
            # timezone offset and then replacing the timezone.
            if self._time_zone is not None:
                utcoff = self._time_zone.utcoffset(
                    datetime.min if val < 0 else datetime.max
                )
                if utcoff:
                    usoff = 1_000_000 * int(utcoff.total_seconds())
                    try:
                        ts = pg_datetime_epoch + timedelta(
                            microseconds=val + usoff
                        )
                    except OverflowError:
                        pass  # will raise downstream
                    else:
                        return ts.replace(tzinfo=self._time_zone)

            if val <= 0:
                raise e.DataError(
                    "timestamp too small (before year 1)"
                ) from None
            else:
                raise e.DataError(
                    "timestamp too large (after year 10K)"
                ) from None


@cython.final
cdef class IntervalLoader(CLoader):

    format = PQ_TEXT
    cdef int _style

    def __cinit__(self, oid: int, context: Optional[AdaptContext] = None):

        cdef const char *ds = _get_intervalstyle(self._pgconn)
        if ds[0] == b'p' and ds[8] == 0:  # postgres
            self._style = INTERVALSTYLE_POSTGRES
        else:  # iso_8601, sql_standard, postgres_verbose
            self._style = INTERVALSTYLE_OTHERS

    cdef object cload(self, const char *data, size_t length):
        if self._style == INTERVALSTYLE_OTHERS:
            return self._cload_notimpl(data, length)

        cdef int days = 0, secs = 0, us = 0
        cdef char sign
        cdef int val
        cdef const char *ptr = data
        cdef const char *sep
        cdef const char *end = ptr + length

        # If there are spaces, there is a [+|-]n [days|months|years]
        while True:
            if ptr[0] == b'-' or ptr[0] == b'+':
                sign = ptr[0]
                ptr += 1
            else:
                sign = 0

            sep = strchr(ptr, b' ')
            if sep == NULL or sep > end:
                break

            val = 0
            ptr = _parse_date_values(ptr, end, &val, 1)
            if ptr == NULL:
                s = bytes(data).decode("utf8", "replace")
                raise e.DataError(f"can't parse interval {s!r}")

            if sign == b'-':
                val = -val

            if ptr[1] == b'y':
                days = 365 * val
            elif ptr[1] == b'm':
                days = 30 * val
            elif ptr[1] == b'd':
                days = val
            else:
                s = bytes(data).decode("utf8", "replace")
                raise e.DataError(f"can't parse interval {s!r}")

            # Skip the date part word.
            ptr = strchr(ptr + 1, b' ')
            if ptr != NULL and ptr < end:
                ptr += 1
            else:
                break

        # Parse the time part. An eventual sign was already consumed in the loop
        cdef int vals[3]
        memset(vals, 0, sizeof(vals))
        if ptr != NULL:
            ptr = _parse_date_values(ptr, end, vals, ARRAYSIZE(vals))
            if ptr == NULL:
                s = bytes(data).decode("utf8", "replace")
                raise e.DataError(f"can't parse interval {s!r}")

            secs = vals[2] + 60 * (vals[1] + 60 * vals[0])

            if ptr[0] == b'.':
                ptr = _parse_micros(ptr + 1, &us)

        if sign == b'-':
            secs = -secs
            us = -us

        try:
            return cdt.timedelta_new(days, secs, us)
        except OverflowError as ex:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse interval {s!r}: {ex}") from None

    cdef object _cload_notimpl(self, const char *data, size_t length):
        s = bytes(data).decode("utf8", "replace")
        style = _get_intervalstyle(self._pgconn).decode()
        raise NotImplementedError(
            f"can't parse interval with IntervalStyle {style!r}: {s!r}"
        )


@cython.final
cdef class IntervalBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int64_t val = endian.be64toh((<uint64_t *>data)[0])
        cdef int32_t days = endian.be32toh(
            (<uint32_t *>(data + sizeof(int64_t)))[0])
        cdef int32_t months = endian.be32toh(
            (<uint32_t *>(data + sizeof(int64_t) + sizeof(int32_t)))[0])

        cdef int years
        with cython.cdivision(True):
            if months > 0:
                years = months // 12
                months %= 12
                days += 30 * months + 365 * years
            elif months < 0:
                months = -months
                years = months // 12
                months %= 12
                days -= 30 * months + 365 * years

        # Work only with positive values as the cdivision behaves differently
        # with negative values, and cdivision=False adds overhead.
        cdef int64_t aval = val if val >= 0 else -val
        cdef int us, ussecs, usdays

        # Group the micros in biggers stuff or timedelta_new might overflow
        with cython.cdivision(True):
            ussecs = <int>(aval // 1_000_000)
            us = aval % 1_000_000

            usdays = ussecs // 86_400
            ussecs %= 86_400

        if val < 0:
            ussecs = -ussecs
            usdays = -usdays
            us = -us

        try:
            return cdt.timedelta_new(days + usdays, ussecs, us)
        except OverflowError as ex:
            raise e.DataError(f"can't parse interval: {ex}")


cdef const char *_parse_date_values(
    const char *ptr, const char *end, int *vals, int nvals
):
    """
    Parse *nvals* numeric values separated by non-numeric chars.

    Write the result in the *vals* array (assumed zeroed) starting from *start*.

    Return the pointer at the separator after the final digit.
    """
    cdef int ival = 0
    while ptr < end:
        if b'0' <= ptr[0] <= b'9':
            vals[ival] = vals[ival] * 10 + (ptr[0] - <char>b'0')
        else:
            ival += 1
            if ival >= nvals:
                break

        ptr += 1

    return ptr


cdef const char *_parse_micros(const char *start, int *us):
    """
    Parse microseconds from a string.

    Micros are assumed up to 6 digit chars separated by a non-digit.

    Return the pointer at the separator after the final digit.
    """
    cdef const char *ptr = start
    while ptr[0]:
        if b'0' <= ptr[0] <= b'9':
            us[0] = us[0] * 10 + (ptr[0] - <char>b'0')
        else:
            break

        ptr += 1

    # Pad the fraction of second to get millis
    if us[0] and ptr - start < 6:
        us[0] *= _uspad[ptr - start]

    return ptr


cdef int _parse_timezone_to_seconds(const char **bufptr, const char *end):
    """
    Parse a timezone from a string, return Python timezone object.

    Modify the buffer pointer to point at the first character after the
    timezone parsed. In case of parse error make it NULL.
    """
    cdef const char *ptr = bufptr[0]
    cdef char sgn = ptr[0]

    # Parse at most three groups of digits
    cdef int vals[3]
    memset(vals, 0, sizeof(vals))

    ptr = _parse_date_values(ptr + 1, end, vals, ARRAYSIZE(vals))
    if ptr == NULL:
        return 0

    cdef int off = 60 * (60 * vals[0] + vals[1]) + vals[2]
    return -off if sgn == b"-" else off


cdef object _timezone_from_seconds(int sec, __cache={}):
    cdef object pysec = sec
    cdef PyObject *ptr = PyDict_GetItem(__cache, pysec)
    if ptr != NULL:
        return <object>ptr

    delta = cdt.timedelta_new(0, sec, 0)
    tz = timezone(delta)
    __cache[pysec] = tz
    return tz


cdef object _get_timestamp_load_error(
    pq.PGconn pgconn, const char *data, ex: Optional[Exception] = None
):
    s = bytes(data).decode("utf8", "replace")

    def is_overflow(s):
        if not s:
            return False

        ds = _get_datestyle(pgconn)
        if not ds.startswith(b"P"):  # Postgres
            return len(s.split()[0]) > 10  # date is first token
        else:
            return len(s.split()[-1]) > 4  # year is last token

    if s == "-infinity" or s.endswith("BC"):
        return e.DataError("timestamp too small (before year 1): {s!r}")
    elif s == "infinity" or is_overflow(s):
        return e.DataError(f"timestamp too large (after year 10K): {s!r}")
    else:
        return e.DataError(f"can't parse timestamp {s!r}: {ex or '(unknown)'}")


cdef _timezones = {}
_timezones[None] = timezone_utc
_timezones[b"UTC"] = timezone_utc


cdef object _timezone_from_connection(pq.PGconn pgconn):
    """Return the Python timezone info of the connection's timezone."""
    if pgconn is None:
        return timezone_utc

    cdef bytes tzname = libpq.PQparameterStatus(pgconn._pgconn_ptr, b"TimeZone")
    cdef PyObject *ptr = PyDict_GetItem(_timezones, tzname)
    if ptr != NULL:
        return <object>ptr

    sname = tzname.decode() if tzname else "UTC"
    try:
        zi = ZoneInfo(sname)
    except (KeyError, OSError):
        logger.warning(
            "unknown PostgreSQL timezone: %r; will use UTC", sname
        )
        zi = timezone_utc
    except Exception as ex:
        logger.warning(
            "error handling PostgreSQL timezone: %r; will use UTC (%s - %s)",
            sname,
            type(ex).__name__,
            ex,
        )
        zi = timezone.utc

    _timezones[tzname] = zi
    return zi


cdef const char *_get_datestyle(pq.PGconn pgconn):
    cdef const char *ds
    if pgconn is not None:
        ds = libpq.PQparameterStatus(pgconn._pgconn_ptr, b"DateStyle")
        if ds is not NULL and ds[0]:
            return ds

    return b"ISO, DMY"


cdef const char *_get_intervalstyle(pq.PGconn pgconn):
    cdef const char *ds
    if pgconn is not None:
        ds = libpq.PQparameterStatus(pgconn._pgconn_ptr, b"IntervalStyle")
        if ds is not NULL and ds[0]:
            return ds

    return b"postgres"
