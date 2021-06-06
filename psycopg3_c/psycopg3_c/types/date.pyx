"""
Cython adapters for date/time types.
"""

# Copyright (C) 2021 The Psycopg Team

from libc.string cimport strchr
from cpython cimport datetime as cdt
from cpython.dict cimport PyDict_GetItem
from cpython.object cimport PyObject, PyObject_CallFunctionObjArgs
from cpython.version cimport PY_VERSION_HEX

cdef extern from "Python.h":
    const char *PyUnicode_AsUTF8AndSize(unicode obj, Py_ssize_t *size) except NULL
    object PyTimeZone_FromOffset(object offset)

# Hack to compile on Python 3.6
cdef extern from *:
    """
#if PY_VERSION_HEX < 0x03070000
#define PyTimeZone_FromOffset(x) x
#endif
    """

from psycopg3_c._psycopg3 cimport endian

from datetime import date, time, timedelta, datetime, timezone

from psycopg3 import errors as e


# Initialise the datetime C API
cdt.import_datetime()

DEF ORDER_YMD = 0
DEF ORDER_DMY = 1
DEF ORDER_MDY = 2
DEF ORDER_PGDM = 3
DEF ORDER_PGMD = 4

DEF INTERVALSTYLE_OTHERS = 0
DEF INTERVALSTYLE_SQL_STANDARD = 1

DEF PG_DATE_EPOCH_DAYS = 730120  # date(2000, 1, 1).toordinal()
DEF PY_DATE_MIN_DAYS = 1  # date.min.toordinal()

cdef object date_toordinal = date.toordinal
cdef object date_fromordinal = date.fromordinal
cdef object time_utcoffset = time.utcoffset
cdef object timedelta_total_seconds = timedelta.total_seconds
cdef object pg_datetimetz_epoch = datetime(2000, 1, 1, tzinfo=timezone.utc)
cdef object pg_datetime_epoch = datetime(2000, 1, 1)

cdef object _month_abbr = {
    n: i
    for i, n in enumerate(
        b"Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split(), 1
    )
}


@cython.final
cdef class DateDumper(CDumper):

    format = PQ_TEXT

    def __cinit__(self):
        self.oid = oids.DATE_OID

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

    def __cinit__(self):
        self.oid = oids.DATE_OID

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

    def __cinit__(self):
        self.oid = oids.TIME_OID

    cpdef upgrade(self, obj, format):
        if not obj.tzinfo:
            return self
        else:
            return TimeTzDumper(self.cls)


@cython.final
cdef class TimeTzDumper(_BaseTimeTextDumper):

    def __cinit__(self):
        self.oid = oids.TIMETZ_OID


@cython.final
cdef class TimeBinaryDumper(_BaseTimeDumper):

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.TIME_OID

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

    def __cinit__(self):
        self.oid = oids.TIMETZ_OID

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


cdef class _BaseDateTimeDumper(CDumper):

    cpdef get_key(self, obj, format):
        # Use (cls,) to report the need to upgrade (downgrade, actually) to a
        # dumper for naive timestamp.
        if obj.tzinfo:
            return self.cls
        else:
            return (self.cls,)

    cpdef upgrade(self, obj: time, format):
        raise NotImplementedError


cdef class _BaseDateTimeTextDumper(_BaseDateTimeDumper):

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
cdef class DateTimeTzDumper(_BaseDateTimeTextDumper):

    def __cinit__(self):
        self.oid = oids.TIMESTAMPTZ_OID

    cpdef upgrade(self, obj, format):
        if obj.tzinfo:
            return self
        else:
            return DateTimeDumper(self.cls)


@cython.final
cdef class DateTimeDumper(_BaseDateTimeTextDumper):

    def __cinit__(self):
        self.oid = oids.TIMESTAMP_OID


@cython.final
cdef class DateTimeTzBinaryDumper(_BaseDateTimeDumper):

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.TIMESTAMPTZ_OID

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
            return DateTimeBinaryDumper(self.cls)


@cython.final
cdef class DateTimeBinaryDumper(_BaseDateTimeDumper):

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.TIMESTAMP_OID

    cdef Py_ssize_t cdump(self, obj, bytearray rv, Py_ssize_t offset) except -1:
        delta = obj - pg_datetime_epoch

        cdef int64_t micros = cdt.timedelta_microseconds(delta) + 1_000_000 * (
            86_400 * <int64_t>cdt.timedelta_days(delta)
                + <int64_t>cdt.timedelta_seconds(delta))

        cdef char *buf = CDumper.ensure_size(rv, offset, sizeof(int64_t))
        (<int64_t *>buf)[0] = endian.htobe64(micros)
        return sizeof(int64_t)


@cython.final
cdef class TimeDeltaDumper(CDumper):

    format = PQ_TEXT
    cdef int _style

    def __cinit__(self):
        self.oid = oids.INTERVAL_OID

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)

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
            s = str(obj)
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
cdef class TimeDeltaBinaryDumper(CDumper):

    format = PQ_BINARY

    def __cinit__(self):
        self.oid = oids.INTERVAL_OID

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

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)

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

    cdef object cload(self, const char *data, size_t length):
        if length != 10:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"date not supported: {s!r}")

        cdef int vals[3]
        vals[0] = vals[1] = vals[2] = 0

        cdef size_t i
        cdef int ival = 0
        for i in range(length):
            if b'0' <= data[i] <= b'9':
                vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
            else:
                ival += 1
                if ival >= 3:
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
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse date {s!r}: {ex}") from None


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

        DEF HO = 0
        DEF MI = 1
        DEF SE = 2
        DEF MS = 3
        cdef int vals[4]
        vals[HO] = vals[MI] = vals[SE] = vals[MS] = 0

        # Parse the first 3 groups of digits
        cdef size_t i
        cdef int ival = HO
        for i in range(length):
            if b'0' <= data[i] <= b'9':
                vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
            else:
                ival += 1
                if ival >= MS:
                    break

        # Parse the 4th group of digits. Count the digits parsed
        cdef int msdigits = 0
        for i in range(i + 1, length):
            if b'0' <= data[i] <= b'9':
                vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
                msdigits += 1
            else:
                s = bytes(data).decode("utf8", "replace")
                raise e.DataError(f"can't parse time {s!r}")

        # Pad the fraction of second to get millis
        if vals[MS]:
            while msdigits < 6:
                vals[MS] *= 10
                msdigits += 1

        try:
            return cdt.time_new(vals[HO], vals[MI], vals[SE], vals[MS], None)
        except ValueError as ex:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse date {s!r}: {ex}") from None


@cython.final
cdef class TimeBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int64_t val = endian.be64toh((<uint64_t *>data)[0])
        cdef int h, m, s, ms

        with cython.cdivision(True):
            ms = val % 1_000_000
            val //= 1_000_000

            s = val % 60
            val //= 60

            m = val % 60
            h = val // 60

        try:
            return cdt.time_new(h, m, s, ms, None)
        except ValueError:
            raise e.DataError(
                f"time not supported by Python: hour={h}"
            ) from None


@cython.final
cdef class TimetzLoader(CLoader):

    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):

        DEF HO = 0
        DEF MI = 1
        DEF SE = 2
        DEF MS = 3
        DEF OH = 4
        DEF OM = 5
        DEF OS = 6
        cdef int vals[7]
        vals[HO] = vals[MI] = vals[SE] = vals[MS] = 0
        vals[OH] = vals[OM] = vals[OS] = 0

        # Parse the first 3 groups of digits
        cdef size_t i = 0
        cdef int ival = HO
        for i in range(length):
            if b'0' <= data[i] <= b'9':
                vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
            else:
                ival += 1
                if ival >= MS:
                    break

        # Are there millis?
        cdef int msdigits = 0
        if data[i] == b".":
            # Parse the 4th group of digits. Count the digits parsed
            for i in range(i + 1, length):
                if b'0' <= data[i] <= b'9':
                    vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
                    msdigits += 1
                else:
                    ival += 1
                    break

            # Pad the fraction of second to get millis
            while msdigits < 6:
                vals[MS] *= 10
                msdigits += 1

        # Parse the timezone
        cdef char sgn = data[i]
        ival = OH
        for i in range(i + 1, length):
            if b'0' <= data[i] <= b'9':
                vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
            else:
                ival += 1
                if ival > OS:
                    s = bytes(data).decode("utf8", "replace")
                    raise e.DataError(f"can't parse timetz {s!r}")

        # Calculate timezone
        cdef int off = 60 * (60 * vals[OH] + vals[OM])
        # Python < 3.7 didn't support seconds in the timezones
        if PY_VERSION_HEX >= 0x03070000:
            off += vals[OS]
        if sgn == b"-":
            off = -off

        tz = timezone_from_seconds(off)
        try:
            return cdt.time_new(vals[HO], vals[MI], vals[SE], vals[MS], tz)
        except ValueError as ex:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse timetz {s!r}: {ex}") from None


@cython.final
cdef class TimetzBinaryLoader(CLoader):

    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        cdef int64_t val = endian.be64toh((<uint64_t *>data)[0])
        cdef int32_t off = endian.be32toh((<uint32_t *>(data + sizeof(int64_t)))[0])
        cdef int h, m, s, ms

        with cython.cdivision(True):
            ms = val % 1_000_000
            val //= 1_000_000

            s = val % 60
            val //= 60

            m = val % 60
            h = val // 60

            # Python < 3.7 didn't support seconds in the timezones
            if PY_VERSION_HEX >= 0x03070000:
                off = off // 60 * 60

        tz = timezone_from_seconds(-off)
        try:
            return cdt.time_new(h, m, s, ms, tz)
        except ValueError:
            raise e.DataError(
                f"time not supported by Python: hour={h}"
            ) from None


@cython.final
cdef class TimestampLoader(CLoader):

    format = PQ_TEXT
    cdef int _order

    def __init__(self, oid: int, context: Optional[AdaptContext] = None):
        super().__init__(oid, context)

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
        if data[length - 1] == b'C':  # ends with BC
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"BC timestamps not supported, got {s!r}")

        if self._order == ORDER_PGDM or self._order == ORDER_PGMD:
            return self._cload_pg(data, length)

        DEF D1 = 0
        DEF D2 = 1
        DEF D3 = 2
        DEF HO = 3
        DEF MI = 4
        DEF SE = 5
        DEF MS = 6
        cdef int vals[7]
        vals[D1] = vals[D2] = vals[D3] = 0
        vals[HO] = vals[MI] = vals[SE] = vals[MS] = 0

        # Parse the first 6 groups of digits
        cdef size_t i
        cdef int ival = D1
        for i in range(length):
            if b'0' <= data[i] <= b'9':
                vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
            else:
                ival += 1
                if ival >= MS:
                    break

        # Parse the 7th group of digits. Count the digits parsed
        cdef int msdigits = 0
        if data[i] == b'.':
            for i in range(i + 1, length):
                if b'0' <= data[i] <= b'9':
                    vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
                    msdigits += 1
                else:
                    s = bytes(data).decode("utf8", "replace")
                    raise e.DataError(f"can't parse timestamp {s!r}")

            # Pad the fraction of second to get millis
            if vals[MS]:
                while msdigits < 6:
                    vals[MS] *= 10
                    msdigits += 1

        # Resolve the YMD order
        cdef int y, m, d
        if self._order == ORDER_YMD:
            y = vals[D1]
            m = vals[D2]
            d = vals[D3]
        elif self._order == ORDER_DMY:
            d = vals[D1]
            m = vals[D2]
            y = vals[D3]
        else: # self._order == ORDER_MDY
            m = vals[D1]
            d = vals[D2]
            y = vals[D3]

        try:
            return cdt.datetime_new(
                y, m, d, vals[HO], vals[MI], vals[SE], vals[MS], None)
        except ValueError as ex:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse timestamp {s!r}: {ex}") from None

    cdef object _cload_pg(self, const char *data, size_t length):
        DEF HO = 0
        DEF MI = 1
        DEF SE = 2
        DEF MS = 3
        DEF YE = 4
        cdef int vals[5]
        vals[HO] = vals[MI] = vals[SE] = vals[MS] = vals[YE] = 0

        # Find Wed Jun 02 or Wed 02 Jun
        cdef char *seps[3]
        seps[0] = strchr(data, b' ')
        seps[1] = strchr(seps[0] + 1, b' ') if seps[0] != NULL else NULL
        seps[2] = strchr(seps[1] + 1, b' ') if seps[1] != NULL else NULL
        if seps[2] == NULL:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse timestamp {s!r}")

        # Parse the following 3 groups of digits
        cdef size_t i = seps[2] - data
        cdef int ival = HO
        for i in range(i + 1, length):
            if b'0' <= data[i] <= b'9':
                vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
            else:
                ival += 1
                if ival >= MS:
                    break

        # Parse the ms group of digits. Count the digits parsed
        cdef int msdigits = 0
        if data[i] == b'.':
            for i in range(i + 1, length):
                if b'0' <= data[i] <= b'9':
                    vals[ival] = vals[ival] * 10 + (data[i] - <char>b'0')
                    msdigits += 1
                else:
                    break

            # Pad the fraction of second to get millis
            if vals[MS]:
                while msdigits < 6:
                    vals[MS] *= 10
                    msdigits += 1

        # Parse the year
        for i in range(i + 1, length):
            if b'0' <= data[i] <= b'9':
                vals[YE] = vals[YE] * 10 + (data[i] - <char>b'0')
            else:
                break

        # Resolve the MD order
        cdef int m, d
        try:
            if self._order == ORDER_PGDM:
                d = int(seps[0][1 : seps[1] - seps[0]])
                m = _month_abbr[seps[1][1 : seps[2] - seps[1]]]
            else: # self._order == ORDER_PGMD
                m = _month_abbr[seps[0][1 : seps[1] - seps[0]]]
                d = int(seps[1][1 : seps[2] - seps[1]])
        except (KeyError, ValueError):
            s = data.decode("utf8", "replace")
            raise e.DataError(f"can't parse timestamp: {s!r}") from None

        try:
            return cdt.datetime_new(
                vals[YE], m, d, vals[HO], vals[MI], vals[SE], vals[MS], None)
        except ValueError as ex:
            s = bytes(data).decode("utf8", "replace")
            raise e.DataError(f"can't parse timestamp {s!r}: {ex}") from None


cdef object timezone_from_seconds(int sec, __cache={}):
    cdef object pysec = sec
    cdef PyObject *ptr = PyDict_GetItem(__cache, pysec)
    if ptr != NULL:
        return <object>ptr

    delta = cdt.timedelta_new(0, sec, 0)
    if PY_VERSION_HEX >= 0x03070000:
        tz = PyTimeZone_FromOffset(delta)
    else:
        tz = timezone(delta)
    __cache[pysec] = tz
    return tz


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
