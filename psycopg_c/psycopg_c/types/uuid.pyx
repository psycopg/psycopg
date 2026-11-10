cimport cython
from cpython.object cimport PyObject_GenericSetAttr


cdef extern from *:
    """
static const int8_t hex_to_int_map[] = {
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 0-15
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 16-31
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 32-47
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,            // 48-63 ('0'-'9')
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 64-79 ('A'-'F')
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 80-95
    -1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 96-111 ('a'-'f')
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 112-127
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 128-143
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 144-159
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 160-175
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 176-191
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 192-207
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 208-223
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,  // 224-239
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1   // 240-255
};
"""
    const int8_t[256] hex_to_int_map


cdef extern from *:
    """
#if PY_VERSION_HEX < 0x030C0000
#include "structmember.h"
#define Py_T_OBJECT_EX T_OBJECT_EX
#define Py_READONLY READONLY
#endif

/* Return the offset of a writable object slot of a type, given its
 * descriptor, or -1 if the descriptor is not of the expected kind.
 */
static Py_ssize_t
uuid_slot_offset(PyObject *descr)
{
    PyMemberDef *m;

    if (Py_TYPE(descr) != &PyMemberDescr_Type) {
        return -1;
    }
    m = ((PyMemberDescrObject *)descr)->d_member;
    if (m->type != Py_T_OBJECT_EX || (m->flags & Py_READONLY)) {
        return -1;
    }
    return m->offset;
}

/* Create an instance of a type and store two objects in its slots, without
 * passing through __new__, __init__, __setattr__.
 */
static PyObject *
uuid_new_with_slots(
    PyObject *type, Py_ssize_t off1, PyObject *val1,
    Py_ssize_t off2, PyObject *val2)
{
    PyObject *rv = ((PyTypeObject *)type)->tp_alloc((PyTypeObject *)type, 0);
    if (!rv) {
        return NULL;
    }
    Py_INCREF(val1);
    *(PyObject **)((char *)rv + off1) = val1;
    Py_INCREF(val2);
    *(PyObject **)((char *)rv + off2) = val2;
    return rv;
}

/* Return a Python int from a 16 bytes big-endian unsigned buffer. */
static PyObject *
uuid_int_from_bytes(const char *buf)
{
#if PY_VERSION_HEX >= 0x030D0000
    return PyLong_FromNativeBytes(buf, 16,
        Py_ASNATIVEBYTES_BIG_ENDIAN | Py_ASNATIVEBYTES_UNSIGNED_BUFFER);
#else
    return _PyLong_FromByteArray((const unsigned char *)buf, 16, 0, 0);
#endif
}
"""
    Py_ssize_t uuid_slot_offset(object descr)
    object uuid_new_with_slots(
        object type, Py_ssize_t off1, object val1, Py_ssize_t off2, object val2)
    object uuid_int_from_bytes(const char *buf)


# Python objects and slots offsets, initialised by _uuid_init()
cdef object _uuid_type = None
cdef object _uuid_safe_unknown = None
cdef Py_ssize_t _uuid_int_offset = -1
cdef Py_ssize_t _uuid_safe_offset = -1


cdef int _uuid_init() except -1:
    global _uuid_type, _uuid_safe_unknown, _uuid_int_offset, _uuid_safe_offset

    if _uuid_type is not None:
        return 0

    # Importing the uuid module is slow, so import it only on request.
    import uuid

    # UUID stores its state in the 'int' and 'is_safe' slots, and is otherwise
    # read-only. Writing these slots directly is much faster than calling
    # UUID(). Fail loudly if a future Python changes the UUID layout.
    d = uuid.UUID.__dict__
    if d.get("__slots__") == ("int", "is_safe", "__weakref__"):
        _uuid_int_offset = uuid_slot_offset(d["int"])
        _uuid_safe_offset = uuid_slot_offset(d["is_safe"])
    if _uuid_int_offset < 0 or _uuid_safe_offset < 0:
        raise TypeError("unexpected uuid.UUID layout: please report this bug")

    _uuid_safe_unknown = uuid.SafeUUID.unknown
    _uuid_type = uuid.UUID
    return 0


cdef object _uuid_from_bytes(const char *buf):
    """Return a UUID from 16 bytes in network order."""
    cdef object value = uuid_int_from_bytes(buf)
    return uuid_new_with_slots(
        _uuid_type, _uuid_int_offset, value,
        _uuid_safe_offset, _uuid_safe_unknown)


cdef inline object _uuid_from_bytes_setattr(const char *buf):
    """Return a UUID from 16 bytes in network order, using setattr.

    Currently unused: it is about 1.6x slower than _uuid_from_bytes(). It can
    replace it if we build with the Limited API (abi3 wheels), where the
    PyMemberDescrObject internals used to find the slot offsets are not
    available. It only relies on UUID storing its state in the 'int' and
    'is_safe' attributes, which is what uuid.py itself does.
    """
    cdef object value = uuid_int_from_bytes(buf)
    cdef object rv = PyObject_CallFunctionObjArgs(
        object.__new__, <PyObject *>_uuid_type, NULL)
    PyObject_GenericSetAttr(rv, "int", value)
    PyObject_GenericSetAttr(rv, "is_safe", _uuid_safe_unknown)
    return rv

cdef class _UUIDLoader(CLoader):

    def __cinit__(self, oid: int, context: AdaptContext | None = None):
        _uuid_init()


@cython.final
cdef class UUIDLoader(_UUIDLoader):
    format = PQ_TEXT

    cdef object cload(self, const char *data, size_t length):
        cdef uint64_t high = 0
        cdef uint64_t low = 0
        cdef size_t i
        cdef int ndigits = 0
        cdef int8_t c

        for i in range(length):
            c = data[i]
            if hex_to_int_map[c] == -1:
                continue

            if ndigits < 16:
                high = (high << 4) | hex_to_int_map[c]
            else:
                low = (low << 4) | hex_to_int_map[c]
            ndigits += 1

        if ndigits != 32:
            raise ValueError("Invalid UUID string")

        cdef uint64_t be[2]
        be[0] = endian.htobe64(high)
        be[1] = endian.htobe64(low)
        return _uuid_from_bytes(<const char *>be)


@cython.final
cdef class UUIDBinaryLoader(_UUIDLoader):
    format = PQ_BINARY

    cdef object cload(self, const char *data, size_t length):
        if length != 16:
            raise ValueError("Invalid UUID data")
        return _uuid_from_bytes(data)
