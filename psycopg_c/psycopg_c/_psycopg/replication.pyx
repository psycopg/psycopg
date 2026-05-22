
from cpython.mem cimport PyMem_Calloc, PyMem_Free
from libc.stdint cimport int32_t, uint8_t, uint16_t, uint32_t, uint64_t
from libc.string cimport strlen
from cpython.list cimport PyList_New, PyList_SET_ITEM, PyList_SetItem
from cpython.tuple cimport PyTuple_GET_ITEM, PyTuple_New, PyTuple_SET_ITEM

from psycopg_c._psycopg cimport endian

from psycopg import errors as e


def parse_pgoutput_row(
    data,
    Py_ssize_t offset,
    tx: Transformer,
    object format,
    object unchanged_sentinel,
):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef int nfields = endian.be16toh((<uint16_t*>(ptr + offset))[0])
    offset += sizeof(uint16_t)
    cdef list row = PyList_New(nfields)
    cdef tuple adapted_row

    cdef int col
    cdef Py_ssize_t length
    cdef char col_type
    cdef bint * unchanged_indices
    cdef bint has_unchanged_indices = False

    try:
        for col in range(nfields):
            col_type = (ptr + offset)[0]
            offset += sizeof(col_type)

            if col_type == "n":
                # Null value
                field = None
            elif col_type == "u":
                # Unchanged TOAST value
                # DISCUSS: it might be more reasonable to handle this in the Transformer
                if not has_unchanged_indices:
                    unchanged_indices = <bint *>PyMem_Calloc(
                        <size_t>nfields, <size_t>sizeof(bint)
                    )
                    has_unchanged_indices = True
                field = None
                unchanged_indices[col] = True
            elif col_type in ("t", "b"):
                if format == PQ_TEXT:
                    if col_type == "b":
                        raise e.DataError("Expected TEXT format but got BINARY format")
                elif col_type == "t":
                    raise e.DataError("Expected BINARY format but got TEXT format")

                length = <Py_ssize_t>endian.be32toh((<uint32_t*>(ptr + offset))[0])
                offset += sizeof(uint32_t)
                if length <= 0:
                    raise e.DataError("bad replication data: negative length")

                assert_expected_bufend_lte_bufend(ptr + length, bufend)

                field = PyMemoryView_FromObject(
                    ViewBuffer._from_buffer(data, ptr + offset, length)
                )
                offset += length
            else:
                raise e.DataError(f"Unknown column data type: {col_type}")

            Py_INCREF(field)
            PyList_SET_ITEM(row, <Py_ssize_t>col, field)

        adapted_row = tx.load_sequence(row)
        if has_unchanged_indices:
            for i in range(nfields):
                if unchanged_indices[i] is True:
                    assert row[i] is None
                    field = unchanged_sentinel
                else:
                    field = <object>PyTuple_GET_ITEM(adapted_row, <Py_ssize_t>i)
                Py_INCREF(field)
                PyList_SetItem(row, <Py_ssize_t>i, field)
            result = row
        else:
            result = adapted_row

        return result, offset

    finally:
        if has_unchanged_indices:
            PyMem_Free(<void *>unchanged_indices)


cdef inline void assert_expected_bufend_lte_bufend(
    unsigned char* expected_bufend, unsigned char* bufend
) except *:
    if expected_bufend > bufend:
        raise e.DataError(
            f"bad replication data: {expected_bufend - bufend} bytes of missing data"
        )


cdef inline void assert_expected_bufend_gte_bufend(
    unsigned char* expected_bufend, unsigned char* bufend
) except *:
    if expected_bufend < bufend:
        raise e.DataError(
            f"bad replication data: {bufend - expected_bufend} bytes of unexpected data"
        )


def parse_xlogdata(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint64_t wal_start
    cdef uint64_t wal_end
    cdef uint64_t microseconds
    cdef unsigned char *expected_bufend = ptr + 3 * sizeof(uint64_t)

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    wal_start = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    wal_end = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    microseconds = endian.be64toh((<uint64_t*>ptr)[0])

    cdef tuple row = (wal_start, wal_end, microseconds)

    return row


def parse_primarykeepalive(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint64_t wal_end
    cdef uint64_t microseconds
    cdef uint8_t reply_asap
    cdef unsigned char *expected_bufend = (
        ptr + 2 * sizeof(uint64_t) + sizeof(reply_asap)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    wal_end = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    microseconds = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    reply_asap = (<uint8_t*>ptr)[0]

    cdef tuple row = (wal_end, microseconds, <bint>reply_asap)

    return row


def parse_emit_message(data, bint is_streaming, encoding):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint32_t xid
    cdef uint8_t transactional
    cdef uint64_t lsn
    cdef char* prefix
    cdef char* content
    # add 2 bytes each for the content and prefix
    cdef unsigned char *expected_bufend = (
        ptr + sizeof(transactional) + sizeof(lsn) + 4
    )

    if is_streaming:
        expected_bufend += sizeof(xid)
        xid = endian.be32toh((<uint32_t*>ptr)[0])
        ptr += sizeof(uint32_t)

    # TODO: better error handling
    assert_expected_bufend_lte_bufend(expected_bufend, bufend)

    transactional = (<uint8_t*>ptr)[0]
    ptr += sizeof(uint8_t)
    lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    prefix = <char*>ptr
    ptr += strlen(prefix) + 1
    cdef uint32_t length = endian.be32toh((<uint32_t*>ptr)[0])
    ptr += sizeof(length)
    content = <char*>ptr

    cdef tuple row
    if is_streaming:
        row = (
            xid,
            <bint>transactional,
            lsn,
            prefix.decode(encoding),
            content.decode(encoding),
        )
    else:
        row = (
            None,
            <bint>transactional,
            lsn,
            prefix.decode(encoding),
            content[:length].decode(encoding),
        )

    return row


def parse_relation(data, bint is_streaming, str encoding, type column_cls):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint32_t xid
    cdef uint32_t relation_id
    cdef char* namespace
    cdef char* relation_name
    cdef uint8_t replica_identity
    cdef uint16_t ncolumns

    # TODO: better error handling
    # add 2 bytes each for the namespace and relation_name
    cdef unsigned char *expected_bufend = (
        ptr + sizeof(relation_id) + sizeof(replica_identity) + sizeof(ncolumns) + 4
    )

    # Per-column
    cdef uint8_t flags
    cdef char* col_name
    cdef uint32_t type_id
    cdef int32_t type_modifier

    if is_streaming:
        expected_bufend += sizeof(xid)
        xid = endian.be32toh((<uint32_t*>ptr)[0])
        ptr += sizeof(uint32_t)

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)

    relation_id = endian.be32toh((<uint32_t*>ptr)[0])
    ptr += sizeof(uint32_t)
    namespace = <char*>ptr
    ptr += strlen(namespace) + 1
    relation_name = <char*>ptr
    ptr += strlen(relation_name) + 1
    replica_identity = (<uint8_t*>ptr)[0]
    ptr += sizeof(uint8_t)
    ncolumns = endian.be16toh((<uint16_t*>ptr)[0])
    ptr += sizeof(uint16_t)

    expected_bufend += ncolumns * (
        sizeof(flags) + sizeof(type_id) + sizeof(type_modifier) + 2
    )
    assert_expected_bufend_lte_bufend(expected_bufend, bufend)

    cdef tuple columns = PyTuple_New(ncolumns)
    for i in range(ncolumns):
        flags = (<uint8_t*>ptr)[0]
        ptr += sizeof(uint8_t)
        col_name = <char*>ptr
        ptr += strlen(col_name) + 1
        type_id = endian.be32toh((<uint32_t*>ptr)[0])
        ptr += sizeof(uint32_t)
        type_modifier = endian.be32toh((<int32_t*>ptr)[0])
        ptr += sizeof(int32_t)
        col_obj = column_cls(
            flags=flags,
            name=col_name.decode(encoding),
            type_id=type_id,
            type_modifier=type_modifier,
        )
        Py_INCREF(col_obj)
        PyTuple_SET_ITEM(columns, i, col_obj)

    if is_streaming:
        return (
            xid,
            relation_id,
            namespace.decode(encoding),
            relation_name.decode(encoding),
            chr(replica_identity),
            columns,
        )
    else:
        return (
            None,
            relation_id,
            namespace.decode(encoding),
            relation_name.decode(encoding),
            chr(replica_identity),
            columns,
        )


def parse_type(data, bint is_streaming, encoding):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint32_t xid
    cdef uint32_t type_id
    cdef char* namespace
    cdef char* name

    # TODO: better error handling
    # add 2 bytes each for the namespace and name
    cdef unsigned char *expected_bufend = (
        ptr + sizeof(type_id) + 4
    )

    if is_streaming:
        expected_bufend += sizeof(xid)
        xid = endian.be32toh((<uint32_t*>ptr)[0])
        ptr += sizeof(uint32_t)

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)

    type_id = endian.be32toh((<uint32_t*>ptr)[0])
    ptr += sizeof(uint32_t)
    namespace = <char*>ptr
    ptr += strlen(namespace) + 1
    name = <char*>ptr

    cdef tuple row
    if is_streaming:
        row = (xid, type_id, namespace.decode(encoding), name.decode(encoding))
    else:
        row = (None, type_id, namespace.decode(encoding), name.decode(encoding))

    return row


def unpack_begin(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint64_t final_lsn
    cdef uint64_t commit_ts_micro
    cdef uint32_t xid
    cdef unsigned char *expected_bufend = (
        ptr + 2 * sizeof(uint64_t) + sizeof(xid)
    )
    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    final_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    commit_ts_micro = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    xid = endian.be32toh((<uint32_t*>ptr)[0])

    cdef tuple row = (final_lsn, commit_ts_micro, xid)

    return row


def unpack_commit(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint8_t flags
    cdef uint64_t final_lsn
    cdef uint64_t end_lsn
    cdef uint64_t commit_ts_micro
    cdef unsigned char *expected_bufend = (
        ptr + 3 * sizeof(uint64_t) + sizeof(flags)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    flags = (<uint8_t*>ptr)[0]
    ptr += sizeof(uint8_t)
    final_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    end_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    commit_ts_micro = endian.be64toh((<uint64_t*>ptr)[0])

    cdef tuple row = (flags, final_lsn, end_lsn, commit_ts_micro)

    return row


def parse_truncate(data, bint is_streaming):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef int32_t xid
    cdef uint32_t nrelations
    cdef uint8_t options
    cdef list relation_ids  # uint32_t*
    cdef unsigned char *expected_bufend = (
        ptr + sizeof(nrelations) + sizeof(options)
    )

    if is_streaming:
        expected_bufend += sizeof(xid)
        xid = endian.be32toh((<uint32_t*>ptr)[0])
        ptr += sizeof(uint32_t)
    nrelations = endian.be32toh((<uint32_t*>ptr)[0])
    ptr += sizeof(uint32_t)

    expected_bufend += sizeof(int32_t) * nrelations

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    options = (<uint8_t*>ptr)[0]
    ptr += sizeof(uint8_t)
    relation_ids = [endian.be32toh((<uint32_t*>ptr)[i]) for i in range(nrelations)]

    cdef tuple row
    if is_streaming:
        row = (xid, options, relation_ids)
    else:
        row = (None, options, relation_ids)

    return row


def unpack_stream_start(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint32_t xid
    cdef uint8_t flags

    cdef unsigned char *expected_bufend = (
        ptr + sizeof(xid) + sizeof(flags)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    xid = endian.be32toh((<uint32_t*>ptr)[0])
    ptr += sizeof(uint32_t)
    flags = (<uint8_t*>ptr)[0]

    cdef tuple row = (xid, flags)

    return row


def unpack_stream_commit(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint32_t xid
    cdef uint8_t flags
    cdef uint64_t commit_lsn
    cdef uint64_t end_lsn
    cdef uint64_t commit_ts_micro
    cdef unsigned char *expected_bufend = (
        ptr + 3 * sizeof(uint64_t) + sizeof(flags) + sizeof(xid)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    xid = endian.be32toh((<uint32_t*>ptr)[0])
    ptr += sizeof(uint32_t)
    flags = (<uint8_t*>ptr)[0]
    ptr += sizeof(uint8_t)
    commit_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    end_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    commit_ts_micro = endian.be64toh((<uint64_t*>ptr)[0])

    cdef tuple row = (xid, flags, commit_lsn, end_lsn, commit_ts_micro)

    return row


def unpack_stream_abort(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint32_t xid
    cdef uint32_t subxid

    cdef unsigned char *expected_bufend = (
        ptr + 2 * sizeof(uint32_t)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    xid = endian.be32toh((<uint32_t*>ptr)[0])
    ptr += sizeof(uint32_t)
    subxid = endian.be32toh((<uint32_t*>ptr)[0])

    cdef tuple row = (xid, subxid)

    return row


def unpack_stream_abort_parallel(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint32_t xid
    cdef uint32_t subxid
    cdef uint64_t abort_lsn
    cdef uint64_t abort_ts_micro
    cdef unsigned char *expected_bufend = (
        ptr + 2 * sizeof(uint64_t) + 2 * sizeof(uint32_t)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    xid = endian.be32toh((<uint32_t*>ptr)[0])
    ptr += sizeof(uint32_t)
    subxid = endian.be32toh((<uint32_t*>ptr)[0])
    ptr += sizeof(uint32_t)
    abort_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    abort_ts_micro = endian.be64toh((<uint64_t*>ptr)[0])

    cdef tuple row = (xid, subxid, abort_lsn, abort_ts_micro)

    return row


def unpack_begin_prepare(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint64_t prepare_lsn
    cdef uint64_t end_lsn
    cdef uint64_t prepare_ts_micro
    cdef uint32_t xid
    cdef unsigned char *expected_bufend = (
        ptr + 3 * sizeof(uint64_t) + sizeof(xid)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    prepare_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    end_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    prepare_ts_micro = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    xid = endian.be32toh((<uint32_t*>ptr)[0])

    cdef tuple row = (prepare_lsn, end_lsn, prepare_ts_micro, xid)

    return row


# TODO: the following two functions are structurally the same
def unpack_prepare(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint8_t flags
    cdef uint64_t prepare_lsn
    cdef uint64_t end_lsn
    cdef uint64_t prepare_ts_micro
    cdef uint32_t xid
    cdef unsigned char *expected_bufend = (
        ptr + 3 * sizeof(uint64_t) + sizeof(xid) + sizeof(flags)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    flags = (<uint8_t*>ptr)[0]
    ptr += sizeof(uint8_t)
    prepare_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    end_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    prepare_ts_micro = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    xid = endian.be32toh((<uint32_t*>ptr)[0])

    cdef tuple row = (flags, prepare_lsn, end_lsn, prepare_ts_micro, xid)

    return row


def unpack_commit_prepared(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint8_t flags
    cdef uint64_t commit_lsn
    cdef uint64_t end_lsn
    cdef uint64_t commit_ts_micro
    cdef uint32_t xid
    cdef unsigned char *expected_bufend = (
        ptr + 3 * sizeof(uint64_t) + sizeof(xid) + sizeof(flags)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    flags = (<uint8_t*>ptr)[0]
    ptr += sizeof(uint8_t)
    commit_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    end_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    commit_ts_micro = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    xid = endian.be32toh((<uint32_t*>ptr)[0])

    cdef tuple row = (flags, commit_lsn, end_lsn, commit_ts_micro, xid)

    return row


def unpack_rollback_prepared(data):
    cdef unsigned char *ptr
    cdef Py_ssize_t bufsize
    _buffer_as_string_and_size(data, <char **>&ptr, &bufsize)
    cdef unsigned char *bufend = ptr + bufsize

    cdef uint8_t flags
    cdef uint64_t end_lsn
    cdef uint64_t rollback_lsn
    cdef uint64_t prepare_ts_micro
    cdef uint64_t rollback_ts_micro
    cdef uint32_t xid
    cdef unsigned char *expected_bufend = (
        ptr + 4 * sizeof(uint64_t) + sizeof(xid) + sizeof(flags)
    )

    assert_expected_bufend_lte_bufend(expected_bufend, bufend)
    assert_expected_bufend_gte_bufend(expected_bufend, bufend)

    flags = (<uint8_t*>ptr)[0]
    ptr += sizeof(uint8_t)
    end_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    rollback_lsn = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    prepare_ts_micro = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    rollback_ts_micro = endian.be64toh((<uint64_t*>ptr)[0])
    ptr += sizeof(uint64_t)
    xid = endian.be32toh((<uint32_t*>ptr)[0])

    cdef tuple row = (
        flags, end_lsn, rollback_lsn, prepare_ts_micro, rollback_ts_micro, xid
    )

    return row
