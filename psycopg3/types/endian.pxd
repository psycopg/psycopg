# convert values  between host and big-/little-endian byte order
# http://man7.org/linux/man-pages/man3/endian.3.html

from libc.stdint cimport uint16_t, uint32_t, uint64_t


cdef extern from "<endian.h>" nogil:

    cdef uint16_t htobe16(uint16_t host_16bits)
    cdef uint16_t htole16(uint16_t host_16bits)
    cdef uint16_t be16toh(uint16_t big_endian_16bits)
    cdef uint16_t le16toh(uint16_t little_endian_16bits)

    cdef uint32_t htobe32(uint32_t host_32bits)
    cdef uint32_t htole32(uint32_t host_32bits)
    cdef uint32_t be32toh(uint32_t big_endian_32bits)
    cdef uint32_t le32toh(uint32_t little_endian_32bits)

    cdef uint64_t htobe64(uint64_t host_64bits)
    cdef uint64_t htole64(uint64_t host_64bits)
    cdef uint64_t be64toh(uint64_t big_endian_64bits)
    cdef uint64_t le64toh(uint64_t little_endian_64bits)
