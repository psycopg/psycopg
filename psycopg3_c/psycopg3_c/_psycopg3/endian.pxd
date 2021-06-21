# convert values  between host and big-/little-endian byte order
# http://man7.org/linux/man-pages/man3/endian.3.html

from libc.stdint cimport uint16_t, uint32_t, uint64_t

IF UNAME_SYSNAME == "Darwin":

    cdef extern from "<libkern/OSByteOrder.h>" nogil:
        cdef uint16_t OSSwapHostToBigInt16(uint16_t x) 
        cdef uint16_t OSSwapHostToLittleInt16(uint16_t x)
        cdef uint16_t OSSwapBigToHostInt16(uint16_t x)
        cdef uint16_t OSSwapLittleToHostInt16(uint16_t x)

        cdef uint32_t OSSwapHostToBigInt32(uint32_t x)
        cdef uint32_t OSSwapHostToLittleInt32(uint32_t x)
        cdef uint32_t OSSwapBigToHostInt32(uint32_t x)
        cdef uint32_t OSSwapLittleToHostInt32(uint32_t x)

        cdef uint64_t OSSwapHostToBigInt64(uint64_t x)
        cdef uint64_t OSSwapHostToLittleInt64(uint64_t x)
        cdef uint64_t OSSwapBigToHostInt64(uint64_t x)
        cdef uint64_t OSSwapLittleToHostInt64(uint64_t x)

    cdef inline uint16_t htobe16(uint16_t host_16bits):
        return OSSwapHostToBigInt16(host_16bits)
    cdef inline uint16_t htole16(uint16_t host_16bits):
        return OSSwapHostToLittleInt16(host_16bits)
    cdef inline uint16_t be16toh(uint16_t big_endian_16bits):
        return OSSwapBigToHostInt16(big_endian_16bits)
    cdef inline uint16_t le16toh(uint16_t little_endian_16bits):
        return OSSwapLittleToHostInt16(little_endian_16bits)

    cdef inline uint32_t htobe32(uint32_t host_32bits):
        return OSSwapHostToBigInt32(host_32bits)
    cdef inline uint32_t htole32(uint32_t host_32bits):
        return OSSwapHostToLittleInt32(host_32bits)
    cdef inline uint32_t be32toh(uint32_t big_endian_32bits):
        return OSSwapBigToHostInt32(big_endian_32bits)
    cdef inline uint32_t le32toh(uint32_t little_endian_32bits):
        return OSSwapLittleToHostInt32(little_endian_32bits)

    cdef inline uint64_t htobe64(uint64_t host_64bits):
        return OSSwapHostToBigInt64(host_64bits)
    cdef inline uint64_t htole64(uint64_t host_64bits):
        return OSSwapHostToLittleInt64(host_64bits)
    cdef inline uint64_t be64toh(uint64_t big_endian_64bits):
        return OSSwapBigToHostInt64(big_endian_64bits)
    cdef inline uint64_t le64toh(uint64_t little_endian_64bits):
        return OSSwapLittleToHostInt64(little_endian_64bits)
ELSE:
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
