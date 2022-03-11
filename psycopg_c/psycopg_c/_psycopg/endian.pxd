"""
Access to endian conversion function
"""

# Copyright (C) 2020 The Psycopg Team

from libc.stdint cimport uint16_t, uint32_t, uint64_t

cdef extern from * nogil:
    # from https://gist.github.com/panzi/6856583
    # Improved in:
    # https://github.com/linux-sunxi/sunxi-tools/blob/master/include/portable_endian.h
    """
// "License": Public Domain
// I, Mathias Panzenböck, place this file hereby into the public domain. Use it at your own risk for whatever you like.
// In case there are jurisdictions that don't support putting things in the public domain you can also consider it to
// be "dual licensed" under the BSD, MIT and Apache licenses, if you want to. This code is trivial anyway. Consider it
// an example on how to get the endian conversion functions on different platforms.

#ifndef PORTABLE_ENDIAN_H__
#define PORTABLE_ENDIAN_H__

#if (defined(_WIN16) || defined(_WIN32) || defined(_WIN64)) && !defined(__WINDOWS__)

#	define __WINDOWS__

#endif

#if defined(__linux__) || defined(__CYGWIN__)

#	include <endian.h>

#elif defined(__APPLE__)

#	include <libkern/OSByteOrder.h>

#	define htobe16(x) OSSwapHostToBigInt16(x)
#	define htole16(x) OSSwapHostToLittleInt16(x)
#	define be16toh(x) OSSwapBigToHostInt16(x)
#	define le16toh(x) OSSwapLittleToHostInt16(x)

#	define htobe32(x) OSSwapHostToBigInt32(x)
#	define htole32(x) OSSwapHostToLittleInt32(x)
#	define be32toh(x) OSSwapBigToHostInt32(x)
#	define le32toh(x) OSSwapLittleToHostInt32(x)

#	define htobe64(x) OSSwapHostToBigInt64(x)
#	define htole64(x) OSSwapHostToLittleInt64(x)
#	define be64toh(x) OSSwapBigToHostInt64(x)
#	define le64toh(x) OSSwapLittleToHostInt64(x)

#	define __BYTE_ORDER    BYTE_ORDER
#	define __BIG_ENDIAN    BIG_ENDIAN
#	define __LITTLE_ENDIAN LITTLE_ENDIAN
#	define __PDP_ENDIAN    PDP_ENDIAN

#elif defined(__OpenBSD__) ||  defined(__NetBSD__) || defined(__FreeBSD__) || defined(__DragonFly__)

#	include <sys/endian.h>

/* For functions still missing, try to substitute 'historic' OpenBSD names */
#ifndef be16toh
#	define be16toh(x) betoh16(x)
#endif
#ifndef le16toh
#	define le16toh(x) letoh16(x)
#endif
#ifndef be32toh
#	define be32toh(x) betoh32(x)
#endif
#ifndef le32toh
#	define le32toh(x) letoh32(x)
#endif
#ifndef be64toh
#	define be64toh(x) betoh64(x)
#endif
#ifndef le64toh
#	define le64toh(x) letoh64(x)
#endif

#elif defined(__WINDOWS__)

#	include <winsock2.h>
#       ifndef _MSC_VER
#	    include <sys/param.h>
#       endif

#	if BYTE_ORDER == LITTLE_ENDIAN

#		define htobe16(x) htons(x)
#		define htole16(x) (x)
#		define be16toh(x) ntohs(x)
#		define le16toh(x) (x)

#		define htobe32(x) htonl(x)
#		define htole32(x) (x)
#		define be32toh(x) ntohl(x)
#		define le32toh(x) (x)

#		define htobe64(x) htonll(x)
#		define htole64(x) (x)
#		define be64toh(x) ntohll(x)
#		define le64toh(x) (x)

#	elif BYTE_ORDER == BIG_ENDIAN

		/* that would be xbox 360 */
#		define htobe16(x) (x)
#		define htole16(x) __builtin_bswap16(x)
#		define be16toh(x) (x)
#		define le16toh(x) __builtin_bswap16(x)

#		define htobe32(x) (x)
#		define htole32(x) __builtin_bswap32(x)
#		define be32toh(x) (x)
#		define le32toh(x) __builtin_bswap32(x)

#		define htobe64(x) (x)
#		define htole64(x) __builtin_bswap64(x)
#		define be64toh(x) (x)
#		define le64toh(x) __builtin_bswap64(x)

#	else

#		error byte order not supported

#	endif

#	define __BYTE_ORDER    BYTE_ORDER
#	define __BIG_ENDIAN    BIG_ENDIAN
#	define __LITTLE_ENDIAN LITTLE_ENDIAN
#	define __PDP_ENDIAN    PDP_ENDIAN

#else

#	error platform not supported

#endif

#endif
    """
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
