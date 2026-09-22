#!/bin/bash

# Build the libpq and openssl static libraries.

set -euo pipefail

postgres_version="${PYODIDE_LIBPQ_VERSION:-18.4}"
openssl_version="${PYODIDE_OPENSSL_VERSION:-3.6.4}"

export WASM_LIBRARY_DIR="${WASM_LIBRARY_DIR:-/tmp/psycopg-pyodide-libs}"
build_stamp="${WASM_LIBRARY_DIR}/.psycopg-pyodide-build-complete"
build_id="postgresql=${postgres_version};openssl=${openssl_version}"

cache_is_valid() {
    local output

    [[ -f "${build_stamp}" ]] || return 1
    [[ "$(< "${build_stamp}")" == "${build_id}" ]] || return 1
    [[ -x "${WASM_LIBRARY_DIR}/bin/pg_config" ]] || return 1

    for output in \
        lib/libpq.a \
        include/libpq-fe.h \
        include/libpq-events.h \
        include/pg_config.h \
        include/pg_config_manual.h \
        include/pg_config_os.h \
        include/postgres_ext.h \
        include/libpq/libpq-fs.h
    do
        [[ -s "${WASM_LIBRARY_DIR}/${output}" ]] || return 1
    done
}

if cache_is_valid; then
    echo "Pyodide libpq already available: build skipped" >&2
    exit 0
fi

rm -f "${build_stamp}"

build_dir="$(mktemp -d)"
trap 'rm -rf "${build_dir}"' EXIT

mkdir -p "${WASM_LIBRARY_DIR}"
cd "${build_dir}"

# 1. Build openssl

openssl_archive="openssl-${openssl_version}.tar.gz"
curl -fsSLO \
    "https://github.com/openssl/openssl/releases/download/openssl-${openssl_version}/${openssl_archive}"
tar xzf "${openssl_archive}"
cd "openssl-${openssl_version}"

# Map socket functions to proper syscalls
# https://github.com/openssl/openssl/blob/master/include/internal/sockets.h

patch -p1 <<'PATCH'
diff --git a/include/internal/sockets.h b/include/internal/sockets.h
--- a/include/internal/sockets.h
+++ b/include/internal/sockets.h
@@ -176,6 +176,11 @@
 #define get_last_socket_error_is_eintr() (get_last_socket_error() == WSAEINTR)
 #define readsocket(s, b, n) recv((s), (b), (n), 0)
 #define writesocket(s, b, n) send((s), (b), (n), 0)
+#elif defined(__EMSCRIPTEN__)
+#define ioctlsocket(a, b, c) ioctl(a, b, c)
+#define closesocket(s) close(s)
+#define readsocket(s, b, n) recv((s), (b), (n), 0)
+#define writesocket(s, b, n) send((s), (b), (n), 0)
 #elif defined(__DJGPP__)
 #define closesocket(s) close_s(s)
 #define readsocket(s, b, n) read_s(s, b, n)
PATCH

# Build openssl

CFLAGS="-fPIC -fvisibility=hidden" emconfigure ./Configure linux-generic32 \
    no-shared \
    no-asm \
    no-tests \
    no-apps \
    no-docs \
    no-dso \
    no-engine \
    no-module \
    no-threads \
    --with-rand-seed=getrandom \
    --openssldir=/etc/ssl \
    --libdir=lib \
    --cross-compile-prefix= \
    CC=emcc \
    AR=emar \
    RANLIB=emranlib \
    NM=llvm-nm \
    --prefix="${WASM_LIBRARY_DIR}"
emmake make build_libs
emmake make install_dev

# 2. Build postgres

cd "${build_dir}"
postgres_archive="postgresql-${postgres_version}.tar.bz2"
curl -fsSLO \
    "https://ftp.postgresql.org/pub/source/v${postgres_version}/${postgres_archive}"
tar xjf "${postgres_archive}"
cd "postgresql-${postgres_version}"

# getsockopt(SO_PEERCRED) is not supported in emscripten

patch -p1 <<'PATCH'
diff --git a/src/port/getpeereid.c b/src/port/getpeereid.c
--- a/src/port/getpeereid.c
+++ b/src/port/getpeereid.c
@@ -34 +34 @@ getpeereid(int sock, uid_t *uid, gid_t *gid)
-#if defined(SO_PEERCRED)
+#if defined(SO_PEERCRED) && !defined(__EMSCRIPTEN__)
PATCH

root_dir="$(pwd)"
CPPFLAGS="-I${WASM_LIBRARY_DIR}/include" \
CFLAGS="-fPIC" \
LDFLAGS="-L${WASM_LIBRARY_DIR}/lib" \
LIBS="-lssl -lcrypto" \
FLEX=true \
emconfigure ./configure \
    --prefix="${WASM_LIBRARY_DIR}" \
    --with-ssl=openssl \
    --without-bonjour \
    --without-gssapi \
    --without-icu \
    --without-ldap \
    --without-libcurl \
    --without-liburing \
    --without-lz4 \
    --without-readline \
    --without-zlib \
    --without-zstd

# Since we are building a static library, we need to merge all the dependencies
# into a single archive. psycopg links only with -lpq. Unlike a shared library,
# static libpq.a does not pull in its dependent archives, so merge them into a
# self-contained archive instead of coupling every downstream package to their
# link order.

emmake make -C src/backend generated-headers
emmake make -C src/common libpgcommon_shlib.a
emmake make -C src/port libpgport_shlib.a
emmake make -C src/interfaces/libpq install-lib-static
emmake make -C src/include all

# Also copy headers to the expected location

mkdir -p \
    "${WASM_LIBRARY_DIR}/bin" \
    "${WASM_LIBRARY_DIR}/lib" \
    "${WASM_LIBRARY_DIR}/include/libpq"
cp src/interfaces/libpq/libpq-fe.h src/interfaces/libpq/libpq-events.h \
    "${WASM_LIBRARY_DIR}/include/"
cp src/include/pg_config.h src/include/pg_config_manual.h \
    src/include/pg_config_os.h src/include/postgres_ext.h \
    "${WASM_LIBRARY_DIR}/include/"
cp src/include/libpq/libpq-fs.h "${WASM_LIBRARY_DIR}/include/libpq/"

cat > libpq-selfcontained.mri <<EOF
create ${WASM_LIBRARY_DIR}/lib/libpq.a
addlib ${root_dir}/src/interfaces/libpq/libpq.a
addlib ${root_dir}/src/common/libpgcommon_shlib.a
addlib ${root_dir}/src/port/libpgport_shlib.a
addlib ${WASM_LIBRARY_DIR}/lib/libssl.a
addlib ${WASM_LIBRARY_DIR}/lib/libcrypto.a
save
end
EOF
emar -M < libpq-selfcontained.mri
emranlib "${WASM_LIBRARY_DIR}/lib/libpq.a"

# Since we are cross compiling libpq, the generated pg_config
# is a WebAssembly module that cannot be executed on the build machine.
# Therefore, we provide a simple shell script that replaces the output
# of pg_config with the expected values.

cat > "${WASM_LIBRARY_DIR}/bin/pg_config" <<EOF
#!/bin/sh

prefix=\$(CDPATH= cd -P "\$(dirname "\$0")/.." && pwd)

case "\$1" in
    --includedir) printf '%s\n' "\${prefix}/include" ;;
    --libdir) printf '%s\n' "\${prefix}/lib" ;;
    --libs) printf '%s\n' "-lpq" ;;
    --ldflags) printf '%s\n' "-L\${prefix}/lib" ;;
    --version) printf '%s\n' "PostgreSQL ${postgres_version}" ;;
    *) printf '%s\n' "unsupported pg_config option: \$1" >&2; exit 1 ;;
esac
EOF
chmod +x "${WASM_LIBRARY_DIR}/bin/pg_config"
printf '%s\n' "${build_id}" > "${build_stamp}"
