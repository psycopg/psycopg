#!/bin/bash

# Build a modern version of libpq and depending libs from source on Centos 5,
# Alpine or macOS

set -euo pipefail

postgres_version="${LIBPQ_VERSION}"
openssl_version="${OPENSSL_VERSION}"

# Latest release: https://kerberos.org/dist/
krb5_version="1.21.3"

# Latest release: https://openldap.org/software/download/
ldap_version="2.6.9"

export LIBPQ_BUILD_PREFIX=${LIBPQ_BUILD_PREFIX:-/tmp/libpq.build}

case "$(uname)" in
    Darwin)
        ID=macos
        library_suffix=dylib
        ;;

    Linux)
        source /etc/os-release
        library_suffix=so
        ;;

    *)
        echo "$0: unexpected Operating system: '$(uname)'" >&2
        exit 1
        ;;
esac

# Install packages required for test and wheels build, regardless of whether
# we will build the libpq or not.
case "$ID" in
    alpine)
        apk add --no-cache tzdata krb5-libs
        ;;
esac

if [[ -f "${LIBPQ_BUILD_PREFIX}/lib/libpq.${library_suffix}" ]]; then
    echo "libpq already available: build skipped" >&2
    exit 0
fi

# Install packages required to build the libpq.
case "$ID" in
    centos | almalinux)
        yum update -y
        yum install -y flex cyrus-sasl-devel krb5-devel pam-devel \
            perl-IPC-Cmd perl-Time-Piece zlib-devel
        ;;

    alpine)
        apk upgrade
        apk add --no-cache flex krb5-dev linux-pam-dev openldap-dev \
            openssl-dev zlib-dev
        ;;

    macos)
        brew install automake cyrus-sasl libtool m4
        # If available, libpq seemingly insists on linking against homebrew's
        # openssl no matter what so remove it. Since homebrew's curl depends on
        # it, force use of system curl.
        brew uninstall --force --ignore-dependencies openssl gettext curl
        if [ -z "${MACOSX_ARCHITECTURE:-}" ]; then
            MACOSX_ARCHITECTURE="$(uname -m)"
        fi
        # Set the deployment target to be <= to that of the oldest supported Python version.
        # e.g. https://www.python.org/downloads/release/python-380/
        if [ "$MACOSX_ARCHITECTURE" == "x86_64" ]; then
            export MACOSX_DEPLOYMENT_TARGET=10.9
        else
            export MACOSX_DEPLOYMENT_TARGET=11.0
        fi
        ;;

    *)
        echo "$0: unexpected Linux distribution: '$ID'" >&2
        exit 1
        ;;
esac


if [ "$ID" == "macos" ]; then
    make_configure_standard_flags=( \
        --prefix=${LIBPQ_BUILD_PREFIX} \
        "CPPFLAGS=-I${LIBPQ_BUILD_PREFIX}/include/ -arch $MACOSX_ARCHITECTURE" \
        "LDFLAGS=-L${LIBPQ_BUILD_PREFIX}/lib -arch $MACOSX_ARCHITECTURE" \
    )
else
    make_configure_standard_flags=( \
        --prefix=${LIBPQ_BUILD_PREFIX} \
        CPPFLAGS=-I${LIBPQ_BUILD_PREFIX}/include/ \
        "LDFLAGS=-L${LIBPQ_BUILD_PREFIX}/lib -L${LIBPQ_BUILD_PREFIX}/lib64" \
    )
fi

if [ "$ID" == "centos" ] || [ "$ID" == "almalinux" ]|| [ "$ID" == "macos" ]; then
  if [[ ! -f "${LIBPQ_BUILD_PREFIX}/openssl.cnf" ]]; then

    # Build openssl if needed
    openssl_tag="openssl-${openssl_version}"
    openssl_dir="openssl-${openssl_tag}"
    if [ ! -d "${openssl_dir}" ]; then
        curl -fsSL \
            https://github.com/openssl/openssl/archive/${openssl_tag}.tar.gz \
            | tar xzf -

        pushd "${openssl_dir}"

        options=(--prefix=${LIBPQ_BUILD_PREFIX} --openssldir=${LIBPQ_BUILD_PREFIX} \
            zlib -fPIC shared)
        if [ -z "${MACOSX_ARCHITECTURE:-}" ]; then
            ./config ${options[*]}
        else
            ./config "darwin64-$MACOSX_ARCHITECTURE-cc" ${options[*]}
        fi

        make -s depend
        make -s
    else
        pushd "${openssl_dir}"
    fi

    # Install openssl
    make install_sw
    popd

  fi
fi


if [ "$ID" == "macos" ]; then

    # Build kerberos if needed
    krb5_dir="krb5-${krb5_version}/src"
    if [ ! -d "${krb5_dir}" ]; then
        curl -fsSL "https://kerberos.org/dist/krb5/${krb5_version%.*}/krb5-${krb5_version}.tar.gz" \
            | tar xzf -

        pushd "${krb5_dir}"
        ./configure "${make_configure_standard_flags[@]}"
        make -s
    else
        pushd "${krb5_dir}"
    fi

    make install
    popd

fi


if [ "$ID" == "centos" ] || [ "$ID" == "almalinux" ]|| [ "$ID" == "macos" ]; then
  if [[ ! -f "${LIBPQ_BUILD_PREFIX}/lib/libldap.${library_suffix}" ]]; then

    # Build openldap if needed
    ldap_tag="${ldap_version}"
    ldap_dir="openldap-${ldap_tag}"
    if [ ! -d "${ldap_dir}" ]; then
        curl -fsSL \
            https://www.openldap.org/software/download/OpenLDAP/openldap-release/openldap-${ldap_tag}.tgz \
            | tar xzf -

        pushd "${ldap_dir}"

        ./configure "${make_configure_standard_flags[@]}" --enable-backends=no --enable-null

        make -s depend
        make -s -C libraries/liblutil/
        make -s -C libraries/liblber/
        make -s -C libraries/libldap/
    else
        pushd "${ldap_dir}"
    fi

    # Install openldap
    make -C libraries/liblber/ install
    make -C libraries/libldap/ install
    make -C include/ install
    chmod +x ${LIBPQ_BUILD_PREFIX}/lib/{libldap,liblber}*.${library_suffix}*
    popd

  fi
fi


# Build libpq if needed
postgres_tag="REL_${postgres_version//./_}"
postgres_dir="postgres-${postgres_tag}"
if [ ! -d "${postgres_dir}" ]; then
    curl -fsSL \
        https://github.com/postgres/postgres/archive/${postgres_tag}.tar.gz \
        | tar xzf -

    pushd "${postgres_dir}"

    # Change the gssencmode default to 'disable' to avoid various troubles
    # related to unwanted GSSAPI interaction. See #1136.
    patch -f -p1 <<HERE
diff --git a/src/interfaces/libpq/fe-connect.c b/src/interfaces/libpq/fe-connect.c
index 454d2ea3fb7..52c64ba3292 100644
--- a/src/interfaces/libpq/fe-connect.c
+++ b/src/interfaces/libpq/fe-connect.c
@@ -132,7 +132,7 @@ static int	ldapServiceLookup(const char *purl, PQconninfoOption *options,
 #define DefaultSSLNegotiation	"postgres"
 #ifdef ENABLE_GSS
 #include "fe-gssapi-common.h"
-#define DefaultGSSMode "prefer"
+#define DefaultGSSMode "disable"
 #else
 #define DefaultGSSMode "disable"
 #endif
HERE

    if [ "$ID" != "macos" ]; then
        # Match the default unix socket dir default with what defined on Ubuntu and
        # Red Hat, which seems the most common location
        sed -i 's|#define DEFAULT_PGSOCKET_DIR .*'\
'|#define DEFAULT_PGSOCKET_DIR "/var/run/postgresql"|' \
            src/include/pg_config_manual.h
    fi

    export LD_LIBRARY_PATH="${LIBPQ_BUILD_PREFIX}/lib:${LIBPQ_BUILD_PREFIX}/lib64"

    ./configure "${make_configure_standard_flags[@]}" --sysconfdir=/etc/postgresql-common \
        --with-gssapi --with-openssl --with-pam --with-ldap \
        --without-readline --without-icu
    make -s -C src/interfaces/libpq
    make -s -C src/bin/pg_config
    make -s -C src/include
else
    pushd "${postgres_dir}"
fi

# Install libpq
make -C src/interfaces/libpq install
make -C src/bin/pg_config install
make -C src/include install
popd

find ${LIBPQ_BUILD_PREFIX} -name \*.${library_suffix}.\* -type f -exec strip --strip-unneeded {} \;
