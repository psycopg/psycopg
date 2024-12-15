"""
We use vcpkg in github actions to build psycopg-binary.

This is a stub to work as `pg_config --libdir` or `pg_config --includedir` to make it work with vcpkg.

You will need install vcpkg and set `VCPKG_ROOT `env
and run `vcpkg install libpq:x64-windows-release` before you use this script
"""

import os
import pathlib
import sys
import platform


def main():
    # only x64-windows
    if not (sys.platform == "win32" and platform.machine() == "AMD64"):
        raise Exception("this script should only be used in x64-windows")

    what = sys.argv[1]

    if what == "--help":
        print(__doc__)
        return

    # on github actions it's `VCPKG_INSTALLATION_ROOT`
    if "VCPKG_INSTALLATION_ROOT" not in os.environ:
        print("failed to find VCPKG ROOT path", file=sys.stderr)
        sys.exit(1)

    vcpkg_root = pathlib.Path(os.environ["VCPKG_INSTALLATION_ROOT"])
    vcpkg_platform_root = vcpkg_root.joinpath("installed/x64-windows-release").resolve()
    if not vcpkg_root.joinpath("packages/libpq_x64-windows-release").exists():
        print("libpq not installed with vcpkg", file=sys.stderr)
        sys.exit(1)

    if what == "--libdir":
        print(str(vcpkg_platform_root.joinpath("lib")))
        return
    if what == "--includedir":
        print(str(vcpkg_platform_root.joinpath("include")))
        return

    print(
        "unexpected command: {!r}\n this maybe out-of-sync between 'psycopg_c/setup.py' and 'tools/build/pg_config_vcpkg_stub/pg_config_vcpkg_stub/__init__.py'".format(
            sys.argv[1:]
        ),
        file=sys.stderr,
    )
    sys.exit(1)
