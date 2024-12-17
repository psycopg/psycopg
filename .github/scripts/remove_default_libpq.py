import ctypes.util
import os
from pathlib import Path

IS_CI = bool(os.environ.get("CI"))

while libname := ctypes.util.find_library("libpq.dll"):
    if libname:
        dll_file = Path(libname).resolve()
        print("find libpq.dll", dll_file)
        # only remove this in CI to avoid someone run it on their machine
        if IS_CI:
            dll_file.unlink()
        else:
            break

# github actions default pg
p = Path(r"C:\Program Files\PostgreSQL\14\bin")

if p.exists():
    for file in p.iterdir():
        if file.name == "libpq.dll":
            print("find libpq.dll", file)
            if IS_CI:
                file.unlink()
