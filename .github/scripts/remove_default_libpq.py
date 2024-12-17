import ctypes.util
import os
from pathlib import Path

IS_CI = bool(os.environ.get("CI"))

while libname := ctypes.util.find_library("libpq.dll"):
    if libname:
        dll_file = Path(libname).resolve()
        print(dll_file)
        # only remove this in CI to avoid someone run it on their machine
        if IS_CI:
            dll_file.unlink()
