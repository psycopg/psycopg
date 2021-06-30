#!/usr/bin/env python3

# Create the psycopg-binary package by renaming and patching psycopg-c

import os
import re
import shutil
from pathlib import Path
from typing import Union

curdir = Path(__file__).parent
pdir = curdir / "../.."
target = pdir / "psycopg_binary"

if target.exists():
    raise Exception(f"path {target} already exists")


def sed_i(pattern: str, repl: str, filename: Union[str, Path]) -> None:
    with open(filename, "rb") as f:
        data = f.read()
    newdata = re.sub(pattern.encode("utf8"), repl.encode("utf8"), data)
    if newdata != data:
        with open(filename, "wb") as f:
            f.write(newdata)


shutil.copytree(pdir / "psycopg_c", target)
shutil.move(str(target / "psycopg_c"), str(target / "psycopg_binary"))
shutil.move(str(target / "README-binary.rst"), str(target / "README.rst"))
sed_i("psycopg-c", "psycopg-binary", target / "setup.cfg")
sed_i(
    r"__impl__\s*=.*", '__impl__ = "binary"', target / "psycopg_binary/pq.pyx"
)
for dirpath, dirnames, filenames in os.walk(target):
    for filename in filenames:
        if os.path.splitext(filename)[1] not in (".pyx", ".pxd", ".py"):
            continue
        sed_i(r"\bpsycopg_c\b", "psycopg_binary", Path(dirpath) / filename)
