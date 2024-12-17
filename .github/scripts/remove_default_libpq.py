import os
from pathlib import Path

IS_CI = bool(os.environ.get("CI"))

for path in os.environ["PATH"].split(os.pathsep):
    p = Path(path)
    if not p.exists():
        continue

    for file in p.iterdir():
        if file.name == "libpq.dll":
            print(file)
            # only remove this in CI to avoid someone run it on their machine
            if IS_CI:
                file.unlink()
