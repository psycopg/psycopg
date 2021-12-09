import re
import subprocess as sp

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "mypy: the test uses mypy (the marker is set automatically"
        " on tests using the fixture)",
    )


def pytest_collection_modifyitems(items):
    for item in items:
        if "mypy" in item.fixturenames:
            # add a mypy tag so we can address these tests only
            item.add_marker(pytest.mark.mypy)

            # All the tests using mypy are slow
            item.add_marker(pytest.mark.slow)


@pytest.fixture(scope="session")
def mypy(tmp_path_factory):
    cache_dir = tmp_path_factory.mktemp(basename="mypy_cache")
    src_dir = tmp_path_factory.mktemp("source")

    class MypyRunner:
        def run_on_file(self, filename):
            cmdline = f"""
                mypy
                --strict
                --show-error-codes --no-color-output --no-error-summary
                --config-file= --cache-dir={cache_dir}
                """.split()
            cmdline.append(filename)
            return sp.run(cmdline, stdout=sp.PIPE, stderr=sp.STDOUT)

        def run_on_source(self, source):
            fn = src_dir / "tmp.py"
            with fn.open("w") as f:
                f.write(source)

            return self.run_on_file(str(fn))

        def get_revealed(self, line):
            """return the type from an output of reveal_type"""
            return re.sub(
                r".*Revealed type is (['\"])([^']+)\1.*", r"\2", line
            ).replace("*", "")

    return MypyRunner()
