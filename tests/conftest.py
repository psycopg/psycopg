import sys
import asyncio
from typing import List

import pytest

pytest_plugins = (
    "tests.fix_db",
    "tests.fix_pq",
    "tests.fix_mypy",
    "tests.fix_faker",
    "tests.fix_proxy",
    "tests.fix_psycopg",
    "tests.pool.fix_pool",
)


def pytest_configure(config):
    markers = [
        "slow: this test is kinda slow (skip with -m 'not slow')",
        # There are troubles on travis with these kind of tests and I cannot
        # catch the exception for my life.
        "subprocess: the test import psycopg after subprocess",
        "timing: the test is timing based and can fail on cheese hardware",
        "dns: the test requires dnspython to run",
        "postgis: the test requires the PostGIS extension to run",
    ]

    for marker in markers:
        config.addinivalue_line("markers", marker)


def pytest_addoption(parser):
    parser.addoption(
        "--loop",
        choices=["default", "uvloop"],
        default="default",
        help="The asyncio loop to use for async tests.",
    )

    parser.addoption(
        "--no-collect-ok",
        action="store_true",
        help="If no test is collected, exit with 0 instead of 5"
        " (useful with --lfnf=none).",
    )

    parser.addoption(
        "--allow-fail",
        metavar="NUM",
        type=int,
        default=0,
        help="Tolerate up to NUM failures. Use carefully.",
    )


def pytest_report_header(config):
    loop = config.getoption("--loop")
    if loop == "default":
        return []

    return [f"asyncio loop: {loop}"]


@pytest.fixture
def event_loop(request):
    """Return the event loop to test asyncio-marked tests."""
    # pytest-asyncio reset the the loop config after each test, so set
    # set them each time

    loop = request.config.getoption("--loop")
    if loop == "uvloop":
        import uvloop

        uvloop.install()
    else:
        assert loop == "default"

    loop = None
    if sys.platform == "win32":
        if sys.version_info < (3, 7):
            loop = asyncio.SelectorEventLoop()
            asyncio.set_event_loop(loop)
        else:
            asyncio.set_event_loop_policy(
                asyncio.WindowsSelectorEventLoopPolicy()
            )
    if not loop:
        loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


allow_fail_messages: List[str] = []


def pytest_sessionfinish(session, exitstatus):
    no_collect_ok = session.config.getoption("--no-collect-ok")
    if exitstatus == pytest.ExitCode.NO_TESTS_COLLECTED and no_collect_ok:
        session.exitstatus = pytest.ExitCode.OK

    allow_fail = session.config.getoption("--allow-fail")
    if exitstatus == pytest.ExitCode.TESTS_FAILED:
        if session.testsfailed <= allow_fail:
            allow_fail_messages.append(f"Tests failed: {session.testsfailed}")
            allow_fail_messages.append(f"Failures allowed: {allow_fail}")
            session.exitstatus = pytest.ExitCode.OK


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if allow_fail_messages:
        terminalreporter.section("failed tests ignored")
        for msg in allow_fail_messages:
            terminalreporter.line(msg)
