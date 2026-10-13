#!/usr/bin/env python
"""Run the COPY benchmark with different flush thresholds.

Print the results as a Markdown table, and add it to the GitHub Actions job
summary if running in CI.

The tests/scripts/bench-copy.yml workflow runs this script on different platforms.
In order to use it, copy it to .github/workflows/ and push it to a branch.
Results are only comparable within the same run, as the CI hardware changes
over time.
"""

from __future__ import annotations

import os
import re
import sys
import logging
import platform
import statistics
import subprocess as sp
from pathlib import Path
from argparse import ArgumentParser, Namespace

import psycopg

FINDINGS = """
Results of October 2026 (#1437), Python 3.14, PostgreSQL 18, 3 runs per
threshold, on ubuntu-latest, macos-latest, windows-2025 (Python impl only).

- Fast scenario (client-bound): no measurable difference between thresholds
  with the C implementation on Linux and macOS: the macOS penalty for large
  buffers reported in #745 didn't reproduce. With the Python implementation,
  flushing at every block was ~3% slower on Linux, and possibly on Windows;
  macOS runs were too noisy to tell.

- Slow scenario (server-bound): same times for all thresholds, except "never",
  which was ~25% slower on Windows (maybe because of the libpq 64K send limit
  on Windows, followed by a memmove of the remaining buffer). Memory above the
  baseline: ~1-2MB up to 1M, ~11-14MB at 10M, 60-180MB with "never".

So we dropped the macOS-only flush at every block (PREFER_FLUSH) in favour of
a 1M threshold on all platforms. Thresholds from 128K to 10M were not
distinguishable on time.
"""

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

COPYTEST = Path(__file__).parent / "copytest.py"

THRESHOLDS = {
    "always": 0,
    "128K": 128 * 1024,
    "1M": 1024 * 1024,
    "10M": 10 * 1024 * 1024,
    "never": 2**62,
}

SCENARIOS = {
    # Many small records: the client is the bottleneck.
    "fast": ["--nrecs", "1000000", "--nfields", "10", "--colsize", "20"],
    # Large records on a server slower than the client.
    "slow": [
        *("--nrecs", "20000", "--nfields", "10", "--colsize", "1000"),
        *("--server-delay", "0.05"),
    ],
}


def main() -> None:
    args = parse_cmdline()
    out = [
        f"## COPY benchmark: {platform.system()}, Python {platform.python_version()},"
        f" psycopg {psycopg.pq.__impl__} implementation"
    ]
    for scenario in args.scenarios:
        out.append(run_scenario(args, scenario))

    report = "\n\n".join(out) + "\n"
    print(report)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as f:
            f.write(report)


def run_scenario(args: Namespace, scenario: str) -> str:
    times: dict[str, list[float]] = {name: [] for name in THRESHOLDS}
    rsss: dict[str, list[float]] = {name: [] for name in THRESHOLDS}

    # Interleave the thresholds to spread the effect of noisy neighbours.
    for i in range(args.repeat):
        for name, threshold in THRESHOLDS.items():
            t, rss = run_copytest(args, scenario, threshold)
            logger.info(
                "%s, %s, run %d: %.3f sec, max rss %s MB",
                *(scenario, name, i + 1, t, rss),
            )
            times[name].append(t)
            if rss is not None:
                rsss[name].append(rss)

    lines = [
        f"### Scenario: {scenario} ({' '.join(SCENARIOS[scenario])})",
        "",
        "| flush threshold | median time (s) | min time (s) | max time (s)"
        " | max rss (MB) |",
        "|---|--:|--:|--:|--:|",
    ]
    for name in THRESHOLDS:
        srss = f"{max(rsss[name]):.1f}" if rsss[name] else "n/a"
        lines.append(
            f"| {name} | {statistics.median(times[name]):.3f}"
            f" | {min(times[name]):.3f} | {max(times[name]):.3f} | {srss} |"
        )
    return "\n".join(lines)


def run_copytest(
    args: Namespace, scenario: str, threshold: int
) -> tuple[float, float | None]:
    cmdline = [sys.executable, str(COPYTEST), "--dsn", args.dsn]
    cmdline.extend(SCENARIOS[scenario])
    cmdline.extend(["--flush-threshold", str(threshold)])
    p = sp.run(cmdline, capture_output=True, text=True)
    if p.returncode:
        sys.stderr.write(p.stdout + p.stderr)
        raise SystemExit(f"copytest failed with exit code {p.returncode}")

    if not (m := re.search(r"time to copy: ([\d.]+) sec", p.stderr)):
        raise SystemExit(f"time not found in copytest output:\n{p.stderr}")
    t = float(m.group(1))
    rss = (
        float(m.group(1))
        if (m := re.search(r"max rss: ([\d.]+) MB", p.stderr))
        else None
    )
    return t, rss


def parse_cmdline() -> Namespace:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("PSYCOPG_TEST_DSN", ""),
        help="database connection string [default: $PSYCOPG_TEST_DSN]",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="number of runs for each threshold [default: %(default)s]",
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        metavar="SCENARIO",
        help=f"scenarios to run, among {', '.join(SCENARIOS)} [default: all]",
    )
    args = parser.parse_args()
    if not args.scenarios:
        args.scenarios = list(SCENARIOS)
    elif unknown := set(args.scenarios) - set(SCENARIOS):
        parser.error(f"unknown scenarios: {', '.join(sorted(unknown))}")
    return args


if __name__ == "__main__":
    main()
