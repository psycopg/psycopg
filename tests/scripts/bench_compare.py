#!/usr/bin/env python
"""Compare the performance of different psycopg configurations.

Run the benchmark.py benchmarks of *this* tree for each configuration passed
with --tree. A configuration is a psycopg source tree, either a path or a git
ref (checked out in a temporary worktree), optionally with environment
variables or arguments for the benchmark.py copy command of its own. For
example:

    # Compare master with the current tree
    ./bench_compare.py --tree base=master --tree head=.

    # Compare the wait functions of the current tree
    ./bench_compare.py --impl c \\
        --tree poll=. --env poll=PSYCOPG_WAIT_FUNC=wait_poll \\
        --tree c=. --env c=PSYCOPG_WAIT_FUNC=wait_c

    # Compare copy options
    ./bench_compare.py --copy-only --tree libpq=. \\
        --tree queued=. --copy-arg queued=--writer=QueuedLibpqWriter

The first configuration is the baseline the others are compared with. The
measures of the different configurations are interleaved, rotating the order
at every round, so that a drift in the machine's performance doesn't favour
any of them.

Print the results as Markdown tables, and add them to the CI job summary if
$GITHUB_STEP_SUMMARY is set, or to the file passed with --output.

The C implementation (--impl c) can only be used if all the configurations
use the same tree, because the C extension installed is built for a single
tree: mixing it with the Python code of another tree would measure neither.

With --check, report the measures of each configuration worse than the base
beyond a tolerance: times (both median and minimum) by more than 10%, loop
stalls by more than 10 ms, idle CPU above 5% of a core, max rss by more than
20% and 10 MB. The measures found worse are measured again, with twice the
rounds, before reporting them, to filter out noise. With --check=fail, exit
with status 1 if any is found.

BEWARE: the numbers are only comparable within the same run. On shared
machines, such as CI runners, differences below 5-10% are often noise; the
minimum time is usually more stable than the median.
"""

from __future__ import annotations

import os
import sys
import json
import shutil
import logging
import platform
import tempfile
import statistics
import subprocess as sp
from pathlib import Path
from argparse import ArgumentParser, Namespace
from contextlib import contextmanager
from collections import defaultdict
from dataclasses import dataclass, field
from collections.abc import Callable, Iterator

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

HERE = Path(__file__).parent
ROOT = HERE.parent.parent
BENCHMARK = HERE / "benchmark.py"

# Arguments for the benchmark.py copy command.
COPY_SCENARIOS = {
    # Many small records: the client is the bottleneck.
    "fast": ["--nrows", "1000000", "--ncols", "10", "--colsize", "20"],
    # Large records on a server slower than the client.
    "slow": [
        *("--nrows", "20000", "--ncols", "10", "--colsize", "1000"),
        *("--server-delay", "0.05"),
    ],
}


@dataclass
class Config:
    name: str
    spec: str  # the path or ref passed on the command line
    path: Path = Path()
    is_ref: bool = False
    env: dict[str, str] = field(default_factory=dict)
    # What makes this configuration different, to report it
    vars: dict[str, str] = field(default_factory=dict)
    copy_args: list[str] = field(default_factory=list)


# Measures by table, row, configuration. One value per round.
Results = dict[str, dict[str, dict[str, list[float]]]]

# A set of (table, row) to measure, or None to measure everything.
Only = set[tuple[str, str]] | None

IDLE_TABLE = "idle CPU (% of a core)"
COPY_TIME_TABLE = "copy time (s)"
COPY_RSS_TABLE = "copy max rss (MB)"


def time_table(mode: str) -> str:
    return f"{mode} time (ms)"


def stall_table(mode: str) -> str:
    return f"{mode} loop max stall (ms)"


@dataclass
class Regression:
    config: str
    table: str
    row: str
    base: float
    value: float
    reason: str


def main() -> None:
    args = parse_cmdline()
    regressions: list[Regression] = []
    confirmed: Only = set()
    with checkout_trees(args) as configs:
        results = run_rounds(args, configs, args.rounds)
        if args.check:
            regressions = find_regressions(args, configs, results)
            if regressions and args.confirm_rounds:
                # Measure again the rows found worse, to filter out the noise.
                confirmed = {(r.table, r.row) for r in regressions}
                logger.info(
                    "measuring again %d possible regressions with %d rounds",
                    len(confirmed),
                    args.confirm_rounds,
                )
                again = run_rounds(args, configs, args.confirm_rounds, confirmed)
                for table, row in confirmed:
                    if row in again.get(table, {}):
                        results[table][row] = again[table][row]
                regressions = find_regressions(args, configs, results)

    report = format_report(args, configs, results, regressions, confirmed)
    print(report)
    if output := (args.output or os.environ.get("GITHUB_STEP_SUMMARY")):
        with open(output, "a") as f:
            f.write(report)

    if args.check:
        annotate(args, regressions)
        if regressions and args.check == "fail":
            sys.exit(1)


@contextmanager
def checkout_trees(args: Namespace) -> Iterator[list[Config]]:
    """Return the configurations, checking out the refs in temporary worktrees."""
    configs = [make_config(args, spec) for spec in args.tree]
    tmpdir: Path | None = None
    worktrees: dict[str, Path] = {}
    try:
        for config in configs:
            path = Path(config.spec)
            if is_tree(path):
                config.path = path.resolve()
                continue
            if config.spec not in worktrees:
                if tmpdir is None:
                    tmpdir = Path(tempfile.mkdtemp(prefix="bench-"))
                wt = tmpdir / f"tree{len(worktrees)}"
                logger.info("checking out %s in %s", config.spec, wt)
                git("worktree", "add", "--detach", str(wt), config.spec)
                worktrees[config.spec] = wt
            config.path = worktrees[config.spec]
            config.is_ref = True

        if args.impl != "python" and len({c.path for c in configs}) > 1:
            raise SystemExit(
                f"--impl {args.impl} needs all the configurations in the same tree"
            )

        for config in configs:
            setup_env(args, config)
        yield configs

    finally:
        for wt in worktrees.values():
            git("worktree", "remove", "--force", str(wt))
        if tmpdir is not None:
            shutil.rmtree(tmpdir, ignore_errors=True)


def make_config(args: Namespace, spec: str) -> Config:
    name, sep, tree = spec.partition("=")
    if not (sep and name and tree):
        raise SystemExit(f"bad tree, NAME=PATH or NAME=REF expected: {spec!r}")
    config = Config(name, tree)
    for cname, var in args.env:
        if cname == name:
            k, sep, v = var.partition("=")
            if not (sep and k):
                raise SystemExit(f"bad variable, NAME=VAR=VALUE expected: {var!r}")
            config.vars[k] = v
    config.copy_args = [arg for cname, arg in args.copy_arg if cname == name]
    return config


def is_tree(path: Path) -> bool:
    return (path / "psycopg" / "psycopg" / "__init__.py").exists()


def git(*args: str) -> str:
    p = sp.run(["git", "-C", str(ROOT), *args], capture_output=True, text=True)
    if p.returncode:
        raise SystemExit(f"git {' '.join(args)} failed:\n{p.stderr}")
    return p.stdout


def setup_env(args: Namespace, config: Config) -> None:
    config.env = dict(os.environ)
    config.env["PYTHONPATH"] = os.pathsep.join(
        [str(config.path / "psycopg"), *filter(None, [os.environ.get("PYTHONPATH")])]
    )
    config.env["PSYCOPG_IMPL"] = args.impl
    config.env.update(config.vars)

    # Make sure that the tree's package is imported, and not another one
    # installed in the environment, for instance in editable mode.
    out = run(
        config,
        ["-c", "import psycopg; print(psycopg.__file__, psycopg.pq.__impl__)"],
    )
    fn, impl = out.rsplit(maxsplit=1)
    # normcase(), because Windows paths may differ in case.
    pkg, root = (os.path.normcase(Path(p).resolve()) for p in (fn, config.path))
    if os.path.commonpath([pkg, root]) != root:
        raise SystemExit(f"{config.name} imports psycopg from {fn} instead")
    if impl != args.impl:
        raise SystemExit(f"{config.name} uses the psycopg {impl} implementation")
    logger.info("%s: psycopg %s implementation from %s", config.name, impl, fn)


def run_rounds(
    args: Namespace, configs: list[Config], rounds: int, only: Only = None
) -> Results:
    results: Results = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for round_ in range(rounds):
        shift = round_ % len(configs)
        for config in configs[shift:] + configs[:shift]:
            logger.info("round %d of %d, %s", round_ + 1, rounds, config.name)
            if not args.copy_only:
                run_benches(args, config, results, only)
            if not args.no_copy:
                run_copies(args, config, results, only)
    return results


def run_benches(args: Namespace, config: Config, results: Results, only: Only) -> None:
    runs = [("time", "async"), ("stall", "async")]
    if not args.no_sync:
        runs.insert(1, ("time", "sync"))
    if not args.no_idle:
        runs += [("idle", "async")]
        if not args.no_sync:
            runs += [("idle", "sync")]

    for command, mode in runs:
        cmdline = [str(BENCHMARK), command, "--json", "-q", "--rounds", "1"]
        cmdline += ["--dsn", args.dsn]
        if mode == "sync":
            cmdline.append("--sync")
        if command == "idle":
            if only is not None and not any(
                t == IDLE_TABLE and r.startswith(f"{mode} ") for t, r in only
            ):
                continue
        else:
            workloads = args.workload
            if only is not None:
                table = time_table(mode) if command == "time" else stall_table(mode)
                if not (workloads := sorted(r for t, r in only if t == table)):
                    continue
            cmdline += [f"--workload={w}" for w in workloads]
        out = run(config, cmdline)
        for line in out.splitlines():
            rec = json.loads(line)
            (m,) = rec["measures"]
            if command == "time":
                table, row = time_table(mode), rec["workload"]
                value = m["wall"] * 1000.0
            elif command == "stall":
                table, row = stall_table(mode), rec["workload"]
                value = m["stall"] * 1000.0
            else:
                table, row = IDLE_TABLE, f"{mode} {rec['workload']}"
                value = m["cpu"] / m["wall"] * 100.0
            results[table][row][config.name].append(value)


def run_copies(args: Namespace, config: Config, results: Results, only: Only) -> None:
    # One copy per process, for the max rss to be meaningful.
    for scenario in args.copy_scenario:
        for mode in ("sync", "async"):
            row = f"{scenario} {mode}"
            if only is not None and not (
                (COPY_TIME_TABLE, row) in only or (COPY_RSS_TABLE, row) in only
            ):
                continue
            cmdline = [str(BENCHMARK), "copy", "--json", "-q", "--rounds", "1"]
            cmdline += ["--dsn", args.dsn, *COPY_SCENARIOS[scenario]]
            cmdline += config.copy_args
            if mode == "sync":
                cmdline.append("--sync")
            (line,) = run(config, cmdline).splitlines()
            (m,) = json.loads(line)["measures"]
            results[COPY_TIME_TABLE][row][config.name].append(m["wall"])
            if m["maxrss"] is not None:
                results[COPY_RSS_TABLE][row][config.name].append(m["maxrss"])


def find_regressions(
    args: Namespace, configs: list[Config], results: Results
) -> list[Regression]:
    """Return the measures of each configuration worse than the base."""
    rv = []
    base = configs[0].name
    for table, rows in results.items():
        for row, by_config in rows.items():
            for config in configs[1:]:
                b, v = by_config[base], by_config[config.name]
                if not (b and v):
                    continue
                if reason := check_measure(args, table, b, v):
                    rv.append(
                        Regression(
                            config.name,
                            table,
                            row,
                            statistics.median(b),
                            statistics.median(v),
                            reason,
                        )
                    )
    return rv


def check_measure(
    args: Namespace, table: str, base: list[float], values: list[float]
) -> str | None:
    """Return the reason why the values are worse than the base, if they are."""
    bmed, vmed = statistics.median(base), statistics.median(values)
    if table == IDLE_TABLE:
        if vmed > args.idle_max:
            return f"above {args.idle_max:g}% of a core"
    elif "stall" in table:
        if vmed - bmed > args.stall_tolerance:
            return f"{vmed - bmed:+.1f} ms, over {args.stall_tolerance:g} ms"
    elif table == COPY_RSS_TABLE:
        if bmed and (
            (vmed / bmed - 1.0) * 100.0 > args.rss_tolerance
            and vmed - bmed > args.rss_min
        ):
            return (
                f"{(vmed / bmed - 1.0) * 100.0:+.0f}% ({vmed - bmed:+.1f} MB),"
                f" over {args.rss_tolerance:g}% and {args.rss_min:g} MB"
            )
    elif "time" in table:
        bmin, vmin = min(base), min(values)
        if bmed and bmin:
            dmed = (vmed / bmed - 1.0) * 100.0
            dmin = (vmin / bmin - 1.0) * 100.0
            if dmed > args.time_tolerance and dmin > args.time_tolerance:
                return (
                    f"median {dmed:+.0f}%, min {dmin:+.0f}%,"
                    f" over {args.time_tolerance:g}%"
                )
    return None


def annotate(args: Namespace, regressions: list[Regression]) -> None:
    """Report the regressions as GitHub Actions annotations, if running there."""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        return
    level = "error" if args.check == "fail" else "warning"
    where = f"{platform.system()} {platform.machine()}, psycopg {args.impl}"
    for r in regressions:
        print(
            f"::{level} title=Possible performance regression ({where})::"
            f"{r.config}: {r.table}, {r.row}: {r.reason}"
        )


def run(config: Config, cmdline: list[str]) -> str:
    """Run a Python script in a configuration, return its output."""
    p = sp.run(
        [sys.executable, *cmdline], env=config.env, capture_output=True, text=True
    )
    if p.returncode:
        sys.stderr.write(p.stdout + p.stderr)
        raise SystemExit(f"{cmdline[0]} failed with exit code {p.returncode}")
    return p.stdout


def describe(config: Config) -> str:
    parts = [config.spec if config.is_ref else str(config.path)]
    parts += [f"{k}={v}" for k, v in config.vars.items()]
    parts += config.copy_args
    return ", ".join(parts)


def format_report(
    args: Namespace,
    configs: list[Config],
    results: Results,
    regressions: list[Regression],
    confirmed: Only,
) -> str:
    out = [
        f"## psycopg benchmark: {platform.system()} {platform.machine()},"
        f" Python {platform.python_version()}",
        "",
        f"psycopg {args.impl} implementation. Configurations: "
        + ", ".join(f"{c.name} ({describe(c)})" for c in configs)
        + ".",
        f"Median of {args.rounds} rounds, relative difference to {configs[0].name}."
        " For the times, the minimum too, which on a busy machine is often"
        " more stable than the median.",
    ]
    for table, rows in results.items():
        stats: list[tuple[str, Callable[[list[float]], float]]] = [
            ("", statistics.median)
        ]
        if "time" in table:
            stats.append((" min", min))
        out += [
            "",
            f"### {table}",
            "",
            "| | "
            + " | ".join(c.name + label for c in configs for label, _ in stats)
            + " |",
            "|---|" + "--:|" * len(configs) * len(stats),
        ]
        for row, by_config in rows.items():
            cells = []
            for config in configs:
                for _, stat in stats:
                    base = stat(by_config[configs[0].name])
                    value = stat(by_config[config.name])
                    cell = f"{value:.2f}"
                    if config is not configs[0] and base:
                        cell += f" ({(value / base - 1.0) * 100.0:+.0f}%)"
                    cells.append(cell)
            label = f"{row} *" if confirmed and (table, row) in confirmed else row
            out.append(f"| {label} | " + " | ".join(cells) + " |")

    if confirmed:
        out += [
            "",
            f"\\* measured again with {args.confirm_rounds} rounds, to confirm"
            " a possible regression.",
        ]

    if args.check:
        out += ["", "### Possible regressions", ""]
        if regressions:
            out += [
                "| | measure | base | value | reason |",
                "|---|---|--:|--:|---|",
            ]
            for r in regressions:
                out.append(
                    f"| {r.config} | {r.table}: {r.row} | {r.base:.2f}"
                    f" | {r.value:.2f} | {r.reason} |"
                )
        else:
            out.append("None found.")

    return "\n".join(out) + "\n"


def parse_cmdline() -> Namespace:
    parser = ArgumentParser(
        description=(__doc__ or "").split("\n\n")[0],
        epilog="See the module docstring for examples.",
    )
    parser.add_argument(
        "--tree",
        metavar="NAME=PATH|REF",
        action="append",
        required=True,
        help="a configuration to measure (repeat it, the first is the base)",
    )
    parser.add_argument(
        "--env",
        metavar="NAME=VAR=VALUE",
        action="append",
        default=[],
        type=lambda s: tuple(s.split("=", 1)),
        help="set an environment variable in the configuration NAME",
    )
    parser.add_argument(
        "--copy-arg",
        metavar="NAME=ARG",
        action="append",
        default=[],
        type=lambda s: tuple(s.split("=", 1)),
        help="pass an argument to the copy command in the configuration NAME",
    )
    parser.add_argument(
        "--impl",
        choices=["python", "c"],
        default="python",
        help="psycopg implementation to use [default: %(default)s]",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("PSYCOPG_TEST_DSN", ""),
        help="database connection string [default: $PSYCOPG_TEST_DSN]",
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=5,
        help="how many times to repeat each measure [default: %(default)s]",
    )
    parser.add_argument(
        "--workload",
        action="append",
        default=[],
        help="only run this benchmark.py workload (can be repeated)",
    )
    parser.add_argument(
        "--copy-scenario",
        action="append",
        choices=list(COPY_SCENARIOS),
        help="only run this copy scenario (can be repeated)",
    )
    parser.add_argument(
        "--copy-only",
        action="store_true",
        help="only run the copy command",
    )
    parser.add_argument(
        "--no-copy", action="store_true", help="don't run the copy command"
    )
    parser.add_argument(
        "--no-sync", action="store_true", help="don't measure sync connections"
    )
    parser.add_argument(
        "--no-idle", action="store_true", help="don't measure the idle CPU"
    )
    parser.add_argument(
        "--output", help="append the report to this file [default: no file]"
    )
    g = parser.add_argument_group("regression check")
    g.add_argument(
        "--check",
        choices=["warn", "fail"],
        help="report the measures worse than the base; with 'fail' exit with"
        " status 1 if any is found",
    )
    g.add_argument(
        "--confirm-rounds",
        type=int,
        help="rounds to measure again the measures found worse, 0 to skip"
        " [default: twice --rounds]",
    )
    g.add_argument(
        "--time-tolerance",
        type=float,
        default=10.0,
        metavar="PERCENT",
        help="max increase of median and min times [default: %(default)s]",
    )
    g.add_argument(
        "--stall-tolerance",
        type=float,
        default=10.0,
        metavar="MS",
        help="max increase of the loop stall [default: %(default)s]",
    )
    g.add_argument(
        "--idle-max",
        type=float,
        default=5.0,
        metavar="PERCENT",
        help="max idle CPU, as percentage of a core [default: %(default)s]",
    )
    g.add_argument(
        "--rss-tolerance",
        type=float,
        default=20.0,
        metavar="PERCENT",
        help="max increase of the max rss [default: %(default)s]",
    )
    g.add_argument(
        "--rss-min",
        type=float,
        default=10.0,
        metavar="MB",
        help="min increase of the max rss to be reported [default: %(default)s]",
    )
    args = parser.parse_args()
    if args.confirm_rounds is None:
        args.confirm_rounds = 2 * args.rounds

    names = [spec.partition("=")[0] for spec in args.tree]
    if len(set(names)) < len(names):
        parser.error("the configurations must have different names")
    for opt in ("env", "copy_arg"):
        for item in getattr(args, opt):
            if len(item) != 2 or not item[1]:
                parser.error(f"bad --{opt.replace('_', '-')}: {'='.join(item)!r}")
        if unknown := {name for name, _ in getattr(args, opt)} - set(names):
            parser.error(
                f"--{opt.replace('_', '-')} for unknown configurations:"
                f" {', '.join(sorted(unknown))}"
            )
    if not args.copy_scenario:
        args.copy_scenario = list(COPY_SCENARIOS)
    return args


if __name__ == "__main__":
    main()
