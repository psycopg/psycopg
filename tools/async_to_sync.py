#!/usr/bin/env python
"""Convert async code in the project to sync code.

Note: the version of Python used to run this script affects the output.

Hint: in order to explore the AST of a module you can run:

    python -m ast path/to/module.py

"""

from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor
import sys
import logging
import subprocess as sp
from copy import deepcopy
from typing import Any, Literal
from pathlib import Path
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter
from importlib.metadata import version

import ast_comments as ast

# The version of Python officially used for the conversion.
# Output may differ in other versions.
# Should be consistent with the Python version used in lint.yml
PYVER = "3.11"

ALL_INPUTS = """
    psycopg/psycopg/_conninfo_attempts_async.py
    psycopg/psycopg/_copy_async.py
    psycopg/psycopg/connection_async.py
    psycopg/psycopg/cursor_async.py
    psycopg_pool/psycopg_pool/null_pool_async.py
    psycopg_pool/psycopg_pool/pool_async.py
    psycopg_pool/psycopg_pool/sched_async.py
    tests/pool/test_pool_async.py
    tests/pool/test_pool_common_async.py
    tests/pool/test_pool_null_async.py
    tests/pool/test_sched_async.py
    tests/test_connection_async.py
    tests/test_conninfo_attempts_async.py
    tests/test_copy_async.py
    tests/test_cursor_async.py
    tests/test_cursor_client_async.py
    tests/test_cursor_common_async.py
    tests/test_cursor_raw_async.py
    tests/test_cursor_server_async.py
    tests/test_notify_async.py
    tests/test_pipeline_async.py
    tests/test_prepared_async.py
    tests/test_tpc_async.py
    tests/test_transaction_async.py
""".split()

PROJECT_DIR = Path(__file__).parent.parent
SCRIPT_NAME = os.path.basename(sys.argv[0])

logger = logging.getLogger()


def main() -> int:
    opt = parse_cmdline()
    if opt.container:
        return run_in_container(opt.container)

    logging.basicConfig(level=opt.log_level, format="%(levelname)s %(message)s")

    current_ver = ".".join(map(str, sys.version_info[:2]))
    if current_ver != PYVER:
        logger.warning(
            "Expecting output generated by Python %s; you are running %s instead.",
            PYVER,
            current_ver,
        )
        logger.warning(
            "You might get spurious changes that will be rejected by the CI linter."
        )
        logger.warning(
            "(use %s {--docker | --podman} to run it with Python %s in a container)",
            sys.argv[0],
            PYVER,
        )

    if not opt.convert_all:
        inputs, outputs = [], []
        for fpin in opt.inputs:
            fpout = fpin.parent / fpin.name.replace("_async", "")
            if fpout.stat().st_mtime >= fpin.stat().st_mtime:
                logger.debug("not converting %s as %s is up to date", fpin, fpout)
                continue
            inputs.append(fpin)
            outputs.append(fpout)
        if not outputs:
            logger.warning("all output files are up to date, nothing to do")
            return 0

    else:
        inputs = opt.inputs
        outputs = [fpin.parent / fpin.name.replace("_async", "") for fpin in inputs]

    if opt.jobs == 1:
        logger.debug("multi-processing disabled")
        for fpin, fpout in zip(inputs, outputs):
            convert(fpin, fpout)
    else:
        with ProcessPoolExecutor(max_workers=opt.jobs) as executor:
            executor.map(convert, inputs, outputs)

    if opt.check:
        return check([str(o) for o in outputs])

    return 0


def convert(fpin: Path, fpout: Path) -> None:
    logger.info("converting %s", fpin)
    with fpin.open() as f:
        source = f.read()

    tree = ast.parse(source, filename=str(fpin))
    tree = async_to_sync(tree, filepath=fpin)
    output = tree_to_str(tree, fpin)

    with fpout.open("w") as f:
        print(output, file=f)

    sp.check_call(["black", "-q", str(fpout)])


def check(outputs: list[str]) -> int:
    try:
        sp.check_call(["git", "diff", "--exit-code"] + outputs)
    except sp.CalledProcessError:
        logger.error("sync and async files... out of sync!")
        return 1

    # Check that all the files to convert are included in the --all list
    cmdline = ["git", "grep", "-l", f"auto-generated by '{SCRIPT_NAME}'", "**.py"]
    maybe_conv = sp.check_output(cmdline, cwd=str(PROJECT_DIR), text=True).split()
    if not maybe_conv:
        logger.error("no file to check? Maybe this script bitrot?")
        return 1
    unk_conv = sorted(
        set(maybe_conv) - set(fn.replace("_async", "") for fn in ALL_INPUTS)
    )
    if unk_conv:
        logger.error(
            "files converted by %s but not included in --all list: %s",
            SCRIPT_NAME,
            ", ".join(unk_conv),
        )
        return 1

    return 0


def run_in_container(engine: Literal["docker", "podman"]) -> int:
    """
    Build an image and run the script in a container.
    """
    tag = f"async-to-sync:{version('ast_comments')}-{PYVER}"

    # Check if the image we want is present.
    cmdline = [engine, "inspect", tag, "-f", "{{ .Id }}"]
    try:
        sp.check_call(cmdline, stdout=sp.DEVNULL, stderr=sp.DEVNULL)
    except sp.CalledProcessError:
        logger.info("building container image with %s", engine)
        containerfile = f"""\
FROM python:{PYVER}

WORKDIR /src

ADD psycopg psycopg
RUN pip install ./psycopg[dev]

ENTRYPOINT ["tools/async_to_sync.py"]
"""
        cmdline = [engine, "build", "--tag", tag, "-f", "-", str(PROJECT_DIR)]
        sp.run(cmdline, check=True, text=True, input=containerfile)

    cmdline = sys.argv[1:]
    cmdline.remove(f"--{engine}")
    cmdline = [engine, "run", "--rm", "-v", f"{PROJECT_DIR}:/src", tag] + cmdline
    logger.info("running in container image %s (%s)", tag, engine)
    sp.check_call(cmdline)
    return 0


def async_to_sync(tree: ast.AST, filepath: Path | None = None) -> ast.AST:
    tree = BlanksInserter().visit(tree)
    tree = RenameAsyncToSync().visit(tree)
    tree = AsyncToSync().visit(tree)
    return tree


def tree_to_str(tree: ast.AST, filepath: Path) -> str:
    rv = f"""\
# WARNING: this file is auto-generated by '{SCRIPT_NAME}'
# from the original file '{filepath.name}'
# DO NOT CHANGE! Change the original file instead.
"""
    rv += unparse(tree)
    return rv


class AsyncToSync(ast.NodeTransformer):
    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AST:
        new_node = ast.FunctionDef(**node.__dict__)
        ast.copy_location(new_node, node)
        self.visit(new_node)
        return new_node

    def visit_AsyncFor(self, node: ast.AsyncFor) -> ast.AST:
        new_node = ast.For(**node.__dict__)
        ast.copy_location(new_node, node)
        self.visit(new_node)
        return new_node

    def visit_AsyncWith(self, node: ast.AsyncWith) -> ast.AST:
        new_node = ast.With(**node.__dict__)
        ast.copy_location(new_node, node)
        self.visit(new_node)
        return new_node

    def visit_Await(self, node: ast.Await) -> ast.AST:
        new_node = node.value
        self.visit(new_node)
        return new_node

    def visit_If(self, node: ast.If) -> ast.AST:
        # Drop `if is_async()` branch.
        #
        # Assume that the test guards an async object becoming sync and remove
        # the async side, because it will likely contain `await` constructs
        # illegal into a sync function.
        if self._is_async_call(node.test):
            for child in node.orelse:
                self.visit(child)
            return node.orelse

        # Manage `if True:  # ASYNC`
        # drop the unneeded branch
        if (stmts := self._async_test_statements(node)) is not None:
            for child in stmts:
                self.visit(child)
            return stmts

        self.generic_visit(node)
        return node

    def _is_async_call(self, test: ast.AST) -> bool:
        if not isinstance(test, ast.Call):
            return False
        if not isinstance(test.func, ast.Name):
            return False
        if test.func.id != "is_async":
            return False
        return True

    def _async_test_statements(self, node: ast.If) -> list[ast.AST] | None:
        if not (
            isinstance(node.test, ast.Constant) and isinstance(node.test.value, bool)
        ):
            return None

        if not (node.body and isinstance(node.body[0], ast.Comment)):
            return None

        comment = node.body[0].value

        if not comment.startswith("# ASYNC"):
            return None

        stmts: list[ast.AST]
        if node.test.value:
            stmts = node.orelse
        else:
            stmts = node.body[1:]  # skip the ASYNC comment
        return stmts


class RenameAsyncToSync(ast.NodeTransformer):
    names_map = {
        "ACT": "CT",
        "ACondition": "Condition",
        "AEvent": "Event",
        "ALock": "Lock",
        "AQueue": "Queue",
        "AWorker": "Worker",
        "AsyncClientCursor": "ClientCursor",
        "AsyncConnectFailedCB": "ConnectFailedCB",
        "AsyncConnection": "Connection",
        "AsyncConnectionCB": "ConnectionCB",
        "AsyncConnectionPool": "ConnectionPool",
        "AsyncCopy": "Copy",
        "AsyncCopyWriter": "CopyWriter",
        "AsyncCursor": "Cursor",
        "AsyncFileWriter": "FileWriter",
        "AsyncGenerator": "Generator",
        "AsyncIterator": "Iterator",
        "AsyncLibpqWriter": "LibpqWriter",
        "AsyncNullConnectionPool": "NullConnectionPool",
        "AsyncPipeline": "Pipeline",
        "AsyncQueuedLibpqWriter": "QueuedLibpqWriter",
        "AsyncRawCursor": "RawCursor",
        "AsyncRowFactory": "RowFactory",
        "AsyncScheduler": "Scheduler",
        "AsyncServerCursor": "ServerCursor",
        "AsyncTransaction": "Transaction",
        "AsyncWriter": "Writer",
        "__aenter__": "__enter__",
        "__aexit__": "__exit__",
        "__aiter__": "__iter__",
        "_copy_async": "_copy",
        "aclose": "close",
        "aclosing": "closing",
        "acommands": "commands",
        "aconn": "conn",
        "aconn_cls": "conn_cls",
        "agather": "gather",
        "alist": "list",
        "anext": "next",
        "apipeline": "pipeline",
        "asleep": "sleep",
        "aspawn": "spawn",
        "asynccontextmanager": "contextmanager",
        "connection_async": "connection",
        "conninfo_attempts_async": "conninfo_attempts",
        "current_task_name": "current_thread_name",
        "cursor_async": "cursor",
        "ensure_table_async": "ensure_table",
        "find_insert_problem_async": "find_insert_problem",
        "pool_async": "pool",
        "psycopg_pool.pool_async": "psycopg_pool.pool",
        "psycopg_pool.sched_async": "psycopg_pool.sched",
        "sched_async": "sched",
        "test_pool_common_async": "test_pool_common",
        "wait_async": "wait",
        "wait_conn_async": "wait_conn",
        "wait_timeout": "wait",
    }
    _skip_imports = {
        "acompat": {"alist", "anext"},
    }

    def visit_Module(self, node: ast.Module) -> ast.AST:
        self._fix_docstring(node.body)
        self.generic_visit(node)
        return node

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> ast.AST:
        self._fix_docstring(node.body)
        node.name = self.names_map.get(node.name, node.name)
        for arg in node.args.args:
            arg.arg = self.names_map.get(arg.arg, arg.arg)
        for arg in node.args.args:
            ann = arg.annotation
            if not ann:
                continue
            if isinstance(ann, ast.Subscript):
                # Remove the [] from the type
                ann = ann.value
            if isinstance(ann, ast.Attribute):
                ann.attr = self.names_map.get(ann.attr, ann.attr)

        self.generic_visit(node)
        return node

    def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.AST:
        self._fix_docstring(node.body)
        self.generic_visit(node)
        return node

    def _fix_docstring(self, body: list[ast.AST]) -> None:
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body[0].value.value = body[0].value.value.replace("Async", "")
            body[0].value.value = body[0].value.value.replace("(async", "(sync")

    def visit_Call(self, node: ast.Call) -> ast.AST:
        if isinstance(node.func, ast.Name) and node.func.id == "TypeVar":
            node = self._visit_Call_TypeVar(node)

        self.generic_visit(node)
        return node

    def _visit_Call_TypeVar(self, node: ast.Call) -> ast.AST:
        for kw in node.keywords:
            if kw.arg != "bound":
                continue
            if not isinstance(kw.value, ast.Constant):
                continue
            if not isinstance(kw.value.value, str):
                continue
            kw.value.value = self._visit_type_string(kw.value.value)

        return node

    def _visit_type_string(self, source: str) -> str:
        # Convert the string to tree, visit, and convert it back to string
        tree = ast.parse(source, type_comments=False)
        tree = async_to_sync(tree)
        rv = unparse(tree)
        return rv

    def visit_ClassDef(self, node: ast.ClassDef) -> ast.AST:
        self._fix_docstring(node.body)
        node.name = self.names_map.get(node.name, node.name)
        node = self._fix_base_params(node)
        self.generic_visit(node)
        return node

    def _fix_base_params(self, node: ast.ClassDef) -> ast.AST:
        # Handle :
        #   class AsyncCursor(BaseCursor["AsyncConnection[Any]", Row]):
        # the base cannot be a token, even with __future__ annotation.
        for base in node.bases:
            if not isinstance(base, ast.Subscript):
                continue

            if isinstance(base.slice, ast.Constant):
                if not isinstance(base.slice.value, str):
                    continue
                base.slice.value = self._visit_type_string(base.slice.value)
            elif isinstance(base.slice, ast.Tuple):
                for elt in base.slice.elts:
                    if not (
                        isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                    ):
                        continue
                    elt.value = self._visit_type_string(elt.value)

        return node

    def visit_ImportFrom(self, node: ast.ImportFrom) -> ast.AST | None:
        # Remove import of async utils eclypsing builtins
        if skips := self._skip_imports.get(node.module):
            node.names = [n for n in node.names if n.name not in skips]
            if not node.names:
                return None

        node.module = self.names_map.get(node.module, node.module)
        for n in node.names:
            n.name = self.names_map.get(n.name, n.name)
        return node

    def visit_Name(self, node: ast.Name) -> ast.AST:
        if node.id in self.names_map:
            node.id = self.names_map[node.id]
        return node

    def visit_Attribute(self, node: ast.Attribute) -> ast.AST:
        if node.attr in self.names_map:
            node.attr = self.names_map[node.attr]
        self.generic_visit(node)
        return node

    def visit_Subscript(self, node: ast.Subscript) -> ast.AST:
        # Manage AsyncGenerator[X, Y] -> Generator[X, None, Y]
        self._manage_async_generator(node)
        # # Won't result in a recursion because we change the args number
        # self.visit(node)
        # return node

        self.generic_visit(node)
        return node

    def _manage_async_generator(self, node: ast.Subscript) -> ast.AST | None:
        if not (isinstance(node.value, ast.Name) and node.value.id == "AsyncGenerator"):
            return None

        if not (isinstance(node.slice, ast.Tuple) and len(node.slice.elts) == 2):
            return None

        node.slice.elts.insert(1, deepcopy(node.slice.elts[1]))
        self.generic_visit(node)
        return node


class BlanksInserter(ast.NodeTransformer):
    """
    Restore the missing spaces in the source (or something similar)
    """

    def generic_visit(self, node: ast.AST) -> ast.AST:
        if isinstance(getattr(node, "body", None), list):
            node.body = self._inject_blanks(node.body)
        super().generic_visit(node)
        return node

    def _inject_blanks(self, body: list[ast.Node]) -> list[ast.AST]:
        if not body:
            return body

        new_body = []
        before = body[0]
        new_body.append(before)
        for i in range(1, len(body)):
            after = body[i]
            nblanks = after.lineno - before.end_lineno - 1
            if nblanks > 0:
                # Inserting one blank is enough.
                blank = ast.Comment(
                    value="",
                    inline=False,
                    lineno=before.end_lineno + 1,
                    end_lineno=before.end_lineno + 1,
                    col_offset=0,
                    end_col_offset=0,
                )
                new_body.append(blank)
            new_body.append(after)
            before = after

        return new_body


def unparse(tree: ast.AST) -> str:
    rv: str = Unparser().visit(tree)
    rv = _fix_comment_on_decorators(rv)
    return rv


def _fix_comment_on_decorators(source: str) -> str:
    """
    Re-associate comments to decorators.

    In a case like:

        1  @deco  # comment
        2  def func(x):
        3     pass

    it seems that Function lineno is 2 instead of 1 (Python 3.10). Because
    the Comment lineno is 1, it ends up printed above the function, instead
    of inline. This is a problem for '# type: ignore' comments.

    Maybe the problem could be fixed in the tree, but this solution is a
    simpler way to start.
    """
    lines = source.splitlines()

    comment_at = None
    for i, line in enumerate(lines):
        if line.lstrip().startswith("#"):
            comment_at = i
        elif not line.strip():
            pass
        elif line.lstrip().startswith("@classmethod"):
            if comment_at is not None:
                lines[i] = lines[i] + "  " + lines[comment_at].lstrip()
                lines[comment_at] = ""
        else:
            comment_at = None

    return "\n".join(lines)


class Unparser(ast._Unparser):
    """
    Try to emit long strings as multiline.

    The normal class only tries to emit docstrings as multiline,
    but the resulting source doesn't pass flake8.
    """

    # Beware: private method. Tested with in Python 3.10, 3.11.
    def _write_constant(self, value: Any) -> None:
        if isinstance(value, str) and len(value) > 50:
            self._write_str_avoiding_backslashes(value)
        else:
            super()._write_constant(value)


def parse_cmdline() -> Namespace:
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "--check", action="store_true", help="return with error in case of differences"
    )
    parser.add_argument(
        "--all", action="store_true", help="process all the files of the project"
    )
    parser.add_argument(
        "-B",
        "--convert-all",
        action="store_true",
        help="process specified files without checking last modification times",
    )
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        metavar="N",
        help=(
            "process files concurrently using at most N workers; "
            "if unspecified, the number of processors on the machine will be used"
        ),
    )
    parser.add_argument(
        "-L",
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logger level.",
    )
    container = parser.add_mutually_exclusive_group()
    container.add_argument(
        "--docker",
        action="store_const",
        const="docker",
        dest="container",
        help=f"run in a docker container with Python {PYVER}",
    )
    container.add_argument(
        "--podman",
        action="store_const",
        const="podman",
        dest="container",
        help=f"run in a podman container with Python {PYVER}",
    )
    parser.add_argument(
        "inputs",
        metavar="FILE",
        nargs="*",
        type=Path,
        help="the files to process (if --all is not specified)",
    )

    opt = parser.parse_args()
    if opt.all and opt.inputs:
        parser.error("can't specify input files and --all together")

    if opt.all:
        opt.inputs = [PROJECT_DIR / Path(fn) for fn in ALL_INPUTS]

    if not opt.inputs:
        parser.error("no input file provided")

    fp: Path
    for fp in opt.inputs:
        if not fp.is_file():
            parser.error("not a file: %s" % fp)
        if "_async" not in fp.name:
            parser.error("file should have '_async' in the name: %s" % fp)

    return opt


if __name__ == "__main__":
    sys.exit(main())
