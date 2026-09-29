import process from "node:process";
import path from "node:path";
import { loadPyodide } from "pyodide";

const workspace = process.env.GITHUB_WORKSPACE ?? process.cwd();
const wheelDir = path.resolve(workspace, process.env.WHEEL_DIR ?? "wheelhouse");
const pytestArgs = process.argv.slice(2);

if (!pytestArgs.length) {
  throw new Error("pass pytest arguments after the runner filename");
}

const env = {
  HOME: "/home/pyodide",
  PSYCOPG_IMPL: process.env.PSYCOPG_IMPL ?? "binary",
  PSYCOPG_TEST_DSN: process.env.PSYCOPG_TEST_DSN ?? "",
  PYTEST_ARGS_JSON: JSON.stringify(pytestArgs),
};

const pyodide = await loadPyodide({ env });

await pyodide.useNodeSockFS();
pyodide.mountNodeFS("/workspace", workspace);
pyodide.mountNodeFS("/wheelhouse", wheelDir);
await pyodide.loadPackage(["micropip", "packaging", "pytest"]);

await pyodide.runPythonAsync(`
import os
from pathlib import Path

import micropip
from packaging.utils import parse_wheel_filename

wheel_dir = Path("/wheelhouse")
wheels = sorted({
    *wheel_dir.glob("*-py3-none-any.whl"),
    *wheel_dir.glob("*-*-*-pyemscripten_*_wasm32.whl"),
})

expected_projects = {"psycopg", "psycopg-binary", "psycopg-pool"}
projects = {}
for wheel in wheels:
    name, _, _, _ = parse_wheel_filename(wheel.name)
    project = str(name)
    if project in projects:
        raise RuntimeError(f"duplicate wheel for {project}: {projects[project]}, {wheel}")
    projects[project] = wheel

if set(projects) != expected_projects:
    missing = sorted(expected_projects - set(projects))
    unexpected = sorted(set(projects) - expected_projects)
    raise RuntimeError(f"unexpected wheel set: missing={missing}, unexpected={unexpected}")

local_requirements = []
for project, wheel in sorted(projects.items()):
    local_requirements.append(f"{project} @ emfs:{wheel}")

await micropip.install(
    local_requirements,
    pre=True,
)
`);

pyodide.runPython(`
import json
import os

def run_pytest():
    os.chdir("/workspace")
    import pytest
    return int(pytest.main(json.loads(os.environ["PYTEST_ARGS_JSON"])))
`);

const runPytest = pyodide.globals.get("run_pytest");
let exitCode;

try {
  exitCode = Number(await runPytest.callPromising());
} finally {
  runPytest.destroy();
}

process.exit(exitCode);
