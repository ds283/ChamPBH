# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Every Ray start of ChamPBH runs in Ray's token mode, named (ray-sqla-align prompt 01, items
T1-T3; after StochasticInstantons' tests/test_ray_auth.py and SecondaryGWKit's
RunDrivers/tests/test_ray_auth_mode.py).

What this pins, and why:
- `config/ray_auth.py` sets `RAY_AUTH_MODE=token` at import unless the environment already holds
  a value. Since Ray 2.59 a driver attaching to an explicit address never turns token mode on by
  itself, while `ray start --head` does whenever `~/.ray/auth_token` exists, so without the module
  ChamPBH's cluster refuses its drivers (ray-sqla-align 01, V0).
- Ray reads the mode into its config when `ray` is imported, and the drivers import `ray` first
  (as does `RunDrivers/registered_run.py`, which runs `main.py` in-process), so the module
  refreshes Ray's config when `ray` is already imported. Ray's own mode is read in a fresh
  interpreter in both import orders: without the refresh, the drivers' order keeps `disabled`
  (SecondaryGWKit's issue
  [02-the-token-mode-setting-comes-after-import-ray-so-every-ray-start-fails]).
- Each of `main.py`, `plot_by_beta.py` and `plot_ScalarModel.py` imports the module at module
  level, before the top-level statement holding its `ray.init`. Read by `ast`; one test per driver,
  so that a failure names the file.
- `ray_cluster.sh` exports the mode, defaulting to `token`, before its one `ray start`. Read as
  text.

No Ray instance is started: each fresh interpreter imports `ray` (a second or two) and reads its
config. Each runs from the repository root with this process's environment less `RAY_AUTH_MODE`,
plus what the test sets; this process's own environment is not changed, the real HOME is not
touched, and nothing is written.
"""

import ast
import os
import re
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RAY_CLUSTER_SH = (
    ROOT / ".documents" / "dk-and-rpk-migration" / "rehearsal" / "ray_cluster.sh"
)
TIMEOUT = 120  # seconds: a hang fails a test instead of blocking the suite


def _fresh(program, environment=None):
    """The last line a fresh interpreter prints running PROGRAM from the repository root, with
    `RAY_AUTH_MODE` removed from this process's environment and ENVIRONMENT added."""
    env = {k: v for k, v in os.environ.items() if k != "RAY_AUTH_MODE"}
    env.update(environment or {})
    env["PYTHONPATH"] = str(ROOT)
    done = subprocess.run(
        [sys.executable, "-c", program],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=TIMEOUT,
    )
    return done.stdout.strip().splitlines()[-1]


MODE_PROBE = "import os, config.ray_auth; print(os.environ.get('RAY_AUTH_MODE'))"

RAY_PROBE = (
    "{first}\n"
    "{second}\n"
    "from ray._raylet import AuthenticationMode, get_authentication_mode\n"
    "print(get_authentication_mode() == AuthenticationMode.TOKEN)\n"
)


def _ray_sees_token_mode(first, second, environment=None):
    """Whether Ray's own config is in token mode after the two imports, in a fresh interpreter.
    `get_authentication_mode` reads the config a driver would start with; nothing is started.
    """
    return _fresh(RAY_PROBE.format(first=first, second=second), environment) == "True"


def _imports_ray_auth(node):
    return isinstance(node, ast.Import) and any(
        alias.name == "config.ray_auth" for alias in node.names
    )


def _calls_ray_init(node):
    return any(
        isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "init"
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == "ray"
        for n in ast.walk(node)
    )


class TestTheModuleSetsTheMode(unittest.TestCase):
    def test_an_unset_mode_becomes_token(self):
        """With nothing set, Ray would choose the mode itself, by how it is started."""
        self.assertEqual(_fresh(MODE_PROBE), "token")

    def test_a_mode_already_set_is_left_as_it_is(self):
        """A person's explicit choice in the environment wins over ChamPBH's."""
        self.assertEqual(_fresh(MODE_PROBE, {"RAY_AUTH_MODE": "disabled"}), "disabled")


class TestRaySeesTheMode(unittest.TestCase):
    def test_ray_imported_first(self):
        """The order of the three drivers and registered_run.py: `ray`, then the module."""
        self.assertTrue(_ray_sees_token_mode("import ray", "import config.ray_auth"))

    def test_the_module_imported_first(self):
        self.assertTrue(_ray_sees_token_mode("import config.ray_auth", "import ray"))

    def test_a_mode_already_set_is_what_ray_sees(self):
        """With `disabled` already set, Ray is not in token mode, whatever the module does."""
        self.assertFalse(
            _ray_sees_token_mode(
                "import ray", "import config.ray_auth", {"RAY_AUTH_MODE": "disabled"}
            )
        )


class TestEachDriverImportsIt(unittest.TestCase):
    """A driver that does not import the module attaches in `disabled` mode and is refused."""

    def _check(self, name):
        tree = ast.parse((ROOT / name).read_text(), filename=name)
        imports = [node.lineno for node in tree.body if _imports_ray_auth(node)]
        self.assertTrue(
            imports, f"{name} does not import config.ray_auth at module level"
        )
        holders = [node.lineno for node in tree.body if _calls_ray_init(node)]
        self.assertTrue(holders, f"{name} has no top-level statement holding ray.init")
        self.assertLess(
            min(imports),
            min(holders),
            f"{name} imports config.ray_auth after the statement holding its ray.init",
        )

    def test_main_py_imports_it_before_ray_init(self):
        self._check("main.py")

    def test_plot_by_beta_py_imports_it_before_ray_init(self):
        self._check("plot_by_beta.py")

    def test_plot_ScalarModel_py_imports_it_before_ray_init(self):
        self._check("plot_ScalarModel.py")


class TestTheClusterScript(unittest.TestCase):
    def test_ray_cluster_sh_exports_the_mode_before_ray_start(self):
        """Without it, `ray start` infers token mode from the token file rather than being told."""
        lines = RAY_CLUSTER_SH.read_text().splitlines()
        exports = [
            i
            for i, line in enumerate(lines)
            if line.strip() == 'export RAY_AUTH_MODE="${RAY_AUTH_MODE:-token}"'
        ]
        starts = [
            i
            for i, line in enumerate(lines)
            if re.match(r'\s*"\$RAY_BIN" start\b', line)
        ]
        self.assertTrue(
            exports,
            "ray_cluster.sh does not export RAY_AUTH_MODE with the :-token default",
        )
        self.assertEqual(
            len(starts), 1, "ray_cluster.sh should have one ray start line"
        )
        self.assertLess(
            min(exports),
            starts[0],
            "ray_cluster.sh exports RAY_AUTH_MODE after its ray start",
        )


if __name__ == "__main__":
    unittest.main()
