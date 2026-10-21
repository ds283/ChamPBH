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

"""What the source says, read with `ast` (rrk-adoption prompt 03, W6 tests 7 and 8).

- **`main.py` runs without RunRegistryKit** (campaign README §6.0, R2): none of `main.py`,
  `plot_by_beta.py`, `plot_ScalarHistory.py` and `utilities.py` imports `runregistrykit` or
  `RunDrivers`. Byte-identity since `3cd2be4` is the log's and the review's `git diff`, not a test,
  so that a later campaign's legitimate edit to `main.py` does not fail it (the orchestrator's note
  for 03, ruling 9).
- **The wrapper's `run.finish(` calls** (SecondaryGWKit's `test_finish_calls.py`): those inside a
  signal handler pass no `fingerprint=True`, and those after the program pass it (P6).
- **`begin`'s two handlers**, in order: `runregistrykit.ConfigError` re-raised, then any other
  `RuntimeError` made a `Refusal` from the error (SecondaryGWKit's `test_clean_refusals.py`, item 2).
- **The `ray.init` wrapper** calls the real `ray.init` with its arguments unchanged, and installs
  the handler after it (P5).
- **`NOTIFY_FLOOR` is `main.py`'s `MIN_NOTIFY_INTERVAL`**, which `heartbeat_means` states.

Nothing is imported from the files read, and nothing is run.
"""

import ast
import unittest
from pathlib import Path

import ray

from RunDrivers import registered_run
from RunDrivers.tests.stubbed_run import REPO_ROOT, WRAPPER

DRIVERS = ("main.py", "plot_by_beta.py", "plot_ScalarHistory.py", "utilities.py")
FORBIDDEN = ("runregistrykit", "RunDrivers")

# the wrapper's signal handlers, by name: `terminal`, installed by `register` before ray.init, and
# `make_killed_handler`'s `killed_handler`, installed after it
_HANDLERS = ("terminal", "killed_handler")


def _tree(path):
    return ast.parse(Path(path).read_text(), filename=str(path))


def _function(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"no function {name}")


def _called_name(call):
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _calls(node, name):
    return [
        n for n in ast.walk(node) if isinstance(n, ast.Call) and _called_name(n) == name
    ]


def _imported_modules(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module is not None:
                yield node.module


class SourceCase(unittest.TestCase):
    def tearDown(self):
        self.assertFalse(ray.is_initialized())


def _finish_calls(path):
    """``[(inside a signal handler, passes fingerprint=True)]`` for every ``run.finish(``."""
    found = []

    def visit(node, inside):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            inside = inside or node.name in _HANDLERS
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "finish"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "run"
        ):
            passes = any(
                k.arg == "fingerprint"
                and isinstance(k.value, ast.Constant)
                and k.value.value is True
                for k in node.keywords
            )
            found.append((inside, passes))
        for child in ast.iter_child_nodes(node):
            visit(child, inside)

    visit(_tree(path), False)
    return found


class TestMainPyRunsWithoutTheRegistry(SourceCase):
    def test_no_driver_imports_the_registry_or_the_wrapper(self):
        for name in DRIVERS:
            with self.subTest(name):
                modules = list(_imported_modules(_tree(REPO_ROOT / name)))
                self.assertTrue(
                    modules, f"{name} imports nothing: the reading is wrong"
                )
                for module in modules:
                    for forbidden in FORBIDDEN:
                        self.assertFalse(
                            module == forbidden or module.startswith(forbidden + "."),
                            f"{name} imports {module}",
                        )


class TestTheFinishCalls(SourceCase):
    def test_handlers_pass_no_fingerprint_and_every_later_finish_passes_it(self):
        calls = _finish_calls(WRAPPER)
        outside = [passes for inside, passes in calls if not inside]
        inside = [passes for inside, passes in calls if inside]
        # finish_after_exec: done, terminal_state, failed and killed; terminal and killed_handler
        self.assertEqual((len(outside), len(inside)), (4, 2))
        self.assertTrue(all(outside), outside)
        self.assertFalse(any(inside), inside)


def _is_begin_call(node):
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "begin"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "runregistrykit"
    )


class TestBeginsHandlers(SourceCase):
    def test_the_one_begin_call_has_the_two_handlers_in_order(self):
        tree = _tree(WRAPPER)
        self.assertEqual(sum(_is_begin_call(n) for n in ast.walk(tree)), 1)
        trys = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Try)
            and any(_is_begin_call(n) for stmt in node.body for n in ast.walk(stmt))
        ]
        self.assertEqual(len(trys), 1)
        node = trys[0]
        self.assertEqual(
            [ast.unparse(h.type) for h in node.handlers],
            ["runregistrykit.ConfigError", "RuntimeError"],
        )
        config, runtime = node.handlers
        self.assertEqual(len(config.body), 1)
        self.assertIsInstance(config.body[0], ast.Raise)
        self.assertIsNone(config.body[0].exc, "not a bare raise")
        raised = runtime.body[0]
        self.assertIsInstance(raised, ast.Raise)
        self.assertEqual(ast.unparse(raised.exc), f"Refusal(str({runtime.name}))")
        self.assertEqual(ast.unparse(raised.cause), runtime.name)


class TestTheRayInitWrapper(SourceCase):
    def test_it_passes_the_arguments_unchanged_then_installs_the_handler(self):
        wrapper = _function(_function(_tree(WRAPPER), "main"), "_registered_ray_init")
        body = wrapper.body
        real = [i for i, stmt in enumerate(body) if _calls(stmt, "_real_ray_init")]
        install = [
            i for i, stmt in enumerate(body) if _calls(stmt, "install_killed_handler")
        ]
        self.assertEqual(len(real), 1)
        self.assertEqual(len(install), 1, "the wrapper does not install the handler")
        self.assertGreater(install[0], real[0], "installed before ray.init")
        call = _calls(body[real[0]], "_real_ray_init")[0]
        self.assertEqual(
            [ast.unparse(a) for a in call.args]
            + [ast.unparse(k) for k in call.keywords],
            ["*init_args", "**init_kwargs"],
        )
        installed = _calls(body[install[0]], "install_killed_handler")[0]
        self.assertEqual([ast.unparse(a) for a in installed.args], ["run", "latch"])

    def test_the_module_imports_nothing_of_ray_at_module_scope(self):
        for node in _tree(WRAPPER).body:
            if isinstance(node, ast.Import):
                self.assertNotIn("ray", [a.name.split(".")[0] for a in node.names])
            if isinstance(node, ast.ImportFrom):
                self.assertNotEqual((node.module or "").split(".")[0], "ray")


class TestTheNotifyFloor(SourceCase):
    def test_it_is_main_pys_min_notify_interval(self):
        values = [
            node.value
            for node in _tree(REPO_ROOT / "main.py").body
            if isinstance(node, ast.Assign)
            and [ast.unparse(t) for t in node.targets] == ["MIN_NOTIFY_INTERVAL"]
        ]
        self.assertEqual(len(values), 1)
        floor = eval(compile(ast.Expression(values[0]), "main.py", "eval"), {})
        self.assertEqual(floor, registered_run.NOTIFY_FLOOR)
        self.assertEqual(floor, 300)


if __name__ == "__main__":
    unittest.main()
