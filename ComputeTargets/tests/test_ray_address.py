"""
Tests for the required ``--ray-address`` (read-only-plots prompt 01; README U1, U2).

Every driver that starts Ray names its cluster; ``auto`` is refused. The parser keeps the switch
optional with no default, and ``ray_address_problem`` is called just before each ``ray.init``.

No test starts Ray or opens a store. The subprocess tests run the three drivers from the
repository root; each stops at the check (or, for ``main.py --inventory``, at the reader's refusal
of an absent store) before Ray. A plot script imports its plotting stack first, so each of the
subprocesses takes about 5 to 10 s, and the class about a minute (more on a loaded machine).
Each subprocess gets ``RAY_ADDRESS=127.0.0.1:9``, an address nothing listens on, so that a
regression which got past the check would fail to connect instead of attaching to another
project's cluster. That is a guard, not the test: each test asserts the message on stderr, since
``argparse`` also exits 2.
"""

import ast
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from config.argument_parser import (
    RAY_ADDRESS_PROBLEM,
    create_argument_parser,
    ray_address_problem,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DRIVERS = ["main.py", "plot_by_beta.py", "plot_ScalarModel.py"]


class TestRayAddressProblem(unittest.TestCase):
    """``ray_address_problem`` refuses a missing address and ``auto``, and accepts the rest."""

    def test_refused(self):
        for address in [None, "", "   ", "auto", "AUTO", " AUTO ", "Auto\n"]:
            with self.subTest(address=address):
                self.assertEqual(ray_address_problem(address), RAY_ADDRESS_PROBLEM)

    def test_message_names_both_forms_and_the_reason(self):
        message = ray_address_problem("auto")
        self.assertTrue(message.startswith("!! --ray-address is required"))
        self.assertIn("127.0.0.1:6380", message)
        self.assertIn("ray_cluster.sh", message)
        self.assertIn("'local'", message)
        self.assertIn("'auto' is refused", message)
        self.assertIn("whichever cluster it finds first", message)

    def test_accepted_unchanged(self):
        for address in ["127.0.0.1:6380", "local", "ray://127.0.0.1:10001", "auto2"]:
            with self.subTest(address=address):
                self.assertIsNone(ray_address_problem(address))


class TestParser(unittest.TestCase):
    """The parser has no default; a command-line or ``--config`` value is kept."""

    def test_no_default(self):
        args = create_argument_parser().parse_args(["--database", "unused.db"])
        self.assertIsNone(args.ray_address)

    def test_command_line_value_kept(self):
        args = create_argument_parser().parse_args(
            ["--database", "unused.db", "--ray-address", "127.0.0.1:6380"]
        )
        self.assertEqual(args.ray_address, "127.0.0.1:6380")

    def test_yaml_config_supplies_it(self):
        with tempfile.TemporaryDirectory() as d:
            config = Path(d) / "run.yaml"
            config.write_text("ray-address: 127.0.0.1:6380\n")
            args = create_argument_parser().parse_args(
                ["--database", "unused.db", "--config", str(config)]
            )
        self.assertEqual(args.ray_address, "127.0.0.1:6380")


def _run(driver, *arguments, timeout=300):
    env = dict(os.environ, RAY_ADDRESS="127.0.0.1:9")
    return subprocess.run(
        [sys.executable, driver, *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )


class TestDriversBySubprocess(unittest.TestCase):
    """Each driver refuses a missing or ``auto`` address with exit 2 and the message, before Ray."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self._dir = Path(self._tmp.name)
        self._absent = self._dir / "absent.db"

    def _assert_refused(self, result):
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn(RAY_ADDRESS_PROBLEM, result.stderr)
        # the driver got past the parser: the provenance line was printed
        self.assertIn(">> packages:", result.stdout)
        self.assertNotIn("Traceback", result.stderr)
        self.assertEqual(os.listdir(self._dir), [])

    def test_plot_by_beta_without_an_address(self):
        self._assert_refused(_run("plot_by_beta.py", "--database", str(self._absent)))

    def test_plot_by_beta_with_auto(self):
        self._assert_refused(
            _run(
                "plot_by_beta.py",
                "--database",
                str(self._absent),
                "--ray-address",
                "auto",
            )
        )

    def test_plot_scalar_model_without_an_address(self):
        self._assert_refused(
            _run("plot_ScalarModel.py", "--database", str(self._absent))
        )

    def test_plot_scalar_model_with_auto(self):
        self._assert_refused(
            _run(
                "plot_ScalarModel.py",
                "--database",
                str(self._absent),
                "--ray-address",
                "auto",
            )
        )

    def test_main_without_an_address(self):
        self._assert_refused(_run("main.py", "--database", str(self._absent)))

    def test_main_with_auto(self):
        self._assert_refused(
            _run("main.py", "--database", str(self._absent), "--ray-address", "auto")
        )

    def test_main_inventory_needs_no_address(self):
        """U1: ``--inventory`` exits before Ray, so it is exempt: the reader refuses the absent
        store (exit 1), the address check is never reached."""
        result = _run("main.py", "--database", str(self._absent), "--inventory")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("!! --inventory:", result.stderr)
        self.assertNotIn("--ray-address", result.stderr)
        self.assertEqual(os.listdir(self._dir), [])


def _top_level(driver):
    return ast.parse((REPO_ROOT / driver).read_text()).body


def _is_call(node, dotted):
    """Whether ``node`` is an expression statement calling ``dotted`` (e.g. ``ray.init``)."""
    if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)):
        return False
    return ast.unparse(node.value.func) == dotted


def _calls_ray_address_problem(node):
    return any(
        isinstance(n, ast.Call) and ast.unparse(n.func) == "ray_address_problem"
        for n in ast.walk(node)
    )


class TestCheckPlacementByAst(unittest.TestCase):
    """In each driver the check is called before ``ray.init`` and exits 2 on a problem."""

    def _positions(self, driver):
        body = _top_level(driver)
        inits = [i for i, n in enumerate(body) if _is_call(n, "ray.init")]
        self.assertEqual(len(inits), 1, f"{driver}: exactly one top-level ray.init")
        checks = [
            i
            for i, n in enumerate(body)
            if isinstance(n, ast.Assign) and _calls_ray_address_problem(n)
        ]
        self.assertEqual(len(checks), 1, f"{driver}: exactly one check")
        return body, checks[0], inits[0]

    def test_check_comes_before_ray_init_and_exits_2(self):
        for driver in DRIVERS:
            with self.subTest(driver=driver):
                body, check, init = self._positions(driver)
                self.assertLess(check, init)
                # the statement after the assignment is `if problem is not None:` ending in
                # sys.exit(2), and it too comes before ray.init
                guard = body[check + 1]
                self.assertIsInstance(guard, ast.If)
                self.assertLess(check + 1, init)
                exits = [
                    n
                    for n in ast.walk(guard)
                    if isinstance(n, ast.Call)
                    and ast.unparse(n.func) == "sys.exit"
                    and [ast.unparse(a) for a in n.args] == ["2"]
                ]
                self.assertEqual(len(exits), 1)
                self.assertTrue(_is_call(guard.body[-1], "sys.exit"))

    def test_the_guard_tests_the_checks_result(self):
        for driver in DRIVERS:
            with self.subTest(driver=driver):
                body, check, _ = self._positions(driver)
                name = body[check].targets[0].id
                self.assertIn(name, ast.unparse(body[check + 1].test))

    def test_main_check_comes_after_the_inventory_branch(self):
        body, check, _ = self._positions("main.py")
        branches = [
            i
            for i, n in enumerate(body)
            if isinstance(n, ast.If) and ast.unparse(n.test) == "args.inventory"
        ]
        self.assertEqual(len(branches), 1)
        self.assertLess(branches[0], check)

    def test_main_check_comes_after_the_drop_check(self):
        body, check, _ = self._positions("main.py")
        drops = [
            i
            for i, n in enumerate(body)
            if isinstance(n, ast.Try) and "tables_to_drop" in ast.unparse(n)
        ]
        self.assertEqual(len(drops), 1)
        self.assertLess(drops[0], check)


if __name__ == "__main__":
    unittest.main()
