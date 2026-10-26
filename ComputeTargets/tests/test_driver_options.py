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
main.py's options after jordan-normalization prompt 08 (D1, D7), through the shared parser
(`config/argument_parser.py`), and the driver's use of them, by `ast`.

(a) every option jordan-normalization removed is refused by name, exit 2, with a message naming
its replacement: on the command line, and as a key of a `--config` YAML file;
(b) `--tuning-tolerance`, `--tuning-max-integrations` and `--n-values`: their defaults are
`ComputeTargets.ScalarHistory`'s constants and [1], a value given replaces the default (never
extends it), and a value that is not positive is refused;
(c) `exponential.yaml` parses, and gives neither the potential type nor the redshift grid's extent;
(d) main.py builds its models over (M, n, beta) from `args.n_values`, its tuning tolerance from
`args.tuning_tolerance`, passes `args.tuning_max_integrations` to `ScalarHistory.compute`, and
no driver reads `args.potential_type`; `ScalarHistory.compute` passes its cap to the task.

No Ray cluster, no datastore, no integration. Instant.
"""

import ast
import contextlib
import inspect
import io
import tempfile
import textwrap
import unittest
from pathlib import Path

from ComputeTargets.ScalarHistory import (
    DEFAULT_TUNING_MAX_INTEGRATIONS,
    DEFAULT_TUNING_TOLERANCE,
    ScalarHistory,
)
from config import argument_parser
from config.argument_parser import REMOVED_OPTIONS, create_argument_parser

ROOT = Path(__file__).resolve().parents[2]
BASE = ["--database", "unused.db"]

# each removed option, a value a run script of before the campaign gave it, and the replacement
# its message must name
REMOVED = {
    "--log10-Lambda-low-eV": ("-2", "--tuning-tolerance"),
    "--log10-Lambda-high-eV": ("1", "--tuning-tolerance"),
    "--samples-per-log10-Lambda-eV": ("6", "--tuning-tolerance"),
    "--Lambda-values-eV": ("1e-3", "--tuning-tolerance"),
    "--Lambda-values-Mp": ("1e-30", "--tuning-tolerance"),
    "--T-stop-GeV": ("1e-3", "--fixed-normalization"),
    "--potential-type": ("Exponential", "--n-values"),
}


def _parse(argv):
    """(namespace, None) for a parse that succeeds, (None, (exit code, stderr)) for a refusal."""
    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            return create_argument_parser().parse_args(argv), None
    except SystemExit as e:
        return None, (e.code, err.getvalue())


class TestRemovedOptions(unittest.TestCase):
    def test_a_every_removed_option_is_known(self):
        """(a) the refused set: the Lambda options, --T-stop-GeV and --potential-type."""
        self.assertEqual(set(REMOVED_OPTIONS), set(REMOVED))
        self.assertFalse(hasattr(argument_parser, "potential_types"))

    def test_a_refused_on_the_command_line_with_the_replacement_named(self):
        for option, (value, replacement) in REMOVED.items():
            with self.subTest(option=option):
                args, refusal = _parse(BASE + [option, value])
                self.assertIsNone(args)
                code, text = refusal
                self.assertEqual(code, 2)
                prompt = REMOVED_OPTIONS[option][0]
                self.assertIn(
                    f"error: {option} was removed by jordan-normalization prompt {prompt}: ",
                    text,
                )
                self.assertIn(replacement, text)
                self.assertNotIn("unrecognized arguments", text)

    def test_a_refused_with_no_value_too(self):
        args, refusal = _parse(BASE + ["--potential-type"])
        self.assertIsNone(args)
        self.assertEqual(refusal[0], 2)
        self.assertIn("--potential-type was removed", refusal[1])

    def test_a_refused_as_a_key_of_a_config_file(self):
        """A --config YAML file of before the campaign (potential type, Lambda) is refused."""
        for key, value, option in (
            ("potential-type", '"Exponential"', "--potential-type"),
            ("log10-Lambda-low-eV", "-2", "--log10-Lambda-low-eV"),
            ("T-stop-GeV", "1e-3", "--T-stop-GeV"),
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "old.yaml"
                path.write_text(f"{key}: {value}\nshards: 16\n")
                args, refusal = _parse(BASE + ["--config", str(path)])
                self.assertIsNone(args)
                self.assertEqual(refusal[0], 2)
                self.assertIn(
                    f"{option} was removed by jordan-normalization", refusal[1]
                )

    def test_a_hidden_from_the_help(self):
        text = create_argument_parser().format_help()
        for option in REMOVED:
            self.assertNotIn(option, text)


class TestNewOptions(unittest.TestCase):
    def test_b_the_defaults(self):
        args, refusal = _parse(BASE)
        self.assertIsNone(refusal)
        self.assertEqual(args.tuning_tolerance, DEFAULT_TUNING_TOLERANCE)
        self.assertEqual(args.tuning_max_integrations, DEFAULT_TUNING_MAX_INTEGRATIONS)
        self.assertEqual(args.n_values, [1])

    def test_b_given_values_replace_the_defaults(self):
        args, _ = _parse(
            BASE
            + [
                "--tuning-tolerance",
                "1e-6",
                "--tuning-max-integrations",
                "5",
                "--n-values",
                "2",
            ]
        )
        self.assertEqual(args.tuning_tolerance, 1e-6)
        self.assertEqual(args.tuning_max_integrations, 5)
        # not [1, 2]: the default is replaced, never extended
        self.assertEqual(args.n_values, [2])
        args, _ = _parse(BASE + ["--n-values", "1", "3"])
        self.assertEqual(args.n_values, [1, 3])

    def test_b_values_that_are_not_positive_are_refused(self):
        for argv in (
            ["--tuning-tolerance", "0"],
            ["--tuning-tolerance", "-1e-5"],
            ["--tuning-max-integrations", "0"],
            ["--n-values", "0"],
            ["--n-values", "1.5"],
        ):
            with self.subTest(argv=argv):
                args, refusal = _parse(BASE + argv)
                self.assertIsNone(args)
                self.assertEqual(refusal[0], 2)
                self.assertIn(f"argument {argv[0]}", refusal[1])

    def test_b_the_help_states_the_defaults_and_the_key(self):
        text = " ".join(create_argument_parser().format_help().split())
        self.assertIn("(default 1e-05)", text)
        self.assertIn("(default 30)", text)
        self.assertIn("(default [1];", text)
        self.assertIn("It is a key of every stored history", text)
        self.assertIn("It is not a key of the history", text)
        self.assertIn("does not recompute it", text)


class TestTheConfigFile(unittest.TestCase):
    def test_c_exponential_yaml(self):
        text = (ROOT / "exponential.yaml").read_text()
        self.assertNotIn("potential-type", text)
        self.assertNotIn("log10-one-plus-z-high", text)
        args, refusal = _parse(BASE + ["--config", str(ROOT / "exponential.yaml")])
        self.assertIsNone(refusal)
        self.assertEqual(
            args.log10_one_plus_z_high, argument_parser.DEFAULT_LOG10_ONE_PLUS_Z_HIGH
        )
        self.assertEqual(args.n_values, [1])
        self.assertEqual(args.shards, 16)


def _tree(name):
    return ast.parse((ROOT / name).read_text(), name)


class TestTheDriver(unittest.TestCase):
    def test_d_no_driver_reads_the_potential_type(self):
        for name in ("main.py", "plot_by_beta.py", "plot_ScalarHistory.py"):
            with self.subTest(script=name):
                reads = [
                    n
                    for n in ast.walk(_tree(name))
                    if isinstance(n, ast.Attribute) and n.attr == "potential_type"
                ]
                self.assertEqual(reads, [])
                self.assertNotIn("DEFAULT_TUNING_TOLERANCE", (ROOT / name).read_text())

    def test_d_main_builds_its_grid_and_tuning_from_the_options(self):
        source = (ROOT / "main.py").read_text()
        tree = _tree("main.py")
        calls = {ast.unparse(n) for n in ast.walk(tree) if isinstance(n, ast.Call)}
        self.assertIn("itertools.product(M_grid, n_values, beta_grid)", calls)
        self.assertIn(
            "m.compute(label=label, max_integrations=args.tuning_max_integrations)",
            calls,
        )
        self.assertIn(
            "pool.object_get('tolerance', tol=args.tuning_tolerance)",
            calls,
        )
        self.assertIn("n_values: List[int] = args.n_values", source)
        self.assertIn(
            "warn_super_planckian(Model_array, phi_init, T_init, units, "
            "model_data['cosmology'])",
            calls,
        )

    def test_d_the_plot_scripts_match_n_and_the_tuning_tolerance(self):
        for name in ("plot_by_beta.py", "plot_ScalarHistory.py"):
            with self.subTest(script=name):
                calls = [
                    n
                    for n in ast.walk(_tree(name))
                    if isinstance(n, ast.Call)
                    and ast.unparse(n.func)
                    in ("plot_inputs.exponential_models", "plot_inputs.parameters")
                ]
                text = {ast.unparse(n.func): ast.unparse(n) for n in calls}
                self.assertIn(
                    "M_table, beta_table, args.n_values, units",
                    text["plot_inputs.exponential_models"],
                )
                self.assertIn(
                    "tuning_tol=args.tuning_tolerance", text["plot_inputs.parameters"]
                )
                self.assertNotIn("tuning_max_integrations", (ROOT / name).read_text())

    def test_d_compute_passes_its_cap_to_the_task(self):
        signature = inspect.signature(ScalarHistory.compute)
        self.assertEqual(
            signature.parameters["max_integrations"].default,
            DEFAULT_TUNING_MAX_INTEGRATIONS,
        )
        tree = ast.parse(textwrap.dedent(inspect.getsource(ScalarHistory.compute)))
        remote = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Call)
            and ast.unparse(n.func) == "compute_scalar_history.remote"
        ]
        self.assertEqual(len(remote), 1)
        keywords = {kw.arg: ast.unparse(kw.value) for kw in remote[0].keywords}
        self.assertEqual(keywords["max_integrations"], "max_integrations")
        self.assertEqual(keywords["tolerance"], "self._tuning_tol.tol")


if __name__ == "__main__":
    unittest.main()
