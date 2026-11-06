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
`--alpha-values` and where the drivers carry it (inelastic-reflection prompt 10, tests 1, 2 and 4).

(1) the option: the default is [0.0], a given list replaces it (never extends it), the YAML key
    `alpha-values` gives the same list, -0.1, 1.0, 1, 2, nan and inf are refused at parse (exit 2,
    the option named, before any Ray), `-0` is 0.0, and `distinct_values` collapses duplicates in
    the order of first appearance;
(2) `main.py`, by `ast`: one `alpha_value` lookup per value of the deduplicated `args.alpha_values`,
    `run_pipeline` called inside a loop over those rows with the loop's own variable, and no
    literal `value=0.0` lookup of `alpha_value` left in any driver or in `plot_inputs.py`; the two
    plot scripts pass the same list to `plot_inputs.parameters` and loop over the rows;
(4) the paths: `alpha_path` gives `alpha=0`, `alpha=0.1`, `alpha=0.03` (and `alpha=0` for `-0` as
    parsed), and both plot scripts build their per-alpha paths through it, with the row's own value;
    every call that titles a figure with the cosmology passes `alpha=` (ruling 2).

No Ray cluster, no datastore, no integration. Instant.
"""

import ast
import contextlib
import io
import math
import tempfile
import unittest
from pathlib import Path

from config.argument_parser import (
    alpha_fraction,
    create_argument_parser,
    distinct_values,
)
from extract_common import alpha_path

ROOT = Path(__file__).resolve().parents[2]
BASE = ["--database", "unused.db"]
DRIVERS = ("main.py", "plot_by_beta.py", "plot_ScalarHistory.py")


def _parse(argv):
    """(namespace, None) for a parse that succeeds, (None, (exit code, stderr)) for a refusal."""
    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            return create_argument_parser().parse_args(argv), None
    except SystemExit as e:
        return None, (e.code, err.getvalue())


def _tree(name):
    return ast.parse((ROOT / name).read_text(), name)


class TestTheOption(unittest.TestCase):
    def test_1_the_default_is_zero_and_a_given_list_replaces_it(self):
        args, refusal = _parse(BASE)
        self.assertIsNone(refusal)
        self.assertEqual(args.alpha_values, [0.0])
        args, _ = _parse(BASE + ["--alpha-values", "0.1"])
        # not [0.0, 0.1]: the default is replaced, never extended (as --n-values)
        self.assertEqual(args.alpha_values, [0.1])
        args, _ = _parse(BASE + ["--alpha-values", "0.03", "0", "0.1"])
        self.assertEqual(args.alpha_values, [0.03, 0.0, 0.1])

    def test_1_the_yaml_key_gives_the_same_list(self):
        for text in (
            "alpha-values: [0.03, 0.1]\n",
            "alpha-values:\n  - 0.03\n  - 0.1\n",
        ):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "alpha.yaml"
                path.write_text(text)
                args, refusal = _parse(BASE + ["--config", str(path)])
                self.assertIsNone(refusal)
                self.assertEqual(args.alpha_values, [0.03, 0.1])
        # and the command line wins over the file, as for any option
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "alpha.yaml"
            path.write_text("alpha-values: [0.03, 0.1]\n")
            args, _ = _parse(BASE + ["--config", str(path), "--alpha-values", "0.5"])
            self.assertEqual(args.alpha_values, [0.5])

    def test_1_a_value_out_of_range_is_refused_at_parse_naming_the_option(self):
        for value in ("-0.1", "1.0", "1", "2", "nan", "inf"):
            with self.subTest(value=value):
                args, refusal = _parse(BASE + ["--alpha-values", value])
                self.assertIsNone(args)
                code, text = refusal
                self.assertEqual(code, 2)
                self.assertIn("argument --alpha-values: must be a fraction", text)
                self.assertIn("0 <= alpha < 1", text)
        # one bad value among good ones refuses the whole list
        args, refusal = _parse(BASE + ["--alpha-values", "0.1", "1.0"])
        self.assertIsNone(args)
        self.assertEqual(refusal[0], 2)

    def test_1_minus_zero_is_zero(self):
        value = alpha_fraction("-0")
        self.assertEqual(value, 0.0)
        self.assertEqual(math.copysign(1.0, value), 1.0)
        args, _ = _parse(BASE + ["--alpha-values", "-0"])
        self.assertEqual(math.copysign(1.0, args.alpha_values[0]), 1.0)
        # so the row, and the directory built from it, have one spelling of zero (ruling 9)
        self.assertEqual(
            alpha_path("out", args.alpha_values[0], "fixed"), Path("out/alpha=0")
        )

    def test_1_duplicates_collapse_in_the_order_of_first_appearance(self):
        self.assertEqual(distinct_values([0.1, 0.0, 0.1, 0.03, 0.0]), [0.1, 0.0, 0.03])
        self.assertEqual(distinct_values([]), [])
        args, _ = _parse(BASE + ["--alpha-values", "0.1", "0", "0.1", "-0"])
        self.assertEqual(args.alpha_values, [0.1, 0.0, 0.1, 0.0])
        self.assertEqual(distinct_values(args.alpha_values), [0.1, 0.0])

    def test_1_the_help_says_what_alpha_is(self):
        text = " ".join(create_argument_parser().format_help().split())
        self.assertIn("--alpha-values ALPHA_VALUES [ALPHA_VALUES ...]", text)
        self.assertIn("kinetic energy", text)
        self.assertIn("a key of every stored history", text)
        self.assertIn("0 is natural mode", text)
        self.assertIn("(the default [0.0])", text)


def _alpha_lookups(tree):
    """The `<pool>.object_get('alpha_value', ...)` calls of a module."""
    return [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "object_get"
        and n.args
        and isinstance(n.args[0], ast.Constant)
        and n.args[0].value == "alpha_value"
    ]


def _assigned(tree, name):
    return [
        ast.unparse(n.value)
        for n in ast.walk(tree)
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == name for t in n.targets)
    ]


def _loops_over(tree, iterable):
    return [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.For) and ast.unparse(n.iter) == iterable
    ]


class TestTheDriversCarryAlpha(unittest.TestCase):
    def test_2_main_looks_up_one_row_per_value_of_the_list(self):
        tree = _tree("main.py")
        self.assertEqual(
            _assigned(tree, "alpha_values"), ["distinct_values(args.alpha_values)"]
        )
        # one-loop-drain prompt 04: the fixed rows, with no model named, then one row per A under the
        # constant (test_a_values_option pins the second)
        lookups = _alpha_lookups(tree)
        self.assertEqual(len(lookups), 2)
        self.assertEqual(
            [(k.arg, ast.unparse(k.value)) for k in lookups[0].keywords],
            [("value", "a")],
        )
        # inside a comprehension over the deduplicated list, in the tolerances' ray.get
        comprehensions = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.ListComp)
            and any(c is lookups[0] for c in ast.walk(n.elt))
        ]
        self.assertEqual(len(comprehensions), 1)
        self.assertEqual(
            ast.unparse(comprehensions[0].generators[0].iter), "alpha_values"
        )
        self.assertIn("*alpha_rows", ast.unparse(tree))

    def test_2_every_pipeline_is_called_once_per_row_with_the_loops_own_row(self):
        for name in DRIVERS:
            with self.subTest(script=name):
                tree = _tree(name)
                loops = _loops_over(tree, "alpha_rows")
                self.assertEqual(len(loops), 1)
                loop = loops[0]
                calls = [
                    n
                    for n in ast.walk(loop)
                    if isinstance(n, ast.Call) and ast.unparse(n.func) == "run_pipeline"
                ]
                self.assertEqual(len(calls), 1)
                self.assertIn(loop.target.id, [ast.unparse(a) for a in calls[0].args])
                self.assertNotIn("alpha_rows[", ast.unparse(tree))
                # the loop sits inside the loop over the cosmologies, after any per-cosmology work
                outer = [
                    n
                    for n in ast.walk(tree)
                    if isinstance(n, ast.For)
                    and ast.unparse(n.iter) in ("model_list",)
                    and any(c is loop for c in ast.walk(n))
                ]
                self.assertEqual(len(outer), 1)

    def test_2_the_plot_scripts_pass_the_deduplicated_list_to_plot_inputs(self):
        for name in ("plot_by_beta.py", "plot_ScalarHistory.py"):
            with self.subTest(script=name):
                tree = _tree(name)
                self.assertEqual(
                    _assigned(tree, "alpha_values"),
                    ["distinct_values(args.alpha_values)"],
                )
                calls = [
                    n
                    for n in ast.walk(tree)
                    if isinstance(n, ast.Call)
                    and ast.unparse(n.func) == "plot_inputs.parameters"
                ]
                self.assertEqual(len(calls), 1)
                self.assertIn("alpha_values=alpha_values", ast.unparse(calls[0]))
                # no lookup of an alpha row in a plot script: plot_inputs.parameters is the one
                self.assertEqual(_alpha_lookups(tree), [])

    def test_2_no_driver_looks_up_a_literal_alpha_of_zero(self):
        for name in (*DRIVERS, "plot_inputs.py"):
            with self.subTest(script=name):
                for call in _alpha_lookups(_tree(name)):
                    for k in call.keywords:
                        self.assertFalse(
                            k.arg == "value" and isinstance(k.value, ast.Constant),
                            f"{name}: {ast.unparse(call)}",
                        )
                self.assertNotIn('alpha_value", value=0.0', (ROOT / name).read_text())


class TestThePaths(unittest.TestCase):
    def test_4_alpha_path_names_the_value(self):
        # the fixed model, whose paths are those of before one-loop-drain prompt 04 (the model has no default)
        self.assertEqual(
            alpha_path("out/label", 0.0, "fixed"), Path("out/label/alpha=0")
        )
        self.assertEqual(
            alpha_path("out/label", 0.1, "fixed"), Path("out/label/alpha=0.1")
        )
        self.assertEqual(
            alpha_path("out/label", 0.03, "fixed"), Path("out/label/alpha=0.03")
        )
        self.assertEqual(
            alpha_path(Path("out"), 0.123456789, "fixed"), Path("out/alpha=0.12346")
        )
        # alpha = 0 is a level of its own, never the base
        self.assertNotEqual(alpha_path("out", 0.0, "fixed"), Path("out"))

    def test_4_both_scripts_build_their_per_alpha_paths_through_it(self):
        expected = {
            "plot_by_beta.py": {
                ("alpha", "drain_model"),
                ("alpha.value", "alpha.drain_model"),
            },
            "plot_ScalarHistory.py": {("model.alpha.value", "model.alpha.drain_model")},
        }
        for name, values in expected.items():
            with self.subTest(script=name):
                calls = [
                    n
                    for n in ast.walk(_tree(name))
                    if isinstance(n, ast.Call) and ast.unparse(n.func) == "alpha_path"
                ]
                self.assertGreaterEqual(len(calls), 1)
                # the row's value, never the float the user typed (ruling 3)
                # and its model, which has no default (one-loop-drain prompt 04, ruling 3)
                for call in calls:
                    self.assertEqual(len(call.args), 3)
                    self.assertIn(
                        (ast.unparse(call.args[1]), ast.unparse(call.args[2])), values
                    )
        # plot_by_beta writes figure 2 and the per-group products through it, in two places
        calls = [
            n
            for n in ast.walk(_tree("plot_by_beta.py"))
            if isinstance(n, ast.Call) and ast.unparse(n.func) == "alpha_path"
        ]
        self.assertEqual(len(calls), 2)

    def test_4_every_figure_titled_with_the_cosmology_says_its_alpha(self):
        """Ruling 2: each label helper's call passes `alpha=`, and the helpers' default is None."""
        for name, helper in (
            ("plot_by_beta.py", "add_beta_summary_labels"),
            ("plot_ScalarHistory.py", "add_ScalarHistory_labels"),
            ("extract_common.py", "add_ScalarHistory_labels"),
        ):
            with self.subTest(script=name, helper=helper):
                calls = [
                    n
                    for n in ast.walk(_tree(name))
                    if isinstance(n, ast.Call) and ast.unparse(n.func) == helper
                ]
                self.assertGreaterEqual(len(calls), 1)
                for call in calls:
                    keywords = [k.arg for k in call.keywords]
                    self.assertIn("alpha", keywords, ast.unparse(call))
                    # the row's model with its value (one-loop-drain prompt 04, ruling 3)
                    self.assertIn("drain_model", keywords, ast.unparse(call))


if __name__ == "__main__":
    unittest.main()
