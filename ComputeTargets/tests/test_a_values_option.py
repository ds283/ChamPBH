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
`--A-values`, the drivers' rows, and the path and label helpers (one-loop-drain prompt 04, prompt §1.1-§1.4, §1.9;
orchestrator note 04, rulings 2, 3, 4 and 5).

(1) the option: no default, so a run without it is today's run; a given list is kept, in order; the YAML key
    `A-values` gives the same list; 10 and 1000 are accepted; 0, -0, -1, nan and inf are each refused at parse
    (exit 2, the option named, the rule's message from `check_drain_model`, before any Ray); duplicates collapse
    in the order of first appearance; the help says what A is, that it names the code's current one-loop model, that
    it is a key, and that alpha = 0 is still built; `--alpha-values` is unchanged;
(2) the drivers, by `ast`: `main.py` looks up one row per A with `drain_model=DRAIN_MODEL_ONE_LOOP`, by the name and
    never a string literal, after the alpha rows; each driver calls `run_pipeline` once per row; both plot scripts
    pass both lists to `plot_inputs.parameters`; no driver writes a model's name; every per-row path and label in
    the drivers and in `extract_common` passes the row's model (ruling 3);
(3) the helpers: `alpha_path` and `alpha_label` under both models, the fixed-law strings by literal in all three
    spellings of alpha (ruling 2), `alpha=0` for -0 as parsed, no default model, and `model_of_law` inverting
    `law_of` (ruling 4).

No Ray cluster, no datastore, no integration. Instant.
"""

import ast
import contextlib
import io
import math
import tempfile
import unittest
from pathlib import Path

from CosmologyConcepts.drain_model import (
    DRAIN_LAWS,
    DRAIN_MODEL_FIXED,
    DRAIN_MODEL_ONE_LOOP,
    DRAIN_MODELS,
    alpha_label,
    law_of,
    model_of_law,
)
from config.argument_parser import (
    create_argument_parser,
    distinct_values,
    drain_strength_A,
)
from extract_common import _cosmology_line, alpha_path

ROOT = Path(__file__).resolve().parents[2]
BASE = ["--database", "unused.db"]
DRIVERS = ("main.py", "plot_by_beta.py", "plot_ScalarHistory.py")
ONE_LOOP = "one-loop-measured-v1"


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
    def test_1_no_default_and_alpha_values_unchanged(self):
        args, refusal = _parse(BASE)
        self.assertIsNone(refusal)
        # a run without --A-values is today's run: the fixed rows, alpha = 0
        self.assertIsNone(args.A_values)
        self.assertEqual(args.alpha_values, [0.0])
        # a run given only --A-values also builds, or reads, alpha = 0
        args, _ = _parse(BASE + ["--A-values", "1"])
        self.assertEqual(args.A_values, [1.0])
        self.assertEqual(args.alpha_values, [0.0])
        # and --alpha-values still takes only a fraction
        args, refusal = _parse(BASE + ["--alpha-values", "1.0"])
        self.assertIsNone(args)
        self.assertIn("argument --alpha-values: must be a fraction", refusal[1])

    def test_1_a_given_list_is_kept_in_order(self):
        args, _ = _parse(BASE + ["--A-values", "2", "0.5", "1"])
        self.assertEqual(args.A_values, [2.0, 0.5, 1.0])
        args, _ = _parse(
            BASE + ["--alpha-values", "0", "0.1", "--A-values", "0.05", "10"]
        )
        self.assertEqual(args.alpha_values, [0.0, 0.1])
        self.assertEqual(args.A_values, [0.05, 10.0])

    def test_1_the_yaml_key_gives_the_same_list(self):
        for text in (
            "A-values: [0.5, 10]\n",
            "A-values:\n  - 0.5\n  - 10\n",
        ):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "A.yaml"
                path.write_text(text)
                args, refusal = _parse(BASE + ["--config", str(path)])
                self.assertIsNone(refusal)
                self.assertEqual(args.A_values, [0.5, 10.0])
        # and the command line wins over the file, as for any option
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "A.yaml"
            path.write_text("A-values: [0.5, 10]\n")
            args, _ = _parse(BASE + ["--config", str(path), "--A-values", "3"])
            self.assertEqual(args.A_values, [3.0])

    def test_1_there_is_no_upper_bound(self):
        for value, expected in (("10", 10.0), ("1000", 1000.0), ("1e6", 1.0e6)):
            with self.subTest(value=value):
                args, refusal = _parse(BASE + ["--A-values", value])
                self.assertIsNone(refusal)
                self.assertEqual(args.A_values, [expected])

    def test_1_a_value_the_law_refuses_is_refused_at_parse_naming_the_option(self):
        for value in ("0", "-0", "-1", "nan", "inf"):
            with self.subTest(value=value):
                args, refusal = _parse(BASE + ["--A-values", value])
                self.assertIsNone(args)
                code, text = refusal
                self.assertEqual(code, 2)
                self.assertIn("argument --A-values:", text)
                # the message is check_drain_model's, stated once (ruling 5): not argparse's "invalid ... value"
                self.assertIn(
                    "--A-values: under the one-loop drain law alpha carries A", text
                )
                self.assertIn("finite and > 0", text)
                self.assertNotIn("invalid drain_strength_A value", text)
        # one bad value among good ones refuses the whole list
        args, refusal = _parse(BASE + ["--A-values", "1", "0"])
        self.assertIsNone(args)
        self.assertEqual(refusal[0], 2)
        # and an unparsable one is refused too
        args, refusal = _parse(BASE + ["--A-values", "x"])
        self.assertIsNone(args)
        self.assertEqual(refusal[0], 2)
        self.assertIn("--A-values", refusal[1])

    def test_1_the_type_returns_a_float_and_minus_zero_is_refused_not_normalised(self):
        self.assertEqual(drain_strength_A("2"), 2.0)
        self.assertIsInstance(drain_strength_A("2"), float)
        with self.assertRaises(Exception):
            drain_strength_A("-0")

    def test_1_duplicates_collapse_in_the_order_of_first_appearance(self):
        args, _ = _parse(BASE + ["--A-values", "1", "0.5", "1", "2", "0.5"])
        self.assertEqual(args.A_values, [1.0, 0.5, 1.0, 2.0, 0.5])
        self.assertEqual(distinct_values(args.A_values), [1.0, 0.5, 2.0])

    def test_1_the_help_says_what_A_is(self):
        text = " ".join(create_argument_parser().format_help().split())
        self.assertIn("--A-values A_VALUES [A_VALUES ...]", text)
        self.assertIn("the strength of the one-loop drain law", text)
        self.assertIn("finite and > 0 with no upper bound", text)
        self.assertIn("the code's current one-loop drain model", text)
        self.assertIn("A is a key of every stored history", text)
        self.assertIn("also builds, or reads, alpha = 0, natural mode", text)
        # --alpha-values' help is as it was
        self.assertIn("0 is natural mode (the default [0.0])", text)


def _object_get_calls(tree):
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


def _calls(tree, name):
    return [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and ast.unparse(n.func) == name
    ]


class TestTheDriversCarryA(unittest.TestCase):
    def test_2_main_looks_up_one_row_per_A_under_the_constant_after_the_alpha_rows(
        self,
    ):
        tree = _tree("main.py")
        self.assertEqual(
            _assigned(tree, "A_values"), ["distinct_values(args.A_values or [])"]
        )
        lookups = _object_get_calls(tree)
        self.assertEqual(len(lookups), 2)
        alpha_lookup, A_lookup = lookups
        self.assertEqual(
            [(k.arg, ast.unparse(k.value)) for k in alpha_lookup.keywords],
            [("value", "a")],
        )
        # by the imported name, never a string
        self.assertEqual(
            [(k.arg, ast.unparse(k.value)) for k in A_lookup.keywords],
            [("value", "A"), ("drain_model", "DRAIN_MODEL_ONE_LOOP")],
        )
        # both are the elements of the tolerances' ray.get, unpacked in that order: the alpha rows, then the A rows
        listcomps = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.ListComp)
            and any(c in (alpha_lookup, A_lookup) for c in ast.walk(n.elt))
        ]
        self.assertEqual(len(listcomps), 2)
        by_lookup = {
            ast.unparse(c.generators[0].iter): any(
                x is A_lookup for x in ast.walk(c.elt)
            )
            for c in listcomps
        }
        self.assertEqual(by_lookup, {"alpha_values": False, "A_values": True})
        gets = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Call)
            and ast.unparse(n.func) == "ray.get"
            and any(isinstance(a, ast.List) for a in n.args)
            and "A_values" in ast.unparse(n)
        ]
        self.assertEqual(len(gets), 1)
        elements = gets[0].args[0].elts
        starred = [
            ast.unparse(e.value.generators[0].iter)
            for e in elements
            if isinstance(e, ast.Starred)
        ]
        self.assertEqual(starred, ["alpha_values", "A_values"])
        self.assertIn("*alpha_rows", ast.unparse(tree))
        # the A grid line is printed only when --A-values is given
        guarded = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.If)
            and ast.unparse(n.test) == "A_values"
            and "populated A grid" in ast.unparse(n)
        ]
        self.assertEqual(len(guarded), 1)

    def test_2_every_driver_calls_run_pipeline_once_per_row(self):
        for name in DRIVERS:
            with self.subTest(script=name):
                tree = _tree(name)
                loops = [
                    n
                    for n in ast.walk(tree)
                    if isinstance(n, ast.For) and ast.unparse(n.iter) == "alpha_rows"
                ]
                self.assertEqual(len(loops), 1)
                calls = _calls(loops[0], "run_pipeline")
                self.assertEqual(len(calls), 1)
                self.assertIn(
                    loops[0].target.id, [ast.unparse(a) for a in calls[0].args]
                )

    def test_2_the_plot_scripts_pass_both_lists_to_plot_inputs(self):
        for name in ("plot_by_beta.py", "plot_ScalarHistory.py"):
            with self.subTest(script=name):
                tree = _tree(name)
                self.assertEqual(
                    _assigned(tree, "A_values"),
                    ["distinct_values(args.A_values or [])"],
                )
                calls = _calls(tree, "plot_inputs.parameters")
                self.assertEqual(len(calls), 1)
                keywords = {k.arg: ast.unparse(k.value) for k in calls[0].keywords}
                self.assertEqual(keywords["alpha_values"], "alpha_values")
                self.assertEqual(keywords["A_values"], "A_values")
                self.assertEqual(_object_get_calls(tree), [])

    def test_2_no_driver_writes_a_models_name(self):
        for name in (*DRIVERS, "plot_inputs.py", "extract_common.py"):
            with self.subTest(script=name):
                text = (ROOT / name).read_text()
                self.assertNotIn(ONE_LOOP, text)
                self.assertNotIn("one-loop-measured", text)

    def test_2_plot_inputs_looks_the_A_rows_up_under_the_constant(self):
        tree = _tree("plot_inputs.py")
        lookups = _object_get_calls(tree)
        self.assertEqual(len(lookups), 2)
        modelled = [l for l in lookups if "drain_model" in [k.arg for k in l.keywords]]
        self.assertEqual(len(modelled), 1)
        self.assertEqual(
            [(k.arg, ast.unparse(k.value)) for k in modelled[0].keywords],
            [("value", "A"), ("drain_model", "DRAIN_MODEL_ONE_LOOP")],
        )

    def test_2_every_per_row_path_and_label_passes_the_rows_model(self):
        """Ruling 3: nothing defaults the model, and every call says which."""
        for name in (*DRIVERS, "extract_common.py"):
            tree = _tree(name)
            for call in _calls(tree, "alpha_path"):
                with self.subTest(script=name, call=ast.unparse(call)):
                    self.assertEqual(len(call.args), 3)
                    self.assertEqual(call.keywords, [])
            for call in _calls(tree, "alpha_label"):
                with self.subTest(script=name, call=ast.unparse(call)):
                    self.assertGreaterEqual(len(call.args), 2)
                    # the model is the second argument: the row's, or the record's
                    self.assertTrue(
                        any(w in ast.unparse(call.args[1]) for w in ("drain_model",)),
                        ast.unparse(call),
                    )
            for call in _calls(tree, "build_history_record"):
                with self.subTest(script=name, call=ast.unparse(call)):
                    self.assertIn("drain_model", [k.arg for k in call.keywords])
            for helper in ("add_beta_summary_labels", "add_ScalarHistory_labels"):
                for call in _calls(tree, helper):
                    with self.subTest(script=name, call=ast.unparse(call)):
                        keywords = [k.arg for k in call.keywords]
                        self.assertIn("alpha", keywords)
                        self.assertIn("drain_model", keywords)

    def test_2_no_driver_formats_alpha_by_hand(self):
        """The three spellings of alpha come only from alpha_label (ruling 2)."""
        for name in DRIVERS:
            with self.subTest(script=name):
                text = (ROOT / name).read_text()
                for fragment in ("α = {", "alpha = {", "alpha.value:g}"):
                    self.assertNotIn(fragment, text)


class TestTheHelpers(unittest.TestCase):
    def test_3_the_fixed_paths_are_those_of_before(self):
        for alpha, expected in (
            (0.0, "out/label/alpha=0"),
            (0.1, "out/label/alpha=0.1"),
            (0.03, "out/label/alpha=0.03"),
            (0.123456789, "out/label/alpha=0.12346"),
        ):
            self.assertEqual(
                alpha_path("out/label", alpha, DRAIN_MODEL_FIXED), Path(expected)
            )
        # alpha = 0 for -0 as parsed
        args, _ = _parse(BASE + ["--alpha-values", "-0"])
        self.assertEqual(
            alpha_path("out", args.alpha_values[0], "fixed"), Path("out/alpha=0")
        )
        self.assertEqual(
            alpha_path("out", args.alpha_values[0], "fixed").name, "alpha=0"
        )

    def test_3_the_one_loop_path_carries_the_model_and_A(self):
        self.assertEqual(
            alpha_path("out/label", 1.0, DRAIN_MODEL_ONE_LOOP),
            Path("out/label/one-loop-measured-v1/A=1"),
        )
        self.assertEqual(
            alpha_path(Path("out"), 0.123456789, DRAIN_MODEL_ONE_LOOP),
            Path("out/one-loop-measured-v1/A=0.12346"),
        )
        self.assertEqual(
            alpha_path("out", 1000.0, DRAIN_MODEL_ONE_LOOP),
            Path("out/one-loop-measured-v1/A=1000"),
        )
        # the same value under the two models is two directories, and a glob of alpha=* never reads the one-loop one
        self.assertNotEqual(
            alpha_path("out", 0.5, DRAIN_MODEL_FIXED),
            alpha_path("out", 0.5, DRAIN_MODEL_ONE_LOOP),
        )
        self.assertFalse(
            alpha_path("out", 0.5, DRAIN_MODEL_ONE_LOOP)
            .relative_to("out")
            .parts[0]
            .startswith("alpha=")
        )
        with self.assertRaises(ValueError):
            alpha_path("out", 1.0, "no-such-model")

    def test_3_the_model_has_no_default(self):
        with self.assertRaises(TypeError):
            alpha_path("out", 0.1)
        with self.assertRaises(TypeError):
            alpha_label(0.1)
        with self.assertRaises(ValueError):
            _cosmology_line("lab", 0.1, None)

    def test_3_the_fixed_labels_are_those_of_before_in_all_three_spellings(self):
        # the titles and _cosmology_line (the default spelling), the run_pipeline header lines, the legend
        self.assertEqual(alpha_label(0.1, "fixed"), "α = 0.1")
        self.assertEqual(alpha_label(0.0, "fixed"), "α = 0")
        self.assertEqual(alpha_label(0.03, "fixed"), "α = 0.03")
        self.assertEqual(alpha_label(0.1, "fixed", "alpha"), "alpha = 0.1")
        self.assertEqual(alpha_label(0.0, "fixed", "alpha"), "alpha = 0")
        self.assertEqual(alpha_label(0.1, "fixed", r"$\alpha$"), r"$\alpha$ = 0.1")
        self.assertEqual(
            _cosmology_line("lab", 0.1, "fixed"), "Cosmology: lab, α = 0.1"
        )
        self.assertEqual(_cosmology_line("lab", None, None), "Cosmology: lab")

    def test_3_the_one_loop_label_is_the_same_in_every_spelling(self):
        for spelling in ("α", "alpha", r"$\alpha$"):
            self.assertEqual(
                alpha_label(1.0, ONE_LOOP, spelling), "A = 1 (one-loop-measured-v1)"
            )
        self.assertEqual(alpha_label(0.5, ONE_LOOP), "A = 0.5 (one-loop-measured-v1)")
        self.assertEqual(
            alpha_label(1000.0, ONE_LOOP), "A = 1000 (one-loop-measured-v1)"
        )
        self.assertEqual(
            _cosmology_line("lab", 2.0, ONE_LOOP),
            "Cosmology: lab, A = 2 (one-loop-measured-v1)",
        )
        with self.assertRaises(ValueError):
            alpha_label(1.0, "no-such-model")

    def test_3_the_constants_are_the_ones_the_strings_above_name(self):
        self.assertEqual(DRAIN_MODEL_FIXED, "fixed")
        self.assertEqual(DRAIN_MODEL_ONE_LOOP, ONE_LOOP)

    def test_3_model_of_law_inverts_law_of(self):
        for model in DRAIN_MODELS:
            self.assertEqual(model_of_law(law_of(model)), model)
        for law in DRAIN_LAWS:
            self.assertEqual(law_of(model_of_law(law)), law)
        self.assertEqual(model_of_law("fixed"), DRAIN_MODEL_FIXED)
        self.assertEqual(model_of_law("one_loop"), DRAIN_MODEL_ONE_LOOP)
        with self.assertRaises(ValueError) as ctx:
            model_of_law("nonsense", "ctx")
        self.assertTrue(str(ctx.exception).startswith("ctx:"))

    def test_3_A_is_finite_and_positive_by_check_drain_model(self):
        # the one statement of the rules: the type and the helper agree
        for text in ("0.001", "1", "1e9"):
            self.assertEqual(drain_strength_A(text), float(text))
        for text in ("0", "nan", "inf", "-2"):
            with self.assertRaises(Exception):
                drain_strength_A(text)
        self.assertTrue(math.isfinite(drain_strength_A("1e300")))


if __name__ == "__main__":
    unittest.main()
