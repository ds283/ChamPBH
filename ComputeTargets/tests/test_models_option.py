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
`--models`, the config files, each potential's one-loop rows and the refusal at parse (power-law-wall prompt 06 §1.1-§1.3,
§1.7, §1.10; README §6.10 R12, P45-P47, P52).

- **the option**: the default `[exponential]`; each value alone; both, in either order; a repeat kept by the parser and
  removed by `distinct_values`, as `--alpha-values`'; an unknown value refused at parse (exit 2); the YAML key `models`;
  the help names each potential's V and A(phi), says the list is a key of nothing and why the default exists;
  `--n-values`' help names both potentials' V; `--potential-type` still refused with its message;
- **the config files**: `exponential.yaml` and `inverse_power.yaml` parse to the same values but `models`;
- **the one-loop rows (P47)**: `--models inverse-power --n-values 1 2 --A-values 1` parses, its drain model
  `one-loop-inverse-power-v1`; `--models exponential inverse-power --A-values 1` gives each potential its own drain model;
  with n = 3, or n = 1 3, under `inverse-power` and `--A-values` the parse is refused (exit 2, naming `--A-values`,
  `--models`, `--n-values`, the potential and the n), also through a parser whose `error` is replaced; the fixed law
  alone with `inverse-power` at n = 3 parses; an exponential command without `--models` parses as before, but at n = 3
  under `--A-values` it is refused since power-law-wall prompt 07c (U14);
  `drain_strength_A` accepts and refuses the values it did, with the same message;
- **the names (§1.1)** and **the hook (P52)**: each value's class and table, the value of a model object or class,
  `spectrum_wall` at n = 1, 2 and 3 on both potentials, each name a wall of `quanta_spectrum.WALLS`.

No Ray cluster, no datastore, no integration. Instant.
"""

import argparse
import contextlib
import io
import math
import tempfile
import unittest
from pathlib import Path

from ComputeTargets import quanta_spectrum as qs
from CosmologyConcepts import M_value, beta_value
from CosmologyConcepts.Models import ExponentialModel, InversePowerModel
from CosmologyConcepts.Models import model_options as mo
from Units import Planck_units
from config.argument_parser import (
    DriverArgumentParser,
    create_argument_parser,
    distinct_values,
    drain_strength_A,
)

ROOT = Path(__file__).resolve().parents[2]
BASE = ["--database", "unused.db"]
PL = "one-loop-inverse-power-v1"
V1 = "one-loop-exponential-v2"


def _parse(argv):
    """(namespace, None) for a parse that succeeds, (None, (exit code, stderr)) for a refusal."""
    err = io.StringIO()
    try:
        with contextlib.redirect_stderr(err):
            return create_argument_parser().parse_args(argv), None
    except SystemExit as e:
        return None, (e.code, err.getvalue())


class TestTheOption(unittest.TestCase):
    def test_the_default_is_the_exponential(self):
        args, refusal = _parse(BASE)
        self.assertIsNone(refusal)
        self.assertEqual(args.models, ["exponential"])

    def test_each_value_alone_and_both_in_either_order(self):
        for given in (
            ["exponential"],
            ["inverse-power"],
            ["exponential", "inverse-power"],
            ["inverse-power", "exponential"],
        ):
            with self.subTest(given=given):
                args, refusal = _parse(BASE + ["--models", *given])
                self.assertIsNone(refusal)
                self.assertEqual(args.models, given)
                self.assertEqual(distinct_values(args.models), given)

    def test_a_repeat_is_removed_by_distinct_values(self):
        args, _ = _parse(
            BASE + ["--models", "inverse-power", "exponential", "inverse-power"]
        )
        self.assertEqual(args.models, ["inverse-power", "exponential", "inverse-power"])
        self.assertEqual(distinct_values(args.models), ["inverse-power", "exponential"])

    def test_an_unknown_value_is_refused_at_parse(self):
        for given in (["power"], ["exponential", "Exponential"], []):
            with self.subTest(given=given):
                args, refusal = _parse(BASE + ["--models", *given])
                self.assertIsNone(args)
                self.assertEqual(refusal[0], 2)
                self.assertIn("--models", refusal[1])

    def test_the_yaml_key(self):
        for text, expected in (
            ("models: [inverse-power]\n", ["inverse-power"]),
            (
                "models:\n  - inverse-power\n  - exponential\n",
                ["inverse-power", "exponential"],
            ),
        ):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "m.yaml"
                path.write_text(text)
                args, refusal = _parse(BASE + ["--config", str(path)])
                self.assertIsNone(refusal)
                self.assertEqual(args.models, expected)
        # the command line wins over the file
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "m.yaml"
            path.write_text("models: [inverse-power]\n")
            args, _ = _parse(BASE + ["--config", str(path), "--models", "exponential"])
            self.assertEqual(args.models, ["exponential"])

    def test_the_help(self):
        text = " ".join(create_argument_parser().format_help().split())
        self.assertIn("--models {exponential,inverse-power}", text)
        self.assertIn("exponential, ExponentialModel, V = V0 exp((M/phi)^n)", text)
        self.assertIn("inverse-power, InversePowerModel, V = V0 [1 + (M/phi)^n]", text)
        self.assertIn("A = exp(beta (phi - phi_ref)/M_P)", text)
        self.assertIn("a key of nothing", text)
        self.assertIn("so that a command without the option keeps its meaning", text)
        # --n-values names both potentials' V
        n_help = text[text.index("--n-values N_VALUES") :]
        self.assertIn("V = V0 exp((M/phi)^n)", n_help)
        self.assertIn("V = V0 [1 + (M/phi)^n]", n_help)

    def test_the_potential_type_is_still_removed(self):
        args, refusal = _parse(BASE + ["--potential-type", "exponential"])
        self.assertIsNone(args)
        self.assertEqual(refusal[0], 2)
        self.assertIn(
            "--potential-type was removed by jordan-normalization prompt 08: there is one model, "
            "ExponentialModel",
            refusal[1],
        )


class TestTheConfigFiles(unittest.TestCase):
    def test_the_two_files_differ_in_models_alone(self):
        exp_args, refusal = _parse(BASE + ["--config", str(ROOT / "exponential.yaml")])
        self.assertIsNone(refusal)
        ip_args, refusal = _parse(BASE + ["--config", str(ROOT / "inverse_power.yaml")])
        self.assertIsNone(refusal)
        self.assertEqual(exp_args.models, ["exponential"])
        self.assertEqual(ip_args.models, ["inverse-power"])
        a, b = vars(exp_args), vars(ip_args)
        self.assertEqual(set(a), set(b))
        differ = sorted(k for k in a if a[k] != b[k])
        self.assertEqual(differ, ["config", "models"])

    def test_the_files_name_the_same_keys_in_the_same_order(self):
        def keys(name):
            return [
                line.split(":")[0]
                for line in (ROOT / name).read_text().splitlines()
                if line.strip()
            ]

        self.assertEqual(keys("exponential.yaml"), keys("inverse_power.yaml"))
        self.assertIn("models", keys("exponential.yaml"))


class TestTheOneLoopRows(unittest.TestCase):
    def test_the_power_law_parses_at_n_1_and_2_under_its_own_model(self):
        args, refusal = _parse(
            BASE
            + ["--models", "inverse-power", "--n-values", "1", "2", "--A-values", "1"]
        )
        self.assertIsNone(refusal)
        self.assertEqual(
            mo.one_loop_drain_model_over("inverse-power", args.n_values), PL
        )

    def test_each_potential_its_own_drain_model(self):
        args, refusal = _parse(
            BASE + ["--models", "exponential", "inverse-power", "--A-values", "1"]
        )
        self.assertIsNone(refusal)
        self.assertEqual(
            {p: mo.one_loop_drain_model_over(p, args.n_values) for p in args.models},
            {"exponential": V1, "inverse-power": PL},
        )
        # the exponential's at n = 1 and 2 only since power-law-wall prompt 07c (U14, ruled (a): "for now, refuse
        # `n >= 3`"); before it, at every n
        self.assertEqual(mo.one_loop_drain_model_over("exponential", [1, 2]), V1)
        with self.assertRaises(ValueError) as ctx:
            mo.one_loop_drain_model_over("exponential", [1, 2, 3])
        self.assertIn("n = 3", str(ctx.exception))
        self.assertIn("U14", str(ctx.exception))

    def test_no_one_loop_law_at_an_n_is_refused_at_parse(self):
        for n_values in (["3"], ["1", "3"]):
            for models in (["inverse-power"], ["exponential", "inverse-power"]):
                with self.subTest(n_values=n_values, models=models):
                    args, refusal = _parse(
                        BASE
                        + [
                            "--models",
                            *models,
                            "--n-values",
                            *n_values,
                            "--A-values",
                            "1",
                        ]
                    )
                    self.assertIsNone(args)
                    code, text = refusal
                    self.assertEqual(code, 2)
                    flat = " ".join(text.split())
                    # the first potential of --models without a law at n = 3 is named: since power-law-wall
                    # prompt 07c (U14) the exponential has none there either
                    for word in (
                        "--A-values",
                        "--models",
                        "--n-values",
                        models[0],
                        "n = 3",
                    ):
                        self.assertIn(word, flat)

    def test_the_refusal_goes_through_parser_error(self):
        """A parser whose `error` is replaced (RunDrivers/registered_run.py's) refuses with the same message."""
        parser = create_argument_parser()
        messages = []

        def error(message):
            messages.append(message)
            raise RuntimeError("refused")

        parser.error = error
        with self.assertRaises(RuntimeError):
            parser.parse_args(
                BASE
                + ["--models", "inverse-power", "--n-values", "3", "--A-values", "1"]
            )
        self.assertEqual(len(messages), 1)
        self.assertIn("--A-values", messages[0])
        self.assertIn("n = 3", messages[0])

    def test_the_fixed_law_alone_parses_at_any_n(self):
        for argv in (
            ["--models", "inverse-power", "--n-values", "3"],
            [
                "--models",
                "inverse-power",
                "--n-values",
                "1",
                "3",
                "--alpha-values",
                "0.1",
            ],
        ):
            with self.subTest(argv=argv):
                args, refusal = _parse(BASE + argv)
                self.assertIsNone(refusal)
                self.assertEqual(args.models, ["inverse-power"])

    def test_the_exponential_at_n_3_is_refused_under_A_values(self):
        """
        U14 (power-law-wall prompt 07c, ruled (a)): the exponential has a one-loop law at n = 1 and 2 only, so
        --A-values with n = 3 is refused at parse, naming the potential and the n; the fixed law alone parses at n = 3.
        """
        args, refusal = _parse(BASE + ["--n-values", "1", "2", "3", "--A-values", "1"])
        self.assertIsNone(args)
        code, text = refusal
        self.assertEqual(code, 2)
        flat = " ".join(text.split())
        for word in ("--A-values", "exponential", "n = 3", "U14"):
            self.assertIn(word, flat)
        args, refusal = _parse(BASE + ["--n-values", "1", "2", "3"])
        self.assertIsNone(refusal)
        self.assertEqual(args.models, ["exponential"])

    def test_an_exponential_command_parses_as_before(self):
        for argv in (
            [],
            ["--A-values", "1", "0.5"],
            ["--n-values", "1", "2", "--A-values", "1"],
            ["--no-alpha-values", "--A-values", "0.05", "1", "10"],
        ):
            with self.subTest(argv=argv):
                args, refusal = _parse(BASE + argv)
                self.assertIsNone(refusal)
                self.assertEqual(args.models, ["exponential"])

    def test_drain_strength_A_accepts_and_refuses_as_before(self):
        for text, value in (
            ("10", 10.0),
            ("1000", 1000.0),
            ("1e6", 1.0e6),
            ("0.05", 0.05),
        ):
            with self.subTest(text=text):
                self.assertEqual(drain_strength_A(text), value)
        for text in ("0", "-0", "-1", "nan", "inf"):
            with self.subTest(text=text):
                with self.assertRaises(argparse.ArgumentTypeError) as ctx:
                    drain_strength_A(text)
                self.assertTrue(
                    str(ctx.exception).startswith(
                        "--A-values: under the one-loop drain law alpha carries A, which must be finite and > 0"
                    ),
                    str(ctx.exception),
                )

    def test_the_driver_parser_class(self):
        self.assertIsInstance(create_argument_parser(), DriverArgumentParser)


class TestTheNames(unittest.TestCase):
    def test_the_values_their_classes_and_tables(self):
        self.assertEqual(mo.MODEL_OPTIONS, ("exponential", "inverse-power"))
        self.assertIs(mo.model_class("exponential"), ExponentialModel)
        self.assertIs(mo.model_class("inverse-power"), InversePowerModel)
        self.assertEqual(mo.model_table("exponential"), "ExponentialModel")
        self.assertEqual(mo.model_table("inverse-power"), "InversePowerModel")
        with self.assertRaises(ValueError):
            mo.model_class("power")

    def test_the_value_of_a_model_object_or_class(self):
        units = Planck_units()
        for value, cls in mo.MODEL_CLASSES.items():
            with self.subTest(value=value):
                model = cls(0, M_value(0, 4e-31), 1, beta_value(0, 2.0), units)
                self.assertEqual(mo.model_option(model), value)
                self.assertEqual(mo.model_option(cls), value)
                self.assertEqual(mo.model_option(model.realize(0.0, 1.0).model), value)
        with self.assertRaises(ValueError):
            mo.model_option(object())

    def test_the_tools_name_the_same_values(self):
        from tools import bbn_from_store, history_and_bbn

        self.assertEqual(tuple(history_and_bbn.MODEL_CLASSES), mo.MODEL_OPTIONS)
        self.assertEqual(tuple(bbn_from_store.MODEL_CLASSES), mo.MODEL_OPTIONS)
        for value in mo.MODEL_OPTIONS:
            self.assertIs(history_and_bbn.MODEL_CLASSES[value], mo.model_class(value))
            self.assertIs(bbn_from_store.MODEL_CLASSES[value], mo.model_class(value))


class TestTheSpectrumWall(unittest.TestCase):
    """P52: spectrum_wall, with no default, on both potentials."""

    def model(self, cls, n, M=4e-31, beta=2.0):
        return cls(0, M_value(0, M), n, beta_value(0, beta), Planck_units())

    def test_the_exponential_is_the_exponential_wall_at_every_n(self):
        for n in (1, 2, 3):
            with self.subTest(n=n):
                wall = self.model(ExponentialModel, n).spectrum_wall
                self.assertEqual(wall, qs.WALL_EXPONENTIAL)
                self.assertIn(wall, qs.WALLS)
                self.assertEqual(mo.spectrum_wall("exponential", n), wall)

    def test_the_power_law_at_n_1_and_2_and_none_at_3(self):
        for n, expected in ((1, qs.WALL_POWER_N1), (2, qs.WALL_POWER_N2)):
            with self.subTest(n=n):
                wall = self.model(InversePowerModel, n).spectrum_wall
                self.assertEqual(wall, expected)
                self.assertIn(wall, qs.WALLS)
                self.assertEqual(mo.spectrum_wall("inverse-power", n), wall)
        with self.assertRaises(ValueError) as ctx:
            self.model(InversePowerModel, 3).spectrum_wall
        self.assertIn("InversePowerModel", str(ctx.exception))
        self.assertIn("spectrum_wall", str(ctx.exception))
        self.assertIn("n=3", str(ctx.exception))

    def test_the_realized_model_forwards_it(self):
        for cls, n in ((ExponentialModel, 2), (InversePowerModel, 1)):
            with self.subTest(cls=cls.__name__):
                model = self.model(cls, n)
                self.assertEqual(
                    model.realize(0.0, 1.0).spectrum_wall, model.spectrum_wall
                )

    def test_no_default_on_the_base(self):
        from ComputeTargets.tests.test_model_interface import _MinimalModel

        model = _MinimalModel(0.5)
        with self.assertRaises(NotImplementedError) as ctx:
            model.spectrum_wall
        self.assertIn("_MinimalModel", str(ctx.exception))

    def test_the_reference_model_does_not_change_it(self):
        for M, beta in ((1.0, 1.0), (4e-31, 2.0), (1e-3, 2.9)):
            for cls, value in (
                (ExponentialModel, "exponential"),
                (InversePowerModel, "inverse-power"),
            ):
                with self.subTest(M=M, beta=beta, cls=cls.__name__):
                    self.assertEqual(
                        self.model(cls, 1, M, beta).spectrum_wall,
                        mo.spectrum_wall(value, 1),
                    )
                    self.assertEqual(
                        self.model(cls, 1, M, beta).one_loop_drain_model,
                        mo.one_loop_drain_model(value, 1),
                    )


if __name__ == "__main__":
    unittest.main()
