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
main.py's order for one potential and for several (power-law-wall prompt 06 §1.4, §1.10; README §6.10 P48; orchestrator
note 06, ruling 3).

main.py parses its arguments and connects to Ray on import, so `execute` is extracted by ast, with the module's own
top-level imports, and run on a stood-in pool that records every `object_get` in order; `ray.get` resolves each lookup to
a stand-in; `run_pipeline`, `build_model_list` and `warn_super_planckian` are stood in, and the quantity arrays are
lists. Nothing else of main.py is changed.

- **one potential** (no `--models`, and `--models exponential`): the lookups, the pipelines and the printed grid lines
  are, item for item and character for character, those of main.py at the dispatch commit (`b1433f4`) for the same
  command, pinned below as literals (taken by this harness on that commit's main.py). A lookup moved would move an
  exponential-only store's serials (07's gate R);
- **one power-law potential**: the same lookups but its model table and its A rows' drain model; its grid line names
  InversePowerModel and V = V0 [1 + (M/phi)^n];
- **two potentials**: the A rows under each potential's own drain model, each potential's (M, n, beta) rows in its own
  table in the order of `--models`, and one pipeline per alpha row of its own (the fixed rows, then its A rows), per
  potential in the order given; the grid line once per potential.

No Ray cluster, no datastore, no integration. About 2 s (the sampling axis is built as main.py builds it).
"""

import ast
import contextlib
import io
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
V1 = "one-loop-exponential-v2"
PL = "one-loop-inverse-power-v1"

COMMAND = [
    "--beta-values",
    "1.5",
    "2",
    "--M-values-Mp",
    "4e-31",
    "--alpha-values",
    "0",
    "0.1",
    "--A-values",
    "1",
    "0.5",
]


class _Ref:
    def __init__(self, cls, kw):
        self.cls = cls
        self.kw = kw


def _describe(cls, kw):
    out = []
    for k in sorted(kw):
        v = kw[k]
        if k == "units":
            continue
        if k == "payload_data" and cls == "redshift":
            v = ("redshifts", len(v), v[0]["log_one_plus_z"], v[-1]["log_one_plus_z"])
        elif k == "payload_data":
            v = tuple(
                tuple(
                    (pk, pv if isinstance(pv, (int, float)) else pv._tag)
                    for pk, pv in sorted(p.items())
                    if pk != "units"
                )
                for p in v
            )
        out.append((k, v))
    return (cls, tuple(out))


def _resolve(ref):
    cls, kw = ref.cls, ref.kw
    if "payload_data" in kw:
        return [
            SimpleNamespace(_tag=f"{cls}{i}", cls=cls, **p)
            for i, p in enumerate(kw["payload_data"])
        ]
    if cls == "alpha_value":
        return SimpleNamespace(
            _tag="alpha", value=kw["value"], drain_model=kw.get("drain_model", "fixed")
        )
    return SimpleNamespace(_tag=cls, cls=cls, **kw)


def run_execute(argv):
    """(lookups, pipelines, printed) of main.py's execute for the command `argv`."""
    path = ROOT / "main.py"
    tree = ast.parse(path.read_text(), str(path))
    imports = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
    execute = [
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "execute"
    ]
    namespace = {"__name__": "test_models_main"}
    exec(
        compile(ast.Module(body=imports, type_ignores=[]), str(path), "exec"), namespace
    )
    exec(
        compile(ast.Module(body=execute, type_ignores=[]), str(path), "exec"), namespace
    )

    lookups, pipelines = [], []

    class Pool:
        def object_get(self, cls, **kw):
            lookups.append(_describe(cls, kw))
            return _Ref(cls, kw)

    def get(x):
        return [_resolve(r) for r in x] if isinstance(x, list) else _resolve(x)

    def run_pipeline(pool, model_data, Model_array, *rest):
        alpha = rest[7]
        pipelines.append(
            (
                (alpha.value, alpha.drain_model),
                tuple((m.cls, m.n, m.beta._tag, m.M._tag) for m in Model_array),
            )
        )

    namespace.update(
        ray=SimpleNamespace(get=get),
        args=namespace["create_argument_parser"]().parse_args(
            ["--database", "unused.db", *argv]
        ),
        DimensionlessQuantityArray=lambda value_array: list(value_array),
        DimensionfulQuantityArray=lambda value_array: list(value_array),
        redshift_array=lambda z_array: list(z_array),
        build_model_list=lambda pool, units: [
            {"label": "QCD_Cosmology", "cosmology": "cosmology"}
        ],
        warn_super_planckian=lambda array, *a: array,
        run_pipeline=run_pipeline,
    )
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        namespace["execute"](Pool(), namespace["Planck_units"]())
    return lookups, pipelines, out.getvalue().splitlines()


# main.py at b1433f4, the dispatch commit, run by this harness on COMMAND
_SOLVERS = [
    ("IntegrationSolver", (("label", label), ("stepping", stepping)))
    for label, stepping in (
        ("solve_ivp+RK45", 0),
        ("solve_ivp+DOP853", 0),
        ("solve_ivp+Radau", 0),
        ("solve_ivp+BDF", 0),
        ("solve_ivp+LSODA", 0),
        ("Radau+kinematic-cap", 0),
        ("Radau+kinematic-cap", 1),
    )
]


def _model_rows(table):
    return (
        table,
        (
            (
                "payload_data",
                (
                    (
                        ("M", "M_value0"),
                        ("beta", "beta_value0"),
                        ("n", 1),
                        ("shard_key", "beta_value0"),
                    ),
                    (
                        ("M", "M_value0"),
                        ("beta", "beta_value1"),
                        ("n", 1),
                        ("shard_key", "beta_value1"),
                    ),
                ),
            ),
        ),
    )


def expected_lookups(A_rows, tables):
    return [
        ("temperature", (("value", 8.212460979206235e-15),)),
        ("phi_value", (("value", 5.0),)),
        ("pi_value", (("value", 0.0),)),
        ("tolerance", (("tol", 1e-08),)),
        ("tolerance", (("tol", 1e-08),)),
        ("tolerance", (("tol", 1e-05),)),
        ("alpha_value", (("value", 0.0),)),
        ("alpha_value", (("value", 0.1),)),
        *[("alpha_value", (("drain_model", m), ("value", A))) for A, m in A_rows],
        *_SOLVERS,
        ("redshift", (("payload_data", ("redshifts", 75001, 0.0, 690.7755278982138)),)),
        ("beta_value", (("payload_data", ((("value", 1.5),), (("value", 2.0),))),)),
        ("M_value", (("payload_data", ((("value", 4e-31),),)),)),
        *[_model_rows(t) for t in tables],
    ]


def expected_pipelines(table, drain_model):
    models = (
        (table, 1, "beta_value0", "M_value0"),
        (table, 1, "beta_value1", "M_value0"),
    )
    return [
        ((0.0, "fixed"), models),
        ((0.1, "fixed"), models),
        ((1.0, drain_model), models),
        ((0.5, drain_model), models),
    ]


BASE_PRINTED = [
    "",
    "** BUILDING GRID OF MODELS TO SAMPLE",
    "   -- using model ExponentialModel, V = V0 exp((M/phi)^n), with n = [ 1 ]",
    "   -- populated beta sample grid with 2 values = [ 1.5, 2 ]",
    "   -- populated M sample grid with 1 value = [ 0.00097413 eV ]",
    "   -- populated alpha grid with 2 values = [ 0, 0.1 ]",
    "   -- populated A grid with 2 values = [ 1, 0.5 ] (one-loop-exponential-v2)",
    "   -- total number of models to integrate = 2",
]


class TestOnePotential(unittest.TestCase):
    def test_the_exponential_is_in_the_order_of_the_dispatch_commit(self):
        for extra in (
            [],
            ["--models", "exponential"],
            ["--models", "exponential", "exponential"],
        ):
            with self.subTest(extra=extra):
                lookups, pipelines, printed = run_execute(COMMAND + extra)
                self.assertEqual(
                    lookups,
                    expected_lookups([(1.0, V1), (0.5, V1)], ["ExponentialModel"]),
                )
                self.assertEqual(pipelines, expected_pipelines("ExponentialModel", V1))
                self.assertEqual(printed, BASE_PRINTED)

    def test_the_power_law_alone(self):
        lookups, pipelines, printed = run_execute(
            COMMAND + ["--models", "inverse-power"]
        )
        self.assertEqual(
            lookups, expected_lookups([(1.0, PL), (0.5, PL)], ["InversePowerModel"])
        )
        self.assertEqual(pipelines, expected_pipelines("InversePowerModel", PL))
        self.assertEqual(
            printed[2],
            "   -- using model InversePowerModel, V = V0 [1 + (M/phi)^n], with n = [ 1 ]",
        )
        self.assertEqual(
            printed[6],
            "   -- populated A grid with 2 values = [ 1, 0.5 ] (one-loop-inverse-power-v1)",
        )
        self.assertEqual(
            printed[:2] + printed[3:6], BASE_PRINTED[:2] + BASE_PRINTED[3:6]
        )
        self.assertEqual(printed[7:], BASE_PRINTED[7:])


class TestTwoPotentials(unittest.TestCase):
    def test_each_potential_its_rows_tables_and_pipelines_in_the_order_given(self):
        for order in (
            ("exponential", "inverse-power"),
            ("inverse-power", "exponential"),
        ):
            with self.subTest(order=order):
                lookups, pipelines, printed = run_execute(
                    COMMAND + ["--models", *order]
                )
                tables = [
                    {
                        "exponential": "ExponentialModel",
                        "inverse-power": "InversePowerModel",
                    }[p]
                    for p in order
                ]
                models = [{"exponential": V1, "inverse-power": PL}[p] for p in order]
                # the A rows, A by A in the order of --A-values, each under every potential's drain model in the order
                # of --models
                A_rows = [(A, m) for A in (1.0, 0.5) for m in models]
                self.assertEqual(lookups, expected_lookups(A_rows, tables))
                self.assertEqual(
                    pipelines,
                    expected_pipelines(tables[0], models[0])
                    + expected_pipelines(tables[1], models[1]),
                )
                grid = [l for l in printed if l.startswith("   -- using model")]
                self.assertEqual(len(grid), 2)
                for line, table in zip(grid, tables):
                    self.assertIn(f"using model {table}, ", line)
                self.assertIn(
                    f"   -- populated A grid with 2 values = [ 1, 0.5 ] ({models[0]} for {order[0]}; "
                    f"{models[1]} for {order[1]})",
                    printed,
                )
                self.assertIn(
                    f"   -- total number of models to integrate = 4 ({order[0]} 2, {order[1]} 2)",
                    printed,
                )

    def test_the_fixed_rows_alone_are_every_potentials(self):
        lookups, pipelines, _ = run_execute(
            [
                "--beta-values",
                "2",
                "--M-values-Mp",
                "4e-31",
                "--models",
                "inverse-power",
                "exponential",
                "--n-values",
                "1",
                "3",
            ]
        )
        self.assertEqual(
            [l for l in lookups if l[0] == "alpha_value"],
            [("alpha_value", (("value", 0.0),))],
        )
        self.assertEqual(
            [(alpha, models[0][0]) for alpha, models in pipelines],
            [
                ((0.0, "fixed"), "InversePowerModel"),
                ((0.0, "fixed"), "ExponentialModel"),
            ],
        )
        self.assertEqual([len(models) for _, models in pipelines], [2, 2])


if __name__ == "__main__":
    unittest.main()
