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
The export root and the `model` column (power-law-wall prompt 06 §1.6, §1.10; README §6.10 R12, P50, P51).

- `export_root(output, label, model)` is `<output>/<label>/<model>/` for both potentials, resolved as the plot scripts
  resolved `<output>`; an unknown potential is refused; the plot scripts form every root through it (by ast: no
  `Path(args.output).resolve() / f"{model_label}"` is left);
- a record of each potential carries `model`, and its histories.csv has `model` directly after `n`, holding the
  potential's value in every row; `build_history_record` has no default for it; a histories.csv of two potentials is
  refused;
- an exponential histories.csv with `model` removed equals, byte for byte, the file the base's write_histories_csv
  writes for the same records (BASE_HISTORIES_CSV, written by extract_common at the dispatch commit `b1433f4`).

No Ray, no store. Instant.
"""

import ast
import csv
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import extract_common as ec
from Units import Planck_units

ROOT = Path(__file__).resolve().parents[2]
UNITS = Planck_units()

# the base's histories.csv of RECORDS (extract_common.write_histories_csv at b1433f4)
BASE_HISTORIES_CSV = (
    b"beta,M_Mp,n,phi_init_Mp,alpha,drain_model,normalization,V_ref_eV4,phi_ref_Mp,phi_0_Mp,tracking,residual_H,residual_phi,tuning_iterations,Yp_BBN,D_over_H,He3_over_H,Li7_over_H,delta_Yp,delta_D_over_H,T_deliver_GeV,phi_1MeV_Mp,phi_70keV_Mp,rho_ratio_1MeV,rho_ratio_70keV,Delta_N_eff_today,Delta_N_eff_1MeV,reflections,injections,injected_fraction,log_R_first_impact,parking_T_GeV,release_T_GeV,birth_failing_share,reflection_failing_share,tracker_failing_share,unassessed_share,drained_share_one_loop,drained_share_gap,drained_share_classical,drained_share_unassessed,parking_share,candidates_one_loop,candidates_gap,candidates_classical,candidates_unassessed,ell_min,log_R_min_drain,unresolvable_assessments,unresolved_declined,all_worst_birth_margin,all_worst_birth_margin_N,all_birth_failing_share,all_worst_reflection_margin,all_worst_reflection_margin_N,all_reflection_failing_share,all_worst_tracker_margin,all_worst_tracker_margin_N,all_tracker_failing_share,bounce_rows,failure_reasons\r\n"
    b"1.185,4e-31,1,5.0,0.1,fixed,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,stand-in failure\r\n"
    b"2.0,4e-31,1,5.0,0.1,fixed,,,,,,,,,0.2468,2.46,1.04,5.42,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,\r\n"
    b"1.5,4e-31,1,5.0,1.0,one-loop-measured-v1,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,no ScalarHistory row in the store\r\n"
    b"2.0,4e-31,1,5.0,1.0,one-loop-measured-v1,,,,,,,,,0.2468,2.46,1.04,5.42,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,\r\n"
)


RECORDS = (
    (2.0, 0.1, "fixed", []),
    (1.185, 0.1, "fixed", ["stand-in failure"]),
    (2.0, 1.0, "one-loop-measured-v1", []),
    (1.5, 1.0, "one-loop-measured-v1", ["no ScalarHistory row in the store"]),
)


def records(model, n=1, drain_model_of=lambda dm: dm):
    out = []
    for beta, alpha, dm, reasons in RECORDS:
        out.append(
            ec.build_history_record(
                beta=beta,
                M_Mp=4e-31,
                n=n,
                phi_init_Mp=5.0,
                alpha=alpha,
                drain_model=drain_model_of(dm),
                model=model,
                scalar=None,
                bbn=(
                    SimpleNamespace(
                        Yp_BBN=0.2468, DOverH=2.46, He3OverH=1.04, Li7OverH=5.42
                    )
                    if not reasons
                    else None
                ),
                baseline=None,
                units=UNITS,
                failure_reasons=reasons,
            )
        )
    return out


def write(rs) -> bytes:
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "histories.csv"
        ec.write_histories_csv(rs, path)
        return path.read_bytes()


def without(data: bytes, column: str) -> bytes:
    rows = list(csv.reader(io.StringIO(data.decode(), newline="")))
    k = rows[0].index(column)
    out = io.StringIO(newline="")
    writer = csv.writer(out)
    for row in rows:
        writer.writerow(row[:k] + row[k + 1 :])
    return out.getvalue().encode()


class TestTheRoot(unittest.TestCase):
    def test_the_root_of_each_potential(self):
        for model in ("exponential", "inverse-power"):
            with self.subTest(model=model):
                self.assertEqual(
                    ec.export_root("data-out", "QCD_Cosmology", model),
                    Path("data-out").resolve() / "QCD_Cosmology" / model,
                )
                self.assertEqual(
                    ec.export_root(Path("/x/out"), "L", model), Path("/x/out/L") / model
                )
        with self.assertRaises(ValueError):
            ec.export_root("data-out", "QCD_Cosmology", "power")

    def test_below_the_root_nothing_moves(self):
        root = ec.export_root("/x/out", "L", "inverse-power")
        self.assertEqual(
            ec.alpha_path(root, 0.1, "fixed"), Path("/x/out/L/inverse-power/alpha=0.1")
        )
        self.assertEqual(
            ec.alpha_path(root, 1.0, "one-loop-measured-pl-v1"),
            Path("/x/out/L/inverse-power/one-loop-measured-pl-v1/A=1"),
        )

    def test_the_plot_scripts_form_every_root_through_it(self):
        for name in ("plot_by_beta.py", "plot_ScalarHistory.py"):
            with self.subTest(script=name):
                text = (ROOT / name).read_text()
                tree = ast.parse(text, name)
                self.assertNotIn('Path(args.output).resolve() / f"{model_label}"', text)
                self.assertNotIn("args.output).resolve()", text)
                calls = [
                    n
                    for n in ast.walk(tree)
                    if isinstance(n, ast.Call) and ast.unparse(n.func) == "export_root"
                ]
                self.assertGreaterEqual(len(calls), 1)
                for call in calls:
                    self.assertEqual(ast.unparse(call.args[0]), "args.output")


class TestTheColumn(unittest.TestCase):
    def test_model_follows_n(self):
        self.assertEqual(ec.CSV_COLUMNS[ec.CSV_COLUMNS.index("n") + 1], "model")
        self.assertEqual(ec.CSV_COLUMNS.count("model"), 1)

    def test_each_potentials_records_and_file(self):
        for model, dm in (
            ("exponential", lambda d: d),
            ("inverse-power", lambda d: d.replace("-v1", "-pl-v1")),
        ):
            with self.subTest(model=model):
                rs = records(model, drain_model_of=dm)
                for r in rs:
                    self.assertEqual(r["model"], model)
                rows = list(csv.reader(io.StringIO(write(rs).decode(), newline="")))
                self.assertEqual(rows[0], ec.CSV_COLUMNS)
                k = rows[0].index("model")
                self.assertEqual(rows[0][k - 1], "n")
                self.assertEqual({row[k] for row in rows[1:]}, {model})
                self.assertEqual(len(rows), 1 + len(RECORDS))

    def test_the_model_has_no_default(self):
        with self.assertRaises(TypeError):
            ec.build_history_record(
                beta=2.0,
                M_Mp=4e-31,
                n=1,
                phi_init_Mp=5.0,
                alpha=0.1,
                drain_model="fixed",
                scalar=None,
                bbn=None,
                baseline=None,
                units=UNITS,
            )
        with self.assertRaises(ValueError):
            records("power")

    def test_a_file_of_two_potentials_is_refused(self):
        with self.assertRaises(ValueError):
            write(records("exponential") + records("inverse-power"))

    def test_the_exponentials_file_is_the_bases_with_model_removed(self):
        data = write(records("exponential"))
        self.assertEqual(without(data, "model"), BASE_HISTORIES_CSV)
        self.assertNotEqual(data, BASE_HISTORIES_CSV)


if __name__ == "__main__":
    unittest.main()
