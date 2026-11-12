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
Build the production w table, CosmologyModels/GenericEOS/Xav_EOS_data.csv, from committed inputs.

Written for electroweak-eos prompt 01 (campaign README §6.1 U2 (b), §6.2 P1). The table is not
typed: it is this script's output, and `--check` says whether the installed CSV is.

The rows, with T0 = 5.011872336272725e-05 GeV (50.1 keV), the notebook's second row:

  source                                                    rows                 count
  the table before this campaign (pipeline_w_table_ec6bca8)  T < T0               14
  the notebook's table (notebook_w_table.csv)               T0 <= T < 100 GeV    126
  the points of 2026-11-12 (notebook_w_points_2026-11-12)   100 <= T <= 380 GeV  29
  the notebook's table                                      T > 380 GeV          1 (w = 1/3 at 386.7 GeV)

170 rows, from 1e-5 to 386.7085 GeV. The inputs are read where they are and not modified:
  prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv (the table at ec6bca8, byte for byte);
  .documents/peakewsurf-kicking-function/data/notebook_w_table.csv;
  .documents/peakewsurf-kicking-function/data/notebook_w_points_2026-11-12.csv.
Each number is parsed by Python's float() and written as repr(float(x)), which reads back to the
same float. (Under NumPy 2 the repr of a numpy.float64 is "np.float64(...)", which pandas reads as a
string and on which Xav_EOS_spline.__init__ fails.)

The script asserts, and stops with exit 1 otherwise:
  - T0 is a row of the old table too, at the same T to 1e-12 relative and the same w to 1e-12, so the
    join is a shared row;
  - the points' 100 GeV row equals the notebook's 100 GeV row bitwise (T and w);
  - T is strictly increasing in the output;
  - each notebook row left out (158.5, 251.2 and 316.2 GeV) lies within 1e-5 in Sigma = 1 - 3w of a
    cubic spline in ln T through the points (scipy make_interp_spline, as Xav_EOS_spline splines),
    the audit's addendum having measured 3.7e-6 and 1e-11.

Run from the repository root:

    ./venv/bin/python tools/build_w_table.py            # writes the CSV
    ./venv/bin/python tools/build_w_table.py --check    # compares, writes nothing; exit 0 or 1
"""

import argparse
import csv
import sys
from pathlib import Path

import numpy as np
from scipy.interpolate import make_interp_spline

ROOT = Path(__file__).resolve().parents[1]

OLD_TABLE = (
    ROOT / "prompts" / "electroweak-eos" / "data" / "pipeline_w_table_ec6bca8.csv"
)
NOTEBOOK_TABLE = (
    ROOT
    / ".documents"
    / "peakewsurf-kicking-function"
    / "data"
    / "notebook_w_table.csv"
)
POINTS = (
    ROOT
    / ".documents"
    / "peakewsurf-kicking-function"
    / "data"
    / "notebook_w_points_2026-11-12.csv"
)
OUTPUT = ROOT / "CosmologyModels" / "GenericEOS" / "Xav_EOS_data.csv"

HEADER = "T_GeV,w"

# the edges of the composition (GeV)
POINTS_T_LO = 100.0
POINTS_T_HI = 380.0

# the join and the left-out rows
JOIN_T_RTOL = 1e-12
JOIN_W_ATOL = 1e-12
LEFT_OUT_SIGMA_ATOL = 1e-5

EXPECTED_COUNTS = {"old": 14, "notebook_low": 126, "points": 29, "notebook_top": 1}


class BuildError(RuntimeError):
    """An assertion of the composition failed."""


def read_rows(path) -> list:
    """The (T, w) rows of a two-column CSV with a header, each number parsed by float()."""
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader)
        return [(float(T), float(w)) for T, w in reader]


def load_inputs() -> dict:
    return {
        "old": read_rows(OLD_TABLE),
        "notebook": read_rows(NOTEBOOK_TABLE),
        "points": read_rows(POINTS),
    }


def compose(old: list, notebook: list, points: list) -> dict:
    """
    The table's rows by source, after every assertion of the module docstring. Returns a dict with
    keys "old", "notebook_low", "points", "notebook_top" (lists of (T, w)), and "left_out" (the
    notebook rows left out, each as (T, w, |Delta Sigma| from the points' spline)).
    """
    if len(notebook) < 2:
        raise BuildError("the notebook's table has fewer than two rows")
    T0, w0_nb = notebook[1]

    # the join: T0 must be a row of the old table, with the same w
    shared = [(T, w) for T, w in old if abs(T - T0) <= JOIN_T_RTOL * T0]
    if len(shared) != 1:
        raise BuildError(
            f"T0 = {T0!r} GeV is not exactly one row of the old table ({len(shared)} found)"
        )
    if abs(shared[0][1] - w0_nb) > JOIN_W_ATOL:
        raise BuildError(
            f"at T0 = {T0!r} GeV the old table's w {shared[0][1]!r} and the notebook's {w0_nb!r} "
            f"differ by {abs(shared[0][1] - w0_nb):.3g} > {JOIN_W_ATOL:g}"
        )

    # the 100 GeV row: the points' and the notebook's must be bitwise equal
    nb_100 = [(T, w) for T, w in notebook if T == POINTS_T_LO]
    pt_100 = [(T, w) for T, w in points if T == POINTS_T_LO]
    if len(nb_100) != 1 or len(pt_100) != 1 or nb_100[0] != pt_100[0]:
        raise BuildError(
            f"the 100 GeV row is not one row of each source, bitwise equal: "
            f"notebook {nb_100!r}, points {pt_100!r}"
        )

    parts = {
        "old": [(T, w) for T, w in old if T < T0],
        "notebook_low": [(T, w) for T, w in notebook if T0 <= T < POINTS_T_LO],
        "points": [(T, w) for T, w in points if POINTS_T_LO <= T <= POINTS_T_HI],
        "notebook_top": [(T, w) for T, w in notebook if T > POINTS_T_HI],
    }
    if len(parts["points"]) != len(points):
        raise BuildError("a point lies outside [100, 380] GeV")

    # the notebook rows left out must lie on a spline through the points
    pts = np.asarray(points, dtype=float)
    spline = make_interp_spline(np.log(pts[:, 0]), pts[:, 1])
    left_out = []
    for T, w in notebook:
        if POINTS_T_LO < T <= POINTS_T_HI:
            dSigma = abs(3.0 * (w - float(spline(np.log(T)))))
            if not dSigma <= LEFT_OUT_SIGMA_ATOL:
                raise BuildError(
                    f"the notebook's row at T = {T!r} GeV is {dSigma:.3g} in Sigma from the points' "
                    f"spline (> {LEFT_OUT_SIGMA_ATOL:g})"
                )
            left_out.append((T, w, dSigma))
    parts["left_out"] = left_out

    rows = table_rows(parts)
    Ts = [T for T, _ in rows]
    for a, b in zip(Ts, Ts[1:]):
        if not b > a:
            raise BuildError(f"T is not strictly increasing: {a!r} then {b!r}")

    return parts


def table_rows(parts: dict) -> list:
    return (
        parts["old"] + parts["notebook_low"] + parts["points"] + parts["notebook_top"]
    )


def render(rows: list) -> str:
    lines = [HEADER] + [f"{float(T)!r},{float(w)!r}" for T, w in rows]
    return "\n".join(lines) + "\n"


def build_text() -> str:
    inputs = load_inputs()
    return render(
        table_rows(compose(inputs["old"], inputs["notebook"], inputs["points"]))
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--check",
        action="store_true",
        help="build the table in memory and compare it with the installed CSV byte for byte; write nothing",
    )
    args = parser.parse_args(argv)

    try:
        inputs = load_inputs()
        parts = compose(inputs["old"], inputs["notebook"], inputs["points"])
    except BuildError as e:
        print(f"!! build_w_table: {e}")
        return 1

    text = render(table_rows(parts))
    counts = {k: len(parts[k]) for k in EXPECTED_COUNTS}
    summary = ", ".join(f"{k} {n}" for k, n in counts.items())
    n_rows = sum(counts.values())

    if args.check:
        installed = OUTPUT.read_bytes() if OUTPUT.exists() else None
        if installed == text.encode():
            print(
                f"build_w_table --check: {OUTPUT.name} is the build ({n_rows} rows: {summary})"
            )
            return 0
        print(
            f"!! build_w_table --check: {OUTPUT.name} differs from the build ({n_rows} rows)"
        )
        return 1

    OUTPUT.write_text(text)
    print(f"build_w_table: wrote {OUTPUT} ({n_rows} rows: {summary})")
    for T, _, dSigma in parts["left_out"]:
        print(
            f"  left out the notebook's row at {T:.6g} GeV: |Delta Sigma| = {dSigma:.3g}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
