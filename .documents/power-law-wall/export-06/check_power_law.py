"""
The gate Export's second item (power-law-wall prompt 06, §2.2): the tip's power-law exports carry the potential and read
back. On the store of build_store.py, as export_tree.py wrote the tip's exports:

    PYTHONPATH=. ./venv/bin/python .documents/power-law-wall/export-06/check_power_law.py \\
        var/power-law-wall/06/store var/power-law-wall/06/export-tip

- the root is `<label>/inverse-power/`; its histories.csv has `model` directly after `n`, `inverse-power` in every row,
  and holds the power law's histories only: one row per power-law history of the manifest, and the grid's marker rows
  (a stored model with no history under a row's key), and no exponential row;
- each history's row equals the stored record, by export-04's rule (check_new_columns.py, ruling 1): for every column
  that histories.csv copies from the ScalarHistory row unconverted (the tuning's dimensionless outputs, the two
  Delta N_eff, the dissipation record's dimensionless fields, the drain record, bounce_rows), a float cell equals the
  raw column by `float.hex`, an integer as a value, a blank cell stands for NULL; the key cells (beta, M_Mp, n, alpha,
  drain_model) are the manifest's; a failure's failure_reasons is its stored failure_reason;
- the history's model in the store is an InversePowerModel at its n (model_type 2, and the model row's n_value), and
  its alpha_value row's drain model is the row's drain_model: `one-loop-measured-pl-v1` under the one-loop law;
- each successful one-loop history has bounces.csv with bounce_rows rows, and each fixed-law history none.

A failure is a stop (exit 1). Writes out/power_law.txt beside this script.
"""

import csv
import json
import sqlite3
import sys
from pathlib import Path

import extract_common as ec
from Units import Planck_units
from extract_common import alpha_path, history_directory

LABEL = "QCD_Cosmology"
POWER = "inverse-power"
MARKER = "no ScalarHistory row in the store"

# the columns histories.csv copies from the ScalarHistory row unconverted (extract_common.build_history_record)
DIRECT = [
    "normalization",
    "tracking",
    "residual_H",
    "residual_phi",
    "tuning_iterations",
    "Delta_N_eff_today",
    "Delta_N_eff_1MeV",
    "injections",
    "injected_fraction",
    "log_R_first_impact",
    "birth_failing_share",
    "reflection_failing_share",
    "tracker_failing_share",
    "unassessed_share",
    *ec.DRAIN_CSV_COLUMNS,
]


def read_table(path: Path):
    with open(path, newline="") as f:
        rows = list(csv.reader(f))
    return rows[0], [dict(zip(rows[0], row)) for row in rows[1:]]


def cell_equals(cell: str, raw) -> bool:
    if raw is None:
        return cell == ""
    if isinstance(raw, float):
        return cell != "" and float(cell).hex() == raw.hex()
    if isinstance(raw, int):
        return cell == str(raw)
    return cell == raw


def main(store_dir: Path, tip_out: Path) -> int:
    lines = []

    def say(text=""):
        lines.append(text)
        print(text)

    stop = False
    units = Planck_units()
    manifest = json.loads((store_dir.parent / "manifest.json").read_text())
    root = tip_out / LABEL / POWER
    con = sqlite3.connect(f"file:{store_dir / 'store-shard0000.db'}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row

    header, rows = read_table(root / "histories.csv")
    k = header.index("model")
    say(f"root: {root.relative_to(tip_out)}")
    say(
        f"histories.csv: {len(rows)} rows; header is CSV_COLUMNS: {header == ec.CSV_COLUMNS}; model at column {k}, "
        f"after n: {header[k - 1] == 'n'}; model cells {sorted({r['model'] for r in rows})}"
    )
    stop |= header != ec.CSV_COLUMNS or header[k - 1] != "n"
    stop |= {r["model"] for r in rows} != {POWER}

    specs = [s for s in manifest if s["model"] == POWER]
    compared = 0
    failures = []
    matched = set()
    for spec in specs:
        matching = [
            i
            for i, r in enumerate(rows)
            if r["drain_model"] == spec["drain_model"]
            and float(r["M_Mp"]) == spec["M_Mp"]
            and float(r["beta"]) == spec["beta"]
            and float(r["alpha"]) == spec["value"]
            and int(r["n"]) == spec["n"]
        ]
        if len(matching) != 1:
            failures.append((spec["name"], "rows", len(matching), 1))
            continue
        matched.add(matching[0])
        row = rows[matching[0]]
        raw = con.execute(
            "select * from ScalarHistory where serial = ?", (spec["serial"],)
        ).fetchone()
        # the stored key: the model's type and n, the alpha row's drain model
        model = con.execute(
            "select n_value from InversePowerModel where serial = ?",
            (raw["model_serial"],),
        ).fetchone()
        alpha = con.execute(
            "select value, drain_model from alpha_value where serial = ?",
            (raw["alpha_serial"],),
        ).fetchone()
        for what, got, want in (
            ("model_type", raw["model_type"], 2),
            ("InversePowerModel.n_value", model["n_value"], spec["n"]),
            ("alpha_value.drain_model", alpha["drain_model"], spec["drain_model"]),
            ("alpha_value.value", alpha["value"], spec["value"]),
        ):
            compared += 1
            if got != want:
                failures.append((spec["name"], what, got, want))
        if spec["failure"]:
            compared += 1
            if row["failure_reasons"] != raw["failure_reason"]:
                failures.append(
                    (
                        spec["name"],
                        "failure_reasons",
                        row["failure_reasons"],
                        raw["failure_reason"],
                    )
                )
            for column in DIRECT:
                compared += 1
                if row[column] != "":
                    failures.append((spec["name"], column, row[column], None))
            continue
        for column in DIRECT + ["bounce_rows"]:
            compared += 1
            if not cell_equals(row[column], raw[column]):
                failures.append((spec["name"], column, row[column], raw[column]))
        # bounces.csv: for a one-loop history, bounce_rows rows; for a fixed-law one, none
        d = (
            alpha_path(root, spec["value"], spec["drain_model"])
            / "csv"
            / history_directory(
                spec["beta"], spec["M_Mp"] * units.PlanckMass / units.eV, spec["n"]
            )
        )
        compared += 1
        if spec["drain_model"] == "fixed":
            if (d / "bounces.csv").exists():
                failures.append((spec["name"], "bounces.csv", "present", "absent"))
        else:
            _, b = read_table(d / "bounces.csv")
            if len(b) != raw["bounce_rows"]:
                failures.append(
                    (spec["name"], "bounces.csv rows", len(b), raw["bounce_rows"])
                )
        compared += 1
        if not (d / "fields.csv").exists():
            failures.append((spec["name"], "fields.csv", "absent", "present"))
    others = [r for i, r in enumerate(rows) if i not in matched]
    say(
        f"the manifest's {len(specs)} power-law histories: each one row; {compared} cells and facts compared against "
        f"the store"
    )
    for r in others:
        say(
            f"   another row: beta={r['beta']} n={r['n']} {r['drain_model']} {r['alpha']}: "
            f"failure_reasons {r['failure_reasons']!r}, normalization {r['normalization']!r}"
        )
        if not (r["failure_reasons"] == MARKER and r["normalization"] == ""):
            failures.append(("unexpected row", r["beta"], r["n"], r["drain_model"]))
    say(f"failures: {len(failures)}")
    for f in failures[:20]:
        say(f"   FAIL {f}")
    stop |= len(failures) > 0
    say()
    say(f"STOP CONDITION HIT: {stop}")
    out = Path(__file__).parent / "out"
    out.mkdir(exist_ok=True)
    (out / "power_law.txt").write_text("\n".join(lines) + "\n")
    return 1 if stop else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]), Path(sys.argv[2])))
