"""
The Export gate's first item (one-loop-drain prompt 04, §2.1; orchestrator note 04, ruling 1): every new cell of the
tip's histories.csv, fields.csv and bounces.csv, read back from the files of export_tree.py, against the raw column of
the store, read by SQL from the store's file (mode=ro).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/export-04/check_new_columns.py \\
        var/one-loop-drain/04/store var/one-loop-drain/04/export-tip

**The rule** (ruling 1), the same for every column of every file and true in any units:

    expected = to_column(from_column(raw, kind, units), kind, units)

a float cell equals `expected` by `float.hex`, an integer or a string as a value, a Boolean cell is 0 or 1, and a
blank cell stands for NULL. A cell that fails is a stop (exit 1).

**The measurement** (never a stop): per column and in the units the store was built with, how many cells differ from the
raw column by `float.hex` (that is, how many cells are not the raw value itself) and the largest |cell - raw|.
"""

import csv
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

import extract_common as ec
from ComputeTargets.tests import dissipation_histories as dh
from ObjectFactories.ScalarHistory import (
    BOUNCE_COLUMNS,
    MASS_COLUMNS,
    from_column,
    to_column,
)

BOOLEANS = ("resolved", "weighted")


def read_table(path: Path):
    with open(path, newline="") as f:
        rows = list(csv.reader(f))
    return rows[0], [dict(zip(rows[0], row)) for row in rows[1:]]


class Tally:
    def __init__(self):
        self.compared = 0
        self.failures = []
        self.per_column = defaultdict(
            lambda: [0, 0, 0.0]
        )  # compared, differing from raw, max |cell - raw|

    def check(self, label, column, cell, raw, kind, units, boolean=False):
        self.compared += 1
        entry = self.per_column[column]
        entry[0] += 1
        if raw is None:
            ok = cell == ""
        elif boolean:
            ok = cell == str(int(bool(raw)))
        elif isinstance(raw, float):
            expected = to_column(from_column(raw, kind, units), kind, units)
            ok = float(cell).hex() == expected.hex()
            if float(cell).hex() != raw.hex():
                entry[1] += 1
            entry[2] = max(entry[2], abs(float(cell) - raw))
        elif isinstance(raw, int):
            ok = cell == str(raw)
        else:
            ok = cell == raw
        if not ok:
            self.failures.append((label, column, cell, raw))


def main(store_dir: Path, tip: Path):
    manifest = json.loads((store_dir.parent / "manifest.json").read_text())
    units = dh.cosmology().units
    con = sqlite3.connect(f"file:{store_dir / 'store-shard0000.db'}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    lines = []

    def say(text=""):
        lines.append(text)
        print(text)

    say(f"units of the store: GeV = {units.GeV!r}, M_P = {units.PlanckMass!r}")
    stop = False
    totals = {}

    # ---- histories.csv ------------------------------------------------------------------------------
    tally = Tally()
    for kind in ("histories_fixed.csv", "histories_one_loop.csv"):
        header, rows = read_table(tip / kind)
        assert header == ec.CSV_COLUMNS, kind
        for spec in manifest:
            matching = [
                r
                for r in rows
                if r["drain_model"] == spec["drain_model"]
                and float(r["M_Mp"]) == spec["M_Mp"]
                and float(r["beta"]) == spec["beta"]
                and float(r["alpha"]) == spec["value"]
            ]
            if len(matching) != 1:
                continue
            row = matching[0]
            raw = con.execute(
                "select * from ScalarHistory where serial = ?", (spec["serial"],)
            ).fetchone()
            for column in ec.DRAIN_CSV_COLUMNS:
                tally.check(spec["name"], column, row[column], raw[column], None, units)
            # the model column is a string, the stored key's
            tally.compared += 1
            if row["drain_model"] != spec["drain_model"]:
                tally.failures.append(
                    (
                        spec["name"],
                        "drain_model",
                        row["drain_model"],
                        spec["drain_model"],
                    )
                )
    totals["histories.csv"] = tally
    say(
        f"histories.csv: {tally.compared} cells compared (the drain block of {len(ec.DRAIN_CSV_COLUMNS)} columns and "
        f"drain_model, for every history of the manifest: {len(manifest)} rows including 2 failures)"
    )

    # ---- fields.csv ---------------------------------------------------------------------------------
    tally = Tally()
    for spec in manifest:
        if spec["failure"]:
            continue
        header, rows = read_table(tip / "fields" / spec["name"] / "fields.csv")
        assert header[
            header.index("Delta_gstar") + 1 : header.index("Delta_gstar") + 3
        ] == [c for _, c, _ in MASS_COLUMNS]
        raw = {}
        for r in con.execute(
            "select r.log_one_plus_z as axis, v.log_m_eff_Mp, v.log_m_tracker_Mp from ScalarHistoryValue v "
            "join redshift r on r.serial = v.z_serial where v.history_serial = ?",
            (spec["serial"],),
        ):
            raw[float(r["axis"]).hex()] = r
        assert len(raw) == len(rows), (spec["name"], len(raw), len(rows))
        for row in rows:
            record = raw[float(row["log_one_plus_z"]).hex()]
            for attribute, column, kind in MASS_COLUMNS:
                tally.check(
                    spec["name"], column, row[column], record[column], kind, units
                )
    totals["fields.csv"] = tally
    say(
        f"fields.csv: {tally.compared} cells compared (two mass columns per sample, matched by redshift, "
        f"over {sum(1 for s in manifest if not s['failure'])} histories)"
    )

    # ---- bounces.csv --------------------------------------------------------------------------------
    tally = Tally()
    n_rows = 0
    for spec in manifest:
        if spec["failure"] or spec["drain_model"] == "fixed":
            assert not (tip / "bounces" / spec["name"]).exists(), spec["name"]
            continue
        header, rows = read_table(tip / "bounces" / spec["name"] / "bounces.csv")
        assert header == [c for _, c, _ in BOUNCE_COLUMNS]
        raw = con.execute(
            "select * from ScalarHistoryBounce where history_serial = ? order by ordinal",
            (spec["serial"],),
        ).fetchall()
        bounce_rows = con.execute(
            "select bounce_rows from ScalarHistory where serial = ?", (spec["serial"],)
        ).fetchone()[0]
        assert len(raw) == len(rows) == bounce_rows, (
            spec["name"],
            len(raw),
            len(rows),
            bounce_rows,
        )
        n_rows += len(rows)
        for k, (row, stored) in enumerate(zip(rows, raw)):
            assert int(row["ordinal"]) == stored["ordinal"] == k
            for field, column, kind in BOUNCE_COLUMNS:
                tally.check(
                    spec["name"],
                    column,
                    row[column],
                    stored[column],
                    kind,
                    units,
                    boolean=field in BOOLEANS,
                )
    totals["bounces.csv"] = tally
    say(
        f"bounces.csv: {tally.compared} cells compared ({n_rows} rows of {len(BOUNCE_COLUMNS)} columns, "
        f"in ordinal order, over the one-loop histories)"
    )

    # ---- the verdict and the measurement ---------------------------------------------------------------
    say()
    for name, tally in totals.items():
        say(f"{name}: failures of the rule: {len(tally.failures)}")
        for failure in tally.failures[:10]:
            say(f"   FAIL {failure}")
        stop |= len(tally.failures) > 0
    say()
    say(
        "measurement (ruling 1; in the units the store was built with, never a stop): per column, cells compared,"
    )
    say(
        "cells differing from the raw column by float.hex, and the largest |cell - raw|"
    )
    for name, tally in totals.items():
        say(f"-- {name}")
        differing_total = 0
        for column, (compared, differing, largest) in tally.per_column.items():
            differing_total += differing
            if differing:
                say(
                    f"   {column:<28} compared {compared:>7}  differ {differing:>6}  max|cell-raw| {largest:.3e}"
                )
        say(
            f"   cells differing from raw, all columns: {differing_total} of {tally.compared}"
        )
        flat = [c for c, v in tally.per_column.items() if v[1] == 0 and v[0] > 0]
        say(
            f"   columns with no differing cell: {len(flat)} of {len(tally.per_column)}"
        )
    say()
    say(f"STOP CONDITION HIT: {stop}")
    (Path(__file__).parent / "out" / "new_columns.txt").write_text(
        "\n".join(lines) + "\n"
    )
    return 1 if stop else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]), Path(sys.argv[2])))
