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
The three exports of the one-loop drain law (one-loop-drain prompt 04, prompt §1.5-§1.7, §1.9; orchestrator note 04,
ruling 1): histories.csv's `drain_model` and drain block, fields.csv's two mass columns, and bounces.csv.

**The rule** (ruling 1), the same for every new column of every file and true in any units. Each new cell,
read back from the file, equals the value the store's own conversions give from the raw column, by `float.hex`
for a float, as a value for an integer and a string, 0 or 1 for a Boolean, and a blank cell for NULL:

    expected = to_column(from_column(raw, kind, units), kind, units)

For a column with kind None this is the raw value itself. No column is singled out and nothing here relies on a
unit being 1 (M_P = 1 in Planck units is an accident of the units): the rule is also exercised under a second
`UnitsLike` whose GeV and M_P are both different from 1, on constructed rows (`Mpc_units`). The raw columns are read
by SQL from the temporary store's file, as `test_dissipation_store` reads its rows.

- histories.csv: the header holds `drain_model` after `alpha` and `DRAIN_CSV_COLUMNS` before `failure_reasons`; a
  fixed record leaves every drain cell blank; a one-loop record writes each field (counts as integers); the sort
  with both models present, and a fixed-only file in today's order; `plot_Delta_N_eff` draws one curve per
  (model, value), fixed first;
- a one-loop history (P-2197 at A = 1, a few seconds of integration, as `test_drain_store`) read back from a
  temporary store: the drain cells, fields.csv's two mass cells per sample (matched by redshift) and every cell of
  bounces.csv obey the rule; a fixed-law history at alpha > 0 and at alpha = 0: the drain cells are blank, the
  mass cells obey the rule, there are no bounce rows to write; a one-loop history with no bounce row: bounces.csv
  holds its header alone;
- the first-sample-wise comparison with the stored column is also *measured* (how many cells differ from the raw
  column, and by how much); that is `.documents/one-loop-drain/export-04/`, never an assertion here.

No Ray cluster, no persistent datastore, no PRyMordial solve.
"""

import csv
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np

import extract_common as ec
from ComputeTargets.ScalarHistory import BounceRow, DrainHistoryValues
from ComputeTargets.tests import dissipation_histories as dh
from ObjectFactories.ScalarHistory import (
    BOUNCE_COLUMNS,
    MASS_COLUMNS,
    bounce_row_columns,
    from_column,
    to_column,
)
from ObjectFactories.tests.test_alpha_figures import _curves, _draw
from ObjectFactories.tests.test_dissipation_store import NATURAL, TEST_1
from ObjectFactories.tests.test_drain_store import (
    ONE_LOOP,
    P2197,
    _DrainStoreCase,
    one_loop_payload,
)
from Units.Mpc_units import Mpc_units

FIXED = "fixed"
BOOLEAN_FIELDS = ("resolved", "weighted")


def expected_cell(raw, kind, units):
    """Ruling 1: the value the store's own conversions give from the raw column."""
    if raw is None or isinstance(raw, (bool, int, str)):
        return raw
    return to_column(from_column(raw, kind, units), kind, units)


def check_cell(case, cell, raw, kind, units, what, boolean=False):
    """A cell read back from a file against the raw column, by ruling 1's rule."""
    if raw is None:
        case.assertEqual(cell, "", what)
        return
    if boolean:
        case.assertEqual(cell, str(int(bool(raw))), what)
        return
    expected = expected_cell(raw, kind, units)
    if isinstance(expected, float):
        case.assertEqual(float(cell).hex(), expected.hex(), what)
    elif isinstance(expected, int):
        case.assertEqual(cell, str(expected), what)
        case.assertEqual(int(cell), expected, what)
    else:
        case.assertEqual(cell, expected, what)


def read_csv(path):
    with open(path, newline="") as f:
        rows = list(csv.reader(f))
    return rows[0], [dict(zip(rows[0], row)) for row in rows[1:]]


def quiet_frame(model, units):
    """build_fields_frame with BBN and Q unavailable, as the 2026.10.0 store's histories have them in this test."""
    unavailable = SimpleNamespace(available=False)
    return ec.build_fields_frame(unavailable, unavailable, model, units)


# ---- histories.csv ----------------------------------------------------------------------------------------------


def _scalar(drain=None):
    """A stand-in ScalarHistory for build_history_record, with the drain record given."""
    scalar = SimpleNamespace(
        dissipation=None,
        tuning=mock.MagicMock(),
        extra_metadata=None,
        first_bounce=None,
        fixed_T_values=mock.MagicMock(
            phi_Einstein_1MeV=None,
            density_NP_ratio_1MeV=None,
            phi_Einstein_70keV=None,
            density_NP_ratio_70keV=None,
        ),
        drain_history=drain,
    )
    return scalar


def _record(alpha, drain_model, beta=1.0, M_Mp=0.5, n=1, scalar=None, units=None):
    return ec.build_history_record(
        beta=beta,
        M_Mp=M_Mp,
        n=n,
        phi_init_Mp=5.0,
        alpha=alpha,
        drain_model=drain_model,
        scalar=scalar,
        bbn=None,
        baseline=None,
        units=units if units is not None else mock.MagicMock(),
    )


def _drain_values(**override):
    """A constructed DrainHistoryValues: floats, counts as integers and some None."""
    fields = {}
    for k, name in enumerate(DrainHistoryValues._fields):
        if name.startswith("candidates_") or name in (
            "unresolvable_assessments",
            "unresolved_declined",
            "bounce_rows",
        ):
            fields[name] = k + 1
        else:
            fields[name] = 0.1 * (k + 1) + 1.0e-17
    fields.update(override)
    return DrainHistoryValues(**fields)


class TestHistoriesCsv(unittest.TestCase):
    def test_the_header_holds_the_model_after_alpha_and_the_drain_block_before_the_failure(
        self,
    ):
        columns = ec.CSV_COLUMNS
        self.assertEqual(columns[columns.index("alpha") + 1], "drain_model")
        block = columns.index("failure_reasons")
        self.assertEqual(
            columns[block - len(ec.DRAIN_CSV_COLUMNS) : block], ec.DRAIN_CSV_COLUMNS
        )
        self.assertEqual(ec.DRAIN_CSV_COLUMNS, list(DrainHistoryValues._fields))
        # after DISSIPATION_CSV_COLUMNS, with nothing between
        last = columns.index(ec.DISSIPATION_CSV_COLUMNS[-1])
        self.assertEqual(columns[last + 1], ec.DRAIN_CSV_COLUMNS[0])
        self.assertEqual(len(columns), len(set(columns)))

    def test_a_fixed_record_leaves_every_drain_cell_blank(self):
        for scalar in (None, _scalar(None), SimpleNamespace(dissipation=None)):
            with self.subTest(scalar=type(scalar).__name__):
                if scalar is not None and not hasattr(scalar, "tuning"):
                    continue
                record = _record(0.1, FIXED, scalar=scalar)
                with tempfile.TemporaryDirectory() as tmp:
                    path = Path(tmp) / "histories.csv"
                    ec.write_histories_csv([record], path)
                    header, rows = read_csv(path)
                self.assertEqual(header, ec.CSV_COLUMNS)
                self.assertEqual(rows[0]["drain_model"], FIXED)
                for name in ec.DRAIN_CSV_COLUMNS:
                    self.assertEqual(rows[0][name], "", name)

    def test_a_one_loop_record_writes_each_field_counts_as_integers(self):
        drain = _drain_values(ell_min=None, all_worst_birth_margin_N=None)
        record = _record(1.0, ONE_LOOP, scalar=_scalar(drain))
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "histories.csv"
            ec.write_histories_csv([record], path)
            header, rows = read_csv(path)
        self.assertEqual(rows[0]["drain_model"], ONE_LOOP)
        self.assertEqual(rows[0]["alpha"], "1.0")
        for name in ec.DRAIN_CSV_COLUMNS:
            value = getattr(drain, name)
            if value is None:
                self.assertEqual(rows[0][name], "", name)
            elif isinstance(value, int):
                self.assertEqual(rows[0][name], str(value), name)
            else:
                self.assertEqual(float(rows[0][name]).hex(), value.hex(), name)
        # the counts are integers, not 3.0
        self.assertNotIn(".", rows[0]["candidates_one_loop"])
        self.assertNotIn(".", rows[0]["bounce_rows"])

    def test_the_sort_with_both_models_and_a_fixed_only_file_in_todays_order(self):
        records = [
            _record(1.0, ONE_LOOP, beta=2.0),
            _record(0.5, ONE_LOOP, beta=1.0),
            _record(0.5, FIXED, beta=3.0),
            _record(0.0, FIXED, beta=1.0, M_Mp=1e-3),
            _record(0.5, FIXED, beta=1.0, M_Mp=0.25),
            _record(1.0, ONE_LOOP, beta=1.0, M_Mp=0.25),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "histories.csv"
            ec.write_histories_csv(records, path)
            _, rows = read_csv(path)
        self.assertEqual(
            [(r["drain_model"], r["alpha"], r["M_Mp"], r["beta"]) for r in rows],
            [
                (FIXED, "0.0", "0.001", "1.0"),
                (FIXED, "0.5", "0.25", "1.0"),
                (FIXED, "0.5", "0.5", "3.0"),
                (ONE_LOOP, "0.5", "0.5", "1.0"),
                (ONE_LOOP, "1.0", "0.25", "1.0"),
                (ONE_LOOP, "1.0", "0.5", "2.0"),
            ],
        )
        # fixed rows alone: the order of (alpha, M, n, beta) it has always had
        fixed = [r for r in records if r["drain_model"] == FIXED]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "histories.csv"
            ec.write_histories_csv(fixed, path)
            _, rows = read_csv(path)
        self.assertEqual(
            [(r["alpha"], r["M_Mp"], r["n"], r["beta"]) for r in rows],
            [
                ("0.0", "0.001", "1", "1.0"),
                ("0.5", "0.25", "1", "1.0"),
                ("0.5", "0.5", "1", "3.0"),
            ],
        )


class TestDeltaNEffUnderTwoModels(unittest.TestCase):
    def _delta_record(self, alpha, drain_model, beta, today, one_MeV):
        record = _record(alpha, drain_model, beta=beta)
        record["Delta_N_eff_today"] = today
        record["Delta_N_eff_1MeV"] = one_MeV
        return record

    def test_the_same_value_under_both_models_makes_two_curves_fixed_first(self):
        records = [
            self._delta_record(0.5, ONE_LOOP, 1.0, 0.030, 0.029),
            self._delta_record(0.5, ONE_LOOP, 2.0, 0.020, 0.019),
            self._delta_record(0.5, FIXED, 2.0, 0.060, 0.059),
            self._delta_record(0.5, FIXED, 1.0, 0.070, 0.069),
            self._delta_record(0.1, FIXED, 1.0, 0.010, 0.009),
            self._delta_record(1.0, ONE_LOOP, 1.0, 0.040, 0.039),
            self._delta_record(0.0, FIXED, 1.0, 0.0, 0.0),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "Delta_N_eff.pdf"
            wrote, fig, _ = _draw(ec.plot_Delta_N_eff, records, path, "t")
            self.assertTrue(wrote)
            self.assertTrue(path.is_file())
        upper, lower = fig.axes
        labels = [l.get_label() for l in upper.lines]
        self.assertEqual(
            labels,
            [
                r"$\alpha$ = 0.1",
                r"$\alpha$ = 0.5",
                "A = 0.5 (one-loop-measured-v1)",
                "A = 1 (one-loop-measured-v1)",
            ],
        )
        self.assertEqual([l.get_label() for l in lower.lines], labels)
        curves = _curves(upper)
        # the same value 0.5 under the two models is two different curves
        self.assertEqual(curves[r"$\alpha$ = 0.5"], ([1.0, 2.0], [0.070, 0.060]))
        self.assertEqual(
            curves["A = 0.5 (one-loop-measured-v1)"], ([1.0, 2.0], [0.030, 0.020])
        )

    def test_a_one_loop_record_alone_is_drawn_and_alpha_zero_alone_is_not(self):
        with tempfile.TemporaryDirectory() as tmp:
            wrote, fig, _ = _draw(
                ec.plot_Delta_N_eff,
                [self._delta_record(2.0, ONE_LOOP, 1.0, 0.1, 0.09)],
                Path(tmp) / "d.pdf",
                "",
            )
            self.assertTrue(wrote)
            self.assertEqual(
                [l.get_label() for l in fig.axes[0].lines],
                ["A = 2 (one-loop-measured-v1)"],
            )
            wrote, fig, _ = _draw(
                ec.plot_Delta_N_eff,
                [self._delta_record(0.0, FIXED, 1.0, 0.0, 0.0)],
                Path(tmp) / "e.pdf",
                "",
            )
            self.assertFalse(wrote)


# ---- the writers under a second UnitsLike, on constructed rows -----------------------------------------------------


def _constructed_bounce_rows():
    """Three constructed rows exercising every kind of column: a candidate, a candidate with None fields (an
    unassessed one), and a plateau event; values with no relation to any unit."""
    full = dict(
        ordinal=0,
        source="candidate",
        candidate_index=0,
        N=21.25,
        N_turn=21.0,
        kind="reflection",
        resolved=True,
        log_T_Jordan=-3.2,
        delta_K=1.5e-12,
        log_rho_kin=-31.0,
        nonadiabaticity=0.37,
        alpha_eff=0.6,
        w_gate=0.9,
        K_before=2.5e-13,
        regime="one_loop",
        ell=3.5,
        L=12.0,
        log_R=1.75,
        log_E=-40.5,
        log_H=-45.25,
        log_k_ex=-44.0,
        log_m=-43.0,
        log_q=-47.5,
        share=0.125,
        weighted=True,
        birth_margin=5.5,
        reflection_margin=4.5,
        reflection_margin_N=21.5,
        tracker_margin=3.5,
        tracker_margin_N=22.5,
    )
    unassessed = dict(
        full,
        ordinal=1,
        candidate_index=1,
        N=23.0,
        regime="unassessed",
        resolved=False,
        weighted=False,
        ell=None,
        L=None,
        log_R=None,
        log_E=None,
        log_H=None,
        log_k_ex=None,
        log_q=None,
        share=None,
        birth_margin=None,
    )
    event = {name: None for name in BounceRow._fields}
    event.update(
        ordinal=2,
        source="plateau",
        N=24.0,
        log_T_Jordan=-4.0,
        log_m=-42.0,
    )
    return (BounceRow(**full), BounceRow(**unassessed), BounceRow(**event))


def _constructed_values(units, with_masses=True):
    """Two constructed samples of a ScalarHistoryValue's attributes, as fields.csv reads them."""
    values = []
    for k in range(2):
        z = SimpleNamespace(store_id=10 + k, z=10.0 ** (3 - k), log_one_plus_z=8.0 - k)
        values.append(
            SimpleNamespace(
                z=z,
                log_one_plus_z_Jordan=8.1 - k,
                raw_N=1.0 + k,
                phi_Einstein=0.5 * units.PlanckMass,
                pi_Einstein=0.25 * units.PlanckMass,
                H_Einstein=1.0e-3 * units.PlanckMass,
                H_Jordan=2.0e-3 * units.PlanckMass,
                log_rhorad_Einstein=-40.0 + 4.0 * np.log(units.GeV),
                log_rhorad_Jordan=-41.0 + 4.0 * np.log(units.GeV),
                log_fm=-3.0,
                log_T_Jordan=-30.0 + np.log(units.GeV) + k,
                gstar_rho=100.0,
                gstar_s=99.0,
                dgstar_rho_dlogT=0.5,
                dgstar_s_dlogT=0.4,
                Sigma=0.2,
                friction_term=1.0e-4 * units.PlanckMass,
                reflecting_term=2.0e-4 * units.PlanckMass,
                kicking_term=3.0e-4 * units.PlanckMass,
                rho_chi_over_rho_rad=0.01 * k,
                log_m_eff=-20.5 + 0.25 * k + np.log(units.PlanckMass),
                log_m_tracker=None if k == 1 else -21.0 + np.log(units.PlanckMass),
            )
        )
    return values


class TestTheWritersUnderASecondUnitsLike(unittest.TestCase):
    """Ruling 1's test of units: Mpc_units has GeV and M_P both different from 1 (so does Planck_units for GeV,
    but its M_P is 1, which is the accident the rule does not lean on)."""

    def units(self):
        units = Mpc_units()
        self.assertNotEqual(units.GeV, 1.0)
        self.assertNotEqual(units.PlanckMass, 1.0)
        return units

    def test_every_bounces_cell_is_bounce_row_columns_by_float_hex(self):
        units = self.units()
        rows = _constructed_bounce_rows()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "csv" / "bounces.csv"
            ec.write_bounces_csv(rows, units, path)
            header, cells = read_csv(path)
        # the stored columns, in the table's order, without history_serial
        self.assertEqual(header, [column for _, column, _ in BOUNCE_COLUMNS])
        self.assertNotIn("history_serial", header)
        self.assertEqual(len(cells), 3)
        compared = 0
        for row, line in zip(rows, cells):
            stored = bounce_row_columns(row, units)
            for field, column, kind in BOUNCE_COLUMNS:
                value = stored[column]
                if value is None:
                    self.assertEqual(line[column], "", column)
                elif isinstance(value, bool):
                    self.assertEqual(line[column], str(int(value)), column)
                elif isinstance(value, float):
                    self.assertEqual(
                        float(line[column]).hex(), value.hex(), f"{column}"
                    )
                elif isinstance(value, int):
                    self.assertEqual(line[column], str(value), column)
                else:
                    self.assertEqual(line[column], value, column)
                compared += 1
        self.assertEqual(compared, 3 * len(BOUNCE_COLUMNS))
        # the conversions really are not the identity here (a column holding a log of an M_P-valued quantity)
        converted = bounce_row_columns(rows[0], units)
        self.assertNotEqual(converted["log_H_Mp"], rows[0].log_H)
        self.assertNotEqual(converted["log_T_Jordan_GeV"], rows[0].log_T_Jordan)
        # the Booleans are 0 and 1, None a blank cell
        self.assertEqual((cells[0]["resolved"], cells[0]["weighted"]), ("1", "1"))
        self.assertEqual((cells[1]["resolved"], cells[1]["weighted"]), ("0", "0"))
        self.assertEqual((cells[2]["resolved"], cells[2]["weighted"]), ("", ""))
        self.assertEqual(cells[2]["candidate_index"], "")

    def test_the_mass_cells_are_to_column_of_the_value_by_float_hex(self):
        units = self.units()
        values = _constructed_values(units)
        model = SimpleNamespace(available=True, values=values)
        frame = quiet_frame(model, units)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fields.csv"
            frame.to_csv(path, header=True, index=False)
            header, cells = read_csv(path)
        names = [column for _, column, _ in MASS_COLUMNS]
        self.assertEqual(
            header[header.index("Delta_gstar") + 1 : header.index("Delta_gstar") + 3],
            names,
        )
        by_axis = {float(c["log_one_plus_z"]): c for c in cells}
        for value in values:
            line = by_axis[value.z.log_one_plus_z]
            for attribute, column, kind in MASS_COLUMNS:
                x = getattr(value, attribute)
                if x is None:
                    self.assertEqual(line[column], "", column)
                else:
                    self.assertEqual(
                        float(line[column]).hex(),
                        to_column(x, kind, units).hex(),
                        column,
                    )
        # not the identity: ln m / M_P differs from ln m here
        self.assertNotEqual(
            to_column(values[0].log_m_eff, MASS_COLUMNS[0][2], units),
            values[0].log_m_eff,
        )

    def test_the_existing_columns_are_arithmetic_of_before_in_these_units(self):
        """phi/M_P, ln rho - 4 ln GeV and T/GeV are the old arithmetic, whatever the units."""
        units = self.units()
        values = _constructed_values(units)
        frame = quiet_frame(SimpleNamespace(available=True, values=values), units)
        row = frame.sort_values("log_one_plus_z", ascending=False).iloc[0]
        first = max(values, key=lambda v: v.z.log_one_plus_z)
        self.assertEqual(row["phi_Einstein_Mp"], first.phi_Einstein / units.PlanckMass)
        self.assertEqual(
            row["log_rhorad_Einstein_GeV4"],
            first.log_rhorad_Einstein - 4.0 * np.log(units.GeV),
        )


# ---- the exports of stored histories ------------------------------------------------------------------------------


class _ExportCase(_DrainStoreCase):
    """Writes histories to the temporary store and reads them back; checks the exports by ruling 1's rule."""

    def populated(self, got):
        return got

    def check_drain_cells(self, got, serial, drain_model, alpha):
        raw = self.raw(serial)
        scalar = SimpleNamespace(
            dissipation=None,
            tuning=mock.MagicMock(),
            extra_metadata=None,
            first_bounce=None,
            fixed_T_values=mock.MagicMock(
                phi_Einstein_1MeV=None,
                density_NP_ratio_1MeV=None,
                phi_Einstein_70keV=None,
                density_NP_ratio_70keV=None,
            ),
            drain_history=got.drain_history,
        )
        record = _record(alpha, drain_model, scalar=scalar, units=self.units)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "histories.csv"
            ec.write_histories_csv([record], path)
            _, rows = read_csv(path)
        line = rows[0]
        self.assertEqual(line["drain_model"], drain_model)
        for name in ec.DRAIN_CSV_COLUMNS:
            check_cell(self, line[name], raw[name], None, self.units, name)
        return line

    def check_mass_cells(self, got, serial):
        raw = self.raw_samples(serial)
        frame = quiet_frame(got, self.units)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "fields.csv"
            frame.to_csv(path, header=True, index=False)
            _, cells = read_csv(path)
        by_axis = {float(c["log_one_plus_z"]): c for c in cells}
        self.assertEqual(len(by_axis), len(raw))
        compared = 0
        for value in got.values:
            line = by_axis[value.z.log_one_plus_z]
            for attribute, column, kind in MASS_COLUMNS:
                check_cell(
                    self,
                    line[column],
                    raw[value.z.store_id][column],
                    kind,
                    self.units,
                    f"z={value.z.store_id} {column}",
                )
                compared += 1
        self.assertEqual(compared, 2 * len(raw))
        return compared

    def check_bounces_csv(self, got, serial):
        raw = self.raw_bounces(serial)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bounces.csv"
            ec.write_bounces_csv(got.bounces, self.units, path)
            header, cells = read_csv(path)
        self.assertEqual(header, [column for _, column, _ in BOUNCE_COLUMNS])
        self.assertEqual(len(cells), self.raw(serial)["bounce_rows"])
        self.assertEqual(len(cells), len(raw))
        for k, (line, stored) in enumerate(zip(cells, raw)):
            self.assertEqual(stored["ordinal"], k)
            self.assertEqual(int(line["ordinal"]), k)
            for field, column, kind in BOUNCE_COLUMNS:
                check_cell(
                    self,
                    line[column],
                    stored[column],
                    kind,
                    self.units,
                    f"row {k} {column}",
                    boolean=field in BOUNCE_FIELDS_BOOLEAN,
                )
        return len(cells) * len(BOUNCE_COLUMNS)


BOUNCE_FIELDS_BOOLEAN = BOOLEAN_FIELDS


class TestAOneLoopHistoryExports(_ExportCase):
    """P-2197 at A = 1 (about 2-4 s of integration, as test_drain_store): the three exports against the raw
    columns."""

    def test_the_three_exports_obey_the_rule(self):
        payload = one_loop_payload(*P2197)
        _, serial = self.write_one_loop(P2197, payload)
        got = self.read_one_loop(P2197)
        self.assertTrue(got.available)
        # histories.csv's drain cells
        line = self.check_drain_cells(got, serial, ONE_LOOP, 1.0)
        self.assertEqual(line["alpha"], "1.0")
        self.assertGreater(int(line["bounce_rows"]), 0)
        self.assertGreater(int(line["candidates_one_loop"]), 0)
        # fields.csv's mass cells, per sample matched by redshift
        self.assertGreater(self.check_mass_cells(got, serial), 0)
        # bounces.csv: bounce_rows rows in ordinal order, every cell
        cells = self.check_bounces_csv(got, serial)
        self.assertEqual(cells, int(line["bounce_rows"]) * len(BOUNCE_COLUMNS))

    def test_a_one_loop_history_with_no_bounce_row_has_a_header_alone(self):
        payload = one_loop_payload(*P2197)
        payload = dict(payload)
        payload["bounces"] = type(payload["bounces"])(
            rows=(), K_final=payload["bounces"].K_final
        )
        _, serial = self.write_one_loop(P2197, payload)
        got = self.read_one_loop(P2197)
        self.assertEqual(got.bounces, ())
        self.assertEqual(self.raw(serial)["bounce_rows"], 0)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bounces.csv"
            ec.write_bounces_csv(got.bounces, self.units, path)
            with open(path, newline="") as f:
                rows = list(csv.reader(f))
        self.assertEqual(rows, [[column for _, column, _ in BOUNCE_COLUMNS]])


class TestAFixedLawHistoryExports(_ExportCase):
    """The fixed law at alpha > 0 and at alpha = 0: the drain cells are blank, the mass cells obey the rule, and
    there are no bounce rows to write."""

    def check(self, key):
        payload = dh.payload(*key)
        _, serial = self.write(payload, *key)
        got = self.read(*key)
        line = self.check_drain_cells(got, serial, FIXED, key[2])
        for name in ec.DRAIN_CSV_COLUMNS:
            self.assertEqual(line[name], "", name)
        self.assertGreater(self.check_mass_cells(got, serial), 0)
        self.assertEqual(got.bounces, ())
        self.assertEqual(got.alpha.drain_law, "fixed")
        # the writer would write the header alone; the driver does not call it under the fixed law (by ast, in
        # test_a_values_option / TestTheDriverWritesBouncesForOneLoopOnly)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bounces.csv"
            ec.write_bounces_csv(got.bounces, self.units, path)
            self.assertEqual(len(read_csv(path)[1]), 0)

    def test_alpha_positive(self):
        self.check(TEST_1)

    def test_alpha_zero(self):
        self.check(NATURAL)


class TestTheDriverWritesBouncesForOneLoopOnly(unittest.TestCase):
    def test_bounces_csv_is_written_under_the_one_loop_law_beside_fields_csv(self):
        import ast

        root = Path(__file__).resolve().parents[2]
        tree = ast.parse((root / "plot_ScalarHistory.py").read_text())
        writers = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Call) and ast.unparse(n.func) == "write_bounces_csv"
        ]
        self.assertEqual(len(writers), 1)
        guards = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.If) and any(w is writers[0] for w in ast.walk(n))
        ]
        self.assertEqual(
            [ast.unparse(g.test) for g in guards],
            ["model.alpha.drain_law == DRAIN_LAW_ONE_LOOP"],
        )
        self.assertEqual(ast.unparse(writers[0].args[2]), "csv_path / 'bounces.csv'")
        # fields.csv is written to csv_path too, by write_csv_file
        self.assertIn("csv_path / 'fields.csv'", ast.unparse(tree))


if __name__ == "__main__":
    unittest.main()
