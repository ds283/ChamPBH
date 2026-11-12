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
A history in dissipation mode through the store (inelastic-reflection prompt 09, §1.9 tests 1-7
and 10; README §8.1 R22-R25, §8.2 P27, P28, P30, P31).

α is a key of every history, through its own replicated table `alpha_value`. The payload's
per-sample ratio ρ_χ/ρ_R (`"rho_chi_over_rho_rad"`) is a column of `ScalarHistoryValue`. The
whole-history values (the counts, the injected fraction, the ratios and ΔN_eff at 1 MeV, 70 keV and
today, ln 𝓡 at the first impact, the parking and the release, the margins of R25) are columns of
`ScalarHistory`, read back with `_do_not_populate` as one record, `ScalarHistory.dissipation`. The
margins' first failures and counts are under `extra_data["margins"]`, a `Text` column (R23).

Each test goes the whole way on a temporary SQLite file through `_TempStoreCase`, as
`test_tuned_history_round_trip` does: the parameter rows, the α rows and the model rows are made by
the factories (`object_get`); `ScalarHistory.store()` is run with `ray.wait` and `ray.get` patched to
hand it the payload; the factory's `store` writes the row and its samples with the datastore's own
inserters; the row is read back through `object_get`.

The histories (`ComputeTargets/tests/dissipation_histories.py`) are `test_injection`'s setup,
untuned, at the model's seed realization:
- M = 0.5 M_P, β = 2, α = 0.1 (tests 1, 3, 6, 7) and α = 0 (tests 3 and 4);
- M = 10⁻³⁰ M_P, β = 2, α = 0.1, parked and not released (test 2);
- M = 0.5 M_P, β = 5, α = 0.1, parked and released (test 2).

- **1** every column of prompt §1.3, raw (the row) and on the object, bitwise; every sample's ratio
  bitwise against the payload's key and the ledger; `extra_data["margins"]` equal to the payload's
  `Margins` and first failures; the same read with `_do_not_populate`;
- **2** the parking and release columns of a parked and of a released history;
- **3** two α are two rows; α = 0.3 finds neither;
- **4** an α = 0 row holds R24's zeros and NULLs, and its samples' ratios are 0.0;
- **5** a failure row holds every new column NULL and reads back as a failure;
- **6** ΔN_eff at 1 MeV and today against a hand computation (R22), from the cosmology's `G_rho`;
- **7** `extra_data` longer than 256 characters round-trips whole, and the column is `Text` in the
  reflected schema;
- **10** the version: `VERSION_LABEL` is 2026.11.0 (2026.8.0 until reflection-restart prompt 03, which
  moved it without a schema change, 2026.9.0 until one-loop-drain prompt 03, and 2026.10.0 until electroweak-eos
  prompt 01, which moved it with the w table and no schema change) and `FINGERPRINT_FORMAT` is 7 (6 until
  one-loop-drain prompt 03).

"Bitwise" for a converted column (ln T_J, stored with T_J in GeV) is ruling 7's: the row equals the
forward conversion of the payload's value bitwise, and the object equals the inverse conversion of
the row bitwise.

No Ray cluster, no persistent datastore, no PRyMordial solve. Cost: four integrations of 2-6 s each
unloaded (shared through the cache of `dissipation_histories`, so a run of every module integrates
each once), about 25 s at a load of 10; then 1-3 s per test for the 5000-6000 samples written and
read back.
"""

import json
import unittest
from math import exp, log
from unittest import mock

import sqlalchemy as sqla

from ComputeTargets.tests import dissipation_histories as dh
from ObjectFactories.tests.test_version_keyed_lookups import LABEL_A, _TempStoreCase

SM = dh.SM

# the histories
TEST_1 = (2.0, 0.5, 0.1)
NATURAL = (2.0, 0.5, 0.0)
PARKED = (2.0, 1.0e-30, 0.1)
RELEASED = (5.0, 0.5, 0.1)

# the new columns of ScalarHistory, in the table's order (prompt §1.3; orchestrator note 09, ruling 8)
COLUMNS = SM.DissipationValues._fields

# the columns stored with T_J in GeV (ruling 7); every other is stored as the payload has it
CONVERTED = ("parking_log_T_Jordan", "release_log_T_Jordan")

# R24: at alpha = 0 these hold 0 or 0.0, and every other new column is NULL
ZERO_AT_ALPHA_0 = {
    "injections": 0,
    "resolved_candidates": 0,
    "unresolved_candidates": 0,
    "injected_fraction": 0.0,
    "rho_chi_ratio_1MeV": 0.0,
    "rho_chi_ratio_70keV": 0.0,
    "rho_chi_ratio_today": 0.0,
    "Delta_N_eff_1MeV": 0.0,
    "Delta_N_eff_today": 0.0,
}

MARGIN_COLUMNS = tuple(
    f"{prefix}{kind}{suffix}"
    for kind in ("birth", "reflection", "tracker")
    for prefix, suffix in (
        ("worst_", "_margin"),
        ("worst_", "_margin_N"),
        ("", "_failing_share"),
    )
) + ("unassessed_share",)

MARGIN_JSON_KEYS = (
    "first_birth_failure",
    "first_reflection_failure",
    "first_tracker_failure",
    "worst_birth_margin_index",
    "worst_reflection_margin_index",
    "worst_tracker_margin_index",
    "classical_generations",
    "one_loop_generations",
    "classical_gap_generations",
    "weighted_generations",
    "unassessed_candidates",
    "weighted_share",
    "below_weight_share",
)


def expected_columns(payload: dict, units) -> dict:
    """
    Every new column of a success row, as the row should hold it, from the payload's records by the
    definitions of prompt §1.3 and ruling 6, written out here apart from the code under test.
    """
    injections = payload["injections"]
    parking = payload["parking"]
    release = payload["release"]
    margins = payload["margins"]
    dark = payload["dark_radiation_values"]
    log_GeV = log(units.GeV)

    injected = 0.0
    for c in injections:
        injected = injected + c.delta_rho_over_rho_rad
    if parking is not None:
        injected = injected + parking.delta_rho_over_rho_rad

    first = injections[0] if len(injections) > 0 else None
    out = {
        "injections": sum(1 for c in injections if c.delta_K > 0.0),
        "resolved_candidates": sum(1 for c in injections if c.resolved),
        "unresolved_candidates": sum(1 for c in injections if not c.resolved),
        "injected_fraction": injected,
        "rho_chi_ratio_1MeV": dark.rho_chi_ratio_1MeV,
        "rho_chi_ratio_70keV": dark.rho_chi_ratio_70keV,
        "rho_chi_ratio_today": dark.rho_chi_ratio_today,
        "Delta_N_eff_1MeV": dark.Delta_N_eff_1MeV,
        "Delta_N_eff_today": dark.Delta_N_eff_today,
        "log_R_first_impact": (
            first.classical_turning_point.log_R
            if first is not None and first.classical_turning_point is not None
            else None
        ),
        "parking_N": parking.N if parking is not None else None,
        "parking_log_T_Jordan": (
            parking.log_T_Jordan - log_GeV if parking is not None else None
        ),
        "release_N": release.N if release is not None else None,
        "release_log_T_Jordan": (
            release.log_T_Jordan - log_GeV if release is not None else None
        ),
    }
    for name in MARGIN_COLUMNS:
        out[name] = getattr(margins, name) if margins is not None else None
    return out


class _DissipationStoreCase(_TempStoreCase):
    """A store holding test_injection's parameter rows, made by the factories; the histories are
    written by `write`."""

    # a failure names the column that differs, rather than truncating the diff
    maxDiff = None

    def setUp(self):
        super().setUp()
        self.cosmology = dh.cosmology()
        self.units = self.cosmology.units
        units = self.units
        self.store = self.open(LABEL_A)

        get = self.store.object_get
        self.solver = get("IntegrationSolver", label="Radau+kinematic-cap", stepping=1)
        self.solvers = {SM.SCALAR_HISTORY_STEPPER_LABEL: self.solver}
        self.atol = get("tolerance", tol=1e-8)
        self.rtol = get("tolerance", tol=1e-8)
        self.tuning_tol = get("tolerance", tol=1e-5)
        self.T_init = get("temperature", value=dh.T_INIT_GEV * units.GeV, units=units)
        self.phi = get(
            "phi_value", value=dh.PHI_INIT_OVER_MP * units.PlanckMass, units=units
        )
        self.pi = get("pi_value", value=dh.PI_INIT, units=units)
        self._serial = 0

        # the sampled points, with the serials the payloads' samples carry
        with self.store._engine.begin() as conn:
            for z in dh.z_grid():
                self.store._schema["redshift"]["insert"](
                    conn, dict(serial=z.store_id, log_one_plus_z=z.log_one_plus_z)
                )

    def alpha(self, value: float):
        return self.store.object_get("alpha_value", value=value)

    def model_row(self, beta: float, M_over_Mp: float):
        units = self.units
        M = self.store.object_get(
            "M_value", value=M_over_Mp * units.PlanckMass, units=units
        )
        b = self.store.object_get("beta_value", value=beta)
        return self.store.object_get(
            "ExponentialModel", M=M, n=dh.N_EXPONENT, beta=b, units=units
        )

    def query(self, beta, M_over_Mp, alpha, **extra) -> dict:
        q = dict(
            solver_labels=self.solvers,
            cosmology=self.cosmology,
            T_Jordan_init=self.T_init,
            phi_Einstein_init=self.phi,
            pi_Einstein_init=self.pi,
            model=self.model_row(beta, M_over_Mp),
            tuning_tol=self.tuning_tol,
            alpha=self.alpha(alpha),
            atol=self.atol,
            rtol=self.rtol,
            z_grid=None,
            tags=[],
        )
        q.update(extra)
        return q

    def read(self, beta, M_over_Mp, alpha, **extra):
        return self.store.object_get(
            "ScalarHistory", **self.query(beta, M_over_Mp, alpha, **extra)
        )

    def write(self, compute_result, beta, M_over_Mp, alpha):
        """A ScalarHistory on this store's rows, populated by store() from compute_result, and
        written by the factory's store under the next explicit serial; returns it and its serial.
        """
        history = SM.ScalarHistory(
            payload=None,
            solver_labels=self.solvers,
            cosmology=self.cosmology,
            T_Jordan_init=self.T_init,
            phi_Einstein_init=self.phi,
            pi_Einstein_init=self.pi,
            model=self.model_row(beta, M_over_Mp),
            tuning_tol=self.tuning_tol,
            alpha=self.alpha(alpha),
            atol=self.atol,
            rtol=self.rtol,
            label=f"dissipation-{beta:g}-{M_over_Mp:g}-{alpha:g}",
            tags=[],
        )
        history._compute_ref = object()
        with mock.patch.object(
            SM.ray, "wait", lambda refs, timeout: (refs, [])
        ), mock.patch.object(SM.ray, "get", lambda ref: compute_result):
            assert history.store() is True

        self._serial += 1
        serial = self._serial
        table = self.store._tables["ScalarHistory"]
        with self.store._engine.begin() as conn:
            self.store._factories["ScalarHistory"].store(
                history,
                conn,
                table,
                lambda c, row: self.store._inserters["ScalarHistory"](
                    c, dict(row, serial=serial)
                ),
                self.store._tables,
                self.store._inserters,
            )
            conn.execute(
                sqla.update(table)
                .where(table.c.serial == serial)
                .values(validated=True)
            )
            conn.commit()
        return history, serial

    def raw(self, serial: int) -> dict:
        table = self.store._tables["ScalarHistory"]
        with self.store._engine.begin() as conn:
            row = conn.execute(sqla.select(table).where(table.c.serial == serial)).one()
        return row._asdict()

    def raw_ratios(self, serial: int) -> dict:
        """{z serial: rho_chi_over_rho_rad} of the row's samples."""
        table = self.store._tables["ScalarHistoryValue"]
        with self.store._engine.begin() as conn:
            rows = conn.execute(
                sqla.select(table.c.z_serial, table.c.rho_chi_over_rho_rad).where(
                    table.c.history_serial == serial
                )
            ).all()
        return {z: r for z, r in rows}

    def assert_bitwise(self, got, expected, what):
        """Bitwise for a float, equal and of the same type for an int, None for None."""
        if expected is None:
            self.assertIsNone(got, what)
        elif isinstance(expected, bool) or isinstance(expected, int):
            self.assertIs(type(got), int, what)
            self.assertEqual(got, expected, what)
        else:
            self.assertIsInstance(got, float, what)
            self.assertEqual(float(got).hex(), float(expected).hex(), what)

    def assert_row(self, raw: dict, expected: dict):
        for name in COLUMNS:
            self.assert_bitwise(raw[name], expected[name], f"column {name}")

    def assert_object(self, got, raw: dict):
        """The object's record equals the row: as stored, or ln T_J back in the cosmology's
        units (row + ln GeV, ruling 7)."""
        record = got.dissipation
        self.assertIsInstance(record, SM.DissipationValues)
        log_GeV = log(self.units.GeV)
        for name in COLUMNS:
            value = raw[name]
            if name in CONVERTED and value is not None:
                value = value + log_GeV
            self.assert_bitwise(getattr(record, name), value, f"dissipation.{name}")


class TestADissipationHistoryRoundTrips(_DissipationStoreCase):
    """Test 1: M = 0.5 M_P, beta = 2, alpha = 0.1."""

    def test_1_every_column_raw_and_on_the_object(self):
        payload = dh.payload(*TEST_1)
        written, serial = self.write(payload, *TEST_1)
        raw = self.raw(serial)
        expected = expected_columns(payload, self.units)
        self.assert_row(raw, expected)
        self.assertGreater(raw["injections"], 0)
        self.assertIsNotNone(raw["worst_birth_margin"])

        # the object populated by store() holds the payload's values (ln T_J in the cosmology's
        # units, unconverted), and the one read back equals the row (ruling 7)
        for name in COLUMNS:
            value = expected[name]
            if name in CONVERTED and value is not None:
                value = getattr(payload[name.split("_")[0]], "log_T_Jordan")
            self.assert_bitwise(
                getattr(written.dissipation, name), value, f"written {name}"
            )
        got = self.read(*TEST_1)
        self.assertTrue(got.available)
        self.assertEqual(got.store_id, serial)
        self.assertEqual(got.alpha.value, 0.1)
        self.assert_object(got, raw)
        # ln T_J of the object against the payload's (ruling 7: the conversion's rounding, if any)
        if payload["parking"] is not None:
            self.assertAlmostEqual(
                got.dissipation.parking_log_T_Jordan,
                payload["parking"].log_T_Jordan,
                delta=1e-12 * abs(payload["parking"].log_T_Jordan),
            )

    def test_1_every_sample_ratio_bitwise(self):
        payload = dh.payload(*TEST_1)
        data, _, _ = dh.history(*TEST_1)
        ledger = data["dark_radiation"]
        ratios = payload["rho_chi_over_rho_rad"]
        self.assertEqual(len(ratios), len(payload["sample"]))
        _, serial = self.write(payload, *TEST_1)

        # the payload's ratio is the ledger's at each sample's own N and ln rho_R,E (P27)
        positive = 0
        for s, r in zip(payload["sample"], ratios):
            self.assert_bitwise(
                r,
                ledger.ratio(s.raw_N, s.log_rhorad_Einstein),
                f"payload ratio at N={s.raw_N!r}",
            )
            positive += r > 0.0
        self.assertGreater(positive, 0)
        # today's ratio is the last sample's (ruling 3)
        self.assert_bitwise(
            payload["dark_radiation_values"].rho_chi_ratio_today,
            ratios[-1],
            "rho_chi_ratio_today against the last sample",
        )

        # the rows, and the values read back, by redshift serial
        by_serial = {z.store_id: r for z, r in zip(payload["z_grid"], ratios)}
        raw = self.raw_ratios(serial)
        self.assertEqual(set(raw), set(by_serial))
        for z, r in by_serial.items():
            self.assert_bitwise(raw[z], r, f"row ratio z={z}")
        got = self.read(*TEST_1)
        self.assertEqual(len(got.values), len(ratios))
        for value in got.values:
            self.assert_bitwise(
                value.rho_chi_over_rho_rad,
                by_serial[value.z.store_id],
                f"value ratio z={value.z.store_id}",
            )

    def test_1_extra_data_margins_equal_the_payload(self):
        payload = dh.payload(*TEST_1)
        margins = payload["margins"]
        self.assertIsNotNone(margins)
        self.write(payload, *TEST_1)
        got = self.read(*TEST_1).extra_metadata["margins"]

        self.assertEqual(set(got), set(MARGIN_JSON_KEYS))
        for key in MARGIN_JSON_KEYS:
            value = getattr(margins, key)
            if key.startswith("first_"):
                expected = value._asdict() if value is not None else None
                self.assertEqual(got[key], expected, key)
                if value is not None:
                    for field, x in value._asdict().items():
                        if isinstance(x, float):
                            self.assertEqual(got[key][field].hex(), x.hex(), key)
            elif isinstance(value, float):
                self.assertEqual(got[key].hex(), value.hex(), key)
            else:
                self.assertEqual(got[key], value, key)
        # at least one first failure is stored, so the MarginFailure's floats are exercised
        self.assertTrue(any(got[k] is not None for k in MARGIN_JSON_KEYS[:3]))

    def test_1_the_same_with_do_not_populate(self):
        payload = dh.payload(*TEST_1)
        _, serial = self.write(payload, *TEST_1)
        raw = self.raw(serial)
        got = self.read(*TEST_1, _do_not_populate=True)
        self.assertTrue(got.available)
        self.assert_object(got, raw)
        self.assertEqual(got.alpha.value, 0.1)
        self.assertIn("margins", got.extra_metadata)
        with self.assertRaises(RuntimeError):
            got.values


class TestParkedAndReleased(_DissipationStoreCase):
    """Test 2: M = 1e-30 M_P, beta = 2 parks and is not released; M = 0.5 M_P, beta = 5 parks and
    is released."""

    def check(self, key, released: bool):
        payload = dh.payload(*key)
        parking, release = payload["parking"], payload["release"]
        self.assertIsNotNone(parking)
        self.assertEqual(release is not None, released)
        _, serial = self.write(payload, *key)
        raw = self.raw(serial)
        log_GeV = log(self.units.GeV)

        self.assert_bitwise(raw["parking_N"], parking.N, "parking_N")
        self.assert_bitwise(
            raw["parking_log_T_Jordan"],
            parking.log_T_Jordan - log_GeV,
            "parking_log_T_Jordan",
        )
        if released:
            self.assert_bitwise(raw["release_N"], release.N, "release_N")
            self.assert_bitwise(
                raw["release_log_T_Jordan"],
                release.log_T_Jordan - log_GeV,
                "release_log_T_Jordan",
            )
            self.assertLess(raw["parking_N"], raw["release_N"])
        else:
            self.assertIsNone(raw["release_N"])
            self.assertIsNone(raw["release_log_T_Jordan"])

        # the whole row, and the object read back
        self.assert_row(raw, expected_columns(payload, self.units))
        self.assert_object(self.read(*key), raw)

    def test_2_parked_not_released(self):
        self.check(PARKED, released=False)

    def test_2_parked_and_released(self):
        self.check(RELEASED, released=True)


class TestTwoAlphaAreTwoRows(_DissipationStoreCase):
    """Test 3: one model, one set of initial data and tolerances, at alpha = 0 and 0.1."""

    def test_3_each_lookup_finds_its_own_row(self):
        _, serial_0 = self.write(dh.payload(*NATURAL), *NATURAL)
        _, serial_1 = self.write(dh.payload(*TEST_1), *TEST_1)
        self.assertNotEqual(serial_0, serial_1)

        for key, serial in ((NATURAL, serial_0), (TEST_1, serial_1)):
            with self.subTest(alpha=key[2]):
                got = self.read(*key, _do_not_populate=True)
                self.assertTrue(got.available)
                self.assertEqual(got.store_id, serial)
                self.assertEqual(got.alpha.value, key[2])
                self.assertEqual(self.raw(serial)["alpha_serial"], got.alpha.store_id)

        # the two alpha rows are distinct rows of alpha_value
        self.assertNotEqual(self.alpha(0.0).store_id, self.alpha(0.1).store_id)

    def test_3_another_alpha_finds_neither(self):
        self.write(dh.payload(*NATURAL), *NATURAL)
        self.write(dh.payload(*TEST_1), *TEST_1)
        got = self.read(2.0, 0.5, 0.3, _do_not_populate=True)
        self.assertFalse(got.available)


class TestAnAlphaZeroRow(_DissipationStoreCase):
    """Test 4 (R24): zeros where defined, NULL elsewhere, and every sample's ratio 0.0."""

    def test_4_zeros_and_nulls(self):
        payload = dh.payload(*NATURAL)
        written, serial = self.write(payload, *NATURAL)
        raw = self.raw(serial)
        for name in COLUMNS:
            with self.subTest(column=name):
                if name in ZERO_AT_ALPHA_0:
                    self.assert_bitwise(raw[name], ZERO_AT_ALPHA_0[name], name)
                else:
                    self.assertIsNone(raw[name], name)
        # what the payload carries at alpha = 0 agrees
        self.assert_row(raw, expected_columns(payload, self.units))
        self.assert_object(self.read(*NATURAL), raw)
        self.assertNotIn("margins", self.read(*NATURAL).extra_metadata)

    def test_4_every_sample_ratio_is_zero(self):
        payload = dh.payload(*NATURAL)
        _, serial = self.write(payload, *NATURAL)
        ratios = self.raw_ratios(serial)
        self.assertEqual(len(ratios), len(payload["sample"]))
        for z, r in ratios.items():
            self.assert_bitwise(r, 0.0, f"ratio z={z}")


class TestAFailureRow(_DissipationStoreCase):
    """Test 5: a failure row holds every new column NULL and reads back as a failure."""

    def test_5_every_new_column_null(self):
        reason = "step budget exhausted: stand-in"
        written, serial = self.write(
            {"failure": True, "failure_reason": reason}, *TEST_1
        )
        self.assertIsNone(written.dissipation)
        raw = self.raw(serial)
        for name in COLUMNS:
            self.assertIsNone(raw[name], name)
        self.assertIsNone(raw["extra_data"])
        self.assertEqual(self.raw_ratios(serial), {})

        got = self.read(*TEST_1, failure=True)
        self.assertTrue(got.available)
        self.assertTrue(got.failure)
        self.assertEqual(got.failure_reason, reason)
        self.assertIsNone(got.dissipation)
        self.assertEqual(got.alpha.value, 0.1)


class TestDeltaNeffByHand(_DissipationStoreCase):
    """Test 6 (R22, P31): the two expressions, from the cosmology's G_rho at the history's own T_J,
    with r_chi the ledger's at the crossing (and at N_final), by the operations of R22.
    """

    def test_6_delta_N_eff_at_1_MeV_and_today(self):
        data, result, _ = dh.history(*TEST_1)
        cos = self.cosmology
        units = self.units
        ledger = data["dark_radiation"]
        dark = data["dark_radiation_values"]

        # at the first crossing of T_J = 1 MeV, on the step's own interpolant
        k, N = SM._T_Jordan_crossing_step(result, log(1.0 * units.MeV))
        state = SM.StateVector._make(result.solution.interpolants[k](N))
        r = ledger.ratio(N, state.log_rhorad_Einstein)
        g = cos.G_rho(exp(state.log_T_Jordan))
        self.assertGreater(r, 0.0)
        self.assert_bitwise(dark.rho_chi_ratio_1MeV, r, "rho_chi_ratio_1MeV")
        self.assert_bitwise(dark.Delta_N_eff_1MeV, (4.0 / 7.0) * g * r, "1 MeV")

        # today, at N_final
        state = SM.StateVector._make(result.solution(result.N_final))
        r = ledger.ratio(result.N_final, state.log_rhorad_Einstein)
        g = cos.G_rho(exp(state.log_T_Jordan))
        self.assert_bitwise(dark.rho_chi_ratio_today, r, "rho_chi_ratio_today")
        self.assert_bitwise(
            dark.Delta_N_eff_today,
            (4.0 / 7.0) * g * (11.0 / 4.0) ** (4.0 / 3.0) * r,
            "today",
        )

        # and the stored row holds them
        _, serial = self.write(dh.payload(*TEST_1), *TEST_1)
        raw = self.raw(serial)
        self.assert_bitwise(raw["Delta_N_eff_1MeV"], dark.Delta_N_eff_1MeV, "row 1 MeV")
        self.assert_bitwise(
            raw["Delta_N_eff_today"], dark.Delta_N_eff_today, "row today"
        )


class TestExtraDataIsText(_DissipationStoreCase):
    """Test 7 (R23): extra_data over 256 characters round-trips whole; the column is Text."""

    def test_7_long_extra_data_round_trips_whole(self):
        payload = dh.payload(*TEST_1)
        written, serial = self.write(payload, *TEST_1)
        text = self.raw(serial)["extra_data"]
        self.assertGreater(len(text), 256)
        self.assertEqual(text, json.dumps(written.extra_metadata))
        got = self.read(*TEST_1, _do_not_populate=True)
        self.assertEqual(got.extra_metadata, json.loads(text))
        self.assertEqual(json.dumps(got.extra_metadata), text)

    def test_7_the_reflected_column_is_text(self):
        columns = {
            c["name"]: c["type"]
            for c in sqla.inspect(self.store._engine).get_columns("ScalarHistory")
        }
        self.assertIsInstance(columns["extra_data"], sqla.Text)
        self.assertNotIsInstance(columns["extra_data"], sqla.VARCHAR)


class TestTheVersion(unittest.TestCase):
    """Test 10 (P8): the label and the fingerprint format of this prompt."""

    def test_10_the_version_label_and_the_format(self):
        from config.fingerprint import FINGERPRINT_FORMAT, SPEC_DIGESTS
        from config.version import VERSION_LABEL

        # 2026.8.0 at inelastic-reflection prompt 09; 2026.9.0 since reflection-restart prompt 03 (F3 and
        # the guard at a restart), which changed no table, so the format stayed 6; 2026.10.0 and format 7 since
        # one-loop-drain prompt 03 (the drain model's key, the bounce table); 2026.11.0 since electroweak-eos prompt 01
        # (the w table), which changed no table, so the format stayed 7
        self.assertEqual(VERSION_LABEL, "2026.11.0")
        self.assertEqual(FINGERPRINT_FORMAT, 7)
        self.assertIn(6, SPEC_DIGESTS)
        self.assertIn("alpha_value", dict(SPEC_DIGESTS[6]))
