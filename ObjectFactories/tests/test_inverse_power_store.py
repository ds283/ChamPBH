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
InversePowerModel through the store (power-law-wall prompt 05 §1.7, ObjectFactories; README §6.9 P33-P36, P40-P42).

Each store test goes the whole way on a temporary SQLite file through `_TempStoreCase`, with the harness of
`test_drain_store` (`_DrainStoreCase`: the parameter rows and the model rows made by the factories,
`ScalarHistory.store()` run with `ray.wait` and `ray.get` patched to hand it the payload, the factory's `store`
writing the row, its samples and its bounce rows with the datastore's own inserters, the row read back through
`object_get`), with the model row made by the factory of the model named (`InversePowerModel` unless a test names
`ExponentialModel`).

- **the factory**: an `InversePowerModel` row is found again by (M, n, beta), `_deserialized`, and a second lookup
  inserts nothing; n = 1 and n = 2 at the same (M, beta) are two rows; the object is an `InversePowerModel` with the
  stored M, n and beta and `type_id` 2;
- **the two models apart**: the same (M, n, beta) as an `ExponentialModel` and as an `InversePowerModel` is one row in
  each table, with the same serial (1), and a history on each under one key otherwise equal is two rows, which differ
  in `model_type` only among the key's columns; a lookup under each model finds its own history and never the
  other's;
- **two real round trips** (the costs below): one tuned power-law history at laboratory M (4e-31 M_P), beta = 2,
  n = 1, under the fixed law at alpha = 0.1 and one under the one-loop law at A = 1 (`one-loop-inverse-power-v1`),
  each `tune_history`'s own payload at `test_injection`'s setup, stored and read back with every tuning, dissipation
  and drain-record column, the first bounce and the fixed-temperature columns, every sample (its fields, its masses
  and its rho_chi/rho_R) and every bounce row equal, as `test_tuned_history_round_trip`, `test_dissipation_store`
  and `test_drain_store` require of the exponential's; the realized model rebuilt as the `InversePowerModel` it was
  computed with;
- **the key's drain model at entry** (§1.4, P36): an `InversePowerModel` (n = 1, 2) under `one-loop-exponential-v2`,
  and an `ExponentialModel` under `one-loop-inverse-power-v1`, are refused when the `ScalarHistory` is constructed,
  before any tuning or integration; under the fixed model both are accepted, and an `InversePowerModel` at n = 3
  too; at n = 3 under a one-loop model the model's own `ValueError` propagates;
- **the inventory**: a store holding a history of each model reads with no problem, each history's `model` parent
  resolved to its own table;
- **the configuration**: the registry's order and batch size, the sharding, no drop group; the spec, equal to the
  exponential's; the format guard at 8 beside 1-7; `VERSION_LABEL` 2026.13.0 (2026.12.0 until power-law-wall prompt
  07c, which changed no table).

No Ray cluster, no persistent datastore, no PRyMordial solve. Cost: the two tunings, each one integration at its
tracking seed (prompt 04 measured 5.3-6.8 s for these histories, at loads of 9-15), shared by the round-trip tests
through `tuned_power_payload`'s cache; about 1-2 s per round-trip test for the samples written and read back; the
rest is constructed rows, instant.
"""

import contextlib
import functools
import io
import json
from math import log
from unittest import mock
import unittest

import sqlalchemy as sqla
from datastorekit.store_inventory import read_inventory

from ComputeTargets.tests import dissipation_histories as dh
from CosmologyConcepts import M_value, alpha_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models import (
    EXPONENTIAL_MODEL,
    INVERSE_POWER_MODEL,
    ExponentialModel,
    InversePowerModel,
    RealizedModel,
)
from ObjectFactories.tests.test_dissipation_store import (
    COLUMNS as DISSIPATION_COLUMNS,
    expected_columns,
)
from ObjectFactories.tests.test_drain_store import FIXED, ONE_LOOP, _DrainStoreCase

SM = dh.SM
PL = "one-loop-inverse-power-v1"

# the round trips: laboratory M, beta = 2, n = 1 (power-law-wall prompt 04's ip-n1-M4e-31-b2.0-fixed0.1 and
# ip-n1-M4e-31-b2.0-oneloop1), at test_injection's setup (dissipation_histories)
LAB_M = 4.0e-31
BETA = 2.0
N_ROUND_TRIP = 1
ALPHA_FIXED = 0.1
A_ONE_LOOP = 1.0

# the field values at which the rebuilt realized model is compared, in units of M_P
PHI_PROBES_OVER_MP = (1.0e-20, 1.0e-3, 0.5, 5.0)


def power_model(n: int, M_over_Mp: float = LAB_M, beta: float = BETA):
    units = dh.cosmology().units
    return InversePowerModel(
        0, M_value(0, M_over_Mp * units.PlanckMass), n, beta_value(0, beta), units
    )


def exponential_model(n: int, M_over_Mp: float = LAB_M, beta: float = BETA):
    units = dh.cosmology().units
    return ExponentialModel(
        0, M_value(0, M_over_Mp * units.PlanckMass), n, beta_value(0, beta), units
    )


@functools.lru_cache(maxsize=None)
def tuned_power_payload(drain_law: str, value: float) -> dict:
    """tune_history's payload for InversePowerModel at n = 1, laboratory M, beta = 2, at test_injection's setup on
    the golden sampling axis (dissipation_histories.z_grid), under the drain law at alpha (fixed) or A (one-loop).
    """
    cos = dh.cosmology()
    units = cos.units
    with contextlib.redirect_stdout(io.StringIO()):
        data = SM.tune_history(
            cos,
            power_model(N_ROUND_TRIP),
            temperature(0, dh.T_INIT_GEV * units.GeV),
            phi_value(0, dh.PHI_INIT_OVER_MP * units.PlanckMass),
            pi_value(0, dh.PI_INIT),
            dh.z_grid(),
            task_label=f"power-store-{drain_law}-{value:g}",
            alpha=value,
            drain_law=drain_law,
        )
    if data.get("failure", False):
        raise RuntimeError(
            f"tuned_power_payload {drain_law, value}: {data['failure_reason']}"
        )
    return data


class _PowerStoreCase(_DrainStoreCase):
    """_DrainStoreCase whose model rows are made by the factory of `model_table` (InversePowerModel unless set), at
    n = `model_n`."""

    model_table = "InversePowerModel"
    model_n = N_ROUND_TRIP

    def model_row(self, beta: float, M_over_Mp: float):
        units = self.units
        M = self.store.object_get(
            "M_value", value=M_over_Mp * units.PlanckMass, units=units
        )
        b = self.store.object_get("beta_value", value=beta)
        return self.store.object_get(
            self.model_table, M=M, n=self.model_n, beta=b, units=units
        )

    def as_model(self, table: str, n: int = N_ROUND_TRIP):
        """A context in which model rows are of table at n."""
        case = self

        @contextlib.contextmanager
        def ctx():
            saved = case.model_table, case.model_n
            case.model_table, case.model_n = table, n
            try:
                yield
            finally:
                case.model_table, case.model_n = saved

        return ctx()

    def model_rows(self, table: str):
        t = self.store._tables[table]
        with self.store._engine.begin() as conn:
            return [r._asdict() for r in conn.execute(sqla.select(t)).all()]

    def store_cosmology_row(self):
        """The row of the harness's cosmology, which is not a stored row (store id 0) but which every history
        names, so that the inventory resolves a history's parent cosmology."""
        cosmology = self.cosmology
        with self.store._engine.begin() as conn:
            self.store._schema["QCD_Cosmology"]["insert"](
                conn,
                dict(
                    serial=cosmology.store_id,
                    name="Planck2018",
                    omega_m=0.3111,
                    omega_cc=0.6889,
                    h=0.6766,
                    f_baryon=0.1575,
                    T_CMB_Kelvin=cosmology.T_CMB_Kelvin,
                    Neff=3.046,
                ),
            )

    def close_actors(self):
        for store in self._stores:
            with contextlib.redirect_stdout(io.StringIO()):
                store.__exit__(None, None, None)
        self._stores = []


class TestTheFactory(_PowerStoreCase):
    def test_a_row_is_found_again_by_M_n_beta(self):
        units = self.units
        get = self.store.object_get
        M = get("M_value", value=LAB_M * units.PlanckMass, units=units)
        beta = get("beta_value", value=BETA)

        first = get("InversePowerModel", M=M, n=1, beta=beta, units=units)
        self.assertTrue(getattr(first, "_new_insert", False))
        again = get("InversePowerModel", M=M, n=1, beta=beta, units=units)
        self.assertTrue(getattr(again, "_deserialized", False))
        self.assertFalse(getattr(again, "_new_insert", False))
        self.assertEqual(again.store_id, first.store_id)
        self.assertEqual(len(self.model_rows("InversePowerModel")), 1)

        # n = 2 at the same (M, beta) is a second row
        second = get("InversePowerModel", M=M, n=2, beta=beta, units=units)
        self.assertNotEqual(second.store_id, first.store_id)
        rows = self.model_rows("InversePowerModel")
        self.assertEqual(
            sorted((r["M_serial"], r["n_value"], r["beta_serial"]) for r in rows),
            [(M.store_id, 1, beta.store_id), (M.store_id, 2, beta.store_id)],
        )
        # and nothing in the exponential's table
        self.assertEqual(self.model_rows("ExponentialModel"), [])

        for obj, n in ((again, 1), (second, 2)):
            with self.subTest(n=n):
                self.assertIsInstance(obj, InversePowerModel)
                self.assertNotIsInstance(obj, ExponentialModel)
                self.assertEqual(obj.type_id, INVERSE_POWER_MODEL)
                self.assertEqual(obj.type_id, 2)
                self.assertEqual(obj._M.store_id, M.store_id)
                self.assertEqual(obj._M_float, LAB_M * units.PlanckMass)
                self.assertEqual(obj._n, n)
                self.assertEqual(obj._beta.store_id, beta.store_id)
                self.assertEqual(obj._beta_float, BETA)
                self.assertIs(obj.shard_key, obj._beta)


class TestTheTwoModelsApart(_PowerStoreCase):
    def test_one_row_in_each_table_with_the_same_serial(self):
        with self.as_model("ExponentialModel"):
            e = self.model_row(BETA, LAB_M)
        p = self.model_row(BETA, LAB_M)
        self.assertIsInstance(e, ExponentialModel)
        self.assertIsInstance(p, InversePowerModel)
        self.assertEqual(
            (e.type_id, p.type_id), (EXPONENTIAL_MODEL, INVERSE_POWER_MODEL)
        )
        # each table's first row takes serial 1: a join on model_serial alone would pair them (P37)
        self.assertEqual(e.store_id, p.store_id)
        self.assertEqual(e.store_id, 1)
        (row_e,) = self.model_rows("ExponentialModel")
        (row_p,) = self.model_rows("InversePowerModel")
        for column in ("serial", "M_serial", "n_value", "beta_serial"):
            self.assertEqual(row_e[column], row_p[column], column)

    def test_two_histories_under_one_key_and_each_lookup_finds_its_own(self):
        reasons = {}
        serials = {}
        for table in ("ExponentialModel", "InversePowerModel"):
            with self.as_model(table):
                reasons[table] = f"a stand-in failure of {table}"
                _, serials[table] = self.write(
                    SM._failure_payload(reasons[table]), BETA, LAB_M, ALPHA_FIXED
                )
        self.assertNotEqual(serials["ExponentialModel"], serials["InversePowerModel"])
        self.assertEqual(self.count("ScalarHistory"), 2)

        # the two rows differ, among the key's columns, in model_type alone
        raw_e = self.raw(serials["ExponentialModel"])
        raw_p = self.raw(serials["InversePowerModel"])
        key_columns = (
            "version",
            "cosmology_type",
            "cosmology_serial",
            "model_serial",
            "T_Jordan_init_serial",
            "phi_Einstein_init_serial",
            "pi_Einstein_init_serial",
            "atol_serial",
            "rtol_serial",
            "tuning_tol_serial",
            "alpha_serial",
        )
        for column in key_columns:
            self.assertEqual(raw_e[column], raw_p[column], column)
        self.assertEqual(
            (raw_e["model_type"], raw_p["model_type"]),
            (EXPONENTIAL_MODEL, INVERSE_POWER_MODEL),
        )

        for table in ("ExponentialModel", "InversePowerModel"):
            with self.subTest(table=table), self.as_model(table):
                got = self.read(BETA, LAB_M, ALPHA_FIXED, failure=True)
                self.assertTrue(got.available)
                self.assertEqual(got.store_id, serials[table])
                self.assertEqual(got.failure_reason, reasons[table])
                self.assertEqual(type(got._model).__name__, table)

        # an n with no history under either model finds nothing
        with self.as_model("InversePowerModel", 2):
            self.assertFalse(
                self.read(BETA, LAB_M, ALPHA_FIXED, failure=True).available
            )


class _RoundTrip(_PowerStoreCase):
    def check_every_column(self, payload, written, serial, got):
        units = self.units
        raw = self.raw(serial)
        self.assertTrue(got.available)
        self.assertEqual(got.store_id, serial)
        self.assertFalse(got.failure)
        self.assertEqual(raw["model_type"], INVERSE_POWER_MODEL)

        # the tuning (test_tuned_history_round_trip (a)): in Planck_units every column is the payload's value
        self.assertEqual(units.PlanckMass, 1.0)
        for key in SM.TUNING_OUTPUT_KEYS:
            self.assert_same(raw[key], payload[key], f"column {key}")
            self.assert_same(getattr(got.tuning, key), payload[key], f"tuning.{key}")

        # the dissipation record (test_dissipation_store, test 1): the row and the object
        expected = expected_columns(payload, units)
        log_GeV = log(units.GeV)
        for name in DISSIPATION_COLUMNS:
            value = expected[name]
            self.assert_same(raw[name], value, f"column {name}")
            if name in ("parking_log_T_Jordan", "release_log_T_Jordan") and (
                raw[name] is not None
            ):
                self.assert_same(
                    getattr(got.dissipation, name),
                    raw[name] + log_GeV,
                    f"dissipation.{name}",
                )
            else:
                self.assert_same(
                    getattr(got.dissipation, name), raw[name], f"dissipation.{name}"
                )

        # the drain record (test_drain_store)
        if payload.get("drain") is not None and payload["bounces"] is not None:
            self.check_history_columns(payload, serial, got)
        else:
            for name in SM.DrainHistoryValues._fields:
                self.assertIsNone(raw[name], name)
            self.assertIsNone(got.drain_history)

        # the first bounce and the fixed-temperature columns: the row is the forward conversion of the written
        # object's, the object read back the inverse conversion of the row
        bounce = written.first_bounce
        self.assertIsNotNone(bounce)
        self.assert_same(raw["first_bounce_N"], bounce.N, "first_bounce_N")
        self.assert_same(
            raw["first_bounce_log_T_Jordan"],
            bounce.log_T_Jordan - log_GeV,
            "first_bounce_log_T_Jordan",
        )
        self.assert_same(
            raw["first_bounce_phi_Einstein"],
            bounce.phi_Einstein / units.PlanckMass,
            "first_bounce_phi_Einstein",
        )
        self.assertEqual(bool(raw["first_bounce_reflected"]), bounce.reflected)
        back = got.first_bounce
        self.assert_same(back.N, raw["first_bounce_N"], "first_bounce.N")
        self.assert_same(
            back.log_T_Jordan,
            raw["first_bounce_log_T_Jordan"] + log_GeV,
            "first_bounce.log_T_Jordan",
        )
        self.assert_same(
            back.phi_Einstein,
            raw["first_bounce_phi_Einstein"] * units.PlanckMass,
            "first_bounce.phi_Einstein",
        )
        self.assertEqual(back.reflected, bounce.reflected)
        fixed = written.fixed_T_values
        for name in ("density_NP_ratio_1MeV", "density_NP_ratio_70keV"):
            self.assert_same(raw[name], getattr(fixed, name), name)
            self.assert_same(getattr(got.fixed_T_values, name), raw[name], name)
        for name in ("phi_Einstein_1MeV", "phi_Einstein_70keV"):
            value = getattr(fixed, name)
            self.assert_same(
                raw[name], None if value is None else value / units.PlanckMass, name
            )
            self.assert_same(
                getattr(got.fixed_T_values, name),
                None if raw[name] is None else raw[name] * units.PlanckMass,
                name,
            )

        # the history's JSON record
        self.assertEqual(
            got.extra_metadata, json.loads(json.dumps(written.extra_metadata))
        )

        # every sample, matched by its redshift's serial, field by field (test_tuned_history_round_trip (a)), its
        # masses (test_drain_store) and its rho_chi/rho_R (test_dissipation_store)
        by_serial = {
            z.store_id: s for z, s in zip(payload["z_grid"], payload["sample"])
        }
        self.assertEqual(len(got.values), len(payload["sample"]))
        self.assertEqual(len(by_serial), len(payload["sample"]))
        for value in got.values:
            sample = by_serial[value.z.store_id]
            for name in SM.SampleValues._fields:
                self.assertEqual(
                    float(getattr(value, name)).hex(),
                    float(getattr(sample, name)).hex(),
                    f"sample z={value.z.store_id} {name}",
                )
        self.check_masses(payload, serial, got)
        ratios = {
            z.store_id: r
            for z, r in zip(payload["z_grid"], payload["rho_chi_over_rho_rad"])
        }
        for value in got.values:
            self.assert_same(
                value.rho_chi_over_rho_rad,
                ratios[value.z.store_id],
                f"ratio z={value.z.store_id}",
            )

        # the realized model, rebuilt as the InversePowerModel the history was computed with
        rm = got.realized_model
        self.assertIsInstance(rm, RealizedModel)
        self.assertIsInstance(rm.model, InversePowerModel)
        self.assertEqual(rm.model.type_id, INVERSE_POWER_MODEL)
        self.assertEqual(rm.model._n, N_ROUND_TRIP)
        self.assertEqual(rm.model._M_float, LAB_M * units.PlanckMass)
        self.assertEqual(rm.model._beta_float, BETA)
        self.assert_same(rm.normalization, payload["normalization"], "normalization")
        self.assert_same(rm.phi_ref, payload["phi_ref"], "phi_ref")
        computed = power_model(N_ROUND_TRIP).realize(
            payload["normalization"], payload["phi_ref"]
        )
        for x in PHI_PROBES_OVER_MP:
            phi = x * units.PlanckMass
            for method in ("log_V", "d_logV_dphi", "log_Omega"):
                self.assert_same(
                    getattr(rm, method)(phi),
                    getattr(computed, method)(phi),
                    f"{method}({x} M_P)",
                )


class TestAFixedLawPowerHistoryRoundTrips(_RoundTrip):
    """Under the fixed law at alpha = 0.1: one tuning (about 5-10 s), shared through tuned_power_payload."""

    def test_every_column_sample_and_no_bounce_row(self):
        payload = tuned_power_payload(SM.DRAIN_LAW_FIXED, ALPHA_FIXED)
        self.assertIsNone(payload["bounces"])
        written, serial = self.write(payload, BETA, LAB_M, ALPHA_FIXED)
        got = self.read(BETA, LAB_M, ALPHA_FIXED)
        self.assertEqual((got.alpha.value, got.alpha.drain_model), (ALPHA_FIXED, FIXED))
        self.check_every_column(payload, written, serial, got)
        self.assertEqual(self.raw_bounces(serial), [])
        self.assertEqual(got.bounces, ())
        self.assertGreater(self.raw(serial)["injections"], 0)


class TestAOneLoopPowerHistoryRoundTrips(_RoundTrip):
    """Under the one-loop law at A = 1 (one-loop-inverse-power-v1): one tuning (about 5-10 s), shared through
    tuned_power_payload."""

    def test_every_column_sample_and_bounce_row(self):
        payload = tuned_power_payload(SM.DRAIN_LAW_ONE_LOOP, A_ONE_LOOP)
        self.assertEqual(payload["drain"].model, PL)
        with self.under(PL):
            written, serial = self.write(payload, BETA, LAB_M, A_ONE_LOOP)
            got = self.read(BETA, LAB_M, A_ONE_LOOP)
        self.assertEqual((got.alpha.value, got.alpha.drain_model), (A_ONE_LOOP, PL))
        self.assertEqual(got.alpha.drain_law, SM.DRAIN_LAW_ONE_LOOP)
        self.check_every_column(payload, written, serial, got)
        self.check_bounces(payload, serial, got)
        self.assertEqual(written.bounces, payload["bounces"].rows)
        record = got.drain_history
        self.assertEqual(record.candidates_one_loop, len(payload["injections"]))
        self.assertGreater(len(got.bounces), 0)

        # the same key under the exponential's one-loop model, or under the fixed model, finds nothing
        for drain_model, value in ((FIXED, ALPHA_FIXED), (ONE_LOOP, A_ONE_LOOP)):
            with self.subTest(drain_model=drain_model):
                with self.under(drain_model), self.as_model("ExponentialModel"):
                    self.assertFalse(
                        self.read(BETA, LAB_M, value, _do_not_populate=True).available
                    )


class TestTheKeyAtEntry(unittest.TestCase):
    """§1.4 (P36): the ScalarHistory constructor refuses an alpha whose drain model is neither the fixed model nor
    the model's own one-loop model, before anything is tuned or integrated. Instant."""

    def construct(self, model, drain_model: str, value: float):
        never = mock.Mock(side_effect=AssertionError("integrated"))
        with mock.patch.object(SM, "tune_history", never), mock.patch.object(
            SM, "integrate_history", never
        ), mock.patch.object(SM, "integrate_scalar_history", never), mock.patch.object(
            SM, "compute_scalar_history", never
        ):
            history = SM.ScalarHistory(
                payload=None,
                solver_labels={},
                cosmology=dh.cosmology(),
                T_Jordan_init=mock.Mock(),
                phi_Einstein_init=mock.Mock(),
                pi_Einstein_init=mock.Mock(),
                model=model,
                tuning_tol=mock.Mock(tol=1e-5),
                alpha=alpha_value(0, value, drain_model),
                atol=mock.Mock(tol=1e-8),
                rtol=mock.Mock(tol=1e-8),
                z_grid=None,
                label="entry",
            )
        never.assert_not_called()
        return history

    def test_the_other_models_one_loop_key_is_refused(self):
        cases = [
            (power_model(1), ONE_LOOP, PL),
            (power_model(2), ONE_LOOP, PL),
            (exponential_model(1), PL, ONE_LOOP),
        ]
        for model, key, own in cases:
            with self.subTest(model=model.name, key=key):
                with self.assertRaises(ValueError) as ctx:
                    self.construct(model, key, 1.0)
                message = str(ctx.exception)
                for needle in (type(model).__name__, repr(own), repr(key)):
                    self.assertIn(needle, message)
                self.assertIn("one_loop_drain_model", message)

    def test_the_fixed_model_and_the_models_own_are_accepted(self):
        cases = [
            (power_model(1), FIXED, 0.1),
            (power_model(2), FIXED, 0.1),
            (power_model(3), FIXED, 0.1),
            (power_model(3), FIXED, 0.0),
            (exponential_model(1), FIXED, 0.1),
            (exponential_model(2), FIXED, 0.0),
            (power_model(1), PL, 1.0),
            (power_model(2), PL, 1.0),
            (exponential_model(1), ONE_LOOP, 1.0),
            (exponential_model(2), ONE_LOOP, 1.0),
        ]
        for model, key, value in cases:
            with self.subTest(model=model.name, key=key):
                history = self.construct(model, key, value)
                self.assertIs(history._model, model)
                self.assertEqual(history._alpha.drain_model, key)

    def test_n_3_under_a_one_loop_model_is_the_models_own_refusal(self):
        for key in (ONE_LOOP, PL):
            with self.subTest(key=key):
                with self.assertRaises(ValueError) as ctx:
                    self.construct(power_model(3), key, 1.0)
                message = str(ctx.exception)
                self.assertIn("has no one-loop drain law", message)
                self.assertIn("n=3", message)
                self.assertIn("one_loop_drain_model", message)


class TestTheInventory(_PowerStoreCase):
    """A store holding a history of each model (stand-in failure rows, instant) reads with no problem."""

    def test_each_history_resolves_its_model_to_its_own_table(self):
        for table, n in (("ExponentialModel", 1), ("InversePowerModel", 2)):
            with self.as_model(table, n):
                self.write(
                    SM._failure_payload(f"a stand-in failure of {table}"),
                    BETA,
                    LAB_M,
                    ALPHA_FIXED,
                )
        self.store_cosmology_row()
        self.close_actors()

        from config.datastore import factories

        inventory = read_inventory(self.db, factories)
        self.assertEqual(inventory.problems, ())
        self.assertEqual(inventory["ExponentialModel"].count, 1)
        self.assertEqual(inventory["InversePowerModel"].count, 1)
        self.assertEqual(inventory["ScalarHistory"].count, 2)
        resolved = {}
        for record in inventory["ScalarHistory"].records:
            model = inventory.resolve("ScalarHistory", record)["model"]
            self.assertNotIn("unresolved", model)
            resolved[record.key["model_type"]] = (
                model["class"],
                model["key"]["n_value"],
            )
        self.assertEqual(
            resolved,
            {
                EXPONENTIAL_MODEL: ("ExponentialModel", 1),
                INVERSE_POWER_MODEL: ("InversePowerModel", 2),
            },
        )


class TestTheConfiguration(unittest.TestCase):
    def test_the_registry_the_sharding_and_no_drop_group(self):
        from config.datastore import (
            drop_groups,
            factories,
            serial_batch_sizes,
            tables_to_drop,
        )
        from config.sharding import replicated_tables, sharded_tables

        names = list(factories)
        self.assertEqual(
            names.index("InversePowerModel"), names.index("ExponentialModel") + 1
        )
        self.assertEqual(serial_batch_sizes["InversePowerModel"], 50)
        self.assertEqual(
            serial_batch_sizes["InversePowerModel"],
            serial_batch_sizes["ExponentialModel"],
        )
        self.assertEqual(sharded_tables["InversePowerModel"], "shard_key")
        self.assertNotIn("InversePowerModel", replicated_tables)
        for group, tables in drop_groups.items():
            with self.subTest(group=group):
                self.assertNotIn("InversePowerModel", tables)
                self.assertNotIn("ExponentialModel", tables)
        self.assertNotIn(
            "InversePowerModel",
            tables_to_drop(["scalar-history", "adiabatic-history", "bbn-data"]),
        )

    def test_the_spec_is_the_exponentials_and_the_polymorphic_key_maps_both(self):
        from config.datastore import factories

        self.assertEqual(
            factories["InversePowerModel"].inventory_spec(),
            factories["ExponentialModel"].inventory_spec(),
        )
        self.assertEqual(
            factories["InversePowerModel"].register()["columns"][1].name, "n_value"
        )
        parent = factories["ScalarHistory"].inventory_spec().parents["model"]
        self.assertEqual(parent.type_column, "model_type")
        self.assertEqual(
            dict(parent.types),
            {
                EXPONENTIAL_MODEL: "ExponentialModel",
                INVERSE_POWER_MODEL: "InversePowerModel",
            },
        )

    def test_the_format_and_the_label(self):
        from config.fingerprint import FINGERPRINT_FORMAT, SPEC_DIGESTS
        from config.version import VERSION_LABEL

        self.assertEqual(FINGERPRINT_FORMAT, 8)
        self.assertEqual(sorted(SPEC_DIGESTS), [1, 2, 3, 4, 5, 6, 7, 8])
        seven, eight = dict(SPEC_DIGESTS[7]), dict(SPEC_DIGESTS[8])
        self.assertEqual(len(SPEC_DIGESTS[8]), 17)
        self.assertEqual(len(set(eight.values())), 14)
        # the new class's digest is the exponential's; only ScalarHistory's moved (its model parent's types)
        self.assertEqual(eight["InversePowerModel"], eight["ExponentialModel"])
        self.assertEqual(set(eight) - set(seven), {"InversePowerModel"})
        self.assertEqual(set(seven) - set(eight), set())
        self.assertEqual({n for n in seven if seven[n] != eight[n]}, {"ScalarHistory"})
        self.assertEqual(VERSION_LABEL, "2026.13.0")


if __name__ == "__main__":
    unittest.main()
