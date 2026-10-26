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
A tuned ScalarHistory through the store (jordan-normalization prompt 07, S8; orchestrator note 07,
ruling 14): the key is (model, initial data, tuning tolerance), the row holds what the tuning
found, and the history's realized model is rebuilt from it.

The payload is the tuned golden history's (`ComputeTargets/tests/golden_tuned_history.py`,
`data/golden_history_06_tuned.json`) without re-tuning it: one `integrate_history` with the golden
setup at the JSON's stored normalization and phi_ref (read with `float.fromhex`), to T_CMB, plus
the JSON's `tuning` keys. Test (a) first checks that this integration's record has no difference
from the JSON's `history`, so that it is the final integration of the tuning. The sampling axis is
the golden one with the store ids k + 1 (the golden's are k), so that no redshift row has serial 0;
the ids do not enter the integration.

Each test goes the whole way on a temporary SQLite file through `_TempStoreCase`, as
`test_first_bounce_round_trip` does: the parameter rows and the model's row are made by the
factories (`object_get`), `ScalarHistory.store()` is run with `ray.wait` and `ray.get` patched to
hand it the payload, the factory's `store` writes the row and its samples with the datastore's own
inserters, and the row is read back through `object_get`.

- (a) every tuning column bitwise, raw (the row) and on the object (`tuning`); every sample field
  bitwise against the payload's samples; `realized_model` rebuilt equal to the one the history was
  computed with;
- (b) the lookup finds the row by (model, initial data, tuning tolerance), and not with another
  phi*, another tuning tolerance (1e-6) or another model (n = 2, or another beta);
- (c) a failure row stores every tuning column NULL, reads back as a failure, and its `tuning`
  and `realized_model` raise;
- (d) read with `_do_not_populate`, the row gives `tuning` and `realized_model`, and its `values`
  raises;
- (e) that read-back history through `extract_common.build_history_record` and
  `write_histories_csv` into the temporary directory (jordan-normalization prompt 08, D6): the
  header is `CSV_COLUMNS`, and each tuning cell, read back with `float`, equals the stored value
  converted (V_ref in eV^4, phi_ref and phi_0 in M_P, the rest as stored).

No Ray cluster, no persistent datastore, no PRyMordial solve. Cost: the one integration, in
setUpModule (about 5 s unloaded; 20-30 s at a load of 10-20), and about 1-2 s per test for the
5960 samples written and read back.
"""

import contextlib
import csv
import io
import json
from unittest import mock

import sqlalchemy as sqla

import ComputeTargets.tests.test_kinematic_cap_loop as kcl
from ComputeTargets.ScalarHistory import (
    SCALAR_HISTORY_STEPPER_LABEL,
    TUNING_OUTPUT_KEYS,
    SampleValues,
    TuningValues,
    integrate_history,
)
from ComputeTargets.tests import golden_tuned_history as gt
from ComputeTargets.tests.golden_history import first_difference, record_of
from CosmologyConcepts import redshift, redshift_array, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models import ExponentialModel, RealizedModel
from CosmologyConcepts.redshift import log_one_plus_z_grid
from ObjectFactories.tests.test_scalarhistory_failure_reason import _write
from extract_common import CSV_COLUMNS, build_history_record, write_histories_csv
from ObjectFactories.tests.test_version_keyed_lookups import LABEL_A, _TempStoreCase

SM = kcl.SM

_MODULE = {}

# the field values at which the rebuilt realized model is compared, in units of M_P
PHI_PROBES_OVER_MP = (1.0e-3, 0.02, 0.0622, 0.5, 5.0)


def setUpModule():
    """The tuned payload: one integration at the JSON's tuned (normalization, phi_ref)."""
    golden = json.loads(gt.GOLDEN_TUNED_PATH.read_text())
    tuning = {
        key: (
            float.fromhex(value)
            if isinstance(value, str) and key != "seed_kind"
            else value
        )
        for key, value in golden["tuning"].items()
    }

    with contextlib.redirect_stdout(io.StringIO()):
        cosmology = gt.make_cosmology()
    units = cosmology.units
    xs = log_one_plus_z_grid(gt.SAMPLES_PER_LOG10_Z, gt.LOG10_ONE_PLUS_Z_HIGH)
    z_grid = redshift_array(z_array=[redshift(k + 1, x) for k, x in enumerate(xs)])

    realized = gt.make_model(units).realize(tuning["normalization"], tuning["phi_ref"])
    with contextlib.redirect_stdout(io.StringIO()):
        data = integrate_history(
            cosmology,
            temperature(0, gt.T_INIT_GEV * units.GeV),
            cosmology.T_CMB_Kelvin * units.Kelvin,
            phi_value(0, gt.PHI_INIT_OVER_MP * units.PlanckMass),
            pi_value(0, gt.PI_INIT),
            z_grid,
            realized,
            task_label=gt.TASK_LABEL,
            atol=gt.ATOL,
            rtol=gt.RTOL,
        )

    payload = dict(data)
    payload.update({key: tuning[key] for key in TUNING_OUTPUT_KEYS})

    _MODULE.update(
        golden=golden,
        tuning=tuning,
        cosmology=cosmology,
        units=units,
        data=data,
        payload=payload,
    )


class _TunedStoreCase(_TempStoreCase):
    """A store holding the parameter rows of the golden setup and its model, made by the
    factories; the history is written by `write`."""

    def setUp(self):
        super().setUp()
        self.units = _MODULE["units"]
        self.cosmology = _MODULE["cosmology"]
        self.store = self.open(LABEL_A)
        units = self.units

        get = self.store.object_get
        self.solver = get("IntegrationSolver", label="Radau+kinematic-cap", stepping=0)
        self.solvers = {SCALAR_HISTORY_STEPPER_LABEL: self.solver}
        self.atol = get("tolerance", tol=gt.ATOL)
        self.rtol = get("tolerance", tol=gt.RTOL)
        self.tuning_tol = get("tolerance", tol=gt.TOLERANCE)
        self.T_init = get("temperature", value=gt.T_INIT_GEV * units.GeV, units=units)
        self.phi = get(
            "phi_value", value=gt.PHI_INIT_OVER_MP * units.PlanckMass, units=units
        )
        self.pi = get("pi_value", value=gt.PI_INIT, units=units)
        self.M = get("M_value", value=gt.M_OVER_MP * units.PlanckMass, units=units)
        self.beta = get("beta_value", value=gt.BETA)
        self.model = self.model_row(gt.N_EXPONENT, self.beta)

        # the sampled points, with the serials the payload's samples carry
        with self.store._engine.begin() as conn:
            for z in _MODULE["data"]["z_grid"]:
                self.store._schema["redshift"]["insert"](
                    conn, dict(serial=z.store_id, log_one_plus_z=z.log_one_plus_z)
                )

    def model_row(self, n: int, beta) -> ExponentialModel:
        return self.store.object_get(
            "ExponentialModel", M=self.M, n=n, beta=beta, units=self.units
        )

    def query(self, **changes) -> dict:
        q = dict(
            solver_labels=self.solvers,
            cosmology=self.cosmology,
            T_Jordan_init=self.T_init,
            phi_Einstein_init=self.phi,
            pi_Einstein_init=self.pi,
            model=self.model,
            tuning_tol=self.tuning_tol,
            atol=self.atol,
            rtol=self.rtol,
            z_grid=None,
            tags=[],
        )
        q.update(changes)
        return q

    def write(self, compute_result, model=None):
        """A ScalarHistory on this store's rows, populated by store() from compute_result, and
        written by the factory's store."""
        history = SM.ScalarHistory(
            payload=None,
            solver_labels=self.solvers,
            cosmology=self.cosmology,
            T_Jordan_init=self.T_init,
            phi_Einstein_init=self.phi,
            pi_Einstein_init=self.pi,
            model=self.model if model is None else model,
            tuning_tol=self.tuning_tol,
            atol=self.atol,
            rtol=self.rtol,
            label="tuned",
            tags=[],
        )
        history._compute_ref = object()
        with mock.patch.object(
            SM.ray, "wait", lambda refs, timeout: (refs, [])
        ), mock.patch.object(SM.ray, "get", lambda ref: compute_result):
            assert history.store() is True
        _write(self.store, history)
        return history

    def raw_tuning(self) -> dict:
        table = self.store._tables["ScalarHistory"]
        with self.store._engine.begin() as conn:
            row = conn.execute(
                sqla.select(*(table.c[key] for key in TUNING_OUTPUT_KEYS))
            ).one()
        return dict(zip(TUNING_OUTPUT_KEYS, row))

    def assert_same(self, got, expected, what):
        """Bitwise for a float (and of the same type), equal otherwise."""
        self.assertIs(type(got), type(expected), what)
        if isinstance(expected, float):
            self.assertEqual(got.hex(), expected.hex(), what)
        else:
            self.assertEqual(got, expected, what)


class TestATunedHistoryRoundTrips(_TunedStoreCase):
    def test_a_the_payload_is_the_tuned_golden_history(self):
        """(a) One integration at the stored (normalization, phi_ref) is the tuned history: its
        record has no difference from the JSON's."""
        self.assertFalse(_MODULE["data"].get("failure", False))
        self.assertEqual(
            first_difference(_MODULE["golden"]["history"], record_of(_MODULE["data"])),
            [],
        )

    def test_a_every_tuning_column_and_sample_round_trips_bitwise(self):
        """(a) Every tuning column, raw and on the object, and every sample field, bitwise."""
        payload = _MODULE["payload"]
        written = self.write(payload)
        self.assertEqual(
            written.tuning,
            TuningValues(**{key: payload[key] for key in TUNING_OUTPUT_KEYS}),
        )

        # the row: normalization and seed_normalization as they are, the rest in M_P or M_P^4,
        # which in Planck_units is 1.0, so every column is the payload's value bitwise
        raw = self.raw_tuning()
        self.assertEqual(self.units.PlanckMass, 1.0)
        for key in TUNING_OUTPUT_KEYS:
            self.assert_same(raw[key], payload[key], f"column {key}")

        got = self.store.object_get("ScalarHistory", **self.query())
        self.assertTrue(got.available)
        self.assertFalse(got.failure)
        self.assertIsInstance(got.tuning, TuningValues)
        for key in TUNING_OUTPUT_KEYS:
            self.assert_same(getattr(got.tuning, key), payload[key], f"tuning.{key}")

        # every sample, matched by its redshift's serial, field by field
        by_serial = {
            z.store_id: s for z, s in zip(payload["z_grid"], payload["sample"])
        }
        self.assertEqual(len(got.values), len(payload["sample"]))
        self.assertEqual(len(by_serial), len(payload["sample"]))
        # (a sample's fields are numpy float64 in the payload and float when read back: the
        # values are compared bit for bit)
        for value in got.values:
            expected = by_serial[value.z.store_id]
            for name in SampleValues._fields:
                self.assertEqual(
                    float(getattr(value, name)).hex(),
                    float(getattr(expected, name)).hex(),
                    f"sample z={value.z.store_id} {name}",
                )

    def test_a_the_realized_model_is_rebuilt_equal(self):
        """(a) realized_model is the model at the stored normalization and phi_ref, equal to the
        one the history was computed with at five field values."""
        payload = _MODULE["payload"]
        self.write(payload)
        got = self.store.object_get("ScalarHistory", **self.query())

        rm = got.realized_model
        self.assertIsInstance(rm, RealizedModel)
        self.assert_same(rm.normalization, payload["normalization"], "normalization")
        self.assert_same(rm.phi_ref, payload["phi_ref"], "phi_ref")
        self.assertIsInstance(rm.model, ExponentialModel)
        self.assertEqual(rm.model.store_id, self.model.store_id)
        self.assertEqual(rm.model._M.store_id, self.M.store_id)
        self.assertEqual(rm.model._beta.store_id, self.beta.store_id)
        self.assertEqual(rm.model._M_float, gt.M_OVER_MP * self.units.PlanckMass)
        self.assertEqual(rm.model._n, gt.N_EXPONENT)
        self.assertEqual(rm.model._beta_float, gt.BETA)

        computed = gt.make_model(self.units).realize(
            payload["normalization"], payload["phi_ref"]
        )
        for x in PHI_PROBES_OVER_MP:
            phi = x * self.units.PlanckMass
            for method in ("log_V", "d_logV_dphi", "log_Omega"):
                self.assert_same(
                    getattr(rm, method)(phi),
                    getattr(computed, method)(phi),
                    f"{method}({x} M_P)",
                )
        # built once, and cached
        self.assertIs(got.realized_model, rm)


class TestTheKey(_TunedStoreCase):
    def test_b_found_by_its_key_and_not_by_another(self):
        """(b) The same (model, initial data, tuning tolerance) finds the row; another phi*,
        another tuning tolerance, another n or another beta does not."""
        written = self.write(_MODULE["payload"])
        units = self.units

        found = self.store.object_get(
            "ScalarHistory", **self.query(_do_not_populate=True)
        )
        self.assertTrue(found.available)
        self.assertEqual(found.store_id, written.store_id)

        get = self.store.object_get
        others = {
            "another phi*": dict(
                phi_Einstein_init=get(
                    "phi_value", value=4.0 * units.PlanckMass, units=units
                )
            ),
            "another tuning tolerance": dict(tuning_tol=get("tolerance", tol=1.0e-6)),
            "another n": dict(model=self.model_row(2, self.beta)),
            "another beta": dict(
                model=self.model_row(gt.N_EXPONENT, get("beta_value", value=1.5))
            ),
        }
        for what, change in others.items():
            with self.subTest(what):
                got = self.store.object_get(
                    "ScalarHistory", **self.query(_do_not_populate=True, **change)
                )
                self.assertFalse(got.available, what)
                with self.assertRaises(RuntimeError):
                    got.tuning


class TestAFailureRow(_TunedStoreCase):
    def test_c_a_failure_row_stores_no_tuning(self):
        """(c) A failure stores every tuning column NULL, reads back as a failure, and its tuning
        and realized_model raise."""
        failure = SM._failure_payload("the normalization did not converge: x")
        written = self.write(failure)
        with self.assertRaises(RuntimeError):
            written.tuning

        self.assertEqual(self.raw_tuning(), {key: None for key in TUNING_OUTPUT_KEYS})

        got = self.store.object_get("ScalarHistory", **self.query(failure=True))
        self.assertTrue(got.available)
        self.assertTrue(got.failure)
        self.assertEqual(got.failure_reason, "the normalization did not converge: x")
        with self.assertRaises(RuntimeError):
            got.tuning
        with self.assertRaises(RuntimeError):
            got.realized_model


class TestDoNotPopulate(_TunedStoreCase):
    def test_d_tuning_and_the_realized_model_without_the_samples(self):
        """(d) Read with _do_not_populate: tuning and realized_model are there, values raises."""
        payload = _MODULE["payload"]
        self.write(payload)
        got = self.store.object_get(
            "ScalarHistory", **self.query(_do_not_populate=True)
        )
        self.assertTrue(got.available)
        for key in TUNING_OUTPUT_KEYS:
            self.assert_same(getattr(got.tuning, key), payload[key], f"tuning.{key}")
        self.assert_same(
            got.realized_model.normalization, payload["normalization"], "normalization"
        )
        self.assert_same(got.realized_model.phi_ref, payload["phi_ref"], "phi_ref")
        with self.assertRaises(RuntimeError):
            got.values


class TestTheHistoriesCsv(_TunedStoreCase):
    def test_e_histories_csv_from_the_read_back_history(self):
        """(e) histories.csv's tuning cells are the stored values, converted (prompt 08, D6)."""
        payload = _MODULE["payload"]
        self.write(payload)
        got = self.store.object_get(
            "ScalarHistory", **self.query(_do_not_populate=True)
        )
        units = self.units
        record = build_history_record(
            beta=gt.BETA,
            M_Mp=gt.M_OVER_MP,
            n=gt.N_EXPONENT,
            phi_init_Mp=gt.PHI_INIT_OVER_MP,
            scalar=got,
            bbn=None,
            baseline=None,
            units=units,
        )
        path = self._dir / "csv" / "histories.csv"
        write_histories_csv([record], path)
        with open(path, newline="") as f:
            rows = list(csv.reader(f))
        self.assertEqual(rows[0], CSV_COLUMNS)
        self.assertEqual(len(rows), 2)
        cells = dict(zip(rows[0], rows[1]))

        expected = {
            "normalization": payload["normalization"],
            "V_ref_eV4": payload["V_ref"] / units.eV**4,
            "phi_ref_Mp": payload["phi_ref"] / units.PlanckMass,
            "phi_0_Mp": payload["phi_0"] / units.PlanckMass,
            "tracking": payload["tracking"],
            "residual_H": payload["residual_H"],
            "residual_phi": payload["residual_phi"],
        }
        for column, value in expected.items():
            with self.subTest(column=column):
                self.assertEqual(float(cells[column]).hex(), value.hex())
        self.assertEqual(int(cells["tuning_iterations"]), payload["tuning_iterations"])
        self.assertEqual(int(cells["n"]), gt.N_EXPONENT)
        self.assertEqual(cells["Yp_BBN"], "")


if __name__ == "__main__":
    import unittest

    unittest.main()
