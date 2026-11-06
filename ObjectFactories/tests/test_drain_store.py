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
The one-loop drain law through the store (one-loop-drain prompt 03 §1.8, ObjectFactories; README §8.11 P27-P32;
orchestrator note 03, rulings 1-9).

Each test goes the whole way on a temporary SQLite file through `_TempStoreCase`, with the harness of
`test_dissipation_store` (`_DissipationStoreCase`: the parameter rows and the model rows made by the factories,
`ScalarHistory.store()` run with `ray.wait` and `ray.get` patched to hand it the payload, the factory's `store`
writing the row, its samples and its bounce rows with the datastore's own inserters, the row read back through
`object_get`), with the alpha row of a named drain model.

The one-loop histories are `test_injection`'s setup (n = 1, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to T_CMB, the
default tolerances), untuned at the model's seed realization, under the one-loop law; at each the seed is what
02d's tuning accepted (one integration):
- **P-2197**, M = 4e-31 M_P, beta = 1.185, A = 1: five one-loop candidates, parked (about 2-4 s);
- **L-M8e-18**, M = 8e-18 M_P, beta = 2, A = 2: three one-loop candidates and one in the gap, parked (about 2-4 s);
- **L-M4e-15**, M = 4e-15 M_P, beta = 2, A = 1: 28 classical candidates, parked (about 5-8 s).
The fixed-law histories are `test_dissipation_store`'s (M = 0.5 M_P, beta = 2, alpha = 0.1 and 0), shared through
`dissipation_histories`' cache.

- **the round trip of a one-loop history**: every new column raw on the row and on the object, bitwise to the
  conversion definition (the row equals the forward conversion of the payload's value, the object the inverse
  conversion of the row; ruling 9's helpers); the bounce rows in order, with their count; the sample masses; the
  same read with `_do_not_populate`, without the rows;
- **constructed extremes**: alpha_eff = 2.3e-108, L = 1e-11, and a history of 13 400 bounce rows (02d's largest is
  13 343), which crosses the batch size of 250 many times;
- **the fixed law at alpha > 0 and at alpha = 0**: no bounce row, the drain record's columns NULL, the masses
  written;
- **a failure row**: every new column NULL, no bounce row;
- **the key**: the same value under the two models is two rows and two histories, and a lookup under one finds
  neither the other's row nor its history; a lookup with no model is `fixed`; A = 0 under the one-loop model and
  an unknown model are refused before any row;
- **the guard** (P28): a payload whose model differs from the key's stores nothing;
- **the configuration**: the sharding, the format guard at 7 beside 1-6.

No Ray cluster, no persistent datastore, no PRyMordial solve.
"""

import contextlib
import functools
import io
import unittest
from math import log
from unittest import mock

import sqlalchemy as sqla

from ComputeTargets.tests import dissipation_histories as dh
from CosmologyConcepts import temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from ObjectFactories.ScalarHistory import (
    BOUNCE_COLUMNS,
    MASS_COLUMNS,
    from_column,
    to_column,
)
from ObjectFactories.tests.test_dissipation_store import (
    NATURAL,
    TEST_1,
    _DissipationStoreCase,
)
from ObjectFactories.tests.test_version_keyed_lookups import LABEL_A

SM = dh.SM
FIXED = "fixed"
ONE_LOOP = "one-loop-measured-v1"

# (beta, M/M_P, A) under the one-loop law
P2197 = (1.185, 4.0e-31, 1.0)
GAP = (2.0, 8.0e-18, 2.0)
CLASSICAL = (2.0, 4.0e-15, 1.0)


@functools.lru_cache(maxsize=None)
def one_loop_history(beta: float, M_over_Mp: float, A: float):
    """integrate_history's payload under the one-loop law at test_injection's setup, the seed realization."""
    cos = dh.cosmology()
    units = cos.units
    with contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            cos,
            temperature(0, dh.T_INIT_GEV * units.GeV),
            temperature(1, cos.T_CMB_Kelvin * units.Kelvin),
            phi_value(0, dh.PHI_INIT_OVER_MP * units.PlanckMass),
            pi_value(0, dh.PI_INIT),
            dh.z_grid(),
            dh.realized_model(beta, M_over_Mp),
            task_label=f"one-loop-b{beta:g}-M{M_over_Mp:g}-A{A:g}",
            alpha=A,
            drain_law=SM.DRAIN_LAW_ONE_LOOP,
        )
    if data.get("failure", False):
        raise RuntimeError(
            f"one_loop_history {beta, M_over_Mp, A}: {data['failure_reason']}"
        )
    return data


def one_loop_payload(beta: float, M_over_Mp: float, A: float) -> dict:
    data = one_loop_history(beta, M_over_Mp, A)
    rm = dh.realized_model(beta, M_over_Mp)
    out = dict(data)
    out.update(dh.STAND_IN_TUNING)
    out["normalization"] = float(rm.normalization)
    out["phi_ref"] = float(rm.phi_ref)
    return out


class _DrainStoreCase(_DissipationStoreCase):
    """_DissipationStoreCase with the alpha row of a named drain model (the default: fixed)."""

    drain_model = FIXED

    def alpha(self, value: float):
        return self.store.object_get(
            "alpha_value", value=value, drain_model=self.drain_model
        )

    def under(self, drain_model: str):
        """A context in which alpha rows are of drain_model."""
        case = self

        @contextlib.contextmanager
        def ctx():
            saved = case.drain_model
            case.drain_model = drain_model
            try:
                yield
            finally:
                case.drain_model = saved

        return ctx()

    def write_one_loop(self, key, payload=None):
        beta, M, A = key
        with self.under(ONE_LOOP):
            return self.write(payload or one_loop_payload(*key), beta, M, A)

    def read_one_loop(self, key, **extra):
        beta, M, A = key
        with self.under(ONE_LOOP):
            return self.read(beta, M, A, **extra)

    def raw_bounces(self, serial: int):
        table = self.store._tables["ScalarHistoryBounce"]
        with self.store._engine.begin() as conn:
            rows = conn.execute(
                sqla.select(table)
                .where(table.c.history_serial == serial)
                .order_by(table.c.ordinal)
            ).all()
        return [r._asdict() for r in rows]

    def raw_samples(self, serial: int):
        table = self.store._tables["ScalarHistoryValue"]
        with self.store._engine.begin() as conn:
            rows = conn.execute(
                sqla.select(table).where(table.c.history_serial == serial)
            ).all()
        return {r.z_serial: r._asdict() for r in rows}

    def count(self, table_name: str) -> int:
        table = self.store._tables[table_name]
        with self.store._engine.begin() as conn:
            return conn.execute(
                sqla.select(sqla.func.count()).select_from(table)
            ).scalar()

    def assert_same(self, got, expected, what):
        """Bitwise for a float, equal and of the same type otherwise, None for None."""
        if expected is None:
            self.assertIsNone(got, what)
        elif isinstance(expected, bool):
            self.assertIs(bool(got), expected, what)
            self.assertIn(got, (True, False, 0, 1), what)
        elif isinstance(expected, float):
            self.assertIsInstance(got, float, what)
            self.assertEqual(got.hex(), expected.hex(), what)
        else:
            self.assertEqual(type(got), type(expected), what)
            self.assertEqual(got, expected, what)

    def check_bounces(self, payload, serial, got=None):
        """The bounce rows raw (the forward conversion of the payload's) and on the object (the inverse
        conversion of the row), in order, with their count."""
        rows = payload["bounces"].rows
        raw = self.raw_bounces(serial)
        self.assertEqual(len(raw), len(rows))
        self.assertEqual(self.raw(serial)["bounce_rows"], len(rows))
        units = self.units
        for row, stored in zip(rows, raw):
            for field, column, kind in BOUNCE_COLUMNS:
                value = getattr(row, field)
                expected = (
                    value
                    if isinstance(value, bool) or value is None
                    else to_column(value, kind, units)
                )
                self.assert_same(
                    stored[column], expected, f"row {row.ordinal} column {column}"
                )
                # ruling 9: a nonzero value never converts to 0.0 or to +-inf
                if isinstance(value, float) and value != 0.0:
                    self.assertNotEqual(stored[column], 0.0, column)
                    self.assertTrue(abs(stored[column]) < float("inf"), column)
        if got is not None:
            self.assertEqual(len(got.bounces), len(rows))
            for back, stored in zip(got.bounces, raw):
                for field, column, kind in BOUNCE_COLUMNS:
                    value = stored[column]
                    if field in ("resolved", "weighted") and value is not None:
                        value = bool(value)
                    self.assert_same(
                        getattr(back, field),
                        from_column(value, kind, units),
                        f"bounce {back.ordinal}.{field}",
                    )

    def check_masses(self, payload, serial, got=None):
        raw = self.raw_samples(serial)
        units = self.units
        by_serial = {}
        for z, eff, tr in zip(
            payload["z_grid"], payload["log_m_eff"], payload["log_m_tracker"]
        ):
            by_serial[z.store_id] = (eff, tr)
        self.assertEqual(set(raw), set(by_serial))
        for z, (eff, tr) in by_serial.items():
            for (attribute, column, kind), value in zip(MASS_COLUMNS, (eff, tr)):
                self.assert_same(
                    raw[z][column], to_column(value, kind, units), f"z={z} {column}"
                )
        if got is not None:
            for value in got.values:
                stored = raw[value.z.store_id]
                for attribute, column, kind in MASS_COLUMNS:
                    self.assert_same(
                        getattr(value, attribute),
                        from_column(stored[column], kind, units),
                        f"z={value.z.store_id} {attribute}",
                    )

    def check_history_columns(self, payload, serial, got):
        expected = SM.drain_history_values(payload)
        raw = self.raw(serial)
        for key in SM.DrainHistoryValues._fields:
            self.assert_same(raw[key], getattr(expected, key), f"column {key}")
            self.assert_same(
                getattr(got.drain_history, key), raw[key], f"drain_history.{key}"
            )


class TestAOneLoopHistoryRoundTrips(_DrainStoreCase):
    """2197 at A = 1: one-loop candidates, parked."""

    def test_every_new_column_raw_and_on_the_object(self):
        payload = one_loop_payload(*P2197)
        written, serial = self.write_one_loop(P2197, payload)
        got = self.read_one_loop(P2197)
        self.assertTrue(got.available)
        self.assertEqual(got.store_id, serial)
        self.assertEqual((got.alpha.value, got.alpha.drain_model), (1.0, ONE_LOOP))
        self.assertEqual(got.alpha.drain_law, SM.DRAIN_LAW_ONE_LOOP)
        self.check_bounces(payload, serial, got)
        self.check_masses(payload, serial, got)
        self.check_history_columns(payload, serial, got)
        # the written object holds the payload's records as they are
        self.assertEqual(written.bounces, payload["bounces"].rows)
        self.assertEqual(written.drain_history, SM.drain_history_values(payload))
        # the history's own record: one-loop only, parked, events among the rows
        record = got.drain_history
        self.assertEqual(record.candidates_one_loop, len(payload["injections"]))
        self.assertGreater(record.parking_share, 0.0)
        self.assertGreater(record.bounce_rows, len(payload["injections"]))
        self.assertTrue(any(r.source == "plateau" for r in got.bounces))

    def test_the_same_with_do_not_populate(self):
        payload = one_loop_payload(*P2197)
        _, serial = self.write_one_loop(P2197, payload)
        got = self.read_one_loop(P2197, _do_not_populate=True)
        self.assertTrue(got.available)
        self.check_history_columns(payload, serial, got)
        with self.assertRaises(RuntimeError):
            got.bounces
        with self.assertRaises(RuntimeError):
            got.values

    def test_the_rows_are_in_ordinal_order_and_validated(self):
        payload = one_loop_payload(*P2197)
        _, serial = self.write_one_loop(P2197, payload)
        raw = self.raw_bounces(serial)
        self.assertEqual([r["ordinal"] for r in raw], list(range(len(raw))))
        # validate counts them against bounce_rows, as the samples against z_samples
        history = self.read_one_loop(P2197)
        table = self.store._tables["ScalarHistory"]
        with self.store._engine.begin() as conn, contextlib.redirect_stdout(
            io.StringIO()
        ):
            self.assertTrue(
                self.store._factories["ScalarHistory"].validate(
                    history, conn, table, self.store._tables
                )
            )
            bounce_table = self.store._tables["ScalarHistoryBounce"]
            conn.execute(
                sqla.delete(bounce_table).where(
                    bounce_table.c.ordinal == 0,
                    bounce_table.c.history_serial == serial,
                )
            )
            self.assertFalse(
                self.store._factories["ScalarHistory"].validate(
                    history, conn, table, self.store._tables
                )
            )


class TestRealOneLoopHistories(_DrainStoreCase):
    """A history with a gap candidate (L-M8e-18, A = 2) and one with classical candidates (L-M4e-15, A = 1)."""

    def check(self, key, regime):
        payload = one_loop_payload(*key)
        _, serial = self.write_one_loop(key, payload)
        got = self.read_one_loop(key)
        self.check_bounces(payload, serial, got)
        self.check_masses(payload, serial, got)
        self.check_history_columns(payload, serial, got)
        self.assertGreater(getattr(got.drain_history, f"candidates_{regime}"), 0)
        self.assertIsNotNone(got.drain_history.parking_share)
        return got

    def test_a_gap_candidate(self):
        got = self.check(GAP, "gap")
        self.assertTrue(any(r.regime == "gap" for r in got.bounces))

    def test_classical_candidates(self):
        got = self.check(CLASSICAL, "classical")
        self.assertGreater(got.drain_history.drained_share_classical, 0.0)


class TestConstructedExtremes(_DrainStoreCase):
    """2197's payload with constructed bounce rows: the extremes of 02d, and 13 400 rows. About 5-15 s for the
    rows written and read back."""

    N_ROWS = 13400

    def payload(self):
        payload = one_loop_payload(*P2197)
        template = payload["bounces"].rows[0]
        rows = []
        for k in range(self.N_ROWS):
            rows.append(
                template._replace(
                    ordinal=k,
                    candidate_index=k,
                    N=20.0 + k * 1.0e-3,
                    N_turn=20.0 + k * 1.0e-3,
                    alpha_eff=2.3e-108 if k % 2 == 0 else template.alpha_eff,
                    L=1.0e-11 if k % 3 == 0 else template.L,
                    log_R=-239.0 if k % 5 == 0 else template.log_R,
                    delta_K=(
                        template.delta_K * 2.3e-108 if k % 2 == 0 else template.delta_K
                    ),
                    weighted=(k % 7 == 0),
                    ell=None if k % 11 == 0 else template.ell,
                )
            )
        payload["bounces"] = SM.BounceRecord(
            rows=tuple(rows), K_final=payload["bounces"].K_final
        )
        return payload

    def test_the_extremes_and_the_row_count(self):
        payload = self.payload()
        _, serial = self.write_one_loop(P2197, payload)
        got = self.read_one_loop(P2197)
        self.assertEqual(got.drain_history.bounce_rows, self.N_ROWS)
        self.check_bounces(payload, serial, got)
        self.assertEqual(got.bounces[0].alpha_eff, 2.3e-108)
        self.assertEqual(got.bounces[0].L, 1.0e-11)
        self.assertEqual(got.bounces[0].log_R, -239.0)


class TestTheFixedLaw(_DrainStoreCase):
    """The fixed law at alpha = 0.1 and at alpha = 0 (test_dissipation_store's histories): no bounce row, the drain
    record's columns NULL, the masses written."""

    def check(self, key):
        payload = dh.payload(*key)
        self.assertIsNone(payload["bounces"])
        written, serial = self.write(payload, *key)
        got = self.read(*key)
        self.assertEqual(self.raw_bounces(serial), [])
        raw = self.raw(serial)
        for name in SM.DrainHistoryValues._fields:
            self.assertIsNone(raw[name], name)
        self.assertIsNone(got.drain_history)
        self.assertIsNone(written.drain_history)
        self.assertEqual(got.bounces, ())
        self.check_masses(payload, serial, got)
        self.assertEqual(got.alpha.drain_model, FIXED)
        # the masses are real numbers where they are defined
        self.assertGreater(sum(m is not None for m in payload["log_m_eff"]), 0)

    def test_alpha_positive(self):
        self.check(TEST_1)

    def test_alpha_zero(self):
        self.check(NATURAL)


class TestAFailureRow(_DrainStoreCase):
    def test_every_new_column_null(self):
        with self.under(ONE_LOOP):
            written, serial = self.write(
                {"failure": True, "failure_reason": "stand-in"}, *P2197
            )
        raw = self.raw(serial)
        for name in SM.DrainHistoryValues._fields:
            self.assertIsNone(raw[name], name)
        self.assertEqual(self.raw_bounces(serial), [])
        got = self.read_one_loop(P2197, failure=True)
        self.assertTrue(got.failure)
        self.assertIsNone(got.drain_history)


class TestTheKey(_DrainStoreCase):
    def test_two_models_are_two_rows_and_two_histories(self):
        with self.under(ONE_LOOP):
            a_one_loop = self.alpha(1.0e-1)
        a_fixed = self.alpha(1.0e-1)
        self.assertNotEqual(a_one_loop.store_id, a_fixed.store_id)
        self.assertEqual(self.count("alpha_value"), 2)

        # a one-loop history at A = 0.1 is not cheap; a failure row stands in for it, the fixed one is real
        beta, M, alpha = TEST_1
        _, serial_fixed = self.write(dh.payload(*TEST_1), *TEST_1)
        with self.under(ONE_LOOP):
            _, serial_one_loop = self.write(
                {"failure": True, "failure_reason": "stand-in"}, beta, M, alpha
            )
        got_fixed = self.read(*TEST_1, _do_not_populate=True)
        with self.under(ONE_LOOP):
            got_one_loop = self.read(*TEST_1, _do_not_populate=True)
        self.assertEqual(got_fixed.store_id, serial_fixed)
        self.assertEqual(got_one_loop.store_id, serial_one_loop)
        self.assertFalse(got_fixed.failure)
        self.assertTrue(got_one_loop.failure)
        self.assertEqual(self.raw(serial_fixed)["alpha_serial"], a_fixed.store_id)
        self.assertEqual(self.raw(serial_one_loop)["alpha_serial"], a_one_loop.store_id)

    def test_a_lookup_with_no_model_is_fixed(self):
        with self.under(ONE_LOOP):
            self.alpha(0.5)
        self.assertEqual(self.count("alpha_value"), 1)
        plain = self.store.object_get("alpha_value", value=0.5)
        self.assertEqual(plain.drain_model, FIXED)
        self.assertEqual(self.count("alpha_value"), 2)
        self.assertEqual(
            self.store.object_get("alpha_value", value=0.5, drain_model=FIXED).store_id,
            plain.store_id,
        )

    def test_the_refusals_write_nothing(self):
        for kwargs in (
            dict(value=0.0, drain_model=ONE_LOOP),
            dict(value=-1.0, drain_model=ONE_LOOP),
            dict(value=1.5, drain_model=FIXED),
            dict(value=0.1, drain_model="one-loop-measured-v0"),
        ):
            with self.subTest(**kwargs):
                with self.assertRaises(ValueError):
                    self.store.object_get("alpha_value", **kwargs)
                self.assertEqual(self.count("alpha_value"), 0)

    def test_read_table_returns_the_pairs(self):
        with self.under(ONE_LOOP):
            self.alpha(2.0)
        self.alpha(0.0)
        table = self.store._tables["alpha_value"]
        with self.store._engine.begin() as conn:
            rows = self.store._factories["alpha_value"].read_table(conn, table)
        self.assertEqual(
            [(a.value, a.drain_model) for a in rows], [(0.0, FIXED), (2.0, ONE_LOOP)]
        )


class TestTheGuard(_DrainStoreCase):
    """P28, ruling 3: a success payload whose drain model is not the key's is refused before anything is set or
    stored; a missing record is accepted under the fixed model only."""

    def refused(self, payload, drain_model, key):
        beta, M, alpha = key
        with self.under(drain_model):
            history = SM.ScalarHistory(
                payload=None,
                solver_labels=self.solvers,
                cosmology=self.cosmology,
                T_Jordan_init=self.T_init,
                phi_Einstein_init=self.phi,
                pi_Einstein_init=self.pi,
                model=self.model_row(beta, M),
                tuning_tol=self.tuning_tol,
                alpha=self.alpha(alpha),
                atol=self.atol,
                rtol=self.rtol,
                label="guard",
                tags=[],
            )
        history._compute_ref = object()
        with mock.patch.object(
            SM.ray, "wait", lambda refs, timeout: (refs, [])
        ), mock.patch.object(SM.ray, "get", lambda ref: payload):
            with self.assertRaises(RuntimeError):
                history.store()
        self.assertIsNone(history._failure)
        self.assertIsNone(history._values)
        self.assertEqual(self.count("ScalarHistory"), 0)

    def test_a_one_loop_payload_under_a_fixed_key(self):
        self.refused(one_loop_payload(*P2197), FIXED, (P2197[0], P2197[1], 0.5))

    def test_a_fixed_payload_under_a_one_loop_key(self):
        self.refused(dh.payload(*TEST_1), ONE_LOOP, TEST_1)

    def test_no_drain_record_under_a_one_loop_key(self):
        payload = dict(one_loop_payload(*P2197))
        payload["drain"] = None
        self.refused(payload, ONE_LOOP, P2197)

    def test_no_drain_record_under_a_fixed_key_is_accepted(self):
        payload = dict(dh.payload(*TEST_1))
        del payload["drain"]
        _, serial = self.write(payload, *TEST_1)
        self.assertTrue(self.read(*TEST_1).available)

    def test_a_payload_without_the_masses_is_refused(self):
        payload = dict(dh.payload(*TEST_1))
        del payload["log_m_tracker"]
        with self.assertRaises(RuntimeError):
            self.write(payload, *TEST_1)


class TestTheConfiguration(unittest.TestCase):
    def test_the_sharding_and_the_format(self):
        from config.fingerprint import FINGERPRINT_FORMAT, SPEC_DIGESTS
        from config.sharding import replicated_tables, sharded_tables

        self.assertEqual(sharded_tables["ScalarHistoryBounce"], "shard_key")
        self.assertIn("alpha_value", replicated_tables)
        self.assertNotIn("ScalarHistoryBounce", replicated_tables)
        self.assertEqual(FINGERPRINT_FORMAT, 7)
        self.assertEqual(sorted(SPEC_DIGESTS), [1, 2, 3, 4, 5, 6, 7])
        six, seven = dict(SPEC_DIGESTS[6]), dict(SPEC_DIGESTS[7])
        # format 6's pin is as it was: alpha_value shared beta_value's digest
        self.assertEqual(six["alpha_value"], six["beta_value"])
        self.assertNotEqual(seven["alpha_value"], seven["beta_value"])
        self.assertEqual({n for n in seven if seven[n] != six[n]}, {"alpha_value"})


if __name__ == "__main__":
    unittest.main()
