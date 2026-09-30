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
Version-keyed lookups of the compute targets (run-integrity prompt 01, README §6.1).

No Ray cluster, no PRyMordial solve, no persistent datastore. Each test builds the
undecorated `Datastore.__ray_actor_class__` on a SQLite file in a `tempfile`
directory, and reopens the same file under a second version label. About 1 s.

Rows are inserted through the datastore's own inserter,
`store._schema[<table>]["insert"]`, so the `version` column is filled by
production code (`Datastore._insert`). There is no serial broker, so each row is
given an explicit serial. The objects a lookup keys on (cosmology, potential,
coupling, parameter values, the model proxy) are stand-ins that carry only the
attributes the factories read. The pattern is that of
`prompts/run-integrity/planning-probes/datastore_version_probe.py`.

`config.version` is imported inside the tests that need it, not at module level,
so that on the tree before prompt 01 each test fails on its own assertion rather
than the whole module failing to import.
"""

import ast
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace as NS

import sqlalchemy as sqla

from Datastore.SQL.Datastore import Datastore

DS = Datastore.__ray_actor_class__

LABEL_A = "2026.3.0"
LABEL_B = "2026.4.0"

KEYED_TABLES = {"ScalarModel", "AdiabaticHistory", "BBNData"}

_UNITS = NS(PlanckMass=1.0, GeV=1.0, MeV=1.0e-3)


def _stand_in(store_id, type_id=0):
    return NS(store_id=store_id, type_id=type_id, units=_UNITS)


_cosmology = _stand_in(1, 7)
_potential = _stand_in(2, 3)
_coupling = _stand_in(4, 5)
_T_init, _T_stop = _stand_in(10), _stand_in(11)
_phi, _pi = _stand_in(12), _stand_in(13)
_atol, _rtol = _stand_in(14), _stand_in(15)

_MODEL_SERIAL = 1


def _scalar_model_query(**extra):
    q = dict(
        solver_labels={},
        cosmology=_cosmology,
        T_Jordan_init=_T_init,
        T_Jordan_stop=_T_stop,
        phi_Einstein_init=_phi,
        pi_Einstein_init=_pi,
        potential=_potential,
        coupling=_coupling,
        atol=_atol,
        rtol=_rtol,
        z_grid=None,
        tags=[],
        _do_not_populate=True,
    )
    q.update(extra)
    return q


def _model_proxy(store_id=_MODEL_SERIAL):
    return NS(
        store_id=store_id,
        get=lambda: NS(cosmology=_cosmology, coupling=_coupling, potential=_potential),
    )


def _sid(obj):
    return obj.store_id if obj.available else None


def _insert(store, cls_name, row):
    with store._engine.begin() as conn:
        serial = store._schema[cls_name]["insert"](conn, row)
        conn.commit()
    return serial


def _insert_failed_scalar_model(store):
    for serial, log10_tol in ((14, -10.0), (15, -8.0)):
        _insert(store, "tolerance", dict(serial=serial, log10_tol=log10_tol))
    return _insert(
        store,
        "ScalarModel",
        dict(
            serial=_MODEL_SERIAL,
            label="m",
            cosmology_type=7,
            cosmology_serial=1,
            potential_type=3,
            potential_serial=2,
            coupling_type=5,
            coupling_serial=4,
            atol_serial=14,
            rtol_serial=15,
            phi_Einstein_init_serial=12,
            pi_Einstein_init_serial=13,
            T_Jordan_init_serial=10,
            T_Jordan_stop_serial=11,
            failure=True,
            validated=True,
        ),
    )


def _insert_adiabatic_history(store, serial=1):
    # z_samples = 0 and no value rows: build() reads the (empty) value table and
    # checks the count, so the row is returned without any stored samples
    return _insert(
        store,
        "AdiabaticHistory",
        dict(
            serial=serial,
            model_serial=_MODEL_SERIAL,
            label="h",
            z_samples=0,
            compute_time=1.0,
            validated=True,
        ),
    )


def _insert_failed_bbn(store, serial, reason):
    return _insert(
        store,
        "BBNData",
        dict(
            serial=serial,
            model_serial=_MODEL_SERIAL,
            failure=True,
            failure_reason=reason,
            validated=True,
        ),
    )


def _insert_successful_bbn(store, serial):
    return _insert(
        store,
        "BBNData",
        dict(
            serial=serial,
            model_serial=_MODEL_SERIAL,
            failure=False,
            Yp_BBN=0.2468872958,
            DOverH=2.462251065,
            He3OverH=1.042050273,
            Li7OverH=5.423441017,
            small_network=False,
            PRyM_version="test",
            z_samples=0,
            validated=True,
        ),
    )


class _TempStoreCase(unittest.TestCase):
    def setUp(self):
        self._dir = Path(tempfile.mkdtemp(prefix="champbh-datastore-test-"))
        self.db = self._dir / "store.db"
        self._stores = []

    def tearDown(self):
        for store in self._stores:
            store._engine.dispose()
        shutil.rmtree(self._dir, ignore_errors=True)

    def open(self, label):
        store = DS(version_label=label, db_name=self.db)
        self._stores.append(store)
        return store


class TestVersionKeyedLookups(_TempStoreCase):
    def test_a_scalar_model_is_returned_only_under_its_own_label(self):
        """(a) A failed ScalarModel stored under label A is returned under A, and not under B."""
        store_a = self.open(LABEL_A)
        serial = _insert_failed_scalar_model(store_a)

        m = store_a.object_get("ScalarModel", **_scalar_model_query())
        self.assertTrue(m.available)
        self.assertEqual(m.store_id, serial)
        self.assertTrue(m.failure)

        store_b = self.open(LABEL_B)
        self.assertNotEqual(store_a._version.store_id, store_b._version.store_id)

        m_b = store_b.object_get("ScalarModel", **_scalar_model_query())
        self.assertFalse(
            m_b.available,
            f"ScalarModel stored under {LABEL_A} was returned under {LABEL_B} "
            f"(store_id={_sid(m_b)})",
        )

        # and the row is still there under A, unchanged
        store_a2 = self.open(LABEL_A)
        m_a2 = store_a2.object_get("ScalarModel", **_scalar_model_query())
        self.assertTrue(m_a2.available)
        self.assertEqual(m_a2.store_id, serial)

    def test_b_adiabatic_history_is_returned_only_under_its_own_label(self):
        """(b) An AdiabaticHistory keyed to a ScalarModel serial, stored under A, is returned under A, and not under B."""
        store_a = self.open(LABEL_A)
        _insert_failed_scalar_model(store_a)
        serial = _insert_adiabatic_history(store_a)

        h = store_a.object_get("AdiabaticHistory", model_proxy=_model_proxy(), tags=[])
        self.assertTrue(h.available)
        self.assertEqual(h.store_id, serial)

        store_b = self.open(LABEL_B)
        h_b = store_b.object_get(
            "AdiabaticHistory", model_proxy=_model_proxy(), tags=[]
        )
        self.assertFalse(
            h_b.available,
            f"AdiabaticHistory stored under {LABEL_A} was returned under {LABEL_B} "
            f"(store_id={_sid(h_b)})",
        )

        h_a2 = self.open(LABEL_A).object_get(
            "AdiabaticHistory", model_proxy=_model_proxy(), tags=[]
        )
        self.assertTrue(h_a2.available)
        self.assertEqual(h_a2.store_id, serial)

    def test_c1_failed_bbn_row_is_returned_only_under_its_own_label(self):
        """(c) A failed BBNData row stored under A: failure=True returns it under A, not under B."""
        store_a = self.open(LABEL_A)
        _insert_failed_scalar_model(store_a)
        serial = _insert_failed_bbn(store_a, 1, "stored under A")

        query = dict(
            model_proxy=_model_proxy(), tags=[], failure=True, _do_not_populate=True
        )
        d = store_a.object_get("BBNData", **query)
        self.assertTrue(d.available)
        self.assertEqual(d.store_id, serial)
        self.assertEqual(d.failure_reason, "stored under A")

        d_b = self.open(LABEL_B).object_get("BBNData", **query)
        self.assertFalse(
            d_b.available,
            f"failed BBNData stored under {LABEL_A} was returned under {LABEL_B} "
            f"(store_id={_sid(d_b)})",
        )

        d_a2 = self.open(LABEL_A).object_get("BBNData", **query)
        self.assertTrue(d_a2.available)
        self.assertEqual(d_a2.store_id, serial)
        self.assertEqual(d_a2.failure_reason, "stored under A")

    def test_c2_successful_bbn_row_is_returned_only_under_its_own_label(self):
        """(c) A successful BBNData row stored under A: the default lookup returns it under A, not under B."""
        store_a = self.open(LABEL_A)
        _insert_failed_scalar_model(store_a)
        serial = _insert_successful_bbn(store_a, 1)

        query = dict(model_proxy=_model_proxy(), tags=[], _do_not_populate=True)
        d = store_a.object_get("BBNData", **query)
        self.assertTrue(d.available)
        self.assertEqual(d.store_id, serial)

        d_b = self.open(LABEL_B).object_get("BBNData", **query)
        self.assertFalse(
            d_b.available,
            f"BBNData stored under {LABEL_A} was returned under {LABEL_B} "
            f"(store_id={_sid(d_b)})",
        )

        d_a2 = self.open(LABEL_A).object_get("BBNData", **query)
        self.assertTrue(d_a2.available)
        self.assertEqual(d_a2.store_id, serial)

    def test_c3_vectorized_lookup_is_keyed_too(self):
        """The vectorized route (payload_data=[...]) is keyed the same way as the scalar one."""
        store_a = self.open(LABEL_A)
        serial = _insert_failed_scalar_model(store_a)
        payloads = [_scalar_model_query(), _scalar_model_query()]

        got_a = store_a.object_get("ScalarModel", payload_data=payloads)
        self.assertEqual([m.store_id for m in got_a], [serial, serial])

        got_b = self.open(LABEL_B).object_get("ScalarModel", payload_data=payloads)
        self.assertEqual([m.available for m in got_b], [False, False])

    def test_d_keyed_build_without_the_serial_raises(self):
        """(d) A keyed factory's build(), called directly with a payload lacking the reserved key, raises."""
        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)
        _insert_adiabatic_history(store)
        _insert_failed_bbn(store, 1, "stored under A")

        payloads = {
            "ScalarModel": _scalar_model_query(),
            "AdiabaticHistory": dict(model_proxy=_model_proxy(), tags=[]),
            "BBNData": dict(
                model_proxy=_model_proxy(),
                tags=[],
                failure=True,
                _do_not_populate=True,
            ),
        }
        for cls_name, payload in payloads.items():
            with self.subTest(cls_name=cls_name):
                factory = store._factories[cls_name]
                with store._engine.begin() as conn:
                    with self.assertRaises(RuntimeError):
                        factory.build(
                            payload=payload,
                            conn=conn,
                            table=store._tables[cls_name],
                            inserter=store._inserters[cls_name],
                            tables=store._tables,
                            inserters=store._inserters,
                        )

    def test_d2_payload_is_copied_and_the_reserved_key_is_the_datastores(self):
        """object_get() does not mutate the caller's payload, and refuses a caller-supplied serial."""
        from config.version import VERSION_SERIAL_KEY

        store = self.open(LABEL_A)
        _insert_failed_scalar_model(store)

        payload = _scalar_model_query()
        before = dict(payload)
        store.object_get("ScalarModel", payload_data=[payload])
        self.assertEqual(payload, before)
        self.assertNotIn(VERSION_SERIAL_KEY, payload)

        with self.assertRaises(KeyError):
            store.object_get(
                "ScalarModel", **_scalar_model_query(**{VERSION_SERIAL_KEY: 1})
            )

    def test_d3_only_the_compute_targets_are_keyed(self):
        """Exactly ScalarModel, AdiabaticHistory and BBNData are keyed; no parameter, value or tag table."""
        store = self.open(LABEL_A)
        keyed = {
            name
            for name, record in store._schema.items()
            if record.get("key_on_version", False)
        }
        self.assertEqual(keyed, KEYED_TABLES)

    def test_d4_key_on_version_needs_a_version_column(self):
        """_build_schema refuses a factory that keys on the version without a version column."""

        class _BadFactory:
            def register(self):
                return {
                    "version": False,
                    "key_on_version": True,
                    "columns": [sqla.Column("x", sqla.Integer)],
                }

        # a bare instance: only the attributes _build_schema touches
        store = DS.__new__(DS)
        store._factories = {"Bad": _BadFactory()}
        store._schema = {}
        store._tables = {}
        store._inserters = {}
        store._metadata = sqla.MetaData()

        with self.assertRaises(RuntimeError):
            store._build_schema()

    def test_e_parameter_tables_are_not_keyed(self):
        """(e) An ExponentialCoupling for the same beta under A and then B has one row and one serial.

        A regression guard: this passes on the tree before prompt 01 as well, and must
        keep passing. Parameter tables carry a version column but are not keyed on it
        (README §0.4).
        """
        store_a = self.open(LABEL_A)
        beta = store_a.object_get("beta_value", value=0.5, serial=3)
        c_a = store_a.object_get(
            "ExponentialCoupling", beta=beta, units=_UNITS, serial=21
        )

        store_b = self.open(LABEL_B)
        c_b = store_b.object_get(
            "ExponentialCoupling", beta=beta, units=_UNITS, serial=22
        )

        self.assertEqual(c_a.store_id, 21)
        self.assertEqual(c_b.store_id, c_a.store_id)

        table = store_b._tables["ExponentialCoupling"]
        with store_b._engine.begin() as conn:
            n = conn.execute(sqla.select(sqla.func.count()).select_from(table)).scalar()
        self.assertEqual(n, 1)


class TestOneVersionLabel(unittest.TestCase):
    SCRIPTS = ("main.py", "plot_by_beta.py", "plot_ScalarModel.py")

    def test_f_scripts_import_the_label_and_define_none(self):
        """(f) main.py, plot_by_beta.py and plot_ScalarModel.py import VERSION_LABEL from config.version and assign none.

        The scripts are parsed with ast, not imported: main.py calls ray.init at import.
        """
        root = Path(__file__).resolve().parents[2]
        for script in self.SCRIPTS:
            with self.subTest(script=script):
                tree = ast.parse((root / script).read_text(), filename=script)

                assigns = [
                    node.lineno
                    for node in ast.walk(tree)
                    if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign))
                    and any(
                        isinstance(t, ast.Name) and t.id == "VERSION_LABEL"
                        for t in (
                            node.targets
                            if isinstance(node, ast.Assign)
                            else [node.target]
                        )
                    )
                ]
                self.assertEqual(
                    assigns, [], f"{script} assigns VERSION_LABEL at lines {assigns}"
                )

                imports = [
                    node
                    for node in ast.walk(tree)
                    if isinstance(node, ast.ImportFrom)
                    and node.module == "config.version"
                    and any(a.name == "VERSION_LABEL" for a in node.names)
                ]
                self.assertEqual(
                    len(imports),
                    1,
                    f"{script} does not import VERSION_LABEL from config.version",
                )


if __name__ == "__main__":
    unittest.main()
