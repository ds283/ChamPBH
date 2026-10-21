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

No Ray cluster, no PRyMordial solve, no persistent datastore. About 1 s.

The harness, `_TempStoreCase`, builds datastorekit's actor without Ray, inside ChamPBH's copy
of the package's stand-in cluster (`StoreFixtures/standin_pool.py`), on a SQLite file in a
`tempfile` directory (dk-and-rpk-migration prompt 08). `open(label)` first opens a real
`ShardedPool` on one stand-in shard under `label`, which finds or writes the label's `version`
row, and closes it; then it builds the undecorated actor (`standin_pool.DatastoreClass`) on that
shard's file with ChamPBH's registry and replicated tables and a stand-in serial broker shared by
every actor of the test, notifies the broker of the file's largest serials, and gives the actor the
label's serial with `set_version`, as the pool does. Reopening the same file under a second label
writes that label's row beside the first. The pattern is the package's
`test_version_keyed_lookups.TestTheActor` (DatastoreKit, `v0.3.0`).

Rows are inserted through the actor's own inserter, `store._schema[<table>]["insert"]`, so the
`version` column is filled by the package's `Datastore._insert`. Each row is given an explicit
serial, so no serial is leased. The objects a lookup keys on (cosmology, potential, coupling,
parameter values, the model proxy) are stand-ins that carry only the attributes the factories
read. The pattern was that of `prompts/run-integrity/planning-probes/datastore_version_probe.py`.

The package's own suite tests the layer: that a caller's payload is copied and the reserved key is
the datastore's, and that `key_on_version` needs a `version` column. These tests are of ChamPBH's
factories, registry and scripts.
"""

import ast
import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace as NS

import ray
import sqlalchemy as sqla

from StoreFixtures import standin_pool as sp
from config.datastore import factories, serial_batch_sizes
from config.sharding import replicated_tables

LABEL_A = "2026.3.0"
LABEL_B = "2026.4.0"

KEYED_TABLES = {"ScalarHistory", "AdiabaticHistory", "BBNData"}

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


def _scalar_history_query(**extra):
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


def _insert_failed_scalar_history(store):
    for serial, log10_tol in ((14, -10.0), (15, -8.0)):
        _insert(store, "tolerance", dict(serial=serial, log10_tol=log10_tol))
    return _insert(
        store,
        "ScalarHistory",
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
            scalar_history_serial=_MODEL_SERIAL,
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
            scalar_history_serial=_MODEL_SERIAL,
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
            scalar_history_serial=_MODEL_SERIAL,
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
    """
    The package's actor on a one-shard store in a temporary directory, inside a stand-in cluster
    (the module docstring). Each test's actors share one stand-in serial broker; every actor is
    closed in tearDown, which also asserts that Ray was never initialised.
    """

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix="champbh-datastore-test-")
        self.addCleanup(tmp.cleanup)
        self._dir = Path(tmp.name)
        self.db = self._dir / "store.db"

        self.cluster = sp.StandinCluster()
        active = self.cluster.active()
        active.__enter__()
        self.addCleanup(active.__exit__, None, None, None)

        # one broker for every actor of the test, as a pool gives its actors one
        self.broker = sp.Handle(
            sp.BrokerClass(name="SerialPoolBroker"),
            "SerialPoolBroker",
            self.cluster,
            None,
        )
        self._stores = []

    def tearDown(self):
        for store in self._stores:
            with contextlib.redirect_stdout(io.StringIO()):
                store.__exit__(None, None, None)
        self._stores = []
        self.assertFalse(ray.is_initialized())

    def open(self, label):
        """
        The package's actor on the store's one shard, under ``label``, with the label's version
        serial set, as ``ShardedPool`` builds and prepares each of its actors.
        """
        # a real pool on one stand-in shard finds or writes the label's version row
        pool = self.cluster.open_pool(
            self.db,
            shards=1,
            version_label=label,
            serial_batch_sizes=serial_batch_sizes,
        )
        serial = pool._version.store_id
        shard_file = pool._shard_db_files[0]
        self.cluster.close_pool(pool)

        with contextlib.redirect_stdout(io.StringIO()):
            store = sp.DatastoreClass(
                version_label=label,
                db_name=shard_file,
                replicated_tables=list(replicated_tables),
                factories=factories,
                serial_batch_sizes=serial_batch_sizes,
                my_name=f"probe{len(self._stores):04d}-store",
                serial_broker=self.broker,
            )
        self._stores.append(store)

        # the broker learns the file's largest serials before the actor inserts anything, and the
        # actor is given the label's serial, as ShardedPool does at open
        self.broker.notify_largest_store_ids.remote(
            store.read_largest_store_ids()
        ).get()
        store.set_version(serial)
        return store


class TestVersionKeyedLookups(_TempStoreCase):
    def test_a_scalar_history_is_returned_only_under_its_own_label(self):
        """(a) A failed ScalarHistory stored under label A is returned under A, and not under B."""
        store_a = self.open(LABEL_A)
        serial = _insert_failed_scalar_history(store_a)

        m = store_a.object_get("ScalarHistory", **_scalar_history_query())
        self.assertTrue(m.available)
        self.assertEqual(m.store_id, serial)
        self.assertTrue(m.failure)

        store_b = self.open(LABEL_B)
        self.assertNotEqual(store_a._version_serial, store_b._version_serial)

        m_b = store_b.object_get("ScalarHistory", **_scalar_history_query())
        self.assertFalse(
            m_b.available,
            f"ScalarHistory stored under {LABEL_A} was returned under {LABEL_B} "
            f"(store_id={_sid(m_b)})",
        )

        # and the row is still there under A, unchanged
        store_a2 = self.open(LABEL_A)
        m_a2 = store_a2.object_get("ScalarHistory", **_scalar_history_query())
        self.assertTrue(m_a2.available)
        self.assertEqual(m_a2.store_id, serial)

    def test_b_adiabatic_history_is_returned_only_under_its_own_label(self):
        """(b) An AdiabaticHistory keyed to a ScalarHistory serial, stored under A, is returned under A, and not under B."""
        store_a = self.open(LABEL_A)
        _insert_failed_scalar_history(store_a)
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
        _insert_failed_scalar_history(store_a)
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
        _insert_failed_scalar_history(store_a)
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
        serial = _insert_failed_scalar_history(store_a)
        payloads = [_scalar_history_query(), _scalar_history_query()]

        got_a = store_a.object_get("ScalarHistory", payload_data=payloads)
        self.assertEqual([m.store_id for m in got_a], [serial, serial])

        got_b = self.open(LABEL_B).object_get("ScalarHistory", payload_data=payloads)
        self.assertEqual([m.available for m in got_b], [False, False])

    def test_d_keyed_build_without_the_serial_raises(self):
        """(d) A keyed factory's build(), called directly with a payload lacking the reserved key, raises."""
        store = self.open(LABEL_A)
        _insert_failed_scalar_history(store)
        _insert_adiabatic_history(store)
        _insert_failed_bbn(store, 1, "stored under A")

        payloads = {
            "ScalarHistory": _scalar_history_query(),
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

    def test_d3_only_the_compute_targets_are_keyed(self):
        """Exactly ScalarHistory, AdiabaticHistory and BBNData are keyed; no parameter, value or tag table."""
        store = self.open(LABEL_A)
        keyed = {
            name
            for name, record in store._schema.items()
            if record.get("key_on_version", False)
        }
        self.assertEqual(keyed, KEYED_TABLES)

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
    SCRIPTS = ("main.py", "plot_by_beta.py", "plot_ScalarHistory.py")

    def test_f_scripts_import_the_label_and_define_none(self):
        """(f) main.py, plot_by_beta.py and plot_ScalarHistory.py import VERSION_LABEL from config.version and assign none.

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
