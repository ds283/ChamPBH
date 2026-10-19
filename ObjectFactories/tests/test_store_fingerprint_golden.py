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

"""This repository's golden store fingerprint, and the module that takes it (rrk-adoption prompt
02; campaign README §0.3 and §6.2, P8).

What goes into a fingerprint is this repository's: the factories' inventory specs, the registry in
`config.datastore`, and the format number in `config.fingerprint`. How it is encoded and digested
is DatastoreKit's (`datastorekit.store_fingerprint`), and when and where it is taken is
RunRegistryKit's, through the fingerprinter `config.fingerprint` holds and `runregistrykit.toml`
names. The model is SecondaryGWKit's `test_store_fingerprint_golden.py`.

- `TestGolden`: the fingerprint of the store `_GoldenStoreCase.build_store` builds is the committed
  golden file `GOLDEN`.
- `TestConfigFingerprint`: `config/fingerprint.py` and the config's naming of it.
- `TestCommandLine`: `python -m runregistrykit store fingerprint`, once, by subprocess, on that
  store.

**The golden store.** `build_store` is this module's own, written in the form of
`tools/tests/test_inventory_report._InventoryStoreCase` (the harness of
`test_version_keyed_lookups._TempStoreCase`: a real one-shard pool writes the version row and is
closed, then the package's undecorated actor inserts rows with no explicit serial, stamped with
fixed times) and importing nothing from `tools.tests`, so that a golden store's rows cannot move
when another module's helpers do. It holds one row of each of the 21 classes with a spec, and five
`ScalarModel` rows, one for each of the five potential types, so that every polymorphic key of the
spec is resolved; one is tagged and the rest are not, so that two tag sets are read. Every literal
is fixed.

**The golden file** is under `data/`, written once and never overwritten (P8). It was written from
`config.fingerprint.fingerprinter.take(primary, None)` as `json.dumps(fp, indent=2, sort_keys=True)
+ "\\n"`, so its `taken` is `null`; the tests take with `taken={}` and compare with `taken`
removed. **A later format adds a file beside it**, named for the prompt that changed the format
(`store_fingerprint_<campaign>-<prompt>.json`), in the same commit that bumps
`config.fingerprint.FINGERPRINT_FORMAT` and adds its pin, and `GOLDEN` is pointed at the new file;
the old file stays, so that the history of formats is the history of the files.

No Ray is initialised (the harness asserts it). Nothing under `var/` is opened or created.
`TestConfigFingerprint` and `TestCommandLine` read `runregistrykit.toml` at the repository root.
"""

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import ray
import sqlalchemy as sqla
from datastorekit.store_fingerprint import ENCODING_FORMAT
from datastorekit.store_inventory import read_inventory
from runregistrykit import find_client_root
from runregistrykit.fingerprint import (
    DatastoreKitFingerprinter,
    Fingerprinter,
    load_fingerprinter,
)

import CosmologyConcepts.ConformalCouplings.model_ids as coupling_ids
import CosmologyConcepts.Potentials.model_ids as potential_ids
import CosmologyModels.model_ids as cosmology_ids
import config.datastore
import config.fingerprint
from StoreFixtures import standin_pool as sp
from config.fingerprint import FINGERPRINT_FORMAT
from ObjectFactories.tests.test_version_keyed_lookups import _TempStoreCase

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA = Path(__file__).resolve().parent / "data"
GOLDEN = DATA / "store_fingerprint_rrk-adoption-02.json"

LABEL = "rrk-adoption-02-golden"
T0 = datetime(2026, 10, 19, 12, 0, 0)

# the five stored potential classes, by the id ScalarModel's spec keys them by
POTENTIALS = {
    potential_ids.INVERSE_POWER_POTENTIAL: "InversePowerPotential",
    potential_ids.EXPONENTIAL_POTENTIAL: "ExponentialPotential",
    potential_ids.REFLECTING_POTENTIAL: "ReflectingPotential",
    potential_ids.STAROBINSKY_POTENTIAL: "StarobinskyPotential",
    potential_ids.RECLINER_POTENTIAL: "ReclinerPotential",
}


def without_taken(fingerprint) -> dict:
    out = dict(fingerprint)
    out.pop("taken", None)
    return out


def _filled(table: sqla.Table, row: dict) -> dict:
    """``row`` with a zero for every NOT NULL column it leaves out that has no default."""
    out = dict(row)
    for column in table.columns:
        if (
            column.name in out
            or column.name in ("serial", "version", "timestamp")
            or column.nullable
            or column.primary_key
            or column.default is not None
        ):
            continue
        if isinstance(column.type, sqla.Float):
            out[column.name] = 0.0
        elif isinstance(column.type, sqla.Boolean):
            out[column.name] = False
        else:
            out[column.name] = 0
    return out


class _GoldenStoreCase(_TempStoreCase):
    """The harness's one-shard store, filled with the golden store's rows, its actors closed
    before it is read."""

    # a failure names the class and the field that differ, rather than truncating the diff
    maxDiff = None

    def insert(self, store, cls_name, row, at=None):
        table = store._tables[cls_name]
        with store._engine.begin() as conn:
            return store._schema[cls_name]["insert"](
                conn, _filled(table, row), insert_timestamp=at or T0
            )

    def close_actors(self):
        for store in self._stores:
            with contextlib.redirect_stdout(io.StringIO()):
                store.__exit__(None, None, None)
        self._stores = []

    def build_store(self):
        """
        The golden store: the version row, one row of each of the other 20 classes, and five
        ScalarModel rows (one per potential type, each on its own potential row), the first of
        them tagged. The first ScalarModel, the AdiabaticHistory and the BBNData row have value
        rows. Returns the primary file.
        """
        store = self.open(LABEL)

        def ins(cls_name, row, at=None):
            return self.insert(store, cls_name, row, at)

        tag = ins("store_tag", {"label": "golden"})
        z = [ins("redshift", {"z": value}) for value in (0.0, 0.5, 1.0, 1.5, 2.0)]
        atol = ins("tolerance", {"log10_tol": -10.0})
        rtol = ins("tolerance", {"log10_tol": -8.0})
        beta = ins("beta_value", {"value": 25.0})
        M = ins("M_value", {"value_eV": 2.436e24})
        Lambda = ins("Lambda_value", {"value_eV": 1.0e-3})
        T_init = ins("temperature", {"value_GeV": 2.0e4})
        T_stop = ins("temperature", {"value_GeV": 2.3493e-13})
        phi = ins("phi_value", {"value_PlanckMass": 5.0})
        pi = ins("pi_value", {"value_PlanckMass": 0.0})
        cosmology = ins(
            "QCD_Cosmology",
            dict(
                name="Planck2018",
                omega_m=0.3111,
                omega_cc=0.6889,
                h=0.6766,
                f_baryon=0.1575,
                T_CMB_Kelvin=2.7255,
                Neff=3.046,
            ),
        )
        solver = ins("IntegrationSolver", {"label": "solve_ivp+RK45", "stepping": 0})
        coupling = ins("ExponentialCoupling", {"beta_serial": beta})

        # one row of each potential class, n_value where the spec reads it
        potential_rows = {}
        for type_id, cls_name in POTENTIALS.items():
            row = dict(M_serial=M, Lambda_serial=Lambda)
            if cls_name in ("InversePowerPotential", "ExponentialPotential"):
                row["n_value"] = 2 if cls_name == "InversePowerPotential" else 1
            potential_rows[type_id] = ins(cls_name, row)

        model = dict(
            cosmology_type=cosmology_ids.XAV_IMPROVED_EOS_IDENTIFIER,
            cosmology_serial=cosmology,
            coupling_type=coupling_ids.EXPONENTIAL_COUPLING,
            coupling_serial=coupling,
            atol_serial=atol,
            rtol_serial=rtol,
            phi_Einstein_init_serial=phi,
            pi_Einstein_init_serial=pi,
            T_Jordan_init_serial=T_init,
            T_Jordan_stop_serial=T_stop,
            validated=True,
            failure=False,
            solver_serial=solver,
            z_samples=3,
        )
        models = {}
        for k, (type_id, cls_name) in enumerate(POTENTIALS.items()):
            models[type_id] = ins(
                "ScalarModel",
                dict(
                    model,
                    label=f"golden-ScalarModel-{cls_name}",
                    potential_type=type_id,
                    potential_serial=potential_rows[type_id],
                ),
                at=T0 + timedelta(minutes=1 + k),
            )
        first = models[potential_ids.INVERSE_POWER_POTENTIAL]
        ins("ScalarModel_tags", {"model_serial": first, "tag_serial": tag})
        for serial in z[:3]:
            ins("ScalarModelValue", {"model_serial": first, "z_serial": serial})

        history = ins(
            "AdiabaticHistory",
            dict(
                model_serial=first, label="golden-history", z_samples=2, validated=True
            ),
            at=T0 + timedelta(minutes=10),
        )
        for serial in z[:2]:
            ins(
                "AdiabaticHistoryValue", {"history_serial": history, "z_serial": serial}
            )

        bbn = ins(
            "BBNData",
            dict(
                model_serial=first,
                label="golden-BBNData",
                failure=False,
                PRyM_version="bf24c3d",
                z_samples=4,
                validated=False,
            ),
            at=T0 + timedelta(minutes=11),
        )
        for serial in z[:4]:
            ins("BBNDataValue", {"bbn_serial": bbn, "z_serial": serial})

        self.close_actors()
        return self.db

    def fingerprint(self, primary, taken):
        return config.fingerprint.fingerprinter.take(primary, taken)[0]


class TestGolden(_GoldenStoreCase):
    """The fingerprint of the golden store is the committed golden file."""

    def test_the_store_reads_with_no_problem_and_every_class(self):
        primary = self.build_store()
        inventory = read_inventory(primary, config.datastore.factories)
        self.assertEqual(inventory.problems, ())
        self.assertEqual(len(inventory.classes), 21)
        for name, entry in inventory.classes.items():
            self.assertGreaterEqual(entry.count, 1, name)
        # every polymorphic key of ScalarModel's spec is resolved: one model per potential type
        self.assertEqual(inventory["ScalarModel"].count, len(POTENTIALS))
        for cls_name in POTENTIALS.values():
            self.assertEqual(inventory[cls_name].count, 1, cls_name)

    def test_the_golden_fingerprint(self):
        golden = json.loads(GOLDEN.read_text())
        self.assertEqual(golden["fingerprint_format"], FINGERPRINT_FORMAT)
        self.assertEqual(golden["encoding_format"], ENCODING_FORMAT)
        self.assertIsNone(golden["taken"])
        self.assertEqual(golden["problems"], {})

        fp = self.fingerprint(self.build_store(), {})
        self.assertEqual(fp["problems"], {})
        self.assertEqual(fp["taken"], {})
        self.assertEqual(without_taken(fp), without_taken(golden))

        # and it is deterministic: a second store, built from the same literals on a fresh broker
        # and file, gives the same
        self.db = self._dir / "second.db"
        self.broker = sp.Handle(
            sp.BrokerClass(name="SerialPoolBroker"),
            "SerialPoolBroker",
            self.cluster,
            None,
        )
        again = self.fingerprint(self.build_store(), {})
        self.assertEqual(without_taken(again), without_taken(golden))

    def test_two_tag_sets_are_read(self):
        fp = self.fingerprint(self.build_store(), {})
        tag_sets = [t["tags"] for t in fp["classes"]["ScalarModel"]["tag_sets"]]
        self.assertEqual(sorted(tag_sets), [[], ["golden"]])


class TestConfigFingerprint(unittest.TestCase):
    """`config/fingerprint.py`, and `runregistrykit.toml`'s naming of its fingerprinter."""

    def test_the_config_names_the_fingerprinter(self):
        client = find_client_root(client_root=REPO_ROOT)
        # the loader refuses (with a ConfigError) a config that names a class, or nothing
        loaded = load_fingerprinter(client)
        self.assertEqual(
            client.stores, {"fingerprinter": "config.fingerprint:fingerprinter"}
        )
        self.assertIs(loaded, config.fingerprint.fingerprinter)
        self.assertIsInstance(loaded, DatastoreKitFingerprinter)
        self.assertIsInstance(loaded, Fingerprinter)
        self.assertEqual(loaded.fingerprint_format, FINGERPRINT_FORMAT)
        self.assertIs(loaded.factories, config.datastore.factories)


class TestCommandLine(_GoldenStoreCase):
    """`python -m runregistrykit store fingerprint`, once, on the golden store, which has no
    sidecar: it prints the golden digest and exits 0, with nothing recorded."""

    def test_store_fingerprint(self):
        primary = self.build_store()
        golden = json.loads(GOLDEN.read_text())
        runs_root = self._dir / "runs"
        runs_root.mkdir()
        before = sorted(p.name for p in self._dir.iterdir())
        var = REPO_ROOT / "var"
        var_before = var.exists()

        argv = [
            sys.executable,
            "-m",
            "runregistrykit",
            "--client-root",
            str(REPO_ROOT),
            "store",
            "fingerprint",
            str(primary),
            "--runs-root",
            str(runs_root),
        ]
        env = dict(os.environ)
        env.pop("RAY_ADDRESS", None)
        done = subprocess.run(
            argv, cwd=REPO_ROOT, env=env, capture_output=True, text=True, timeout=300
        )
        self.assertEqual(done.returncode, 0, done.stdout + done.stderr)
        lines = done.stdout.splitlines()
        # the command pads its labels into columns
        squeezed = [" ".join(line.split()) for line in lines]
        self.assertIn(f"digest: {golden['digest']}", squeezed)
        self.assertIn(f"format: {golden['fingerprint_format']}", squeezed)
        self.assertEqual(golden["fingerprint_format"], 1)
        self.assertEqual(
            lines[-1].strip(), "recorded: none recorded (the sidecar is absent)"
        )

        # nothing was written beside the store, under the runs root or at the root
        self.assertEqual(sorted(p.name for p in self._dir.iterdir()), before)
        self.assertEqual(list(runs_root.iterdir()), [])
        self.assertEqual(var.exists(), var_before)


if __name__ == "__main__":
    unittest.main()
