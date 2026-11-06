"""
Builds the store of one-loop-drain prompt 04, §2.2: a temporary SQLite store, written through the stand-in cluster
(never Ray) by the harness of ObjectFactories/tests/test_drain_store.py (`_DrainStoreCase`, 03's payload builders),
then copied to var/one-loop-drain/04/store/ with a manifest of its histories.

Run from the repository root:
    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/export-04/build_store.py

The histories (test_injection's setup: n = 1, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to T_CMB, untuned at the
model's seed realization; each payload is integrate_history's, the tuning's keys the stand-ins of the tests):
  F-alpha0     fixed law, M = 0.5 M_P, beta = 2, alpha = 0                      (test_dissipation_store NATURAL)
  F-alpha0.1   fixed law, M = 0.5 M_P, beta = 2, alpha = 0.1: reflects and drains (TEST_1)
  F-fail       a fixed-law failure row, M = 4e-31 M_P, beta = 1.185, alpha = 0.1
  O-2197       one-loop law, M = 4e-31 M_P, beta = 1.185, A = 1: five one-loop candidates, parked (P-2197)
  O-classical  one-loop law, M = 4e-15 M_P, beta = 2, A = 1: 28 classical candidates, parked (L-M4e-15)
  O-gap        one-loop law, M = 8e-18 M_P, beta = 2, A = 2: three one-loop candidates and one in the gap (L-M8e-18)
  O-fail       a one-loop failure row, M = 4e-31 M_P, beta = 1.185, A = 0.5
"""

import json
import shutil
import sys
import time
import unittest
from pathlib import Path

from ComputeTargets.tests import dissipation_histories as dh
from ObjectFactories.tests.test_dissipation_store import NATURAL, TEST_1
from ObjectFactories.tests.test_drain_store import (
    CLASSICAL,
    GAP,
    ONE_LOOP,
    P2197,
    _DrainStoreCase,
    one_loop_payload,
)

DEST = Path("var/one-loop-drain/04/store")
FIXED = "fixed"


class Build(_DrainStoreCase):
    def test_build(self):
        manifest = []

        def record(name, kind, model, key, serial, failure=False):
            beta, M, value = key
            manifest.append(
                dict(
                    name=name,
                    kind=kind,
                    drain_model=model,
                    beta=beta,
                    M_Mp=M,
                    value=value,
                    serial=serial,
                    failure=failure,
                )
            )
            print(
                f"@@ {name}: serial {serial} ({time.strftime('%H:%M:%S')})", flush=True
            )

        for name, key in (("F-alpha0", NATURAL), ("F-alpha0.1", TEST_1)):
            _, serial = self.write(dh.payload(*key), *key)
            record(name, "fixed", FIXED, key, serial)
        fail = {"failure": True, "failure_reason": "stand-in failure for the export"}
        key = (P2197[0], P2197[1], 0.1)
        _, serial = self.write(fail, *key)
        record("F-fail", "fixed", FIXED, key, serial, failure=True)
        for name, key in (
            ("O-2197", P2197),
            ("O-classical", CLASSICAL),
            ("O-gap", GAP),
        ):
            _, serial = self.write_one_loop(key)
            record(name, "one_loop", ONE_LOOP, key, serial)
        key = (P2197[0], P2197[1], 0.5)
        with self.under(ONE_LOOP):
            _, serial = self.write(fail, *key)
        record("O-fail", "one_loop", ONE_LOOP, key, serial, failure=True)

        # close the actors (a store file is copied only when no actor holds it), then copy the directory
        for store in self._stores:
            store.__exit__(None, None, None)
        self._stores = []
        if DEST.exists():
            shutil.rmtree(DEST)
        shutil.copytree(self._dir, DEST)
        (DEST.parent / "manifest.json").write_text(json.dumps(manifest, indent=1))
        print(f"@@ store copied to {DEST}: {sorted(p.name for p in DEST.iterdir())}")


if __name__ == "__main__":
    unittest.main(argv=[sys.argv[0]])
