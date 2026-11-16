"""
Builds the store of power-law-wall prompt 06, §2.1: a temporary SQLite store holding both potentials, written through
the stand-in cluster (never Ray) by prompt 05's harness (`_PowerStoreCase` of
ObjectFactories/tests/test_inverse_power_store.py, on `_DrainStoreCase` and `_DissipationStoreCase`), then copied to
DEST/store/ with a manifest of its histories (DEST/manifest.json).

Run from the root of the tree whose code builds it (the base worktree of the dispatch commit, prompt 06 §2.1), with
that root on PYTHONPATH:

    cd TREE && PYTHONPATH=. /path/to/venv/bin/python /path/to/build_store.py --dest /abs/path/var/power-law-wall/06

The histories: laboratory M (4e-31 M_P), beta = 2, at test_injection's setup (dissipation_histories: phi* = 5 M_P,
pi* = 0, T* = 2e4 GeV, to T_CMB, on the golden sampling axis), each tuned by tune_history in this process (the payload
05's round trips store), under the fixed law at alpha = 0.1 and the one-loop law at A = 1 (the model's own drain model):
  E1-fixed0.1    ExponentialModel n = 1, fixed, alpha = 0.1
  E1-oneloop1    ExponentialModel n = 1, one-loop-measured-v1, A = 1
  P1-fixed0.1    InversePowerModel n = 1, fixed, alpha = 0.1
  P1-oneloop1    InversePowerModel n = 1, one-loop-measured-pl-v1, A = 1
  P2-fixed0.1    InversePowerModel n = 2, fixed, alpha = 0.1
  P2-oneloop1    InversePowerModel n = 2, one-loop-measured-pl-v1, A = 1
and one failure row per potential, at beta = 1.185 (a stand-in failure, as export-04's):
  E1-fail        ExponentialModel n = 1, fixed, alpha = 0.1
  P1-fail        InversePowerModel n = 1, one-loop-measured-pl-v1, A = 1

Each successful history is given a stand-in BBNData row (constructed values, z_samples = 0, no samples), so that
plot_by_beta's build_beta_plot passes its "no valid BBN data" return and writes each (M, n) group's data.csv. The values
are stand-ins of the export and not a PRyMordial solve.
"""

import argparse
import contextlib
import io
import json
import shutil
import sys
import time
import unittest
from pathlib import Path

from ComputeTargets.tests import dissipation_histories as dh
from CosmologyConcepts import temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from ObjectFactories.tests.test_inverse_power_store import (
    _PowerStoreCase,
    exponential_model,
    power_model,
)

SM = dh.SM
FIXED = "fixed"
ONE_LOOP = "one-loop-measured-v1"
PL = "one-loop-measured-pl-v1"
LAB_M = 4.0e-31
BETA = 2.0
BETA_FAIL = 1.185
FAIL_REASON = "stand-in failure for the export"

# (name, option value, table, n, drain model, value)
HISTORIES = (
    ("E1-fixed0.1", "exponential", "ExponentialModel", 1, FIXED, 0.1),
    ("E1-oneloop1", "exponential", "ExponentialModel", 1, ONE_LOOP, 1.0),
    ("P1-fixed0.1", "inverse-power", "InversePowerModel", 1, FIXED, 0.1),
    ("P1-oneloop1", "inverse-power", "InversePowerModel", 1, PL, 1.0),
    ("P2-fixed0.1", "inverse-power", "InversePowerModel", 2, FIXED, 0.1),
    ("P2-oneloop1", "inverse-power", "InversePowerModel", 2, PL, 1.0),
)
FAILURES = (
    ("E1-fail", "exponential", "ExponentialModel", 1, FIXED, 0.1),
    ("P1-fail", "inverse-power", "InversePowerModel", 1, PL, 1.0),
)

DEST = None


def tuned_payload(table: str, n: int, drain_model: str, value: float) -> dict:
    """tune_history's payload at laboratory M, beta = 2, as 05's tuned_power_payload makes it, for either model."""
    cos = dh.cosmology()
    units = cos.units
    model = (
        power_model(n, LAB_M, BETA)
        if table == "InversePowerModel"
        else exponential_model(n, LAB_M, BETA)
    )
    law = SM.DRAIN_LAW_FIXED if drain_model == FIXED else SM.DRAIN_LAW_ONE_LOOP
    with contextlib.redirect_stdout(io.StringIO()):
        data = SM.tune_history(
            cos,
            model,
            temperature(0, dh.T_INIT_GEV * units.GeV),
            phi_value(0, dh.PHI_INIT_OVER_MP * units.PlanckMass),
            pi_value(0, dh.PI_INIT),
            dh.z_grid(),
            task_label=f"export-06-{table}-n{n}-{drain_model}-{value:g}",
            alpha=value,
            drain_law=law,
        )
    if data.get("failure", False):
        raise RuntimeError(
            f"tuned_payload {table, n, drain_model, value}: {data['failure_reason']}"
        )
    return data


class Build(_PowerStoreCase):
    def bbn_row(self, history_serial: int, k: int):
        """A stand-in successful BBNData row of the history (constructed values; no samples)."""
        with self.store._engine.begin() as conn:
            self.store._schema["BBNData"]["insert"](
                conn,
                dict(
                    serial=k,
                    scalar_history_serial=history_serial,
                    failure=False,
                    Yp_BBN=0.2468 + 1.0e-4 * k,
                    DOverH=2.46 + 1.0e-3 * k,
                    He3OverH=1.04 + 1.0e-3 * k,
                    Li7OverH=5.42 + 1.0e-2 * k,
                    label=f"export-06-bbn-{k}",
                    small_network=True,
                    PRyM_version="stand-in",
                    z_samples=0,
                    NP_compute_time=0.5 * k,
                    BBN_compute_time=1.5 * k,
                    validated=True,
                ),
            )
            conn.commit()

    def test_build(self):
        manifest = []
        t_all = time.perf_counter()
        bbn = 0
        for name, option, table, n, drain_model, value in HISTORIES + FAILURES:
            failure = name.endswith("-fail")
            beta = BETA_FAIL if failure else BETA
            t0 = time.perf_counter()
            payload = (
                SM._failure_payload(FAIL_REASON)
                if failure
                else tuned_payload(table, n, drain_model, value)
            )
            cost = time.perf_counter() - t0
            with self.as_model(table, n), self.under(drain_model):
                _, serial = self.write(payload, beta, LAB_M, value)
            entry = dict(
                name=name,
                model=option,
                table=table,
                n=n,
                drain_model=drain_model,
                value=value,
                beta=beta,
                M_Mp=LAB_M,
                serial=serial,
                failure=failure,
                failure_reason=FAIL_REASON if failure else None,
            )
            if not failure:
                bbn += 1
                self.bbn_row(serial, bbn)
                entry["bbn_serial"] = bbn
                entry["tuning_iterations"] = payload["tuning_iterations"]
            manifest.append(entry)
            print(
                f"@@ {name}: serial {serial}; {'failure row' if failure else f'tuned in {cost:.1f} s'} "
                f"({time.strftime('%H:%M:%S')})",
                flush=True,
            )

        # close the actors (a store file is copied only when no actor holds it), then copy the directory
        self.close_actors()
        store = DEST / "store"
        if store.exists():
            shutil.rmtree(store)
        shutil.copytree(self._dir, store)
        (DEST / "manifest.json").write_text(json.dumps(manifest, indent=1) + "\n")
        print(
            f"@@ store copied to {store}: {sorted(p.name for p in store.iterdir())}; "
            f"{len(manifest)} histories; total {time.perf_counter() - t_all:.1f} s"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dest", required=True)
    args, rest = parser.parse_known_args()
    DEST = Path(args.dest).resolve()
    DEST.mkdir(parents=True, exist_ok=True)
    unittest.main(argv=[sys.argv[0], *rest])
