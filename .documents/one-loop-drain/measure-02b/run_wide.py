"""
one-loop-drain prompt 02b §1.3, the driver. It is 02's driver, `measure/run_sample.py`, imported, with its paths
moved to var/one-loop-drain/02b/ and two additions of orchestrator note 02b's ruling 6 (ruling 3):

  overrides of 02's run_sample module globals, before use (each is read there at call time):
    OUT        -> var/one-loop-drain/02b
    TASKS_JSON -> var/one-loop-drain/02b/tasks.json (tasks_wide.py: the nine A, 540 tasks)
    TASK_DIR   -> var/one-loop-drain/02b/tasks
    BATCH_DIR  -> var/one-loop-drain/02b/batches
    SELF       -> this file, so that 02's batch() launches each child through 02b's driver
    read_partial -> read_partial below: 02's, with the partial line's "ells" also dropped from a timed-out or
                    crashed task's line (as 02's drops "regimes" and "injections")

  a copy: child(), below, is 02's child() with one change, that the partial line's "ells" is dropped from the
  reduced record kept for the task's line, beside "regimes" and "injections" (the diff is in the log). It calls
  02's reduce_result and accepted_values through the wrappers below.

  what ruling 6 adds:
    reduce_result: 02's, plus, in the partial line, "ells", each candidate's DrainCandidate.ell in order (None where
      the candidate is not one-loop), and two counts that stay in the task's line too:
        cap_one_loop_by_ell: one-loop candidates where A * DRAIN_C1 / ell >= DRAIN_ALPHA_CAP (the cap acted);
        cap_gap_edge:        gap candidates under "edge" where A * DRAIN_C1 / DRAIN_ELL_EDGE >= DRAIN_ALPHA_CAP;
    accepted_values: 02's, plus "first_impact" (first_impact below).

Everything else is 02's, unchanged: a fresh `./venv/bin/python` per task, the three captures, the streamed partial
files, SIGTERM and then SIGKILL after 30 s on the 3600 s timeout, the peak RSS from os.wait4, the tool-check lines,
and nothing outliving the batch.

    nohup ./venv/bin/python .documents/one-loop-drain/measure-02b/run_wide.py batch NAME IDS_FILE WORKERS OUT.jsonl \\
        [--tag PREFIX] > var/one-loop-drain/02b/batches/NAME/batch.log 2>&1 &

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02b/run_wide.py child TASK_ID PREFIX
"""

import json
import os
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import wide  # noqa: E402  (puts 02's measure/ on sys.path after this directory)

import run_sample as rs02  # noqa: E402  (02's driver, by import)

ROOT = wide.ROOT
OUT = wide.OUT
TASKS_JSON = wide.TASKS_JSON
TASK_DIR = wide.TASK_DIR
BATCH_DIR = wide.BATCH_DIR
SELF = str(Path(__file__).resolve())

rs02.OUT = OUT
rs02.TASKS_JSON = TASKS_JSON
rs02.TASK_DIR = TASK_DIR
rs02.BATCH_DIR = BATCH_DIR
rs02.SELF = SELF

load_tasks = rs02.load_tasks
_read_partial_02 = rs02.read_partial
_reduce_result_02 = rs02.reduce_result
_accepted_values_02 = rs02.accepted_values


def read_partial(path):
    out = _read_partial_02(path)
    for rec in out:
        rec.pop("ells", None)
    return out


rs02.read_partial = read_partial


def _cap_counts(SM, drain):
    """(cap_one_loop_by_ell, cap_gap_edge) of one integration's DrainRecord (ruling 6); None if it has no list."""
    if drain is None or drain.candidates is None:
        return None, None
    A = drain.A
    one_loop = sum(
        1
        for c in drain.candidates
        if c.regime == SM.REGIME_ONE_LOOP
        and A * SM.DRAIN_C1 / c.ell >= SM.DRAIN_ALPHA_CAP
    )
    gap = 0
    if drain.gap_prescription == SM.GAP_PRESCRIPTION_EDGE:
        if A * SM.DRAIN_C1 / SM.DRAIN_ELL_EDGE >= SM.DRAIN_ALPHA_CAP:
            gap = sum(1 for c in drain.candidates if c.regime == SM.REGIME_GAP)
    return one_loop, gap


def reduce_result(SM, k, result, wall, error=None):
    """02's reduce_result, plus each candidate's ell ("ells", partial line only) and the cap counts (ruling 6)."""
    rec = _reduce_result_02(SM, k, result, wall, error=error)
    drain = getattr(error, "drain", None) if error is not None else result.drain
    rec["cap_one_loop_by_ell"], rec["cap_gap_edge"] = _cap_counts(SM, drain)
    rec["ells"] = (
        None
        if drain is None or drain.candidates is None
        else [c.ell for c in drain.candidates]
    )
    return rec


def first_impact(SM, data, K_final):
    """
    The first impact and the cap (prompt 02b §1.3; ruling 6), from the accepted integration's payload:
      - the first candidate's regime, N, ell and ln R (its classical turning point's), and its alpha_eff, the
        fraction of the first impact's kinetic energy drained (0 where nothing drained); its delta_K as a share of
        K(N_final) (None where K(N_final) = 0);
      - the drains (alpha_eff > 0): in all; at N <= parking_N (all of them where the field does not park); after the
        first impact and before parking; and after parking (a released history drains again after its release);
      - the parking leftover, which is the Parking record's delta_K, a ledger entry of its own and not among the
        injections (they are parallel to the candidates): its share of K(N_final), counted apart;
      - the cap: one-loop drains where A c_1 / ell >= alpha_cap (by ell), and one-loop drains with alpha_eff equal to
        DRAIN_ALPHA_CAP exactly (by alpha_eff, the count 02's partials allow); gap drains capped under edge.
    """
    cands = data["drain"].candidates
    inj = data["injections"]
    assert len(cands) == len(inj)
    A = data["drain"].A
    parking = data["parking"]
    out = {"candidates": len(cands)}
    if not cands:
        out["first"] = None
    else:
        c0, i0 = cands[0], inj[0]
        tp = i0.classical_turning_point
        out["first"] = {
            "regime": c0.regime,
            "N": i0.N,
            "ell": c0.ell,
            "log_R": None if tp is None else tp.log_R,
            "alpha_eff": i0.alpha_eff,
            "w_gate": c0.w_gate,
            "delta_K": i0.delta_K,
            "share_of_K": i0.delta_K / K_final if K_final > 0.0 else None,
        }
    drained = [j for j, i in enumerate(inj) if i.alpha_eff > 0.0]
    pN = None if parking is None else parking.N
    before = [j for j in drained if pN is None or inj[j].N <= pN]
    out["parking_N"] = pN
    out["parks"] = parking is not None
    out["drains"] = len(drained)
    out["drains_before_parking"] = len(before)
    out["drains_after_first_before_parking"] = len([j for j in before if j > 0])
    out["drains_after_parking"] = len(drained) - len(before)
    out["parking_leftover_among_injections"] = False
    out["parking_delta_K"] = None if parking is None else parking.delta_K
    out["parking_share_of_K"] = (
        None if parking is None or K_final <= 0.0 else parking.delta_K / K_final
    )
    out["cap_one_loop_by_ell"] = sum(
        1
        for c in cands
        if c.regime == SM.REGIME_ONE_LOOP
        and A * SM.DRAIN_C1 / c.ell >= SM.DRAIN_ALPHA_CAP
    )
    out["cap_one_loop_by_alpha_eff"] = sum(
        1
        for c, i in zip(cands, inj)
        if c.regime == SM.REGIME_ONE_LOOP and i.alpha_eff == SM.DRAIN_ALPHA_CAP
    )
    out["cap_gap_by_alpha_eff"] = sum(
        1
        for c, i in zip(cands, inj)
        if c.regime == SM.REGIME_GAP and i.alpha_eff == SM.DRAIN_ALPHA_CAP
    )
    out["one_loop_drains"] = sum(
        1
        for c, i in zip(cands, inj)
        if c.regime == SM.REGIME_ONE_LOOP and i.alpha_eff > 0.0
    )
    return out


def accepted_values(SM, T, data, N_final, M_units, units):
    """02's accepted_values, plus "first_impact" (ruling 6)."""
    out = _accepted_values_02(SM, T, data, N_final, M_units, units)
    out["first_impact"] = first_impact(SM, data, out["K_final"])
    return out


# ---- the child: a copy of 02's child(), with "ells" dropped from the reduced record (see the log) ------------


def child(task_id: str, prefix: str) -> int:
    import resource
    import traceback

    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT))
    task = load_tasks()[task_id]
    partial_path = TASK_DIR / f"{prefix}{task_id}.partial.jsonl"
    result_path = TASK_DIR / f"{prefix}{task_id}.result.json"
    partial = open(partial_path, "w")

    import importlib

    # the module, not the class ComputeTargets/__init__.py exports under the same name (as the tool imports it)
    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    T = importlib.import_module("tools.history_and_bbn")
    from Units import Planck_units

    reduced = []
    real_isc = SM.integrate_scalar_history

    def wrapped_isc(*a, **kw):
        k = len(reduced)
        t0 = time.perf_counter()
        try:
            result = real_isc(*a, **kw)
        except BaseException as e:
            rec = reduce_result(SM, k, None, time.perf_counter() - t0, error=e)
            partial.write(json.dumps(rec) + "\n")
            partial.flush()
            rec.pop("regimes"), rec.pop("injections"), rec.pop("ells")
            reduced.append(rec)
            raise
        rec = reduce_result(SM, k, result, time.perf_counter() - t0)
        partial.write(json.dumps(rec) + "\n")
        partial.flush()
        rec.pop("regimes"), rec.pop("injections"), rec.pop("ells")
        reduced.append(rec)
        return result

    payload = {}
    real_tune = T.tune_history

    def wrapped_tune(*a, **kw):
        t0 = time.perf_counter()
        data = real_tune(*a, **kw)
        payload["data"] = data
        payload["wall"] = time.perf_counter() - t0
        return data

    class _BBNCapture:
        def __init__(self, real):
            self.real = real
            self.result = None
            self.wall = None

        def _function(self, *a, **kw):
            t0 = time.perf_counter()
            r = self.real._function(*a, **kw)
            self.wall = time.perf_counter() - t0
            self.result = r
            return r

    real_bbn = T.compute_BBN_data
    bbn = _BBNCapture(real_bbn)

    out = {"id": task_id, "argv": task["argv"]}
    SM.integrate_scalar_history = wrapped_isc
    T.tune_history = wrapped_tune
    T.compute_BBN_data = bbn
    try:
        try:
            rc = T.main(task["argv"])
        finally:
            SM.integrate_scalar_history = real_isc
            T.tune_history = real_tune
            T.compute_BBN_data = real_bbn
        sys.stdout.flush()
        out["rc"] = rc
        data = payload.get("data")
        if data is None:
            out["outcome"] = "error"
            out["reason"] = "tune_history returned nothing"
        elif data.get("failure", False):
            out["outcome"] = "failure"
            out["stage"] = "tuning"
            out["reason"] = data.get("failure_reason")
        else:
            out["tuning"] = {k: data[k] for k in SM.TUNING_OUTPUT_KEYS}
            out["tuning_wall"] = payload["wall"]
            units = Planck_units()
            last = [r for r in reduced if r["outcome"] == "returned"][-1]
            out["accepted"] = accepted_values(
                SM, T, data, last["N_final"], task["M"] * units.PlanckMass, units
            )
            out["accepted"]["accepted_steps"] = data["accepted_steps"]
            out["accepted"]["reflections"] = data["reflections"]
            out["accepted"]["RHS_evaluations"] = data["metadata"].RHS_evaluations
            res = bbn.result
            if res is None:
                out["outcome"] = "error"
                out["reason"] = "the BBN stage did not run"
            elif res.get("failure", False):
                out["outcome"] = "failure"
                out["stage"] = "bbn"
                out["reason"] = res.get("failure_reason")
            else:
                out["outcome"] = "success"
                out["bbn"] = {
                    "Yp": res["Yp_BBN"],
                    "DoH": res["DOverH"],
                    "He3oH": res["He3OverH"],
                    "Li7oH": res["Li7OverH"],
                    "network": "small",
                    "PRyM_time": res["BBN_compute_time"],
                    "PRyM_version": res["PRyM_version"],
                    "wall": bbn.wall,
                }
    except BaseException as e:
        out["outcome"] = "error"
        out["reason"] = f"{type(e).__name__}: {e}"
        out["traceback"] = traceback.format_exc()
        print(out["traceback"], file=sys.stderr)
    out["integrations"] = reduced
    out["maxrss"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    result_path.write_text(json.dumps(out) + "\n")
    partial.close()
    return 0


def main():
    role = sys.argv[1]
    if role == "child":
        sys.exit(child(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ""))
    if role == "batch":
        BATCH_DIR.mkdir(parents=True, exist_ok=True)
        TASK_DIR.mkdir(parents=True, exist_ok=True)
        rs02.main()  # 02's batch(), with the globals above
        return
    raise SystemExit(f"unknown role {role}")


if __name__ == "__main__":
    main()
