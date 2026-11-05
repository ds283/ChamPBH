"""
one-loop-drain prompt 02d §1.2, §1.3, the driver (orchestrator note 02d, rulings 1-3, 5, 6). It runs the tool route,
`tools.history_and_bbn.main`, one task per fresh process, and writes one JSON line per task.

A copy, self-contained (prompt 02d §1.2): it imports nothing of 02's or 02b's scripts. Its sources:
  - `.documents/one-loop-drain/measure/run_sample.py` (prompt 02's driver): now_utc, loads, load_tasks, digest,
    reduce_result, accepted_values, child, read_partial, TASK_FIELDS, build_line, batch, main;
  - `.documents/one-loop-drain/measure-02b/run_wide.py` (prompt 02b's driver): first_impact.
Each function names its source in a comment. The changes are those of note 02d's ruling 1, and no others:
  - the paths: var/one-loop-drain/02d/, and SELF is this file;
  - the task fields: no `gap` (the task list's argv is `BETA M --drain-law one_loop --alpha A`);
  - DrainRecord.model where DrainRecord.gap_prescription stood;
  - the R S(L) bound (ruling 7 of note 02) and l2s removed: the classical drain is now realized (prompt 02d §1.3);
  - ruling 6's additions, marked "ruling 6" below: each DrainCandidate kept whole in the partial line
    ("candidates"); the law checked against its own record at every candidate; the cap and the law's edges by
    regime; the accepted integration's values by regime; first_impact's cap by the law's f, since DRAIN_C1 is gone;
  - the timeout's environment variable renamed ONE_LOOP_DRAIN_02D_TIMEOUT_S, default 3600 s.
and one more, recorded in the log as an IMPLEMENTATION CHOICE: reduce_result counts the accepted steps of a returned
integration after its first classical candidate (from the OdeSolution's nodes), for prompt §2 item 7 (the classical
drains' share of the steps).

Two roles, one file:

  batch: the parent, launched detached (note 02d ruling 2):

      nohup ./venv/bin/python .documents/one-loop-drain/measure-02d/run_02d.py batch NAME IDS_FILE WORKERS OUT.jsonl \\
          [--tag PREFIX] > var/one-loop-drain/02d/batches/NAME/batch.log 2>&1 &

    It writes its pid to var/one-loop-drain/02d/batches/NAME/batch.pid, runs the task ids of IDS_FILE (one per line,
    from var/one-loop-drain/02d/tasks.json; a line `tool:<task id>` is a tool check instead) in that order, at most
    WORKERS at once, each in a fresh `./venv/bin/python` from the repository root (PYTHONPATH=.), appends one line per
    task to OUT.jsonl, flushed, and writes batch.end when every child has ended. It logs each start and end, with the
    1-, 5- and 15-minute loads, to stdout (batch.log). A child that runs past TIMEOUT_S (3600 s) is sent SIGTERM,
    then SIGKILL after KILL_GRACE_S (30 s); its line is built from its partial file with outcome `timeout`, and the
    parent waits for it, so that nothing outlives the batch. Every child is reaped with os.wait4, whose ru_maxrss
    (bytes on macOS) is the line's `peak_rss`. --tag prefixes the task's file names under tasks/ (the checks').

    A tool check runs `./venv/bin/python tools/history_and_bbn.py <the task's argv>` itself, its stdout to
    tasks/<tag>tool-<task id>.log, and writes a line {"tool_check": id, "rc", "wall", "peak_rss"}.

  child: one task, in its own process:

      PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02d/run_02d.py child TASK_ID PREFIX

    It calls tools.history_and_bbn.main(task argv), whose stdout is the task's log, with three captures around it,
    each restored after it: ComputeTargets.ScalarHistory.integrate_scalar_history, wrapped before main wraps it, so
    that it sees every integration (each result or exception reduced at once and one JSON line appended to
    tasks/<PREFIX><id>.partial.jsonl and flushed, no reference to the IntegrationResult kept);
    tools.history_and_bbn.tune_history, to keep the payload; and tools.history_and_bbn.compute_BBN_data, replaced by
    an object whose _function calls the real one and keeps its result. It then writes tasks/<PREFIX><id>.result.json,
    which the parent puts into the task's line.
"""

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from hashlib import sha256
from math import exp, log
from pathlib import Path

ROOT = Path("/Users/ds283/Documents/Code/ChamPBH")
# changed (ruling 1): 02d's paths
OUT = ROOT / "var/one-loop-drain/02d"
TASKS_JSON = OUT / "tasks.json"
TASK_DIR = OUT / "tasks"
BATCH_DIR = OUT / "batches"
PYTHON = str(ROOT / "venv/bin/python")
SELF = str(ROOT / ".documents/one-loop-drain/measure-02d/run_02d.py")
TOOL = str(ROOT / "tools/history_and_bbn.py")

# the per-task timeout (prompt 02d §1.2); the environment variable exists only to test the timeout path
# changed (ruling 1): renamed for 02d
TIMEOUT_S = float(os.environ.get("ONE_LOOP_DRAIN_02D_TIMEOUT_S", "3600"))
KILL_GRACE_S = 30.0
POLL_S = 2.0

# removed (ruling 1): L2S_TABLE, the R S(L) bound's table


# source: measure/run_sample.py, now_utc (unchanged)
def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# source: measure/run_sample.py, loads (unchanged)
def loads() -> str:
    return " ".join(f"{x:.2f}" for x in os.getloadavg())


# source: measure/run_sample.py, load_tasks (unchanged)
def load_tasks() -> dict:
    return {t["id"]: t for t in json.loads(TASKS_JSON.read_text())}


# source: measure/run_sample.py, digest (unchanged)
def digest(obj) -> str:
    return sha256(repr(obj).encode()).hexdigest()


# ---- the child ------------------------------------------------------------------------------------


def law_check(SM, drain) -> dict:
    """
    Ruling 6, "Checked": the record's model, and alpha_eff recomputed at every candidate by
    drain_alpha_effective(c.regime, c.ell, c.L, c.log_R, record.A, c.w_gate), compared by repr.
    """
    out = {
        "model": drain.model,
        "model_ok": drain.model == SM.DRAIN_MODEL_ONE_LOOP,
        "alpha_checked": 0,
        "alpha_differs": 0,
        "first_difference": None,
    }
    if drain.candidates is None:
        return out
    for j, c in enumerate(drain.candidates):
        a = SM.drain_alpha_effective(c.regime, c.ell, c.L, c.log_R, drain.A, c.w_gate)
        out["alpha_checked"] += 1
        if repr(a) != repr(c.alpha_eff):
            out["alpha_differs"] += 1
            if out["first_difference"] is None:
                out["first_difference"] = {
                    "index": j,
                    "candidate": list(c),
                    "recomputed": a,
                }
    return out


def law_counts(SM, A, candidates) -> dict:
    """
    Ruling 6, "The cap" and "The edges", over a list of DrainCandidate: the cap by regime (A f >= DRAIN_ALPHA_CAP,
    f = drain_fraction of the candidate's own record), and the law's edges by the module's constants.
    """
    cap = {r: 0 for r in SM.DRAIN_REGIMES}
    edges = {
        "gap_clamp": 0,
        "gap_below_threshold": 0,
        "classical_continuation": 0,
        "classical_S_held": 0,
        "classical_underflow": 0,
    }
    for c in candidates:
        if c.regime != SM.REGIME_UNASSESSED:
            f = SM.drain_fraction(c.regime, c.ell, c.L, c.log_R)
            if A * f >= SM.DRAIN_ALPHA_CAP:
                cap[c.regime] += 1
        if c.regime == SM.REGIME_GAP:
            if 1.0 + SM.DRAIN_GAP_Q * log(c.L) <= 0.0:
                edges["gap_clamp"] += 1
            if c.log_R <= SM.DRAIN_GAP_R0:
                edges["gap_below_threshold"] += 1
        elif c.regime == SM.REGIME_CLASSICAL:
            if c.L > SM.DRAIN_S_TABLE_L[-1]:
                edges["classical_continuation"] += 1
            if c.L < SM.DRAIN_S_TABLE_L[0]:
                edges["classical_S_held"] += 1
            if c.alpha_eff == 0.0 and c.w_gate > 0.0:
                edges["classical_underflow"] += 1
    return {"cap": cap, "edges": edges}


# source: measure/run_sample.py, reduce_result; changed as marked
def reduce_result(SM, k: int, result, wall: float, error=None) -> dict:
    """
    One integration, reduced at once: from an IntegrationResult, or, for an integration that raised, the exception
    and the DrainRecord it carries (None from outside the step loop). Returns the partial line, with the full regime
    sequence, injection triples and DrainCandidate lists under "regimes", "injections" and "candidates".
    """
    import resource

    rec = {"k": k, "wall": wall}
    if error is not None:
        drain = getattr(error, "drain", None)
        rec.update(
            outcome="raised",
            error_type=type(error).__name__,
            error=str(getattr(error, "message", error)),
        )
        injections = None
    else:
        drain = result.drain
        rec.update(
            outcome="returned",
            N_final=result.N_final,
            accepted_steps=result.accepted_steps,
            reflections=len(result.reflections),
            nfev=result.nfev,
            restart_retakes=result.restart_retakes,
            parking_N=None if result.parking is None else result.parking.N,
            release_N=None if result.release is None else result.release.N,
        )
        injections = [(i.N, i.alpha_eff, i.delta_K) for i in result.injections]
    if drain is None:
        rec["drain"] = None
        regimes = None
        candidates = None
    else:
        rec["drain"] = {
            "law": drain.law,
            "A": drain.A,
            # changed (ruling 1): the record's model where its gap prescription stood
            "model": drain.model,
            "unresolvable_assessments": drain.unresolvable_assessments,
            "unresolved_declined": drain.unresolved_declined,
        }
        regimes = (
            None if drain.candidates is None else [c.regime for c in drain.candidates]
        )
        # ruling 6, "Kept": each DrainCandidate as the list of its eight fields, in order
        candidates = (
            None if drain.candidates is None else [list(c) for c in drain.candidates]
        )
    rec["counts"] = (
        None if regimes is None else {r: regimes.count(r) for r in SM.DRAIN_REGIMES}
    )
    rec["regimes_sha256"] = None if regimes is None else digest(regimes)
    rec["injections_sha256"] = None if injections is None else digest(injections)
    # ruling 6, "Checked", "The cap" and "The edges", for every integration with a DrainRecord
    if drain is not None and drain.candidates is not None:
        rec["candidates_sha256"] = digest(candidates)
        rec["law_check"] = law_check(SM, drain)
        rec.update(law_counts(SM, drain.A, drain.candidates))
    else:
        rec["candidates_sha256"] = None
        rec["law_check"] = None if drain is None else law_check(SM, drain)
    # added (IMPLEMENTATION CHOICE, the log): the accepted steps after the first classical candidate, from the
    # OdeSolution's nodes (one per accepted step after N_start), for prompt §2 item 7
    if error is None and drain is not None and drain.candidates is not None:
        ts = result.solution.ts
        rec["solution_nodes"] = len(ts)
        jc = next(
            (
                j
                for j, c in enumerate(drain.candidates)
                if c.regime == SM.REGIME_CLASSICAL
            ),
            None,
        )
        if jc is None:
            rec["steps_after_first_classical"] = 0
        else:
            Nc = result.injections[jc].N
            rec["steps_after_first_classical"] = int(sum(1 for t in ts if t > Nc))
    rec["maxrss"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rec["regimes"] = regimes
    rec["injections"] = injections
    rec["candidates"] = candidates
    return rec


# source: measure-02b/run_wide.py, first_impact; changed as marked
def first_impact(SM, data, K_final):
    """
    The first impact and the cap (prompt 02d §1.3, as prompt 02b §1.3), from the accepted integration's payload:
      - the first candidate's regime, N, ell, L and ln R, and its alpha_eff, the fraction of the first impact's
        kinetic energy drained (0 where nothing drained); its delta_K as a share of K(N_final) (None where
        K(N_final) = 0);
      - the drains (alpha_eff > 0): in all; at N <= parking_N (all of them where the field does not park); after the
        first impact and before parking; and after parking;
      - the parking leftover, which is the Parking record's delta_K, a ledger entry of its own and not among the
        injections: its share of K(N_final), counted apart;
      - the cap by regime: candidates where A f >= alpha_cap, f = drain_fraction of the candidate's record.
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
            # changed (ruling 6): the law's L and ln R, as the record carries them
            "L": c0.L,
            "log_R": None if tp is None else tp.log_R,
            "record_log_R": c0.log_R,
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
    # changed (ruling 6): the cap by regime, by the law's f (DRAIN_C1 and the edge prescription are gone)
    out["cap"] = law_counts(SM, A, cands)["cap"]
    # changed (ruling 6): drains before parking by regime
    out["drains_before_parking_by_regime"] = {
        r: sum(1 for j in before if cands[j].regime == r) for r in SM.DRAIN_REGIMES
    }
    out["one_loop_drains"] = sum(
        1
        for c, i in zip(cands, inj)
        if c.regime == SM.REGIME_ONE_LOOP and i.alpha_eff > 0.0
    )
    return out


def by_regime(SM, data, drain_values) -> dict:
    """
    Ruling 6, "By regime", from the accepted integration (the payload's record): per regime the count, the drains
    (alpha_eff > 0), the K share (drain_values', the tool's), the range of L and of ln R, and the drains with
    w_gate < 1 (U9).
    """
    cands = data["drain"].candidates
    out = {}
    for r in SM.DRAIN_REGIMES:
        sel = [c for c in cands if c.regime == r]
        Ls = [c.L for c in sel if c.L is not None]
        lRs = [c.log_R for c in sel if c.log_R is not None]
        out[r] = {
            "count": len(sel),
            "drains": sum(1 for c in sel if c.alpha_eff > 0.0),
            "K_share": drain_values[f"K_share_{r}"],
            "L_min": min(Ls) if Ls else None,
            "L_max": max(Ls) if Ls else None,
            "log_R_min": min(lRs) if lRs else None,
            "log_R_max": max(lRs) if lRs else None,
            "w_gate_below_1_at_drain": sum(
                1 for c in sel if c.alpha_eff > 0.0 and c.w_gate < 1.0
            ),
        }
    return out


# source: measure/run_sample.py, accepted_values; changed as marked
def accepted_values(SM, T, data: dict, N_final: float, M_units: float, units) -> dict:
    """
    The accepted integration's values (prompt 02d §1.3), from the tuning's payload and the accepted integration's
    N_final.
    """
    from types import SimpleNamespace
    from tools.outward_step_scan import _sha256

    out = {}
    cands = data["drain"].candidates
    injections = data["injections"]
    assert len(cands) == len(injections)
    out["drain_values"] = T.drain_values(data, SimpleNamespace(N_final=N_final))
    out["dissipation"] = T.dissipation_values_printed(data, units)
    out["N_final"] = N_final
    ledger = data["dark_radiation"]
    K_final = ledger.K(N_final)
    out["K_final"] = K_final
    # changed (ruling 1): the record's model
    out["model"] = data["drain"].model

    def log_R(i):
        tp = i.classical_turning_point
        return None if tp is None else tp.log_R

    # drains (delta_K > 0) by regime; the least ln R at a drain; w_gate < 1 at a one-loop candidate (U9)
    drains = {r: 0 for r in SM.DRAIN_REGIMES}
    least = None
    for c, i in zip(cands, injections):
        if i.delta_K > 0.0:
            drains[c.regime] += 1
            lr = log_R(i)
            if lr is not None and (least is None or lr < least):
                least = lr
    out["drains"] = drains
    out["least_log_R_at_drain"] = least
    out["one_loop_w_gate_below_1"] = sum(
        1 for c in cands if c.regime == SM.REGIME_ONE_LOOP and c.w_gate < 1.0
    )
    out["gap_w_gate_below_1"] = sum(
        1 for c in cands if c.regime == SM.REGIME_GAP and c.w_gate < 1.0
    )
    ells = [c.ell for c in cands if c.regime == SM.REGIME_ONE_LOOP]
    out["ell_max"] = max(ells) if ells else None
    alphas = [c.alpha_eff for c in cands if c.alpha_eff > 0.0]
    out["alpha_eff_min"] = min(alphas) if alphas else None
    out["alpha_eff_max"] = max(alphas) if alphas else None

    # the gap's reach
    def first(regime):
        for j, c in enumerate(cands):
            if c.regime == regime:
                return j
        return None

    reach = {}
    for regime in (SM.REGIME_GAP, SM.REGIME_CLASSICAL, SM.REGIME_UNASSESSED):
        j = first(regime)
        reach[regime] = (
            None
            if j is None
            else {"index": j, "N": injections[j].N, "log_R": log_R(injections[j])}
        )
    jg = first(SM.REGIME_GAP)
    if jg is not None:
        before = [j for j in range(jg) if cands[j].regime == SM.REGIME_ONE_LOOP]
        reach["gap"]["ell_one_loop_before"] = cands[before[-1]].ell if before else None
        reach["gap"]["one_loop_after"] = sum(
            1 for c in cands[jg:] if c.regime == SM.REGIME_ONE_LOOP
        )
    jc = first(SM.REGIME_CLASSICAL)
    if jc is not None:
        reach["classical"]["one_loop_after"] = sum(
            1 for c in cands[jc:] if c.regime == SM.REGIME_ONE_LOOP
        )
        reach["classical"]["gap_after"] = sum(
            1 for c in cands[jc:] if c.regime == SM.REGIME_GAP
        )
    last_ol = [j for j, c in enumerate(cands) if c.regime == SM.REGIME_ONE_LOOP]
    reach["last_one_loop"] = (
        None
        if not last_ol
        else {
            "index": last_ol[-1],
            "N": injections[last_ol[-1]].N,
            "log_R": log_R(injections[last_ol[-1]]),
        }
    )
    reach["enters_gap"] = jg is not None
    reach["reaches_classical"] = jc is not None
    out["reach"] = reach

    # removed (ruling 1): the R S(L) bound ("RSL")
    # ruling 6: by regime, the cap and the edges, from the accepted integration
    out["by_regime"] = by_regime(SM, data, out["drain_values"])
    out.update(law_counts(SM, data["drain"].A, cands))
    out["first_impact"] = first_impact(SM, data, K_final)

    last = SM.SampleValues._make(tuple(data["sample"][-1]))
    # the r_chi form's check (note 02's ruling 7): K(N_final) e^{-4 N_final} / rho_R,E(last sample) against
    # rho_chi_ratio_today
    out["r_chi_check"] = {
        "K_form": K_final * exp(-4.0 * N_final) / exp(last.log_rhorad_Einstein),
        "rho_chi_ratio_today": data["dark_radiation_values"].rho_chi_ratio_today,
        "last_raw_N_minus_N_final": last.raw_N - N_final,
    }
    out["n_samples"] = len(data["sample"])
    out["samples_sha256"] = _sha256(data["sample"])
    out["log_one_plus_z_sha256"] = _sha256([z.log_one_plus_z for z in data["z_grid"]])
    return out


# source: measure/run_sample.py, child; changed as marked
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
            # changed (ruling 6): "candidates" dropped from the task's line too
            rec.pop("regimes"), rec.pop("injections"), rec.pop("candidates")
            reduced.append(rec)
            raise
        rec = reduce_result(SM, k, result, time.perf_counter() - t0)
        partial.write(json.dumps(rec) + "\n")
        partial.flush()
        rec.pop("regimes"), rec.pop("injections"), rec.pop("candidates")
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


# ---- the parent -----------------------------------------------------------------------------------


# source: measure/run_sample.py, read_partial; changed as marked
def read_partial(path: Path) -> list:
    out = []
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        # changed (ruling 6): "candidates" dropped as well
        rec.pop("regimes", None), rec.pop("injections", None), rec.pop(
            "candidates", None
        )
        out.append(rec)
    return out


# source: measure/run_sample.py, TASK_FIELDS; changed (ruling 1): no "gap"
TASK_FIELDS = (
    "id",
    "history",
    "group",
    "M",
    "beta",
    "A",
    "shard",
    "serial",
    "stored_alpha",
)


# source: measure/run_sample.py, build_line (unchanged)
def build_line(task, prefix, status, ru, wall, started, ended, timed_out) -> dict:
    line = {k: task[k] for k in TASK_FIELDS}
    line["obtained"] = "run"
    line["tag"] = prefix
    line["started"], line["ended"] = started, ended
    line["wall"] = wall
    line["peak_rss"] = ru.ru_maxrss
    line["exit_status"] = status
    result_path = TASK_DIR / f"{prefix}{task['id']}.result.json"
    if timed_out:
        line["outcome"] = "timeout"
        line["reason"] = f"killed after {TIMEOUT_S:g} s"
        line["integrations"] = read_partial(
            TASK_DIR / f"{prefix}{task['id']}.partial.jsonl"
        )
        return line
    if not result_path.exists():
        line["outcome"] = "crashed"
        line["reason"] = f"no result file; exit status {status}"
        line["integrations"] = read_partial(
            TASK_DIR / f"{prefix}{task['id']}.partial.jsonl"
        )
        return line
    res = json.loads(result_path.read_text())
    for k in (
        "outcome",
        "stage",
        "reason",
        "rc",
        "tuning",
        "tuning_wall",
        "accepted",
        "bbn",
        "integrations",
        "maxrss",
    ):
        if k in res:
            line[k] = res[k]
    return line


# source: measure/run_sample.py, batch; changed as marked
def batch(name: str, ids_file: str, workers: int, out_jsonl: str, prefix: str) -> int:
    bdir = BATCH_DIR / name
    bdir.mkdir(parents=True, exist_ok=True)
    # changed (ruling 1): the task directory is made here, under 02d's path
    TASK_DIR.mkdir(parents=True, exist_ok=True)
    (bdir / "batch.pid").write_text(f"{os.getpid()}\n")
    tasks = load_tasks()
    ids = [x.strip() for x in Path(ids_file).read_text().splitlines() if x.strip()]
    for i in ids:
        tid = i[5:] if i.startswith("tool:") else i
        if tid not in tasks:
            raise SystemExit(f"unknown task {tid}")
    out = open(out_jsonl, "a")
    t_batch = time.time()
    print(
        f"{now_utc()} ** batch {name} start pid={os.getpid()} tasks={len(ids)} workers={workers} "
        f"loads {loads()}",
        flush=True,
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = "."
    queue = list(ids)
    running = {}
    done = 0
    while queue or running:
        while queue and len(running) < workers:
            item = queue.pop(0)
            tool = item.startswith("tool:")
            tid = item[5:] if tool else item
            if tool:
                cmd = [PYTHON, TOOL] + tasks[tid]["argv"]
                log_path = TASK_DIR / f"{prefix}tool-{tid}.log"
                err_path = TASK_DIR / f"{prefix}tool-{tid}.err"
            else:
                cmd = [PYTHON, SELF, "child", tid, prefix]
                log_path = TASK_DIR / f"{prefix}{tid}.log"
                err_path = TASK_DIR / f"{prefix}{tid}.err"
                for suffix in (".partial.jsonl", ".result.json"):
                    p = TASK_DIR / f"{prefix}{tid}{suffix}"
                    if p.exists():
                        p.unlink()
            proc = subprocess.Popen(
                cmd,
                cwd=str(ROOT),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=open(log_path, "w"),
                stderr=open(err_path, "w"),
            )
            running[proc.pid] = {
                "proc": proc,
                "tid": tid,
                "tool": tool,
                "t0": time.time(),
                "started": now_utc(),
                "term": None,
                "killed": False,
            }
            print(
                f"{now_utc()} start {'tool:' if tool else ''}{tid} pid={proc.pid} loads {loads()}",
                flush=True,
            )
        time.sleep(POLL_S)
        for pid in list(running):
            r = running[pid]
            wpid, status, ru = os.wait4(pid, os.WNOHANG)
            if wpid == 0:
                elapsed = time.time() - r["t0"]
                if r["term"] is None and elapsed > TIMEOUT_S:
                    os.kill(pid, signal.SIGTERM)
                    r["term"] = time.time()
                    print(f"{now_utc()} SIGTERM {r['tid']} pid={pid}", flush=True)
                elif (
                    r["term"] is not None
                    and not r["killed"]
                    and time.time() - r["term"] > KILL_GRACE_S
                ):
                    os.kill(pid, signal.SIGKILL)
                    r["killed"] = True
                    print(f"{now_utc()} SIGKILL {r['tid']} pid={pid}", flush=True)
                continue
            r["proc"].returncode = status
            wall = time.time() - r["t0"]
            ended = now_utc()
            del running[pid]
            done += 1
            task = tasks[r["tid"]]
            if r["tool"]:
                line = {
                    "tool_check": r["tid"],
                    "tag": prefix,
                    "rc": os.waitstatus_to_exitcode(status),
                    "wall": wall,
                    "peak_rss": ru.ru_maxrss,
                    "started": r["started"],
                    "ended": ended,
                }
                outcome = f"rc={line['rc']}"
            else:
                line = build_line(
                    task,
                    prefix,
                    os.waitstatus_to_exitcode(status),
                    ru,
                    wall,
                    r["started"],
                    ended,
                    r["term"] is not None,
                )
                outcome = line["outcome"]
            out.write(json.dumps(line) + "\n")
            out.flush()
            print(
                f"{ended} end {'tool:' if r['tool'] else ''}{r['tid']} {outcome} wall={wall:.1f} s "
                f"rss={ru.ru_maxrss / 2**20:.0f} MiB ({done}/{len(ids)}) loads {loads()}",
                flush=True,
            )
    out.close()
    total = time.time() - t_batch
    msg = f"{now_utc()} ** batch {name} end tasks={len(ids)} wall={total:.1f} s loads {loads()}"
    print(msg, flush=True)
    (bdir / "batch.end").write_text(msg + "\n")
    return 0


# source: measure/run_sample.py, main (unchanged)
def main():
    role = sys.argv[1]
    if role == "child":
        sys.exit(child(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ""))
    if role == "batch":
        args = sys.argv[2:]
        prefix = ""
        if "--tag" in args:
            j = args.index("--tag")
            prefix = args[j + 1]
            del args[j : j + 2]
        name, ids_file, workers, out_jsonl = args
        sys.exit(batch(name, ids_file, int(workers), out_jsonl, prefix))
    raise SystemExit(f"unknown role {role}")


if __name__ == "__main__":
    main()
