"""
one-loop-drain prompt 02 §1.3, the driver (orchestrator note 02, rulings 2-5 and 9). It runs the tool route,
`tools.history_and_bbn.main`, one task per fresh process, and writes one JSON line per task.

Two roles, one file:

  batch: the parent, launched detached (ruling 2):

      nohup ./venv/bin/python .documents/one-loop-drain/measure/run_sample.py batch NAME IDS_FILE WORKERS OUT.jsonl \\
          [--tag PREFIX] > var/one-loop-drain/02/batches/NAME/batch.log 2>&1 &

    It writes its pid to var/one-loop-drain/02/batches/NAME/batch.pid, runs the task ids of IDS_FILE (one per line,
    from var/one-loop-drain/02/tasks.json; a line `tool:<task id>` is a tool check instead, below) in that order,
    at most WORKERS at once, each in a fresh `./venv/bin/python` from the repository root (PYTHONPATH=.), appends one
    line per task to OUT.jsonl, flushed, and writes batch.end when every child has ended. It logs each start and
    end, with the 1-, 5- and 15-minute loads, to stdout (batch.log). A child that runs past TIMEOUT_S (3600 s) is sent
    SIGTERM, then SIGKILL after KILL_GRACE_S (30 s); its line is built from its partial file with outcome
    `timeout`, and the parent waits for it, so that nothing outlives the batch (ruling 4). Every child is reaped with
    os.wait4, whose ru_maxrss (bytes on macOS) is the line's `peak_rss` (ruling 5). --tag prefixes the task's file
    names under tasks/ (the spot checks', so that they do not meet a task's own).

    A tool check (ruling 9; prompt §1.3, "The driver is checked against the tool") runs
    `./venv/bin/python tools/history_and_bbn.py <the task's argv>` itself, its stdout to
    tasks/<tag>tool-<task id>.log, and writes a line {"tool_check": id, "rc", "wall", "peak_rss"}. tool_check.py
    compares it with the driver task's stdout.

  child: one task, in its own process (ruling 3):

      PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure/run_sample.py child TASK_ID PREFIX

    It calls tools.history_and_bbn.main(task argv), whose stdout is the task's log. Around the call it makes the three
    captures of ruling 3, each restored after it:
      - ComputeTargets.ScalarHistory.integrate_scalar_history, wrapped before main wraps it, so that it sees every
        integration of the tuning. Each result, or each exception, is reduced at once (reduce_result) and one JSON
        line is appended to tasks/<PREFIX><id>.partial.jsonl and flushed (ruling 4); no reference to the
        IntegrationResult or its solution is kept;
      - tools.history_and_bbn.tune_history, to keep the payload;
      - tools.history_and_bbn.compute_BBN_data, replaced for the call by an object whose _function calls the real one
        and keeps its result.
    It then writes tasks/<PREFIX><id>.result.json (accepted_values, from the payload and the tool's own drain_values
    and dissipation_values_printed), which the parent puts into the task's line. Its own messages go to stderr.

The line (prompt §1.3, "Each line holds"):
  task fields (id, history, group, M, beta, A, gap, shard, serial, stored_alpha), obtained "run", outcome
  ("success", "failure", "error", "timeout" or "crashed"), reason, rc, wall (s, the parent's), peak_rss (bytes),
  started/ended (UTC);
  integrations: per integration, in order, the reduced record (reduce_result without its full lists): N_final,
  accepted_steps, reflections, nfev, restart_retakes, the candidate counts by regime, the drain record's fields, the
  parking N, and the SHA-256 of the regime sequence and of the injections' (N, alpha_eff, delta_K) reprs (ruling 6;
  the full sequences are in the partial file);
  tuning: TUNING_OUTPUT_KEYS' values;
  accepted: accepted_values (below);
  bbn: Y_p, D/H, He3/H, Li7/H, network, PRyMordial's time and version, the BBN stage's wall.
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
OUT = ROOT / "var/one-loop-drain/02"
TASKS_JSON = OUT / "tasks.json"
TASK_DIR = OUT / "tasks"
BATCH_DIR = OUT / "batches"
PYTHON = str(ROOT / "venv/bin/python")
SELF = str(ROOT / ".documents/one-loop-drain/measure/run_sample.py")
TOOL = str(ROOT / "tools/history_and_bbn.py")

# the per-task timeout (prompt §1.3); the environment variable exists only to test the timeout path
TIMEOUT_S = float(os.environ.get("ONE_LOOP_DRAIN_02_TIMEOUT_S", "3600"))
KILL_GRACE_S = 30.0
POLL_S = 2.0

# Table tab:beta of BriefingNotes/production_energy_ratio.tex: L^2 S(L) at L = M/phi_t (README §8.2; ruling 7)
L2S_TABLE = ((40.0, 0.22), (70.0, 0.24), (90.0, 0.25), (110.0, 0.25), (134.0, 0.26))


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def loads() -> str:
    return " ".join(f"{x:.2f}" for x in os.getloadavg())


def load_tasks() -> dict:
    return {t["id"]: t for t in json.loads(TASKS_JSON.read_text())}


def digest(obj) -> str:
    return sha256(repr(obj).encode()).hexdigest()


# ---- the child ------------------------------------------------------------------------------------


def reduce_result(SM, k: int, result, wall: float, error=None) -> dict:
    """
    One integration, reduced at once (ruling 3): from an IntegrationResult, or, for an integration that raised, the
    exception and the DrainRecord it carries (None from outside the step loop). Returns the partial line, with the
    full regime sequence and injection triples under "regimes" and "injections".
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
    else:
        rec["drain"] = {
            "law": drain.law,
            "A": drain.A,
            "gap_prescription": drain.gap_prescription,
            "unresolvable_assessments": drain.unresolvable_assessments,
            "unresolved_declined": drain.unresolved_declined,
        }
        regimes = (
            None if drain.candidates is None else [c.regime for c in drain.candidates]
        )
    rec["counts"] = (
        None if regimes is None else {r: regimes.count(r) for r in SM.DRAIN_REGIMES}
    )
    rec["regimes_sha256"] = None if regimes is None else digest(regimes)
    rec["injections_sha256"] = None if injections is None else digest(injections)
    rec["maxrss"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rec["regimes"] = regimes
    rec["injections"] = injections
    return rec


def l2s(L: float) -> float:
    """L^2 S(L), linear in L between the rows of tab:beta; only called for 40 <= L <= 134."""
    for (L0, s0), (L1, s1) in zip(L2S_TABLE, L2S_TABLE[1:]):
        if L0 <= L <= L1:
            return s0 + (s1 - s0) * (L - L0) / (L1 - L0)
    raise ValueError(L)


def accepted_values(SM, T, data: dict, N_final: float, M_units: float, units) -> dict:
    """
    The accepted integration's values (prompt §1.3), from the tuning's payload and the accepted integration's N_final.
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

    # the R S(L) bound (ruling 7)
    total = 0.0
    n_in, n_out = 0, 0
    L_values = []
    for c, i in zip(cands, injections):
        if c.regime != SM.REGIME_CLASSICAL:
            continue
        tp = i.classical_turning_point
        L = M_units / tp.phi_t
        L_values.append(L)
        if 40.0 <= L <= 134.0:
            n_in += 1
            total += exp(tp.log_R + log(l2s(L) / (L * L)) + i.log_rho_kin + 4.0 * i.N)
        else:
            n_out += 1
    last = SM.SampleValues._make(tuple(data["sample"][-1]))
    out["RSL"] = {
        "classical": n_in + n_out,
        "in_range": n_in,
        "outside_range": n_out,
        "L_min": min(L_values) if L_values else None,
        "L_max": max(L_values) if L_values else None,
        "sum_delta_K_bound": total,
        "share_of_K": total / K_final if K_final > 0.0 else None,
        "r_chi_today": total * exp(-4.0 * N_final) / exp(last.log_rhorad_Einstein),
    }
    # the r_chi form's check (ruling 7): K(N_final) e^{-4 N_final} / rho_R,E(last sample) against rho_chi_ratio_today
    out["r_chi_check"] = {
        "K_form": K_final * exp(-4.0 * N_final) / exp(last.log_rhorad_Einstein),
        "rho_chi_ratio_today": data["dark_radiation_values"].rho_chi_ratio_today,
        "last_raw_N_minus_N_final": last.raw_N - N_final,
    }
    out["n_samples"] = len(data["sample"])
    out["samples_sha256"] = _sha256(data["sample"])
    out["log_one_plus_z_sha256"] = _sha256([z.log_one_plus_z for z in data["z_grid"]])
    return out


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
            rec.pop("regimes"), rec.pop("injections")
            reduced.append(rec)
            raise
        rec = reduce_result(SM, k, result, time.perf_counter() - t0)
        partial.write(json.dumps(rec) + "\n")
        partial.flush()
        rec.pop("regimes"), rec.pop("injections")
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


def read_partial(path: Path) -> list:
    out = []
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        rec.pop("regimes", None), rec.pop("injections", None)
        out.append(rec)
    return out


TASK_FIELDS = (
    "id",
    "history",
    "group",
    "M",
    "beta",
    "A",
    "gap",
    "shard",
    "serial",
    "stored_alpha",
)


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


def batch(name: str, ids_file: str, workers: int, out_jsonl: str, prefix: str) -> int:
    bdir = BATCH_DIR / name
    bdir.mkdir(parents=True, exist_ok=True)
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
