"""
one-loop-drain prompt 05 §1.6 (P47; orchestrator note 05, ruling 11): 02d's 230 successful tasks, each re-integrated once at
its accepted tuning, checked against 02d, stored on a temporary store, exported by 04's writers in the plot scripts'
layout, and the store deleted; then the parts of histories.csv merged.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/derive-05/run_05.py scheduler     (launched by launch_05.sh)
    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/derive-05/run_05.py child TASK_ID
    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/derive-05/run_05.py merge

Everything goes under var/one-loop-drain/05/ (VAR):
  run/<id>.pid, run/<id>.log          each child's pid (written by the scheduler) and its stdout and stderr
  run/<id>.result.json, run/<id>.done the child's result and end marker
  records/<id>.json                   the child's histories.csv record (build_history_record's dict; NaN allowed)
  parts/<id>.csv                      a one-row histories.csv of that record (write_histories_csv)
  stores/<id>/                        the temporary store, deleted once the export is read back
  exports/QCD_Cosmology/              the export root: histories.csv (merged) and the per-history directories
  scheduler.log, scheduler.json, scheduler.done    the scheduler's lines, its summary and its end marker

**The child** (one process per task):
  1. realizes the model from the line's tuning (normalization, phi_ref) and integrates it once with integrate_history under
     the one-loop law at the line's A, with tune_history's T_stop (T_CMB), the tool route's z grid (main.py's density and
     depth, store ids k + 1) and the default tolerances, the sample masses computed (compute_sample_masses at its default).
     A copy of store-03/cost_03.py's step 1 (`run`, its first part), with that one change;
  2. checks every DrainCandidate against the last line of var/one-loop-drain/02d/tasks/<id>.partial.jsonl by repr (cost_03's
     check); a difference is a stop (prompt §1.6, step 2), recorded in the result;
  3. stores the payload, with the line's tuning keys, on a temporary store under VAR/stores/<id>/ and reads it back:
     store-03/cost_03.py's `write_and_read`, imported (the package's undecorated actor in ChamPBH's stand-in cluster; never
     Ray, never registered);
  4. exports the object read back (never the payload): its histories.csv record by build_history_record (bbn and baseline
     None), written as JSON and as a one-row part; fields.csv by plot_ScalarHistory.py's own write_csv_file (extracted by
     ast) with BBN and the adiabatic history unavailable stand-ins (note 04, ruling 6); bounces.csv by write_bounces_csv;
     in EXPORT/<alpha_path>/csv/<history_directory> (tools/tests/quanta_exports.export_history);
  5. reads the export back (the row counts of fields.csv and bounces.csv against the object's samples and bounce rows) and
     deletes the temporary store;
  6. records the wall of each step, its peak RSS, the 1-, 5- and 15-minute loads at its start and end, and its end marker.

**The scheduler** runs at most WORKERS = min(6, floor(8 GiB / 1.48 GiB)) = 5 children at once (02d's largest task peaked at
1.48 GiB), the largest first by 02d's per-integration wall (tuning_wall / tuning_iterations). A child past 4 x its 02d
per-integration wall + 900 s is stopped (SIGTERM, then SIGKILL after 30 s) and counts as a failure with that reason. After
the last child it merges (as `merge`), writes scheduler.json and the end marker scheduler.done, and exits.

**The merge**: write_histories_csv over every record read back from records/, to EXPORT/histories.csv; then a check that
the merged file's header is each part's and its data rows are the parts' data rows, byte for byte (as multisets: the
merged file is sorted by the key, the parts are in task order).
"""

import contextlib
import importlib
import importlib.util
import io
import json
import math
import os
import resource
import shutil
import signal
import subprocess
import sys
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
VAR = ROOT / "var" / "one-loop-drain" / "05"
RUN = VAR / "run"
RECORDS = VAR / "records"
PARTS = VAR / "parts"
STORES = VAR / "stores"
LABEL = "QCD_Cosmology"  # config/model_list.py's label for the cosmology, the plot scripts' <label>
EXPORT = VAR / "exports" / LABEL
SAMPLE = ROOT / "var" / "one-loop-drain" / "02d" / "sample.jsonl"
PARTIALS = ROOT / "var" / "one-loop-drain" / "02d" / "tasks"
PYTHON = ROOT / "venv" / "bin" / "python"

GIB = 1024**3
PEAK_RSS_02D = 1.48 * GIB  # L-M5e-20-A0.1's peak in 02d (orchestrator note 05 §0)
WORKERS = min(6, int(8 * GIB // PEAK_RSS_02D))
LIMIT_FACTOR = 4.0
LIMIT_OFFSET = 900.0


def cost_03():
    spec = importlib.util.spec_from_file_location(
        "cost_03", ROOT / ".documents" / "one-loop-drain" / "store-03" / "cost_03.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def lines():
    return [json.loads(t) for t in SAMPLE.read_text().splitlines()]


def successes():
    return [l for l in lines() if l["outcome"] == "success"]


def wall_02d(line) -> float:
    return line["tuning_wall"] / line["tuning"]["tuning_iterations"]


def last_partial(task_id: str) -> dict:
    return json.loads(
        (PARTIALS / f"{task_id}.partial.jsonl").read_text().splitlines()[-1]
    )


# ---------------------------------------------------------------------------------------------------------------------
# The child
# ---------------------------------------------------------------------------------------------------------------------


def child(task_id: str) -> int:
    os.chdir(ROOT)
    out = {"id": task_id, "load_start": os.getloadavg(), "started": time.time()}
    try:
        out.update(run_task(task_id))
    except BaseException as e:
        out["error"] = f"{type(e).__name__}: {e}"
        out["traceback"] = traceback.format_exc()
    out["peak_rss"] = resource.getrusage(
        resource.RUSAGE_SELF
    ).ru_maxrss  # bytes on macOS
    out["load_end"] = os.getloadavg()
    out["ended"] = time.time()
    (RUN / f"{task_id}.result.json").write_text(json.dumps(out, indent=1) + "\n")
    (RUN / f"{task_id}.done").write_text("done\n")
    return 0


def run_task(task_id: str) -> dict:
    from CosmologyConcepts import (
        M_value,
        beta_value,
        redshift,
        redshift_array,
        temperature,
    )
    from CosmologyConcepts.FieldValues import phi_value, pi_value
    from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
    from CosmologyConcepts.redshift import log_one_plus_z_grid
    from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
    from CosmologyModels.LambdaCDM import Planck2018
    from Units import Planck_units

    import extract_common as ec
    from tools.tests import quanta_exports as qe

    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    hb = importlib.import_module("tools.history_and_bbn")
    c03 = cost_03()

    line = next(l for l in lines() if l["id"] == task_id)
    partial = last_partial(task_id)
    walls = {}

    # ---- 1. realize and integrate (a copy of cost_03.py's step 1, with the masses computed) ----
    t0 = time.perf_counter()
    units = Planck_units()
    with contextlib.redirect_stdout(io.StringIO()):
        cosmology = QCD_Cosmology(0, units, Planck2018())
    model = ExponentialModel(
        0,
        M_value(0, line["M"] * units.PlanckMass),
        1,
        beta_value(0, line["beta"]),
        units,
    )
    tuning = line["tuning"]
    rm = model.realize(tuning["normalization"], tuning["phi_ref"])
    xs = log_one_plus_z_grid(hb.SAMPLES_PER_LOG10_Z, hb.DEFAULT_LOG10_ONE_PLUS_Z_HIGH)
    z_grid = redshift_array(z_array=[redshift(k + 1, x) for k, x in enumerate(xs)])
    with contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            cosmology,
            temperature(0, hb.T_INIT_GEV * units.GeV),
            temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
            phi_value(0, hb.PHI_INIT_MP * units.PlanckMass),
            pi_value(0, hb.PI_INIT),
            z_grid,
            rm,
            task_label=f"derive-05-{task_id}",
            alpha=line["A"],
            drain_law=SM.DRAIN_LAW_ONE_LOOP,
        )
    walls["integrate"] = time.perf_counter() - t0
    if data.get("failure", False):
        raise RuntimeError(f"the integration failed: {data['failure_reason']}")

    # ---- 2. the check against 02d, by repr ----
    candidates = data["drain"].candidates
    checks = {
        "candidates_equal_02d": repr([list(c) for c in candidates])
        == repr(partial["candidates"]),
        "n_candidates": len(candidates),
        "n_candidates_02d": len(partial["candidates"]),
        "n_samples": len(data["sample"]),
        "n_samples_02d": line["accepted"]["n_samples"],
    }

    # ---- 3. store and read back (cost_03.py's write_and_read, imported) ----
    t0 = time.perf_counter()
    payload = dict(data)
    payload.update({key: tuning[key] for key in SM.TUNING_OUTPUT_KEYS})
    store_dir = STORES / task_id
    if store_dir.exists():
        shutil.rmtree(store_dir)
    _, _, got, raw, raw_history = c03.write_and_read(
        SM, store_dir / "store.db", payload, line, cosmology, z_grid
    )
    walls["store"] = time.perf_counter() - t0
    assert got.available and not got.failure

    # ---- 4. export the object read back ----
    t0 = time.perf_counter()
    record, d = qe.export_history(
        EXPORT,
        units,
        got,
        got,
        line["beta"],
        line["M"],
        1,
        line["A"],
        SM.DRAIN_MODEL_ONE_LOOP,
        phi_init_Mp=hb.PHI_INIT_MP,
    )
    (RECORDS / f"{task_id}.json").write_text(json.dumps(record) + "\n")
    ec.write_histories_csv([record], PARTS / f"{task_id}.csv")
    walls["export"] = time.perf_counter() - t0

    # ---- 5. read the export back, and delete the store ----
    t0 = time.perf_counter()
    with open(d / "fields.csv") as f:
        n_fields = sum(1 for _ in f) - 1
    with open(d / "bounces.csv") as f:
        n_bounces = sum(1 for _ in f) - 1
    checks["fields_rows"] = n_fields
    checks["fields_rows_equal"] = n_fields == len(got.values)
    checks["bounces_rows"] = n_bounces
    checks["bounces_rows_equal"] = (
        n_bounces == len(got.bounces) == len(raw) == raw_history["bounce_rows"]
    )
    shutil.rmtree(store_dir)
    checks["store_deleted"] = not store_dir.exists()
    walls["readback_delete"] = time.perf_counter() - t0

    return {
        "M": line["M"],
        "beta": line["beta"],
        "A": line["A"],
        "group": line["group"],
        "directory": str(d.relative_to(EXPORT)),
        "walls": walls,
        "integration_wall_02d": wall_02d(line),
        "checks": checks,
    }


# ---------------------------------------------------------------------------------------------------------------------
# The scheduler
# ---------------------------------------------------------------------------------------------------------------------


def log(text: str):
    with open(VAR / "scheduler.log", "a") as f:
        f.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {text}\n")


def scheduler() -> int:
    for d in (RUN, RECORDS, PARTS, STORES, EXPORT):
        d.mkdir(parents=True, exist_ok=True)
    tasks = sorted(successes(), key=wall_02d, reverse=True)
    log(
        f"scheduler pid {os.getpid()}: {len(tasks)} tasks, {WORKERS} workers, load {os.getloadavg()}"
    )
    env = dict(os.environ, PYTHONPATH=".")
    queue = list(tasks)
    running = {}  # id -> (Popen, started, limit, log file)
    stopped = {}
    t_start = time.time()
    while queue or running:
        while queue and len(running) < WORKERS:
            line = queue.pop(0)
            tid = line["id"]
            for p in (RUN / f"{tid}.done", RUN / f"{tid}.result.json"):
                if p.exists():
                    p.unlink()
            fh = open(RUN / f"{tid}.log", "w")
            proc = subprocess.Popen(
                [str(PYTHON), str(Path(__file__).resolve()), "child", tid],
                cwd=ROOT,
                env=env,
                stdout=fh,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
            )
            (RUN / f"{tid}.pid").write_text(f"{proc.pid}\n")
            limit = LIMIT_FACTOR * wall_02d(line) + LIMIT_OFFSET
            running[tid] = (proc, time.time(), limit, fh)
            log(
                f"start {tid} pid {proc.pid} (02d wall {wall_02d(line):.1f} s, limit {limit:.0f} s)"
            )
        time.sleep(2.0)
        for tid in list(running):
            proc, started, limit, fh = running[tid]
            rc = proc.poll()
            if rc is None and time.time() - started > limit:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(30)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                stopped[tid] = f"stopped past its wall limit of {limit:.0f} s"
                log(f"stop {tid}: {stopped[tid]}")
                rc = proc.returncode
            if rc is not None:
                fh.close()
                del running[tid]
                log(
                    f"end {tid} rc {rc} after {time.time() - started:.1f} s; load {os.getloadavg()}"
                )
    summary = {
        "tasks": len(tasks),
        "workers": WORKERS,
        "wall": time.time() - t_start,
        "stopped": stopped,
    }
    failures = {}
    for line in tasks:
        tid = line["id"]
        rpath = RUN / f"{tid}.result.json"
        if tid in stopped:
            failures[tid] = stopped[tid]
        elif not rpath.exists():
            failures[tid] = "no result"
        else:
            r = json.loads(rpath.read_text())
            if "error" in r:
                failures[tid] = r["error"]
    summary["failures"] = failures
    try:
        summary["merge"] = merge()
    except BaseException as e:
        summary["merge"] = {
            "error": f"{type(e).__name__}: {e}",
            "traceback": traceback.format_exc(),
        }
    summary["load_end"] = os.getloadavg()
    (VAR / "scheduler.json").write_text(json.dumps(summary, indent=1) + "\n")
    log(
        f"done: {len(tasks) - len(failures)} of {len(tasks)} tasks succeeded; wall {summary['wall']:.0f} s"
    )
    (VAR / "scheduler.done").write_text("done\n")
    return 0


def merge() -> dict:
    import extract_common as ec

    records = []
    part_lines = []
    header = None
    for path in sorted(RECORDS.glob("*.json")):
        tid = path.stem
        records.append(json.loads(path.read_text()))
        with open(PARTS / f"{tid}.csv", newline="") as f:
            text = f.read()
        head, *data = text.splitlines(keepends=True)
        if header is None:
            header = head
        assert head == header, tid
        assert len(data) == 1, tid
        part_lines.append(data[0])
    ec.write_histories_csv(records, EXPORT / "histories.csv")
    with open(EXPORT / "histories.csv", newline="") as f:
        head, *merged = f.read().splitlines(keepends=True)
    return {
        "records": len(records),
        "merged_rows": len(merged),
        "header_equal": head == header,
        "rows_equal_as_multisets": sorted(merged) == sorted(part_lines),
    }


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "child":
        sys.exit(child(sys.argv[2]))
    if mode == "scheduler":
        sys.exit(scheduler())
    if mode == "merge":
        print(json.dumps(merge(), indent=1))
        sys.exit(0)
    raise SystemExit(f"unknown mode {mode!r}")
