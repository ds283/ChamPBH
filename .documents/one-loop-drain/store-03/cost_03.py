"""
one-loop-drain prompt 03, Store gate §2.4 (P12; orchestrator note 03, ruling 10): the cost and the volume of the
store's instrumentation, on six of 02d's accepted integrations, each run once, untuned.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/store-03/cost_03.py child TASK_ID
    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/store-03/cost_03.py report

`child` (one process per task, launched detached by launch_03.sh) reads the task's line of
var/one-loop-drain/02d/sample.jsonl and the last line of var/one-loop-drain/02d/tasks/<id>.partial.jsonl (the
accepted integration), and:
  1. realizes the model from the line's tuning (normalization, phi_ref) as tune_history realizes an integration
     (model.realize(norm, phi_ref), the ExponentialModel the tool builds), and integrates it with
     integrate_history under the one-loop law at the line's A, with tune_history's T_stop (T_CMB), the tool's z
     grid (main.py's density and depth; store ids k + 1) and the default tolerances, compute_sample_masses=False,
     as tune_history makes each integration. bounce_record is wrapped by a timer, so the integration's wall is
     integrate_history's less the bounce key's;
  2. times sample_masses on the payload (the accepted integration's realization) and drain_history_values;
  3. writes the history and reads it back on a temporary store under var/one-loop-drain/03/stores/<id>/ (the
     package's undecorated actor in ChamPBH's stand-in cluster, StoreFixtures.standin_pool, as the tests'
     _TempStoreCase; never Ray, never registered): once in full ("full"), and once with the bounce key None and
     every mass None ("base", what a store wrote before this prompt but for two NULL columns), each on a fresh file,
     so that the added write and read is full - base;
  4. checks: every DrainCandidate equal by repr to the partial line's (regime, alpha_eff, ln R and the rest); ln R
     of every assessed candidate equal to its PeakEstimate's bitwise, and the regime and the estimate in
     correspondence; the stored shares, counts and ell_min equal to 02d's drain_values by repr; the rows read back
     equal to the payload's through the conversions; no nonzero value converting to 0.0 or +-inf (ruling 9);
  5. writes var/one-loop-drain/03/<id>.result.json and the end marker var/one-loop-drain/03/<id>.done, with the 1-,
     5- and 15-minute loads at the start and the end.

added = bounce key + sample masses + history record + max(0, (full write + full read) - (base write + base read));
ratio = added / integration. `report` prints the table of every result and the median ratio (the stop: > 0.25).
"""

import contextlib
import io
import json
import os
import statistics
import sys
import time
import traceback
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
VAR = ROOT / "var" / "one-loop-drain" / "03"
SAMPLE = ROOT / "var" / "one-loop-drain" / "02d" / "sample.jsonl"
PARTIALS = ROOT / "var" / "one-loop-drain" / "02d" / "tasks"

TASKS = (
    "P-2197-A1",
    "W-2187-A0.05",
    "L-M8e-18-A0.5",
    "L-M5e-16-A1",
    "C-3016-A1",
    "C-3233-A1",
)


def line_of(task_id: str) -> dict:
    for text in SAMPLE.read_text().splitlines():
        line = json.loads(text)
        if line["id"] == task_id:
            return line
    raise KeyError(task_id)


def last_partial(task_id: str) -> dict:
    return json.loads(
        (PARTIALS / f"{task_id}.partial.jsonl").read_text().splitlines()[-1]
    )


class Store:
    """A one-shard store at path, opened as the tests' _TempStoreCase opens one."""

    def __init__(self, path: Path, label: str):
        from StoreFixtures import standin_pool as sp
        from config.datastore import factories, serial_batch_sizes
        from config.sharding import replicated_tables

        path.parent.mkdir(parents=True, exist_ok=True)
        self.cluster = sp.StandinCluster()
        self._active = self.cluster.active()
        self._active.__enter__()
        broker = sp.Handle(
            sp.BrokerClass(name="SerialPoolBroker"),
            "SerialPoolBroker",
            self.cluster,
            None,
        )
        pool = self.cluster.open_pool(
            path, shards=1, version_label=label, serial_batch_sizes=serial_batch_sizes
        )
        serial = pool._version.store_id
        shard_file = pool._shard_db_files[0]
        self.cluster.close_pool(pool)
        with contextlib.redirect_stdout(io.StringIO()):
            self.store = sp.DatastoreClass(
                version_label=label,
                db_name=shard_file,
                replicated_tables=list(replicated_tables),
                factories=factories,
                serial_batch_sizes=serial_batch_sizes,
                my_name="store03-store",
                serial_broker=broker,
            )
        broker.notify_largest_store_ids.remote(
            self.store.read_largest_store_ids()
        ).get()
        self.store.set_version(serial)

    def close(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.store.__exit__(None, None, None)
        self._active.__exit__(None, None, None)


def write_and_read(SM, path, payload, line, cosmology, z_grid):
    """Write the history (store() and the factory's store) and read it back; returns (write s, read s, the
    object read, the store's raw bounce rows, the history's serial)."""
    import sqlalchemy as sqla
    from config.version import VERSION_LABEL

    holder = Store(path, VERSION_LABEL)
    store = holder.store
    units = cosmology.units
    get = store.object_get
    solver = get("IntegrationSolver", label=SM.SCALAR_HISTORY_STEPPER_LABEL, stepping=1)
    solvers = {SM.SCALAR_HISTORY_STEPPER_LABEL: solver}
    M = get("M_value", value=line["M"] * units.PlanckMass, units=units)
    beta = get("beta_value", value=line["beta"])
    key = dict(
        solver_labels=solvers,
        cosmology=cosmology,
        T_Jordan_init=get("temperature", value=2.0e4 * units.GeV, units=units),
        phi_Einstein_init=get("phi_value", value=5.0 * units.PlanckMass, units=units),
        pi_Einstein_init=get("pi_value", value=0.0, units=units),
        model=get("ExponentialModel", M=M, n=1, beta=beta, units=units),
        tuning_tol=get("tolerance", tol=1e-5),
        alpha=get("alpha_value", value=line["A"], drain_model=SM.DRAIN_MODEL_ONE_LOOP),
        atol=get("tolerance", tol=SM.DEFAULT_ABS_TOLERANCE),
        rtol=get("tolerance", tol=SM.DEFAULT_REL_TOLERANCE),
    )
    with store._engine.begin() as conn:
        for z in z_grid:
            store._schema["redshift"]["insert"](
                conn, dict(serial=z.store_id, log_one_plus_z=z.log_one_plus_z)
            )

    t0 = time.perf_counter()
    history = SM.ScalarHistory(payload=None, label=line["id"], tags=[], **key)
    history._compute_ref = object()
    with mock.patch.object(
        SM.ray, "wait", lambda refs, timeout: (refs, [])
    ), mock.patch.object(SM.ray, "get", lambda ref: payload):
        assert history.store() is True
    table = store._tables["ScalarHistory"]
    with store._engine.begin() as conn:
        store._factories["ScalarHistory"].store(
            history,
            conn,
            table,
            lambda c, row: store._inserters["ScalarHistory"](c, dict(row, serial=1)),
            store._tables,
            store._inserters,
        )
        conn.execute(
            sqla.update(table).where(table.c.serial == 1).values(validated=True)
        )
    t_write = time.perf_counter() - t0

    t0 = time.perf_counter()
    got = store.object_get("ScalarHistory", z_grid=None, **key)
    t_read = time.perf_counter() - t0
    assert got.available and got.store_id == 1

    bounce_table = store._tables["ScalarHistoryBounce"]
    with store._engine.begin() as conn:
        raw = [
            r._asdict()
            for r in conn.execute(
                sqla.select(bounce_table)
                .where(bounce_table.c.history_serial == 1)
                .order_by(bounce_table.c.ordinal)
            ).all()
        ]
        raw_history = (
            conn.execute(sqla.select(table).where(table.c.serial == 1)).one()._asdict()
        )
    holder.close()
    return t_write, t_read, got, raw, raw_history


def child(task_id: str) -> int:
    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT))
    out = {"id": task_id, "load_start": os.getloadavg(), "started": time.time()}
    try:
        out.update(run(task_id))
    except BaseException as e:
        out["error"] = f"{type(e).__name__}: {e}"
        out["traceback"] = traceback.format_exc()
    out["load_end"] = os.getloadavg()
    out["ended"] = time.time()
    (VAR / f"{task_id}.result.json").write_text(json.dumps(out, indent=1) + "\n")
    (VAR / f"{task_id}.done").write_text("done\n")
    return 0


def run(task_id: str) -> dict:
    import importlib

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
    from ObjectFactories.ScalarHistory import BOUNCE_COLUMNS, from_column, to_column
    from Units import Planck_units

    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    hb = importlib.import_module("tools.history_and_bbn")

    line = line_of(task_id)
    partial = last_partial(task_id)
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

    timed = {"bounce": 0.0}
    captured = []
    real_record, real_isc = SM.bounce_record, SM.integrate_scalar_history

    def timed_record(*a, **kw):
        t0 = time.perf_counter()
        r = real_record(*a, **kw)
        timed["bounce"] += time.perf_counter() - t0
        return r

    def capturing(*a, **kw):
        r = real_isc(*a, **kw)
        captured.append(r)
        return r

    t0 = time.perf_counter()
    with mock.patch.object(SM, "bounce_record", timed_record), mock.patch.object(
        SM, "integrate_scalar_history", capturing
    ), contextlib.redirect_stdout(io.StringIO()):
        data = SM.integrate_history(
            cosmology,
            temperature(0, hb.T_INIT_GEV * units.GeV),
            temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
            phi_value(0, hb.PHI_INIT_MP * units.PlanckMass),
            pi_value(0, hb.PI_INIT),
            z_grid,
            rm,
            task_label=f"store-03-{task_id}",
            alpha=line["A"],
            drain_law=SM.DRAIN_LAW_ONE_LOOP,
            compute_sample_masses=False,
        )
    t_total = time.perf_counter() - t0
    if data.get("failure", False):
        raise RuntimeError(f"the integration failed: {data['failure_reason']}")
    result = captured[-1]
    t_bounce = timed["bounce"]
    t_integration = t_total - t_bounce

    t0 = time.perf_counter()
    eff, tracker = SM.sample_masses(rm, cosmology, data["sample"])
    t_masses = time.perf_counter() - t0

    payload = dict(data)
    payload.update({key: tuning[key] for key in SM.TUNING_OUTPUT_KEYS})
    payload["log_m_eff"], payload["log_m_tracker"] = eff, tracker

    t0 = time.perf_counter()
    record = SM.drain_history_values(payload)
    t_record = time.perf_counter() - t0

    base_payload = dict(payload)
    base_payload["bounces"] = None
    base_payload["log_m_eff"] = (None,) * len(eff)
    base_payload["log_m_tracker"] = (None,) * len(tracker)
    stores = VAR / "stores" / task_id
    w_base, r_base, _, raw_base, _ = write_and_read(
        SM, stores / "base" / "store.db", base_payload, line, cosmology, z_grid
    )
    w_full, r_full, got, raw, raw_history = write_and_read(
        SM, stores / "full" / "store.db", payload, line, cosmology, z_grid
    )
    assert raw_base == []

    # ---- the checks ----
    candidates = data["drain"].candidates
    rows = data["bounces"].rows
    checks = {}
    checks["candidates_equal_02d"] = repr([list(c) for c in candidates]) == repr(
        partial["candidates"]
    )
    checks["n_candidates"] = len(candidates)
    checks["n_candidates_02d"] = len(partial["candidates"])
    estimate_of = {
        "one_loop": SM.ESTIMATE_ONE_LOOP,
        "gap": SM.ESTIMATE_CLASSICAL_GAP,
        "classical": SM.ESTIMATE_CLASSICAL,
    }
    log_R_differs, estimate_differs = 0, 0
    for c, e in zip(candidates, result.peak_estimates):
        if c.regime == "unassessed":
            estimate_differs += e is not None
            continue
        log_R_differs += c.log_R.hex() != e.log_R.hex()
        estimate_differs += estimate_of[c.regime] != e.estimate
    checks["log_R_differs"] = log_R_differs
    checks["estimate_differs"] = estimate_differs

    dv = line["accepted"]["drain_values"]
    share_differs = []
    for regime in SM.DRAIN_REGIMES:
        if repr(getattr(record, f"drained_share_{regime}")) != repr(
            dv[f"K_share_{regime}"]
        ):
            share_differs.append(regime)
        if getattr(record, f"candidates_{regime}") != dv[regime]:
            share_differs.append(f"candidates_{regime}")
    if repr(record.parking_share) != repr(dv["K_share_parking"]):
        share_differs.append("parking")
    if repr(record.ell_min) != repr(dv["ell_min"]):
        share_differs.append("ell_min")
    if record.unresolvable_assessments != dv["unresolvable_assessments"]:
        share_differs.append("unresolvable_assessments")
    if record.unresolved_declined != dv["unresolved_declined"]:
        share_differs.append("unresolved_declined")
    checks["drain_values_differ"] = share_differs
    shares = [getattr(record, f"drained_share_{r}") for r in SM.DRAIN_REGIMES] + [
        record.parking_share
    ]
    checks["shares_sum_minus_1"] = None if None in shares else sum(shares) - 1.0

    # the rows read back, through the conversions, and ruling 9
    readback_differs, conversion_stops = 0, []
    for row, back, stored in zip(rows, got.bounces, raw):
        for field, column, kind in BOUNCE_COLUMNS:
            value = getattr(row, field)
            if isinstance(value, float):
                if value != 0.0 and (
                    stored[column] == 0.0 or abs(stored[column]) == float("inf")
                ):
                    conversion_stops.append((row.ordinal, column))
                if getattr(back, field) != from_column(stored[column], kind, units):
                    readback_differs += 1
                if stored[column] != to_column(value, kind, units):
                    readback_differs += 1
    checks["rows_stored"] = len(raw)
    checks["rows_read"] = len(got.bounces)
    checks["readback_differs"] = readback_differs
    checks["conversion_stops"] = conversion_stops
    checks["bounce_rows_column"] = raw_history["bounce_rows"]

    sources = {}
    for r in rows:
        sources[r.source] = sources.get(r.source, 0) + 1
    log_T_from = {"turning_point": 0, "dense": 0, "event": 0}
    for r in rows:
        if r.source != "candidate":
            log_T_from["event"] += 1
        elif r.kind == "turning_point":
            log_T_from["turning_point"] += 1
        else:
            log_T_from["dense"] += 1

    added = (
        t_bounce + t_masses + t_record + max(0.0, (w_full + r_full) - (w_base + r_base))
    )
    return {
        "M": line["M"],
        "beta": line["beta"],
        "A": line["A"],
        "regimes": {r: getattr(record, f"candidates_{r}") for r in SM.DRAIN_REGIMES},
        "n_samples": len(data["sample"]),
        "rows": len(rows),
        "sources": sources,
        "log_T_from": log_T_from,
        "margins_failed": data["margins"] is None,
        "mass_nulls": {
            "log_m_eff": sum(x is None for x in eff),
            "log_m_tracker": sum(x is None for x in tracker),
        },
        "t_integration": t_integration,
        "t_bounce": t_bounce,
        "t_masses": t_masses,
        "t_record": t_record,
        "t_write_full": w_full,
        "t_read_full": r_full,
        "t_write_base": w_base,
        "t_read_base": r_base,
        "t_added": added,
        "ratio": added / t_integration,
        "ratio_whole_write_read": (t_bounce + t_masses + t_record + w_full + r_full)
        / t_integration,
        "integration_wall_02d": line["tuning_wall"]
        / line["tuning"]["tuning_iterations"],
        "checks": checks,
    }


def report() -> int:
    results = [json.loads((VAR / f"{t}.result.json").read_text()) for t in TASKS]
    print(
        f"{'task':16s} {'cand':>6s} {'rows':>6s} {'02d':>6s} {'integ s':>8s} {'bounce':>7s} {'masses':>7s} "
        f"{'record':>7s} {'w full':>7s} {'r full':>7s} {'w base':>7s} {'r base':>7s} {'added':>7s} {'ratio':>6s} "
        f"{'whole':>6s}  nulls(eff,tr)  load start -> end"
    )
    ratios = []
    ok = True
    for r in results:
        if "error" in r:
            print(f"{r['id']:16s} ERROR {r['error']}")
            ok = False
            continue
        c = r["checks"]
        ratios.append(r["ratio"])
        print(
            f"{r['id']:16s} {c['n_candidates']:6d} {r['rows']:6d} {c['n_candidates_02d']:6d} "
            f"{r['t_integration']:8.2f} {r['t_bounce']:7.3f} {r['t_masses']:7.3f} {r['t_record']:7.4f} "
            f"{r['t_write_full']:7.3f} {r['t_read_full']:7.3f} {r['t_write_base']:7.3f} {r['t_read_base']:7.3f} "
            f"{r['t_added']:7.3f} {r['ratio']:6.3f} {r['ratio_whole_write_read']:6.3f}  "
            f"({r['mass_nulls']['log_m_eff']}, {r['mass_nulls']['log_m_tracker']})  "
            f"{tuple(round(x, 2) for x in r['load_start'])} -> {tuple(round(x, 2) for x in r['load_end'])}"
        )
    print()
    for r in results:
        if "error" in r:
            continue
        c = r["checks"]
        good = (
            c["candidates_equal_02d"]
            and c["log_R_differs"] == 0
            and c["estimate_differs"] == 0
            and c["drain_values_differ"] == []
            and c["readback_differs"] == 0
            and c["conversion_stops"] == []
            and c["rows_stored"]
            == c["rows_read"]
            == r["rows"]
            == c["bounce_rows_column"]
        )
        ok &= good
        print(
            f"{r['id']:16s} candidates=02d:{c['candidates_equal_02d']} log_R differs:{c['log_R_differs']} "
            f"estimate differs:{c['estimate_differs']} drain_values differ:{c['drain_values_differ']} "
            f"shares sum-1:{c['shares_sum_minus_1']!r} readback differs:{c['readback_differs']} "
            f"conversion stops:{len(c['conversion_stops'])} rows stored/read/column:"
            f"{c['rows_stored']}/{c['rows_read']}/{c['bounce_rows_column']} sources:{r['sources']} "
            f"ln T_J from:{r['log_T_from']} margins failed:{r['margins_failed']} regimes:{r['regimes']} "
            f"samples:{r['n_samples']}"
        )
    print()
    if len(ratios) > 0:
        median = statistics.median(ratios)
        print(
            f"median ratio added/integration: {median:.4f} (stop above 0.25); max {max(ratios):.4f}"
        )
        ok &= median <= 0.25
    print("RESULT:", "holds" if ok else "FAILS")
    return 0 if ok else 1


if __name__ == "__main__":
    if sys.argv[1] == "child":
        sys.exit(child(sys.argv[2]))
    if sys.argv[1] == "report":
        sys.exit(report())
    raise SystemExit(f"unknown mode {sys.argv[1]!r}")
