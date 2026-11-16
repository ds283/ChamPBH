"""
One tree's exports of the store of build_store.py (power-law-wall prompt 06, §2.1; orchestrator note 06, ruling 8).

Run from the root of the tree whose code is wanted (this worktree, or the base worktree of the dispatch commit), with
that root on PYTHONPATH and this worktree's interpreter:

    cd TREE && PYTHONPATH=. /path/to/venv/bin/python /path/to/export_tree.py \\
        --store /abs/var/power-law-wall/06/store --manifest /abs/var/power-law-wall/06/manifest.json --out OUT

Nothing here is a copy of a tree's code. The plot scripts parse their arguments on import, so they cannot be imported:
their own top-level imports, and their functions `build_beta_plot` and `write_alpha_summaries` (plot_by_beta.py),
`plot_ScalarHistory` and `write_csv_file` (plot_ScalarHistory.py), are *extracted by ast* from the tree's files and
executed unedited, except that `@ray.remote` is not applied (the undecorated function, as a test calls `fn._function`).
They are run with `args.plots` False, the plot scripts' `--no-plots`, so they write the CSV files alone: each (M, n)
group's data.csv, each history's fields.csv and (one-loop law) bounces.csv, and histories.csv, at the paths the tree's
own code gives them. What the drivers do around them (the lookups of each group's histories, BBN rows and failures) is
done here as plot_by_beta.py's run_pipeline does it, on the store's own rows:
- each potential is exported in a pass of its own, as a plot run `--models <potential> --n-values <its n> --no-plots`
  would (prompt 06 §1.5): the exponential at n = 1, the power law at n = 1 and 2, each over the stored (n, beta) of
  its own model rows; the alpha rows are the fixed row alpha = 0.1, then A = 1 under the potential's own one-loop drain
  model (`one-loop-measured-v1` for the exponential, `one-loop-measured-pl-v1` for the power law);
- a group's histories are looked up with failure False and `_do_not_populate`; a model without a successful history is
  looked up again with failure True, and its reason is the stored failure_reason, or "no ScalarHistory row in the
  store", as report_dropped_scalar_histories gives it; each successful history's BBNData row is looked up through its
  proxy, as build_plot_work does through a ScalarHistoryProxy. The proxy here is a stand-in that holds the history
  (`store_id`, `shard_key`, `available`, `cosmology`, `units`, `get`): ScalarHistoryProxy's `ray.put` would start Ray.
  `ray.init` and `ray.put` are patched to raise for the whole export, and Ray is checked not initialised at its end.

The tree decides what it exports. A tree whose extract_common has `export_root` (this prompt's) exports every potential
of the store at `<label>/<model>/`; a tree without it (the base) exports the exponential alone, at `<label>/`, since its
plot scripts look up `ExponentialModel` only. The only tree-aware steps are these and the call of
`write_alpha_summaries`, which takes the potential in the tip.

The store is opened read-only (the package's actor with read_only=True, as the plot scripts' read-only pool opens each
shard) in the stand-in cluster, never Ray. The SHA-256 of every file of the store directory is taken before and after
the export and written to OUT/sha256.json; a change is an error.

Writes, under OUT: `<label>/...` as the tree's code places it, sha256.json, and export.txt (what was exported, and the
drivers' printed lines that the extracted functions print).
"""

import argparse
import ast
import contextlib
import hashlib
import inspect
import io
import json
import sqlite3
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace

from unittest import mock

import ray

import extract_common as ec
from ComputeTargets.tests import dissipation_histories as dh
from StoreFixtures import standin_pool as sp
from Units import Planck_units
from config.datastore import factories, serial_batch_sizes
from config.sharding import replicated_tables

LABEL_A = "2026.3.0"  # the version label the harness gives its stores (test_version_keyed_lookups.LABEL_A)
OUT_LABEL = "QCD_Cosmology"  # the cosmology's label, config/model_list.py
FIXED = "fixed"
MARKER_REASON = "no ScalarHistory row in the store"
TABLES = {"exponential": "ExponentialModel", "inverse-power": "InversePowerModel"}


def digests(directory: Path) -> dict:
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.iterdir())
        if p.is_file()
    }


def script_namespace(path: Path, names, extra: dict) -> dict:
    """
    The script's top-level imports, executed, and the functions `names`, extracted by ast and executed unedited in
    that namespace without their decorators (the undecorated function). `extra` gives the module globals the
    functions read (args, units).
    """
    tree = ast.parse(path.read_text(), str(path))
    imports = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
    functions = [
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names
    ]
    assert sorted(f.name for f in functions) == sorted(names), (path, names)
    for f in functions:
        f.decorator_list = []
    namespace = {"__name__": f"export_tree:{path.name}"}
    exec(
        compile(ast.Module(body=imports, type_ignores=[]), str(path), "exec"), namespace
    )
    namespace.update(extra)
    exec(
        compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"),
        namespace,
    )
    return namespace


def proxy(h):
    """A stand-in ScalarHistoryProxy of a read history, holding it (no ray.put)."""
    return SimpleNamespace(
        store_id=h.store_id,
        shard_key=h.shard_key,
        available=True,
        cosmology=h.cosmology,
        units=h.cosmology.units,
        get=lambda: h,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    root = Path.cwd()
    store_dir = Path(args.store).resolve()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(Path(args.manifest).read_text())
    units = Planck_units()

    before = digests(store_dir)

    tip = hasattr(ec, "export_root")
    potentials = ["exponential", "inverse-power"] if tip else ["exponential"]

    plot_args = SimpleNamespace(output=str(out), plots=False, phi_init_Mp=5.0)
    by_beta = script_namespace(
        root / "plot_by_beta.py",
        ("build_beta_plot", "write_alpha_summaries"),
        dict(args=plot_args, units=units),
    )
    by_history = script_namespace(
        root / "plot_ScalarHistory.py",
        ("plot_ScalarHistory", "write_csv_file"),
        dict(args=plot_args),
    )
    build_beta_plot = by_beta["build_beta_plot"]
    write_alpha_summaries = by_beta["write_alpha_summaries"]
    summaries_take_potential = (
        "potential" in inspect.signature(write_alpha_summaries).parameters
    )
    plot_ScalarHistory = by_history["plot_ScalarHistory"]
    unavailable = SimpleNamespace(available=False, failure=False)

    def no_ray(*a, **k):
        raise RuntimeError("export_tree: nothing here may start Ray")

    cluster = sp.StandinCluster()
    log = []
    printed = io.StringIO()
    with cluster.active(), contextlib.redirect_stdout(printed), mock.patch.object(
        ray, "init", no_ray
    ), mock.patch.object(ray, "put", no_ray):
        shard = store_dir / "store-shard0000.db"
        with sqlite3.connect(f"file:{shard}?mode=ro", uri=True) as con:
            version_serial = con.execute(
                "select serial from version where label = ?", (LABEL_A,)
            ).fetchone()[0]
        broker = sp.Handle(
            sp.BrokerClass(name="SerialPoolBroker"), "SerialPoolBroker", cluster, None
        )
        pool = sp.DatastoreClass(
            version_label=LABEL_A,
            db_name=str(shard),
            replicated_tables=list(replicated_tables),
            factories=factories,
            serial_batch_sizes=serial_batch_sizes,
            my_name="export-readonly-store",
            serial_broker=broker,
            read_only=True,
        )
        pool.set_lookup_version(version_serial)
        get = pool.object_get
        try:
            common = dict(
                cosmology=dh.cosmology(),
                atol=get("tolerance", tol=1e-8),
                rtol=get("tolerance", tol=1e-8),
                tuning_tol=get("tolerance", tol=1e-5),
                T_Jordan_init=get(
                    "temperature", value=dh.T_INIT_GEV * units.GeV, units=units
                ),
                phi_Einstein_init=get(
                    "phi_value",
                    value=dh.PHI_INIT_OVER_MP * units.PlanckMass,
                    units=units,
                ),
                pi_Einstein_init=get("pi_value", value=dh.PI_INIT, units=units),
            )

            for potential in potentials:
                table = TABLES[potential]
                specs = [s for s in manifest if s["model"] == potential]
                # the potential's model rows, M outer, n, beta inner, as plot_inputs returns them
                combos = sorted({(s["M_Mp"], s["n"], s["beta"]) for s in specs})
                models = []
                for M_Mp, n, beta in combos:
                    M = get("M_value", value=M_Mp * units.PlanckMass, units=units)
                    b = get("beta_value", value=beta)
                    models.append(
                        get(table, shard_key=b, M=M, n=n, beta=b, units=units)
                    )
                # the alpha rows: the fixed rows, then the A rows under the potential's own drain model
                rows = list(
                    OrderedDict.fromkeys(
                        (s["drain_model"], s["value"])
                        for s in sorted(specs, key=lambda s: s["drain_model"] != FIXED)
                    )
                )
                records = []
                for drain_model, value in rows:
                    kwargs = (
                        {} if drain_model == FIXED else dict(drain_model=drain_model)
                    )
                    alpha = get("alpha_value", value=value, **kwargs)
                    groups = OrderedDict()
                    for m in models:
                        groups.setdefault((m._M.store_id, m._n), []).append(m)
                    for group in groups.values():
                        scalar_data, bbn_data, scalar_failures = [], [], {}
                        for m in group:
                            query = dict(
                                common,
                                shard_key=m.shard_key,
                                solver_labels=["Radau+kinematic-cap-stepping1"],
                                failure=False,
                                z_grid=None,
                                model=m,
                                alpha=alpha,
                                tags=[],
                                _do_not_populate=True,
                            )
                            h = get("ScalarHistory", **query)
                            if h.available and not h.failure:
                                scalar_data.append(h)
                                d = get(
                                    "BBNData",
                                    shard_key=m.shard_key,
                                    model_proxy=proxy(h),
                                    failure=False,
                                    tags=[],
                                    _do_not_populate=True,
                                )
                                assert d.available and not d.failure, (
                                    potential,
                                    m.name,
                                )
                                bbn_data.append(d)
                                # the history's fields.csv and bounces.csv, populated
                                full = get(
                                    "ScalarHistory",
                                    **{
                                        k: v
                                        for k, v in query.items()
                                        if k != "_do_not_populate"
                                    },
                                )
                                assert full.available and not full.failure
                                plot_ScalarHistory(
                                    OUT_LABEL, full, unavailable, unavailable
                                )
                            else:
                                F = get("ScalarHistory", **dict(query, failure=True))
                                if F.available:
                                    reason = F.failure_reason
                                    if reason is None:
                                        reason = "failed, no failure_reason stored"
                                else:
                                    reason = MARKER_REASON
                                scalar_failures[m._beta.as_float] = reason
                        records.extend(
                            build_beta_plot(
                                OUT_LABEL,
                                group[0],
                                alpha.value,
                                alpha.drain_model,
                                [],
                                bbn_data,
                                scalar_data,
                                None,
                                scalar_failures,
                                {},
                            )
                        )
                        log.append(
                            f"{potential} {drain_model} {value:g} M={group[0]._M.as_float:g} n={group[0]._n}: "
                            f"{len(scalar_data)} histories, failures {sorted(scalar_failures.items())}"
                        )
                if summaries_take_potential:
                    write_alpha_summaries(OUT_LABEL, potential, records)
                else:
                    write_alpha_summaries(OUT_LABEL, records)
        finally:
            pool.__exit__(None, None, None)

    assert not ray.is_initialized(), "Ray was initialised"
    after = digests(store_dir)
    (out / "sha256.json").write_text(
        json.dumps(
            {"before": before, "after": after, "equal": before == after}, indent=1
        )
        + "\n"
    )
    files = sorted(
        str(p.relative_to(out))
        for p in out.rglob("*")
        if p.is_file() and p.parent != out
    )
    (out / "export.txt").write_text(
        f"tree {root}; potentials {potentials}; tip {tip}\n"
        + "\n".join(log)
        + "\n-- printed by the extracted functions --\n"
        + printed.getvalue()
        + "-- files --\n"
        + "\n".join(files)
        + "\n"
    )
    print(
        f"@@ export_tree: {root}: potentials {potentials}; {len(files)} files; "
        f"store unchanged: {before == after}"
    )
    assert before == after, "a store file moved under the export"


if __name__ == "__main__":
    main()
