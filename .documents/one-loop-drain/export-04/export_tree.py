"""
One tree's exports of the store of build_store.py (one-loop-drain prompt 04, §2.2; orchestrator note 04, ruling 6).

Run from the root of the tree whose code is wanted (this checkout, or the base worktree), with that root on
PYTHONPATH and this checkout's interpreter:

    cd TREE && PYTHONPATH=. /path/to/venv/bin/python /path/to/export_tree.py \\
        --store /path/to/var/one-loop-drain/04/store/store.db --manifest /path/to/manifest.json --out OUT

Nothing here is a copy of a tree's code: it calls the tree's own `extract_common` functions, and the tree's own
`write_csv_file` of `plot_ScalarHistory.py` is *extracted by `ast`* from the file and executed unedited (that script
parses its arguments on import, so it cannot be imported). The only tree-aware step is the call of
`build_history_record` and of the path and label helpers, which take the drain model in the tree of this prompt and
not in the base: a keyword (or argument) is added only if the tree's signature has it.

The store is opened read-only (`ShardedPool(read_only=True)`, as the plot scripts do) in the stand-in cluster, never
Ray. The SHA-256 of every file of the store directory is taken before and after the export and written to
OUT/sha256.json.

Writes, under OUT:
  histories_fixed.csv, histories_one_loop.csv   write_histories_csv over the records of each model alone
  fields/<name>/fields.csv                       the tree's write_csv_file, per successful history
  bounces/<name>/bounces.csv                     the tree's write_bounces_csv, per one-loop history (tip only)
  labels.json                                    each product's path and title, as the tree's code gives them
  sha256.json                                    the store's files before and after
"""

import argparse
import ast
import contextlib
import hashlib
import inspect
import io
import json
import sqlite3
import sys
from math import exp, log
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import ray
from matplotlib.figure import Figure
from numpy import nan

import extract_common as ec
from ComputeTargets import AdiabaticHistory, BBNData, ScalarHistory
from ComputeTargets.tests import dissipation_histories as dh
from StoreFixtures import standin_pool as sp
from Units import Planck_units
from Units.base import UnitsLike
from config.datastore import factories, serial_batch_sizes
from config.sharding import replicated_tables

LABEL_A = "2026.3.0"  # the version label ObjectFactories/tests/test_version_keyed_lookups.py gives its stores
FIXED = "fixed"
OUT_LABEL = "label"


def digests(directory: Path) -> dict:
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(directory.iterdir())
        if p.is_file()
    }


def takes(function, name: str) -> bool:
    return name in inspect.signature(function).parameters


def extract_function(path: Path, name: str, namespace: dict):
    """The function `name` of the file at `path`, extracted by ast and executed in `namespace`, unedited."""
    tree = ast.parse(path.read_text(), str(path))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(nodes) == 1, (path, name)
    module = ast.Module(body=nodes, type_ignores=[])
    exec(compile(module, str(path), "exec"), namespace)
    return namespace[name]


def lookups(pool, units):
    """The parameter rows the plot scripts look up, with the values of the harness that wrote the store."""
    get = lambda *a, **k: pool.object_get(*a, **k)

    return SimpleNamespace(
        get=get,
        # the cosmology the harness wrote (the golden tests' make_cosmology), as the harness passes it to its lookups
        cosmology=dh.cosmology(),
        atol=get("tolerance", tol=1e-8),
        rtol=get("tolerance", tol=1e-8),
        tuning_tol=get("tolerance", tol=1e-5),
        T_init=get("temperature", value=dh.T_INIT_GEV * units.GeV, units=units),
        phi=get("phi_value", value=dh.PHI_INIT_OVER_MP * units.PlanckMass, units=units),
        pi=get("pi_value", value=dh.PI_INIT, units=units),
    )


def read_history(pool, units, p, spec, populate: bool):
    alpha_kwargs = (
        {} if spec["drain_model"] == FIXED else {"drain_model": spec["drain_model"]}
    )
    alpha = p.get("alpha_value", value=spec["value"], **alpha_kwargs)
    M = p.get("M_value", value=spec["M_Mp"] * units.PlanckMass, units=units)
    beta = p.get("beta_value", value=spec["beta"])
    model = p.get(
        "ExponentialModel", shard_key=beta, M=M, n=dh.N_EXPONENT, beta=beta, units=units
    )
    payload = {
        "shard_key": model.shard_key,
        "solver_labels": ["Radau+kinematic-cap-stepping1"],
        "failure": spec["failure"],
        "cosmology": p.cosmology,
        "T_Jordan_init": p.T_init,
        "phi_Einstein_init": p.phi,
        "pi_Einstein_init": p.pi,
        "z_grid": None,
        "model": model,
        "tuning_tol": p.tuning_tol,
        "alpha": alpha,
        "atol": p.atol,
        "rtol": p.rtol,
        "tags": [],
    }
    if not populate:
        payload["_do_not_populate"] = True
    return p.get("ScalarHistory", **payload), alpha


def driver_strings(tree_root: Path, alpha_value, drain_model: str, alpha_label):
    """
    The f-strings of the drivers that name the alpha row, evaluated by ast on stand-in names: the printed header
    line of each run_pipeline, and the titles of plot_by_beta.py (found by their text), so that the base and the
    tip are compared on what their own source says.
    """
    out = {}
    row = SimpleNamespace(value=alpha_value, drain_model=drain_model)
    group_model = SimpleNamespace(_M=SimpleNamespace(as_float=0.5), _n=1)
    units = Planck_units()
    for script in ("main.py", "plot_by_beta.py", "plot_ScalarHistory.py"):
        tree = ast.parse((tree_root / script).read_text(), script)
        functions = {
            n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
        }
        for fname, function in functions.items():
            k = 0
            nodes = sorted(
                (n for n in ast.walk(function) if isinstance(n, ast.JoinedStr)),
                key=lambda n: (n.lineno, n.col_offset),
            )
            for node in nodes:
                text = ast.unparse(node)
                if not any(
                    s in text
                    for s in ("RUNNING PIPELINE", "α = ", "alpha=", "alpha_label")
                ):
                    continue
                names = dict(
                    model_label=OUT_LABEL,
                    group_model=group_model,
                    units=units,
                    args=SimpleNamespace(phi_init_Mp=5.0),
                    alpha_label=alpha_label,
                )
                if fname == "run_pipeline":
                    names["alpha"] = row
                else:
                    names["alpha"] = alpha_value
                    names["drain_model"] = drain_model
                try:
                    value = eval(
                        compile(ast.Expression(body=node), script, "eval"), names
                    )
                except (
                    Exception
                ) as e:  # a string of another name than the stand-ins hold
                    value = f"<not evaluated: {type(e).__name__}: {e}>"
                k += 1
                out[f"{script}:{fname}:#{k}"] = value
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    root = Path.cwd()
    primary = Path(args.store).resolve()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(Path(args.manifest).read_text())
    units = Planck_units()

    before = digests(primary.parent)

    # the tree's own functions
    build_record = ec.build_history_record
    tree_has_model = takes(build_record, "drain_model")
    alpha_path = ec.alpha_path
    path_has_model = len(inspect.signature(alpha_path).parameters) == 3
    drain_model_module = None
    try:
        from CosmologyConcepts import drain_model as drain_model_module
    except ImportError:
        pass
    alpha_label = getattr(drain_model_module, "alpha_label", None)

    namespace = {
        "pd": pd,
        "log": log,
        "exp": exp,
        "nan": nan,
        "Path": Path,
        "BBNData": BBNData,
        "AdiabaticHistory": AdiabaticHistory,
        "ScalarHistory": ScalarHistory,
        "UnitsLike": UnitsLike,
    }
    if hasattr(ec, "build_fields_frame"):
        namespace["build_fields_frame"] = ec.build_fields_frame
    write_csv_file = extract_function(
        root / "plot_ScalarHistory.py", "write_csv_file", namespace
    )
    unavailable = SimpleNamespace(available=False)

    cluster = sp.StandinCluster()
    records = {FIXED: [], "one_loop": []}
    labels = {}
    figure_records = []
    with cluster.active(), contextlib.redirect_stdout(io.StringIO()):
        # the one shard file, opened as the plot scripts' read-only pool opens each shard: the package's actor
        # with read_only=True, which writes nothing, and the lookup serial of the store's label (the pool is not
        # used because the harness wrote the shard without registering shard keys in the primary)
        shard = primary.parent / "store-shard0000.db"
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
        try:
            p = lookups(pool, units)
            for spec in manifest:
                name = spec["name"]
                law = "fixed" if spec["drain_model"] == FIXED else "one_loop"
                if spec["failure"]:
                    scalar, alpha_row = None, None
                    reasons = ["stand-in failure for the export"]
                else:
                    scalar, alpha_row = read_history(
                        pool, units, p, spec, populate=False
                    )
                    reasons = []
                    assert scalar.available and not scalar.failure, name
                kwargs = dict(
                    beta=spec["beta"],
                    M_Mp=spec["M_Mp"],
                    n=dh.N_EXPONENT,
                    phi_init_Mp=dh.PHI_INIT_OVER_MP,
                    alpha=spec["value"],
                    scalar=scalar,
                    bbn=None,
                    baseline=None,
                    units=units,
                    failure_reasons=reasons,
                )
                if tree_has_model:
                    kwargs["drain_model"] = spec["drain_model"]
                record = build_record(**kwargs)
                records[law].append(record)
                figure_records.append(record)

                entry = {}
                a_args = (
                    (spec["value"], spec["drain_model"])
                    if path_has_model
                    else (spec["value"],)
                )
                entry["path"] = str(alpha_path(Path("OUT") / OUT_LABEL, *a_args))
                if not spec["failure"]:
                    full, _ = read_history(pool, units, p, spec, populate=True)
                    assert full.available and not full.failure, name
                    csv_dir = out / "fields" / name
                    csv_dir.mkdir(parents=True, exist_ok=True)
                    write_csv_file(unavailable, unavailable, csv_dir, full, units)
                    if law == "one_loop":
                        if hasattr(ec, "write_bounces_csv"):
                            bdir = out / "bounces" / name
                            ec.write_bounces_csv(
                                full.bounces, units, bdir / "bounces.csv"
                            )
                    # the cosmology lines of the figures, from the tree's own label functions
                    label_kwargs = dict(alpha=spec["value"])
                    if tree_has_model:
                        label_kwargs["drain_model"] = spec["drain_model"]
                    fig = Figure()
                    group = SimpleNamespace(_M=1.0, _n=1, _formatter=lambda M: f"{M:g}")
                    ec.add_beta_summary_labels(fig, OUT_LABEL, group, **label_kwargs)
                    entry["add_beta_summary_labels"] = [
                        t.get_text()
                        for t in fig.texts
                        if t.get_text().startswith("Cosmology")
                    ]
                    fig = Figure()
                    ec.add_ScalarHistory_labels(fig, full, OUT_LABEL, **label_kwargs)
                    entry["add_ScalarHistory_labels"] = [
                        t.get_text()
                        for t in fig.texts
                        if t.get_text().startswith("Cosmology")
                    ]
                entry["driver_strings"] = driver_strings(
                    root, spec["value"], spec["drain_model"], alpha_label
                )
                labels[name] = entry
        finally:
            pool.__exit__(None, None, None)

    # histories.csv, once for the fixed-law records alone and once for the one-loop records alone
    ec.write_histories_csv(records[FIXED], out / "histories_fixed.csv")
    ec.write_histories_csv(records["one_loop"], out / "histories_one_loop.csv")

    # the legend of plot_Delta_N_eff (records at alpha > 0), the figure captured as the tests capture it
    figures = []
    real_save = ec._save
    ec._save = lambda fig, path: figures.append(fig) or True
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            for law, rs in records.items():
                for r in rs:
                    r["Delta_N_eff_today"] = r["Delta_N_eff_1MeV"] = 0.01
            ec.plot_Delta_N_eff(figure_records, out / "Delta_N_eff.pdf", "t")
    finally:
        ec._save = real_save
    labels["plot_Delta_N_eff_legend"] = (
        [l.get_label() for l in figures[0].axes[0].lines] if figures else []
    )

    (out / "labels.json").write_text(json.dumps(labels, indent=1, ensure_ascii=False))
    after = digests(primary.parent)
    (out / "sha256.json").write_text(
        json.dumps(
            {"before": before, "after": after, "equal": before == after}, indent=1
        )
    )
    print(
        f"@@ export_tree: {root}: {len(manifest)} histories; store unchanged: {before == after}"
    )
    assert before == after, "a store file moved under the export"


if __name__ == "__main__":
    main()
