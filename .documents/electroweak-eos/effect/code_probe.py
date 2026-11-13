"""
electroweak-eos prompt 02 §1.4 (P16): which tree's code and which w table a driver and its Ray workers use.

Copied from one-loop-drain 06's .documents/one-loop-drain/rehearse-06/code_probe.py and extended. Run from a tree's root
(the main checkout, or the old worktree), as main.py is run:

    PYTHONPATH=. ./venv/bin/python -u <main checkout>/.documents/electroweak-eos/effect/code_probe.py \\
        [--address 127.0.0.1:6380] [--worker-csv PATH]

PYTHONPATH=., the venv's python, config.ray_auth imported first (Ray's token mode, named), then
ray.init(address=ADDRESS). The script's own directory holds no package of ChamPBH, so the imports resolve as they do
under main.py.

From the driver, from one task and from one actor, it prints what 06's printed:
  - ComputeTargets.ScalarHistory.__file__ and ObjectFactories.__file__;
  - config.version.VERSION_LABEL and config.fingerprint.FINGERPRINT_FORMAT;
  - the first five entries of sys.path, os.getcwd() and os.environ.get("PYTHONPATH");
and, from the driver, the job's py_driver_sys_path as the driver set it; and also, from each process:
  - the path CosmologyModels/GenericEOS/Xav_EOS_data.csv resolves to in that process (Xav_EOS_spline reads it by that
    relative path, from the process's working directory), and that file's SHA-256;
  - Sigma = 1 - 3w at 300 GeV from a Xav_EOS_spline built in that process, in GeV units. It is 5.685e-3 on the table
    before electroweak-eos (ec6bca8) and 1.889e-3 on the table of 2026.11.0 (README §9.2 P16).

It exits 1 if a worker's module files, labels, CSV hash or Sigma differ from the driver's, or if a module file or the
CSV of any process lies outside the driver's working directory (the tree's root); 0 otherwise.

--worker-csv PATH is for the breakage record only (orchestrator note 02, ruling 4): the task and the actor then build
their Xav_EOS_spline from PATH, through a redirect of the one pandas.read_csv call the class makes, and report PATH and
its hash, instead of the installed CSV. The installed CSV is never changed. Pointed at the old table from the main
checkout, the probe must exit 1.

It reads no store and writes nothing but stdout.
"""

import config.ray_auth  # noqa: F401  (first: Ray's token mode, as main.py)

import argparse
import hashlib
import os
import sys

import ray

EOS_CSV = "CosmologyModels/GenericEOS/Xav_EOS_data.csv"
T_SIGMA_GEV = 300.0


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _report(where: str, csv_override=None) -> dict:
    import importlib
    from unittest import mock

    import ObjectFactories

    # by the module's name: ComputeTargets exports the class ScalarHistory under the same attribute
    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    import config.fingerprint as fp
    import config.version as ver
    import CosmologyModels.GenericEOS.Xav_EOS_spline as xav_module
    from Units import GeV_units

    units = GeV_units()
    if csv_override is None:
        csv_path = os.path.realpath(EOS_CSV)
        eos = xav_module.Xav_EOS_spline(units)
    else:
        csv_path = os.path.realpath(csv_override)
        real = xav_module.pd.read_csv

        def redirected(arg, *args, **kwargs):
            return real(csv_path, *args, **kwargs)

        with mock.patch.object(xav_module.pd, "read_csv", redirected):
            eos = xav_module.Xav_EOS_spline(units)
    sigma = 1.0 - 3.0 * float(eos.w(T_SIGMA_GEV * units.GeV))

    return {
        "where": where,
        "pid": os.getpid(),
        "ScalarHistory": os.path.realpath(SM.__file__),
        "ObjectFactories": os.path.realpath(ObjectFactories.__file__),
        "Xav_EOS_spline": os.path.realpath(xav_module.__file__),
        "VERSION_LABEL": ver.VERSION_LABEL,
        "FINGERPRINT_FORMAT": fp.FINGERPRINT_FORMAT,
        "EOS_CSV": csv_path,
        "EOS_CSV_SHA256": _sha256(csv_path),
        "Sigma_300GeV": sigma,
        "sys.path[:5]": list(sys.path[:5]),
        "cwd": os.getcwd(),
        "PYTHONPATH": os.environ.get("PYTHONPATH"),
    }


@ray.remote
def probe_task(csv_override=None):
    return _report("task", csv_override)


@ray.remote
class ProbeActor:
    def report(self, csv_override=None):
        return _report("actor", csv_override)


FILES = ("ScalarHistory", "ObjectFactories", "Xav_EOS_spline", "EOS_CSV")
COMPARED = FILES + (
    "VERSION_LABEL",
    "FINGERPRINT_FORMAT",
    "EOS_CSV_SHA256",
    "Sigma_300GeV",
)
SHOWN = COMPARED + ("sys.path[:5]", "cwd", "PYTHONPATH")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Probe the code and the w table a driver's Ray workers use."
    )
    p.add_argument("--address", default="127.0.0.1:6380")
    p.add_argument(
        "--worker-csv",
        default=None,
        help="breakage record only: the task and the actor build Xav_EOS_spline from this file",
    )
    args = p.parse_args(argv)
    override = os.path.realpath(args.worker_csv) if args.worker_csv else None

    root = os.path.realpath(os.getcwd())
    driver = _report("driver")
    ray.init(address=args.address)
    try:
        try:
            jc = ray._private.worker.global_worker.core_worker.get_job_config()
            driver_sys_path = list(jc.py_driver_sys_path)
        except Exception as e:  # noqa: BLE001 - reported, not a verdict
            driver_sys_path = f"(not read: {type(e).__name__}: {e})"
        task = ray.get(probe_task.remote(override))
        actor = ProbeActor.remote()
        actor_report = ray.get(actor.report.remote(override))
        ray.kill(actor)
    finally:
        ray.shutdown()

    print(f"tree root (driver's cwd, realpath): {root}")
    print(f"job py_driver_sys_path: {driver_sys_path}")
    if override:
        print(f"--worker-csv (breakage record): the workers read {override}")
    ok = True
    for r in (driver, task, actor_report):
        print(f"== {r['where']} (pid {r['pid']})")
        for k in SHOWN:
            v = r[k]
            if k == "Sigma_300GeV":
                v = f"{v!r} ({v:.4g})"
            print(f"   {k}: {v}")
        for k in FILES:
            if not r[k].startswith(root + os.sep):
                print(f"   !! {k} is outside the tree root {root}")
                ok = False
    for r in (task, actor_report):
        for k in COMPARED:
            if r[k] != driver[k]:
                print(
                    f"   !! {r['where']}'s {k} differs from the driver's: {r[k]!r} vs {driver[k]!r}"
                )
                ok = False
    print(
        f"RESULT: {'every process imports the tree root code and reads its table' if ok else 'MISMATCH'}"
        f" (VERSION_LABEL {driver['VERSION_LABEL']}, FINGERPRINT_FORMAT {driver['FINGERPRINT_FORMAT']},"
        f" CSV {driver['EOS_CSV_SHA256'][:8]}…{driver['EOS_CSV_SHA256'][-4:]},"
        f" Sigma(300 GeV) {driver['Sigma_300GeV']:.4g})"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
