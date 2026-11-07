"""
one-loop-drain prompt 06 §1.2 (P51; orchestrator note 06, ruling 1): which tree's code a driver and its Ray workers run.

    PYTHONPATH=. ./venv/bin/python -u /Users/ds283/Documents/Code/ChamPBH/.documents/one-loop-drain/rehearse-06/code_probe.py \
        [--address 127.0.0.1:6380]

Run from a tree's root (the main checkout, or the base worktree), as main.py is run: PYTHONPATH=., the venv's python,
config.ray_auth imported first (Ray's token mode, named), then ray.init(address=ADDRESS). The script's own directory holds no
package of ChamPBH, so the imports resolve as they do under main.py.

From the driver, from one task and from one actor, it prints:
  - ComputeTargets.ScalarHistory.__file__ and ObjectFactories.__file__;
  - config.version.VERSION_LABEL and config.fingerprint.FINGERPRINT_FORMAT;
  - the first five entries of sys.path, os.getcwd() and os.environ.get("PYTHONPATH");
and, from the driver, the job's py_driver_sys_path as the driver set it (the job config the core worker holds).

It exits 1 if a worker's module files or labels differ from the driver's, or if a module file of any process lies outside
the driver's working directory (the tree's root); 0 otherwise. It reads no store and writes nothing but stdout; prompt 06
sends its output to a file under the session scratchpad and copies it to .documents/one-loop-drain/rehearse-06/out/.
"""

import config.ray_auth  # noqa: F401  (first: Ray's token mode, as main.py)

import argparse
import os
import sys

import ray


def _report(where: str) -> dict:
    import importlib

    import ObjectFactories

    # by the module's name: ComputeTargets exports the class ScalarHistory under the same attribute
    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    import config.fingerprint as fp
    import config.version as ver

    return {
        "where": where,
        "pid": os.getpid(),
        "ScalarHistory": os.path.realpath(SM.__file__),
        "ObjectFactories": os.path.realpath(ObjectFactories.__file__),
        "VERSION_LABEL": ver.VERSION_LABEL,
        "FINGERPRINT_FORMAT": fp.FINGERPRINT_FORMAT,
        "sys.path[:5]": list(sys.path[:5]),
        "cwd": os.getcwd(),
        "PYTHONPATH": os.environ.get("PYTHONPATH"),
    }


@ray.remote
def probe_task():
    return _report("task")


@ray.remote
class ProbeActor:
    def report(self):
        return _report("actor")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Probe the code a driver's Ray workers import."
    )
    p.add_argument("--address", default="127.0.0.1:6380")
    args = p.parse_args(argv)

    root = os.path.realpath(os.getcwd())
    driver = _report("driver")
    ray.init(address=args.address)
    try:
        try:
            jc = ray._private.worker.global_worker.core_worker.get_job_config()
            driver_sys_path = list(jc.py_driver_sys_path)
        except Exception as e:  # noqa: BLE001 - reported, not a verdict
            driver_sys_path = f"(not read: {type(e).__name__}: {e})"
        task = ray.get(probe_task.remote())
        actor = ProbeActor.remote()
        actor_report = ray.get(actor.report.remote())
        ray.kill(actor)
    finally:
        ray.shutdown()

    print(f"tree root (driver's cwd, realpath): {root}")
    print(f"job py_driver_sys_path: {driver_sys_path}")
    ok = True
    for r in (driver, task, actor_report):
        print(f"== {r['where']} (pid {r['pid']})")
        for k in (
            "ScalarHistory",
            "ObjectFactories",
            "VERSION_LABEL",
            "FINGERPRINT_FORMAT",
            "sys.path[:5]",
            "cwd",
            "PYTHONPATH",
        ):
            print(f"   {k}: {r[k]}")
        for k in ("ScalarHistory", "ObjectFactories"):
            if not r[k].startswith(root + os.sep):
                print(f"   !! {k} is outside the tree root {root}")
                ok = False
    for r in (task, actor_report):
        for k in (
            "ScalarHistory",
            "ObjectFactories",
            "VERSION_LABEL",
            "FINGERPRINT_FORMAT",
        ):
            if r[k] != driver[k]:
                print(
                    f"   !! {r['where']}'s {k} differs from the driver's: {r[k]!r} vs {driver[k]!r}"
                )
                ok = False
    print(
        f"RESULT: {'every process imports the tree root code, labels the driver' + chr(39) + 's' if ok else 'MISMATCH'}"
        f" (VERSION_LABEL {driver['VERSION_LABEL']}, FINGERPRINT_FORMAT {driver['FINGERPRINT_FORMAT']})"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
