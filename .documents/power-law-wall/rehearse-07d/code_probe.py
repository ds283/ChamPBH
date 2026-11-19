"""
power-law-wall prompt 07d §1.1 (P112; orchestrator note 07d, ruling 2): which tree's code and which data files a driver
and its Ray workers use.

Copied from power-law-wall 07's .documents/power-law-wall/rehearse-07/code_probe.py, which was copied from one-loop-drain
06's with electroweak-eos 02's data-file hash and PRyMrates/ added. 07d changes only its expectations: 07's probe compared
the workers with the driver alone; this one also holds every process to the tip's label and format (EXPECTED: 2026.14.0
and 8) and to the data files' hashes 07's tip probe printed
(.documents/power-law-wall/rehearse-07/out/07-probe-tip.txt). Run from this worktree's root, as main.py is run:

    PYTHONPATH=. ./venv/bin/python -u .documents/power-law-wall/rehearse-07d/code_probe.py [--address 127.0.0.1:6380]

PYTHONPATH=., the venv's python, config.ray_auth imported first (Ray's token mode, named), then
ray.init(address=ADDRESS). The script's own directory holds no package of ChamPBH, so the imports resolve as they do
under main.py.

From the driver, from one task and from one actor, it prints:
  - ComputeTargets.ScalarHistory.__file__ and ObjectFactories.__file__;
  - config.version.VERSION_LABEL and config.fingerprint.FINGERPRINT_FORMAT;
  - os.getcwd(), os.environ.get("PYTHONPATH") and the first five entries of sys.path;
  - the SHA-256 of CosmologyModels/GenericEOS/Xav_EOS_data.csv, read by that relative path (Xav_EOS_spline reads it so);
  - the SHA-256 of PRyMrates/ read by that relative path (PRyM_init reads it from os.getcwd()): every regular file under
    it, by its path relative to PRyMrates/, sorted, digested together as, for each file in turn, its relative path in
    UTF-8, a NUL byte, the file's own SHA-256 as 64 hex digits and a newline; with the count of files;
and, from the driver, the job's py_driver_sys_path as the driver set it.

It exits 1 if a worker's module files, labels or hashes differ from the driver's, if a module file of any process lies
outside the driver's working directory (the tree's root), if any process's working directory is not that root, or if any
process's label, format or data-file hashes differ from EXPECTED below; 0 otherwise. It reads no store and
writes nothing but stdout; prompt 07d sends its output to a file under var/power-law-wall/07d-r2/ and copies it to
.documents/power-law-wall/rehearse-07d/out/.
"""

import config.ray_auth  # noqa: F401  (first: Ray's token mode, as main.py)

import argparse
import hashlib
import os
import sys

import ray

EOS_CSV = "CosmologyModels/GenericEOS/Xav_EOS_data.csv"
PRYM_RATES = "PRyMrates"

# 07d's expectations: the tip's label and format (U12; config/version.py, config/fingerprint.py), and the data files'
# hashes as 07's tip probe printed them (rehearse-07/out/07-probe-tip.txt)
EXPECTED = {
    "VERSION_LABEL": "2026.14.0",
    "FINGERPRINT_FORMAT": 8,
    "EOS_CSV_SHA256": "690090c86e285f3ee30d03244a2c242dd2b5b3c3f405e4f94229b0e481d0119d",
    "PRyMrates_files": 84,
    "PRyMrates_SHA256": "49b4cea2006dc6f99a357ca3b87f3f4234409b49c041d1ac6802486586bf9224",
}


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _tree_sha256(root: str):
    rels = []
    for dirpath, _dirs, names in os.walk(root):
        for name in names:
            full = os.path.join(dirpath, name)
            if os.path.isfile(full):
                rels.append(os.path.relpath(full, root))
    rels.sort()
    h = hashlib.sha256()
    for rel in rels:
        h.update(rel.encode("utf-8") + b"\0")
        h.update(_sha256(os.path.join(root, rel)).encode("ascii") + b"\n")
    return h.hexdigest(), len(rels)


def _report(where: str) -> dict:
    import importlib

    import ObjectFactories

    # by the module's name: ComputeTargets exports the class ScalarHistory under the same attribute
    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    import config.fingerprint as fp
    import config.version as ver

    rates, nrates = _tree_sha256(PRYM_RATES)
    return {
        "where": where,
        "pid": os.getpid(),
        "ScalarHistory": os.path.realpath(SM.__file__),
        "ObjectFactories": os.path.realpath(ObjectFactories.__file__),
        "VERSION_LABEL": ver.VERSION_LABEL,
        "FINGERPRINT_FORMAT": fp.FINGERPRINT_FORMAT,
        "cwd": os.path.realpath(os.getcwd()),
        "PYTHONPATH": os.environ.get("PYTHONPATH"),
        "sys.path[:5]": list(sys.path[:5]),
        "EOS_CSV": os.path.realpath(EOS_CSV),
        "EOS_CSV_SHA256": _sha256(EOS_CSV),
        "PRyMrates": os.path.realpath(PRYM_RATES),
        "PRyMrates_files": nrates,
        "PRyMrates_SHA256": rates,
    }


@ray.remote
def probe_task():
    return _report("task")


@ray.remote
class ProbeActor:
    def report(self):
        return _report("actor")


FILES = ("ScalarHistory", "ObjectFactories", "EOS_CSV", "PRyMrates")
COMPARED = FILES + (
    "VERSION_LABEL",
    "FINGERPRINT_FORMAT",
    "cwd",
    "EOS_CSV_SHA256",
    "PRyMrates_files",
    "PRyMrates_SHA256",
)
SHOWN = COMPARED + ("sys.path[:5]", "PYTHONPATH")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Probe the code and the data files a driver's Ray workers use."
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
        for k in SHOWN:
            print(f"   {k}: {r[k]}")
        for k in FILES:
            if not r[k].startswith(root + os.sep):
                print(f"   !! {k} is outside the tree root {root}")
                ok = False
        if r["cwd"] != root:
            print(f"   !! the working directory is not the tree root {root}")
            ok = False
        for k, want in EXPECTED.items():
            if r[k] != want:
                print(f"   !! {k} is {r[k]!r}, not 07d's expected {want!r}")
                ok = False
    for r in (task, actor_report):
        for k in COMPARED:
            if r[k] != driver[k]:
                print(
                    f"   !! {r['where']}'s {k} differs from the driver's: {r[k]!r} vs {driver[k]!r}"
                )
                ok = False
    print(
        "expected in every process: "
        + ", ".join(f"{k} {v}" for k, v in EXPECTED.items())
    )
    print(
        f"RESULT: {'every process imports the tree root code, in the tree root, and reads its data files, at the expected label, format and hashes' if ok else 'MISMATCH'}"
        f" (VERSION_LABEL {driver['VERSION_LABEL']}, FINGERPRINT_FORMAT {driver['FINGERPRINT_FORMAT']},"
        f" Xav_EOS_data.csv {driver['EOS_CSV_SHA256'][:8]}…{driver['EOS_CSV_SHA256'][-4:]},"
        f" PRyMrates/ {driver['PRyMrates_files']} files {driver['PRyMrates_SHA256'][:8]}…{driver['PRyMrates_SHA256'][-4:]})"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
