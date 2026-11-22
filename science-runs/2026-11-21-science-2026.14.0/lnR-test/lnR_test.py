"""
The power walls' one-loop law beyond the ln R it was measured on (2026.14.0 run, README "What stays open").

The law f_n(ln R) (one-loop-inverse-power-v1, InversePowerModel._drain_fraction) was fitted on power-law-wall 02's
stage 2 (.documents/power-law-wall/wall-02/stage2_scan.txt), whose curve points reach ln R = 554.5 at n = 1 and 344.7 at
n = 2. This run's first impacts reach 740 and 490. This script runs stage 2 at ln R beyond the measured range by the same
code (prompts/power-law-wall/source/wall/powerscan.run_point_power, unchanged), and compares each fraction with the
production law's value at that ln R and n:
  - the two largest measured points again (n = 1 at 545.6394, n = 2 at 329.7256, on the 4e-31 curve), to check that this
    run of the code reproduces the record;
  - new points on the 4e-31 curve: n = 1 at ln R = 600, 650, 700, 750; n = 2 at 380, 420, 460, 500;
  - two of them again on the 1e-33 curve (n = 1 at 650, n = 2 at 460): stage 2's fraction depends on ln R and n alone
    (prompts/power-law-wall/source/wall/README.md, "What the floor does"), and this checks it out here.
Measurements, not bounds: nothing is judged against a threshold.

Two limits of the stage-2 code, not of the physics, bound what it can reach. stage2.solve searches outwards for a start
where the lowest mode is adiabatic and refuses one more than 1e15 times beyond the entry point ("no adiabatic start");
and it forms R = exp(ln R), which overflows above ln R ~ 709. With LNR_TEST_START_CAP set (for example 1e30), stage2.py
is loaded with that cap in place of 1e15, by a text substitution at import, and nothing else changed; the files under
prompts/ are not edited. A point the original cap allows runs identically under any larger cap, which the second run
checks on its own repeats.

    PYTHONPATH=. ./venv/bin/python science-runs/2026-11-21-science-2026.14.0/lnR-test/lnR_test.py \\
        > science-runs/2026-11-21-science-2026.14.0/lnR-test/lnR_test.txt
    LNR_TEST_START_CAP=1e30 PYTHONPATH=. ./venv/bin/python science-runs/2026-11-21-science-2026.14.0/lnR-test/lnR_test.py \\
        > science-runs/2026-11-21-science-2026.14.0/lnR-test/lnR_test-cap1e30.txt

Run from the repository root. Six processes, as the scan used; no Ray, no store. Written 2026-11-22 by a Claude Code
session at the user's request ("run the ln R test").
"""

import os
import sys
import time
from math import exp
from multiprocessing import Pool

ROOT = os.getcwd()
WALL = os.path.join(ROOT, "prompts/power-law-wall/source/wall")
sys.path.insert(0, ROOT)
sys.path.insert(0, WALL)

START_CAP = os.environ.get("LNR_TEST_START_CAP")
if START_CAP:  # stage2.py with the start cap replaced, loaded before powerscan imports it
    import types

    src = open(os.path.join(WALL, "stage2.py")).read()
    old = "if p > 1e15 * max(p_entry, 1.0):"
    assert src.count(old) == 1
    mod = types.ModuleType("stage2")
    mod.__file__ = os.path.join(WALL, "stage2.py")
    exec(compile(src.replace(old, f"if p > {float(START_CAP)!r} * max(p_entry, 1.0):"), mod.__file__, "exec"), mod.__dict__)
    sys.modules["stage2"] = mod

import powerscan as ps  # noqa: E402
from CosmologyConcepts.Models.InversePowerModel import (  # noqa: E402
    DRAIN_PL_A,
    DRAIN_PL_C,
    DRAIN_PL_G,
    DRAIN_PL_P,
    DRAIN_PL_R0,
    DRAIN_PL_SWITCH,
)

JOBS = [
    ("4e-31", 1, 545.6393, "repeat"),
    ("4e-31", 2, 329.7256, "repeat"),
    ("4e-31", 1, 600.0, "new"),
    ("4e-31", 1, 650.0, "new"),
    ("4e-31", 1, 700.0, "new"),
    ("4e-31", 1, 750.0, "new"),
    ("4e-31", 2, 380.0, "new"),
    ("4e-31", 2, 420.0, "new"),
    ("4e-31", 2, 460.0, "new"),
    ("4e-31", 2, 500.0, "new"),
    ("1e-33", 1, 650.0, "other curve"),
    ("1e-33", 2, 460.0, "other curve"),
]
# the record's values for the repeats (stage2_scan.txt, frac)
RECORD = {(1, 545.6393): 0.053796, (2, 329.7256): 0.061095}


def f_law(n: int, lnR: float) -> float:
    """InversePowerModel._drain_fraction for a one-loop candidate at ln R > 0, term for term."""
    if lnR < DRAIN_PL_SWITCH[n]:
        y = DRAIN_PL_G[n] * (lnR - DRAIN_PL_R0[n]) ** DRAIN_PL_P[n]
    else:
        y = DRAIN_PL_A[n] / (lnR - DRAIN_PL_C[n])
    return 1.0 - exp(-y)


def run(job):
    M, n, lnR, kind = job
    t0 = time.time()
    lnx = ps.lnx_on_curve(ps.CURVES[M], n, lnR)
    x = exp(lnx)
    try:
        reg, ell = ps.regime(x, n, lnR)
        res = ps.run_point_power(x, n, lnR)
    except Exception as e:  # noqa: BLE001  (a point that fails is a result)
        return f"{M:6s} {n} {lnR:9.4f} {lnx:9.4f} {kind:11s} FAILED {type(e).__name__}: {e}"
    f = res["frac_modes"]
    law = f_law(n, lnR)
    rec = RECORD.get((n, lnR))
    rec_s = f"{rec:9.6f}" if rec is not None else "        -"
    ell_s = "none" if ell is None else f"{ell:8.4f}"
    return (
        f"{M:6s} {n} {lnR:9.4f} {lnx:9.4f} {kind:11s} {reg:>9s} {ell_s:>8s} {f:9.6f} {res['frac_balance']:9.6f} "
        f"{rec_s} {law:9.6f} {law - f:+9.6f} {(law - f) / f:+8.4f} {res['vout']:7.4f} {str(res['returned']):>5s} "
        f"{res['eps_turn']:9.2e} {res['drift']:8.1e} {res['passes']} {time.time() - t0:5.0f}"
    )


if __name__ == "__main__":
    t0 = time.time()
    print(f"# lnR_test.py; start: {os.popen('uptime').read().strip()}")
    print(f"# stage 2's start cap: {float(START_CAP) if START_CAP else 1e15:g} times the entry point")
    print(
        "# M      n      ln R     ln x kind           regime      ell      frac   balance    record       law  "
        "law-frac  rel diff vout/vin  back  eps_turn   |dE/E| passes  s"
    )
    with Pool(6) as pool:
        for line in pool.imap(run, JOBS):
            print(line, flush=True)
    print(f"# end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}")
