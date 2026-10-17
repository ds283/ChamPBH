"""Bisect, with the pipeline's integrator, for the smallest beta whose first T_J minimum (before phi = 0.1 Mp)
lies above 20 GeV. Args: lo, hi, variant, tol, [rtol]. Uses pipe_runs.py in a subprocess per beta.
"""

import json, os, subprocess, sys, tempfile

here = os.path.dirname(os.path.abspath(__file__))
lo, hi, variant, tol = (
    float(sys.argv[1]),
    float(sys.argv[2]),
    sys.argv[3],
    float(sys.argv[4]),
)
rtol = sys.argv[5] if len(sys.argv) > 5 else "1e-8"
d = tempfile.mkdtemp(dir=os.environ.get("TMPDIR"))


def ew(beta):
    subprocess.run(
        [
            sys.executable,
            f"{here}/pipe_runs.py",
            d,
            repr(beta),
            variant,
            "10",
            "41",
            rtol,
        ],
        check=True,
        capture_output=True,
    )
    r = json.load(
        open(f"{d}/summary_{variant}_{repr(beta)[:40]}_rtol{float(rtol):g}.json")
    )[0]
    mins = [t for t in r["turning_points"] if t["type"] == "min"]
    first = mins[0]["T_J_GeV"] if mins else None
    print(f"  beta={beta:.7f} first min T={first}", flush=True)
    return first is not None and first > 20.0


assert ew(hi) and not ew(lo)
while hi - lo > tol:
    m = 0.5 * (lo + hi)
    if ew(m):
        hi = m
    else:
        lo = m
print(f"{variant} rtol={rtol}: beta_c in ({lo:.7f}, {hi:.7f}]")
