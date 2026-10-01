"""P1: delivery and first reflection (beta=2, M=0.5) from N0=20.0016 to N=21.0."""

import sys, pickle, numpy as np
import harness as H  # noqa: E402  (harness chdir's to the repository root)
import os, tempfile

# results (pickles and one-line summaries) go outside the repository unless AUDIT_OUT says otherwise
OUT = (
    os.environ.get(
        "AUDIT_OUT", os.path.join(tempfile.gettempdir(), "integrator-audit-2026-09-30")
    )
    + os.sep
)
os.makedirs(OUT, exist_ok=True)
P = H.P1
N_end = 21.0
which = sys.argv[1] if len(sys.argv) > 1 else "all"
out = {}
lines = []


def rec(key, res):
    out[key] = {k: v for k, v in res.items() if k not in ("policy", "potential")}
    line = H.summarize(res, P["M"], label=key)
    lines.append(line)
    print(line, flush=True)
    if res.get("failed") and res["failed"]["state"] is not None:
        f = res["failed"]
        print(
            f"    failure at N={f['N']:.6f} state={np.array2string(f['state'], precision=6)}",
            flush=True,
        )
    pickle.dump(out, open(OUT + f"p1{which}_results.pkl", "wb"))
    open(OUT + f"p1{which}_summary.txt", "w").write("\n".join(lines) + "\n")


run = lambda **kw: H.run_fragment_loop(
    P["beta"], P["M"], P["N0"], P["state"], N_end, record_steps=True, **kw
)
vel = lambda **kw: H.run_velocity_cap(
    P["beta"], P["M"], P["N0"], P["state"], N_end, record_steps=True, **kw
)

if which in ("a", "all"):
    for tol in (1e-8, 1e-10, 1e-12):
        rec(f"regions@{tol:.0e}", run(strategy="regions", atol=tol, rtol=tol))
        rec(f"none@{tol:.0e}", run(strategy="none", atol=tol, rtol=tol))
    # tolerance at which the shipped scheme is used at small M (paper: 1e-5 / 1e-6)
    rec("regions@1e-05/1e-06", run(strategy="regions", atol=1e-5, rtol=1e-6))
    rec("none@1e-05/1e-06", run(strategy="none", atol=1e-5, rtol=1e-6))
    # atol scaled to the field components (Finding D)
    rec(
        "regions@atolvec(1e-8*M)",
        run(
            strategy="regions",
            atol=np.array([1e-8 * P["M"], 1e-8 * P["M"], 1e-8, 1e-8, 1e-8]),
            rtol=1e-8,
        ),
    )
    rec(
        "none@atolvec(1e-8*M)",
        run(
            strategy="none",
            atol=np.array([1e-8 * P["M"], 1e-8 * P["M"], 1e-8, 1e-8, 1e-8]),
            rtol=1e-8,
        ),
    )
if which in ("b", "all"):
    for meth in ("BDF", "LSODA"):
        rec(f"none-{meth}@1e-08", run(strategy="none", method=meth))
    for cap in (1e-2, 1e-3, 1e-4, 5e-5):
        rec(f"fixedcap-{cap:.0e}@1e-08", run(strategy="fixedcap", global_cap=cap))
if which in ("c", "all"):
    for kind, frac in (
        ("wall", 0.1),
        ("wall", 0.02),
        ("phi", 0.1),
        ("phi", 0.02),
        ("dphi", 0.1),
    ):
        for tol in (1e-8, 1e-10):
            rec(
                f"vel-{kind}-{frac}@{tol:.0e}",
                vel(atol=tol, rtol=tol, frac=frac, cap_kind=kind),
            )
