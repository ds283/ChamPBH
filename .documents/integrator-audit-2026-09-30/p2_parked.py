"""P2: parked inside L2 (beta=2, M=0.5) from N0=25.003 to N=40."""

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
P = H.P2
N_end = 40.0
out = {}
lines = []


def rec(key, res):
    out[key] = {k: v for k, v in res.items() if k not in ("policy", "potential")}
    line = H.summarize(res, P["M"], label=key)
    lines.append(line)
    print(line, flush=True)
    pickle.dump(out, open(OUT + "p2_results.pkl", "wb"))
    open(OUT + "p2_summary.txt", "w").write("\n".join(lines) + "\n")


which = sys.argv[1] if len(sys.argv) > 1 else "all"
if which in ("none", "all"):
    rec(
        "none@1e-08",
        H.run_fragment_loop(
            P["beta"],
            P["M"],
            P["N0"],
            P["state"],
            N_end,
            strategy="none",
            record_steps=True,
        ),
    )
    rec(
        "none@1e-10",
        H.run_fragment_loop(
            P["beta"],
            P["M"],
            P["N0"],
            P["state"],
            N_end,
            strategy="none",
            atol=1e-10,
            rtol=1e-10,
            record_steps=True,
        ),
    )
if which in ("vel", "all"):
    rec(
        "vel-wall-0.1@1e-08",
        H.run_velocity_cap(
            P["beta"],
            P["M"],
            P["N0"],
            P["state"],
            N_end,
            frac=0.1,
            cap_kind="wall",
            record_steps=True,
        ),
    )
if which in ("regions", "all"):
    rec(
        "regions@1e-08",
        H.run_fragment_loop(
            P["beta"],
            P["M"],
            P["N0"],
            P["state"],
            N_end,
            strategy="regions",
            record_steps=True,
        ),
    )
