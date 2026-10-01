"""P3: the grazing phase (beta=1.2, M=0.01) from N0=32.8965 to N=37.5."""

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
P = H.P3
N_end = 37.5
out = {}
lines = []


def rec(key, res):
    out[key] = {k: v for k, v in res.items() if k not in ("policy", "potential")}
    line = H.summarize(res, P["M"], label=key)
    lines.append(line)
    print(line, flush=True)
    pickle.dump(out, open(OUT + "p3_results.pkl", "wb"))
    open(OUT + "p3_summary.txt", "w").write("\n".join(lines) + "\n")


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
    rec(
        "vel-phi-0.1@1e-08",
        H.run_velocity_cap(
            P["beta"],
            P["M"],
            P["N0"],
            P["state"],
            N_end,
            frac=0.1,
            cap_kind="phi",
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
