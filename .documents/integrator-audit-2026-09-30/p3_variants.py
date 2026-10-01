"""P3 variants: velocity caps and fixed caps on the grazing phase (beta=1.2, M=0.01), 32.9 -> 37.5."""

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
    pickle.dump(out, open(OUT + "p3v_results.pkl", "wb"))
    open(OUT + "p3v_summary.txt", "w").write("\n".join(lines) + "\n")


run = lambda **kw: H.run_fragment_loop(
    P["beta"], P["M"], P["N0"], P["state"], N_end, record_steps=True, **kw
)
vel = lambda **kw: H.run_velocity_cap(
    P["beta"], P["M"], P["N0"], P["state"], N_end, record_steps=True, **kw
)
rec("none@1e-08", run(strategy="none"))
rec("vel-phi-0.1@1e-08", vel(frac=0.1, cap_kind="phi"))
rec("vel-wall-0.1@1e-08", vel(frac=0.1, cap_kind="wall"))
rec("vel-phi-0.1@1e-10", vel(frac=0.1, cap_kind="phi", atol=1e-10, rtol=1e-10))
for cap in (1e-3, 1e-4):
    rec(f"fixedcap-{cap:.0e}@1e-08", run(strategy="fixedcap", global_cap=cap))
