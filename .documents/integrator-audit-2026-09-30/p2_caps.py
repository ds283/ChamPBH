"""P2 follow-up: a single global cap (the paper's 'outside' cap) and the velocity cap, 25 -> 40."""

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
    pickle.dump(out, open(OUT + "p2c_results.pkl", "wb"))
    open(OUT + "p2c_summary.txt", "w").write("\n".join(lines) + "\n")


run = lambda **kw: H.run_fragment_loop(
    P["beta"], P["M"], P["N0"], P["state"], N_end, record_steps=True, **kw
)
vel = lambda **kw: H.run_velocity_cap(
    P["beta"], P["M"], P["N0"], P["state"], N_end, record_steps=True, **kw
)
rec("vel-phi-0.1@1e-08", vel(frac=0.1, cap_kind="phi"))
rec("vel-phi-0.1+gcap0.1@1e-08", vel(frac=0.1, cap_kind="phi", h_max_global=0.1))
rec("fixedcap-1e-01@1e-08", run(strategy="fixedcap", global_cap=1e-1))
rec("fixedcap-1e-02@1e-08", run(strategy="fixedcap", global_cap=1e-2))
