"""P1 at small M: the surfing/delivery state is M-independent (phi=0.17 >> M), so the P1 state is
reused with M = 0.01 and 0.001 to measure the first reflection at small M under each scheme.
"""

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
M = float(sys.argv[1])
out = {}
lines = []


def rec(key, res):
    out[key] = {k: v for k, v in res.items() if k not in ("policy", "potential")}
    line = H.summarize(res, M, label=key)
    lines.append(line)
    print(line, flush=True)
    pickle.dump(out, open(OUT + f"p1M{M:g}_results.pkl", "wb"))
    open(OUT + f"p1M{M:g}_summary.txt", "w").write("\n".join(lines) + "\n")


run = lambda **kw: H.run_fragment_loop(
    P["beta"], M, P["N0"], P["state"], N_end, record_steps=True, **kw
)
vel = lambda **kw: H.run_velocity_cap(
    P["beta"], M, P["N0"], P["state"], N_end, record_steps=True, **kw
)
rec("none@1e-08", run(strategy="none"))
rec("vel-phi-0.1@1e-08", vel(frac=0.1, cap_kind="phi"))
rec("vel-phi-0.1@1e-10", vel(frac=0.1, cap_kind="phi", atol=1e-10, rtol=1e-10))
rec("vel-wall-0.1@1e-08", vel(frac=0.1, cap_kind="wall"))
rec("vel-phi-0.1@1e-05/1e-06", vel(frac=0.1, cap_kind="phi", atol=1e-5, rtol=1e-6))
rec("regions@1e-08", run(strategy="regions"))
if M <= 0.001:
    rec("regions@1e-05/1e-06", run(strategy="regions", atol=1e-5, rtol=1e-6))
