"""Which schemes step over the wall, with trial-state exceptions treated as step rejections (custom Radau loop)?"""

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
out = {}
lines = []


def rec(key, res, M):
    out[key] = {k: v for k, v in res.items() if k not in ("policy", "potential")}
    line = H.summarize(res, M, label=key)
    lines.append(line)
    print(line, flush=True)
    pickle.dump(out, open(OUT + "ptotal2_results.pkl", "wb"))
    open(OUT + "ptotal2_summary.txt", "w").write("\n".join(lines) + "\n")


P = H.P1
for M in (0.5, 0.01, 0.001):
    for tol in (1e-8, 1e-10, 1e-12):
        rec(
            f"P1 M={M} none@{tol:.0e}",
            H.run_velocity_cap(
                P["beta"],
                M,
                P["N0"],
                P["state"],
                21.0,
                cap_kind="none",
                atol=tol,
                rtol=tol,
                record_steps=True,
            ),
            M,
        )
    for cap in (1e-1, 1e-2, 5e-3, 2e-3, 1e-3, 3e-4):
        rec(
            f"P1 M={M} fixed-{cap:.0e}@1e-08",
            H.run_velocity_cap(
                P["beta"],
                M,
                P["N0"],
                P["state"],
                21.0,
                cap_kind="fixed",
                h_max_global=cap,
                record_steps=True,
            ),
            M,
        )
    rec(
        f"P1 M={M} vel-phi-0.1+gcap0.1@1e-08",
        H.run_velocity_cap(
            P["beta"],
            M,
            P["N0"],
            P["state"],
            21.0,
            frac=0.1,
            cap_kind="phi",
            h_max_global=0.1,
            record_steps=True,
        ),
        M,
    )
P = H.P3
rec(
    "P3 none@1e-08",
    H.run_velocity_cap(
        P["beta"], P["M"], P["N0"], P["state"], 37.5, cap_kind="none", record_steps=True
    ),
    P["M"],
)
rec(
    "P3 fixed-1e-02@1e-08",
    H.run_velocity_cap(
        P["beta"],
        P["M"],
        P["N0"],
        P["state"],
        37.5,
        cap_kind="fixed",
        h_max_global=1e-2,
        record_steps=True,
    ),
    P["M"],
)
rec(
    "P3 vel-phi-0.1+gcap0.1@1e-08",
    H.run_velocity_cap(
        P["beta"],
        P["M"],
        P["N0"],
        P["state"],
        37.5,
        frac=0.1,
        cap_kind="phi",
        h_max_global=0.1,
        record_steps=True,
    ),
    P["M"],
)
