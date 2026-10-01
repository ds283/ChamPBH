"""Scan M downwards from the P1 delivery state: does each scheme resolve the first reflection?"""

import sys, signal, pickle, numpy as np
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
strategy = sys.argv[1]
Ms = [float(x) for x in sys.argv[2:]]
P = H.P1
N_end = 21.0


class Timeout(Exception):
    pass


def alarm(*a):
    raise Timeout()


signal.signal(signal.SIGALRM, alarm)
for M in Ms:
    signal.alarm(120)
    try:
        if strategy == "regions":
            res = H.run_fragment_loop(
                P["beta"],
                M,
                P["N0"],
                P["state"],
                N_end,
                strategy="regions",
                frag_cap=2000,
                record_steps=True,
            )
        elif strategy == "kin":
            res = H.run_velocity_cap(
                P["beta"],
                M,
                P["N0"],
                P["state"],
                N_end,
                frac=0.1,
                cap_kind="kin",
                h_max_global=0.1,
                jac_factor_max=1e-4,
                record_steps=True,
            )
        else:  # kinref: kinematic cap with the floor-triggered instantaneous reflection
            res = H.run_velocity_cap(
                P["beta"],
                M,
                P["N0"],
                P["state"],
                N_end,
                frac=0.1,
                cap_kind="kin",
                h_max_global=0.1,
                jac_factor_max=1e-4,
                record_steps=True,
                h_floor=1e-11,
                reflect_at_floor=True,
            )
        signal.alarm(0)
        print(H.summarize(res, M, label=f"{strategy} M={M:g}")[:330], flush=True)
        if "steps" in res:
            N, phi, pi, h = res["steps"]
            print(
                f"    accepted h min={h.min():.2e}; phi_min over accepted states={phi.min():.3e}; phi_wall/M~1/109 -> {M/109:.2e}; reflections at (N, phi, pi): {[(f'{a:.6f}', f'{b:.2e}', f'{c:.3e}') for a,b,c in res.get('reflect_N', [])][:3]}",
                flush=True,
            )
    except Timeout:
        print(f"{strategy} M={M:g}: TIMEOUT after 120 s", flush=True)
    except Exception as e:
        signal.alarm(0)
        print(
            f"{strategy} M={M:g}: EXCEPTION {type(e).__name__}: {str(e)[:150]}",
            flush=True,
        )
