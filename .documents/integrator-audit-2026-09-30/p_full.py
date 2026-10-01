"""Full histories from main.py's initial data under the velocity cap (phi kind, frac 0.1, global cap 0.1)."""

import sys, pickle, time, numpy as np
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
beta, M = float(sys.argv[1]), float(sys.argv[2])
tol = float(sys.argv[3]) if len(sys.argv) > 3 else 1e-8
jfmax = float(sys.argv[4]) if len(sys.argv) > 4 else None
kind = sys.argv[5] if len(sys.argv) > 5 else "phi"
reflect = len(sys.argv) > 6 and sys.argv[6] == "reflect"
_prog = {"next": 1.0, "n": 0}


def _progress(N, y):
    _prog["n"] += 1
    if N >= _prog["next"]:
        _prog["next"] = int(N) + 1.0
        print(
            f"  progress N={N:.2f} phi={y[0]:.3e} pi={y[1]:+.3e} steps={_prog['n']} t={time.perf_counter()-_t0:.0f}s",
            flush=True,
        )
    return False


_t0 = time.perf_counter()
res = H.run_velocity_cap(
    beta,
    M,
    0.0,
    H.initial_state(beta),
    1000.0,
    stop_when=_progress,
    frac=0.1,
    cap_kind=kind,
    h_max_global=0.1,
    atol=tol,
    rtol=tol,
    record_steps=True,
    jac_factor_max=jfmax,
    h_floor=(1e-11 if reflect else 1e-9),
    reflect_at_floor=reflect,
)
line = H.summarize(
    res, M, label=f"FULL beta={beta} M={M} {kind}{'+reflect' if reflect else ''}"
)
print(line, flush=True)
b = H.wall_bounces(res, M)
print(
    (
        f"  wall bounces: {len(b)}; first at N={b[0][0]:.5f} phi_min={b[0][1]:.5e} T_J={b[0][2]*1e3:.4f} MeV"
        if b
        else "  no wall bounce"
    ),
    flush=True,
)
s = res["state_final"]
print(
    f"  final N={res['N_final']:.5f} phi={s[0]:.6e} pi={s[1]:+.4e} T_J={np.exp(s[4])/H.GeV:.4e} GeV; wall {res['wall']:.1f} s; nfev {res['nfev']}; steps {res['nsteps']}; rejected-by-exception {res['n_rejected_by_exception']}",
    flush=True,
)
N, phi, pi, h = res["steps"]
print(
    f"  phi_min over history {phi.min():.4e}; accepted h min={h.min():.2e} median={np.median(h):.2e}",
    flush=True,
)
res.pop("policy")
res.pop("potential")
pickle.dump(
    res,
    open(
        OUT
        + f"pfull_beta{beta:g}_M{M:g}_tol{tol:g}_jf{jfmax}_{kind}{'_reflect' if reflect else ''}.pkl",
        "wb",
    ),
)
