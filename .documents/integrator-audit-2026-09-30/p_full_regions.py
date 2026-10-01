"""Full history under the SHIPPED scheme (fragment loop with regions and hard reflection), with a time limit."""

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
beta, M, limit = float(sys.argv[1]), float(sys.argv[2]), int(sys.argv[3])


class Timeout(Exception):
    pass


signal.signal(signal.SIGALRM, lambda *a: (_ for _ in ()).throw(Timeout()))
signal.alarm(limit)
try:
    res = H.run_fragment_loop(
        beta,
        M,
        0.0,
        H.initial_state(beta),
        1000.0,
        strategy="regions",
        frag_cap=100,
        record_steps=True,
    )
    signal.alarm(0)
    print(H.summarize(res, M, label=f"SHIPPED beta={beta} M={M:g}")[:330], flush=True)
    s = res["state_final"]
    print(
        f"  final N={res['N_final']:.5f} phi={s[0]:.6e} T_J={np.exp(s[4])/H.GeV:.4e} GeV; l1={res['l1_entries']} l2={res['l2_entries']} hard={res['hard']} frags={res['fragments']}",
        flush=True,
    )
except Timeout:
    print(f"SHIPPED beta={beta} M={M:g}: TIMEOUT after {limit} s", flush=True)
except Exception as e:
    print(
        f"SHIPPED beta={beta} M={M:g}: {type(e).__name__}: {str(e)[:200]}", flush=True
    )
