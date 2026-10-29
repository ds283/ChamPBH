# released against never-parked (eps0): the final state, N_final, the drained energy, ln T_J after the release
import os, pickle, sys, tempfile
import numpy as np
from math import exp
P = os.environ.get("PROBE_OUT", tempfile.gettempdir()) + os.sep
for tag in ("b5_M0.5", "b10_M0.5", "b25_M0.5", "b1.2_M0.001"):
    a = pickle.load(open(P + tag + "_release.pkl", "rb")); b = pickle.load(open(P + tag + "_eps0.pkl", "rb"))
    N_rel = a["release"][0]; pk = a["parking"]
    da = sum(x.delta_rho_over_rho_rad for x in a["injections"]) + pk.delta_rho_over_rho_rad
    db = sum(x.delta_rho_over_rho_rad for x in b["injections"])
    sa = sorted(a["samples"].items()); sb = sorted(b["samples"].items())
    Na = np.array([s[0] for s in sa]); Ya = np.array([s[1] for s in sa]); Nb = np.array([s[0] for s in sb]); Yb = np.array([s[1] for s in sb])
    m = (Na > pk.N) & (Na < min(a["N_final"], b["N_final"]))
    dT = Ya[m, 4] - np.interp(Na[m], Nb, Yb[:, 4])
    mr = (Na > N_rel) & (Na < min(a["N_final"], b["N_final"]))
    dTr = Ya[mr, 4] - np.interp(Na[mr], Nb, Yb[:, 4])
    fa, fb = np.array(a["final_state"]), np.array(b["final_state"])
    print(f"{tag}: N_final {a['N_final']:.7f} vs {b['N_final']:.7f} (diff {a['N_final']-b['N_final']:.2e}); "
          f"sum drho/rho_R incl. leftover {da:.9g} vs {db:.9g} (rel {da/db-1:.2e}); "
          f"ln T_J diff after parking max {np.abs(dT).max():.2e}, after release max {np.abs(dTr).max() if mr.any() else float('nan'):.2e}; "
          f"final phi rel {fa[0]/fb[0]-1:.2e}, final ln rho_R diff {fa[2]-fb[2]:.2e}")
