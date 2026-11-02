"""
drain-probe (2026-11-02): rerun one stored 2026.8.0 history at its stored normalization (no tuning,
no Ray, no store writes) with the drain set by PROBE_DRAIN_A / PROBE_GAP (unset: the stored alpha).
Run from the scratch tree's root:  PYTHONPATH=. python -u drain_probe.py SERIAL LABEL OUTDIR
"""

import glob, json, os, sqlite3, sys, time
from collections import defaultdict
from math import exp

sys.path.insert(0, ".")
import importlib
SM = importlib.import_module("ComputeTargets.ScalarHistory")
from ComputeTargets.ScalarHistory import integrate_history
from tools.history_and_bbn import _z_grid, T_INIT_GEV, PHI_INIT_MP, PI_INIT
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

serial, label, outdir = int(sys.argv[1]), sys.argv[2], sys.argv[3]
row = None
for f in sorted(
    glob.glob("/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0-shard*.db")
):
    c = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
    r = c.execute(
        """select m.value_eV, b.value, a.value, h.normalization, h.phi_ref, h.rho_chi_ratio_today
           from ScalarHistory h join ExponentialModel e on h.model_serial=e.serial
           join M_value m on e.M_serial=m.serial join beta_value b on e.beta_serial=b.serial
           join alpha_value a on h.alpha_serial=a.serial where h.serial=?""",
        (serial,),
    ).fetchone()
    c.close()
    if r is not None:
        row = r
        break
M_eV, beta, alpha, norm, phi_ref_Mp, rchi_stored = row

units = Planck_units()
cosmology = QCD_Cosmology(0, units, Planck2018())
model = ExponentialModel(0, M_value(0, M_eV * units.eV), 1, beta_value(0, beta), units)
realized = model.realize(norm, phi_ref_Mp * units.PlanckMass)

t0 = time.time()
data = integrate_history(
    cosmology,
    temperature(0, T_INIT_GEV * units.GeV),
    temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
    phi_value(0, PHI_INIT_MP * units.PlanckMass),
    pi_value(0, PI_INIT),
    _z_grid(),
    realized,
    task_label=f"drain-probe-{serial}-{label}",
    alpha=alpha,
)
wall = time.time() - t0
out = {
    "serial": serial, "label": label, "M_eV": M_eV, "beta": beta, "alpha_stored": alpha,
    "A": os.environ.get("PROBE_DRAIN_A"), "gap": os.environ.get("PROBE_GAP", "zero"),
    "rchi_today_stored": rchi_stored, "wall_s": wall,
    "failure": bool(data.get("failure", False)), "failure_reason": data.get("failure_reason"),
}
if not out["failure"]:
    inj = data["injections"]
    dv = data["dark_radiation_values"]
    out["dark_radiation_values"] = dict(dv._asdict()) if dv is not None else None
    out["candidates"] = len(inj)
    out["drains"] = sum(1 for i in inj if i.delta_K > 0.0)
    out["reflections"] = data["reflections"]
    out["accepted_steps"] = data["accepted_steps"]
    out["parking"] = None if data["parking"] is None else str(data["parking"])[:400]
    out["warnings"] = [w[:200] for w in data["warnings"]]
    K = sum(i.delta_K for i in inj)
    out["K_injections"] = K
    log = SM._PROBE_LOG
    by = defaultdict(lambda: {"n": 0, "K": 0.0, "alpha_min": None, "alpha_max": None,
                              "ell_min": None, "ell_max": None, "N_min": None, "N_max": None})
    if log:
        assert len(log) == len(inj), (len(log), len(inj))
        pairs = zip(log, inj)
    else:
        pairs = ((None, i) for i in inj)
    for l, i in pairs:
        if l is None:
            ct = i.classical_turning_point
            regime = "unassessed" if ct is None else ("R<=1" if ct.log_R <= 0 else "R>1")
            ell, a = None, i.alpha_eff
        else:
            _, regime, _, ell, a = l
        b = by[regime]
        b["n"] += 1
        b["K"] += i.delta_K
        for key, v in (("alpha", a), ("ell", ell), ("N", i.N)):
            if v is None:
                continue
            b[key + "_min"] = v if b[key + "_min"] is None else min(b[key + "_min"], v)
            b[key + "_max"] = v if b[key + "_max"] is None else max(b[key + "_max"], v)
    for b in by.values():
        b["K_share"] = b["K"] / K if K > 0 else None
    out["by_regime"] = dict(by)
    cands = []
    for i in inj:
        ct = i.classical_turning_point
        cands.append((i.N, i.N_turn, i.kind, i.H_Einstein_before, i.pi_before, i.log_rho_kin,
                      None if ct is None else ct.log_k_ex, None if ct is None else ct.log_R,
                      None if ct is None else ct.log_omega_t_squared,
                      None if ct is None else ct.phi_t, i.alpha_eff, i.delta_K))
    out["cand_fields"] = ["N", "N_turn", "kind", "H_before", "pi_before", "log_rho_kin",
                          "log_k_ex", "log_R", "log_omega_t_squared", "phi_t", "alpha_eff", "delta_K"]
    out["cands"] = cands
    out["release"] = None if data["release"] is None else str(data["release"])[:400]
    out["N_final"] = float(data["sample"][-1].raw_N)
with open(f"{outdir}/{serial}-{label}.json", "w") as fh:
    json.dump(out, fh, indent=1, default=str)
print("done", json.dumps({k: out.get(k) for k in ("serial", "label", "wall_s", "failure", "candidates")}))
