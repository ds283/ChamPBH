"""
Rerun one stored 2026.8.0 history (fixed normalization, no Ray, no store writes) and examine every
generation, not only the weighted ones: margins, and the excess of rho_chi over its massless value

    E(N) = sum_{g born <= N} dK_g (sqrt(1 + x_g^2) - 1) / K(N),   x_g = m(N) a(N) / q_g,

with m the field's own m_eff at each sample (and the zero-lag tracker's, for comparison), and
r_chi(N) E(N), the extra energy relative to the SM radiation.

    PYTHONPATH=. ./venv/bin/python -u all_generations.py SERIAL OUTDIR
"""

import glob, json, sqlite3, sys, time
from math import exp, log, sqrt, isfinite

import numpy as np

sys.path.insert(0, ".")
import importlib
from ComputeTargets.ScalarHistory import (
    integrate_history,
    log_m_eff_squared,
    tracker_log_m,
    SampleValues,
    MARGIN_THRESHOLD,
)
from tools.history_and_bbn import _z_grid, T_INIT_GEV, PHI_INIT_MP, PI_INIT
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

serial, outdir = int(sys.argv[1]), sys.argv[2]
MP_EV = None
row = None
for f in sorted(glob.glob("var/datastores/science-2026.8.0-shard*.db")):
    c = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
    r = c.execute(
        """select m.value_eV, b.value, a.value, h.normalization, h.phi_ref, h.injections,
           h.worst_tracker_margin, h.worst_reflection_margin, h.unassessed_share, h.extra_data,
           h.rho_chi_ratio_today
           from ScalarHistory h join ExponentialModel e on h.model_serial=e.serial
           join M_value m on e.M_serial=m.serial join beta_value b on e.beta_serial=b.serial
           join alpha_value a on h.alpha_serial=a.serial where h.serial=?""",
        (serial,),
    ).fetchone()
    if r is not None:
        row = r
        break
M_eV, beta, alpha, norm, phi_ref_Mp, inj_st, wt_st, wr_st, us_st, extra, rchi_st = row
bws_st = json.loads(extra)["margins"]["below_weight_share"]

units = Planck_units()
cosmology = QCD_Cosmology(0, units, Planck2018())
M_Mp = M_eV * units.eV / units.PlanckMass
model = ExponentialModel(0, M_value(0, M_eV * units.eV), 1, beta_value(0, beta), units)
realized = model.realize(norm, phi_ref_Mp * units.PlanckMass)

SM = importlib.import_module("ComputeTargets.ScalarHistory")
captured = []
_integrate = SM.integrate_scalar_history


def _cap(*a, **kw):
    res = _integrate(*a, **kw)
    captured[:] = [res]
    return res


SM.integrate_scalar_history = _cap
t0 = time.perf_counter()
data = integrate_history(
    cosmology,
    temperature(0, T_INIT_GEV * units.GeV),
    temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin),
    phi_value(0, PHI_INIT_MP * units.PlanckMass),
    pi_value(0, PI_INIT),
    _z_grid(),
    realized,
    task_label=f"probe-{serial}",
    alpha=alpha,
)
wall = time.perf_counter() - t0
assert not data.get("failure", False), data.get("failure_reason")
result = captured[-1]
ledger = data["dark_radiation"]
gens = data["generations"]
margins = data["margins"]
N_final = result.N_final
K_final = ledger.K(N_final)

samples = sorted((SampleValues._make(tuple(s)) for s in data["sample"]), key=lambda s: s.raw_N)
r_chi = dict(zip([s.raw_N for s in (SampleValues._make(tuple(x)) for x in data["sample"])], data["rho_chi_over_rho_rad"]))

Ns, lm_field, lm_tr = [], [], []
for s in samples:
    Ns.append(s.raw_N)
    lm_field.append(0.5 * log_m_eff_squared(realized, s.phi_Einstein, s.log_rhorad_Einstein, s.log_fm, s.Sigma))
    try:
        lm_tr.append(tracker_log_m(realized, s.phi_Einstein, s.log_rhorad_Einstein, s.log_fm, s.Sigma))
    except Exception:
        lm_tr.append(float("nan"))
Ns, lm_field, lm_tr = map(np.array, (Ns, lm_field, lm_tr))
T_J = np.array([exp(s.log_T_Jordan) / units.GeV for s in samples])
rchi = np.array([r_chi[n] for n in Ns])

gN = np.array([g.N_turn for g in gens])
gq = np.array([g.log_q for g in gens])
gdK = np.array([g.share for g in gens]) * K_final
gw = np.array([g.weighted for g in gens])


def excess(lm):
    E = np.zeros_like(Ns)
    for i, (N, l) in enumerate(zip(Ns, lm)):
        born = gN <= N
        if not born.any() or not isfinite(l):
            continue
        lx = N + l - gq[born]
        x2 = np.exp(np.minimum(2 * lx, 700))
        f = np.where(lx < -20, 0.5 * x2, np.sqrt(1 + x2) - 1)
        E[i] = np.sum(gdK[born] * f) / ledger.K(N)
    return E


E_field, E_tr = excess(lm_field), excess(lm_tr)

# per-generation margin against the field's own mass at every later sample (all generations)
mu_field = np.full(len(gens), np.inf)
for j in range(len(gens)):
    later = Ns > gN[j]
    if later.any():
        mu_field[j] = np.min(gq[j] - Ns[later] - lm_field[later])
mu_tr = np.array([g.tracker_margin if g.tracker_margin is not None else np.inf for g in gens])
mu_refl = np.array([g.reflection_margin if g.reflection_margin is not None else np.inf for g in gens])
share = gdK / K_final

out = {
    "serial": serial, "M_Mp": M_Mp, "beta": beta, "alpha": alpha, "wall_s": wall,
    "stored": {"injections": inj_st, "worst_tracker": wt_st, "worst_reflection": wr_st,
               "below_weight_share": bws_st, "unassessed_share": us_st, "rho_chi_ratio_today": rchi_st},
    "rerun": {"injections": sum(1 for c in data["injections"] if c.delta_K > 0),
              "worst_tracker": margins.worst_tracker_margin, "worst_reflection": margins.worst_reflection_margin,
              "below_weight_share": margins.below_weight_share, "unassessed_share": margins.unassessed_share},
    "generations": len(gens), "weighted": int(gw.sum()),
    "all_min_tracker": float(mu_tr.min()), "all_min_reflection": float(mu_refl.min()),
    "all_min_field_sampled": float(mu_field.min()),
    "below_weight_min_tracker": float(mu_tr[~gw].min()) if (~gw).any() else None,
    "below_weight_min_reflection": float(mu_refl[~gw].min()) if (~gw).any() else None,
    "share_tracker_below_ln10": float(share[mu_tr < MARGIN_THRESHOLD].sum()),
    "share_reflection_below_ln10": float(share[mu_refl < MARGIN_THRESHOLD].sum()),
    "share_reflection_below_0": float(share[mu_refl < 0].sum()),
    "share_field_below_ln10": float(share[mu_field < MARGIN_THRESHOLD].sum()),
    "max_E_field": float(E_field.max()), "N_at_max_E_field": float(Ns[np.argmax(E_field)]),
    "T_GeV_at_max_E_field": float(T_J[np.argmax(E_field)]),
    "max_E_tracker": float(np.nanmax(E_tr)),
    "max_rchi_E_field": float((rchi * E_field).max()),
    "E_field_today": float(E_field[-1]), "E_tracker_today": float(E_tr[-1]),
}
print(json.dumps(out, indent=1))
np.savez(f"{outdir}/gen-{serial}.npz", N=Ns, T_GeV=T_J, lm_field=lm_field, lm_tr=lm_tr, rchi=rchi,
         E_field=E_field, E_tr=E_tr, gN=gN, gq=gq, share=share, weighted=gw, mu_tr=mu_tr,
         mu_refl=mu_refl, mu_field=mu_field)
with open(f"{outdir}/gen-{serial}.json", "w") as fh:
    json.dump(out, fh, indent=1)
