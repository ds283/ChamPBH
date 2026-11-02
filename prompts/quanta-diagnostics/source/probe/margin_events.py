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
for f in sorted(
    glob.glob(
        "/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0-shard*.db"
    )
):
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
cap = {}
_rm = SM.relativistic_margins


def _rmc(candidates, events, *a, **kw):
    cap["candidates"] = list(candidates)
    cap["events"] = list(events)
    return _rm(candidates, events, *a, **kw)


SM.relativistic_margins = _rmc
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
assert not data.get("failure", False), data.get("failure_reason")
import pickle

gens = [tuple(g) for g in data["generations"]]
with open(f"{outdir}/events-{serial}.pkl", "wb") as fh:
    pickle.dump(
        {
            "gen_fields": SM.Generation._fields,
            "gens": gens,
            "cand_fields": SM.MarginCandidate._fields,
            "candidates": [tuple(c) for c in cap["candidates"]],
            "event_fields": SM.MarginEvent._fields,
            "events": [tuple(e) for e in cap["events"]],
            "margins": (
                dict(data["margins"]._asdict())
                if hasattr(data["margins"], "_asdict")
                else None
            ),
        },
        fh,
    )
print("done", len(gens), len(cap["candidates"]), len(cap["events"]))
