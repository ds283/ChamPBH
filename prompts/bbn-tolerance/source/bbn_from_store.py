"""Rebuild a stored history's BBN input from a ChamPBH datastore and re-run PRyMordial.

Run from the ChamPBH repository root with the repo venv:
    PYTHONPATH=. ./venv/bin/python bbn_from_store.py STORE_STEM BETA M_MP [PHI_INIT] [VARIANT ...]
STORE_STEM is the path without '-shardNNNN.db'. VARIANT is one of prod (production cubic
callback, the default), pertE (production callback on r*(1+10^-E)), linear (ratio linear in ln T),
pchip (monotone cubic).
Reads the store read-only. Prints one line per variant.
"""
import glob, sqlite3, sys, time
from math import exp, log
import numpy as np
from scipy.interpolate import PchipInterpolator

import importlib
B = importlib.import_module("ComputeTargets.BBNData")
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

def find_model(stem, beta, M_Mp, phi):
    for f in sorted(glob.glob(stem + "-shard*.db")):
        con = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
        q = """select sm.serial, mv.value_eV, sm.failure, sm.RHS_evaluations
               from ScalarModel sm
               join ExponentialCoupling c on c.serial = sm.coupling_serial
               join beta_value b on b.serial = c.beta_serial
               join ExponentialPotential p on p.serial = sm.potential_serial
               join M_value mv on mv.serial = p.M_serial
               join phi_value ph on ph.serial = sm.phi_Einstein_init_serial
               where abs(b.value - ?) < 1e-9 and abs(ph.value_PlanckMass - ?) < 1e-9"""
        for serial, M_eV, failure, rhs in con.execute(q, (beta, phi)):
            if abs(M_eV / 2.436e27 / M_Mp - 1) < 1e-3:
                vals = con.execute(
                    "select raw_N, log_T_Jordan_GeV, H_Jordan_Mp, log_rhorad_Jordan_Mp4, log_fm "
                    "from ScalarModelValue where model_serial=? order by raw_N", (serial,)).fetchall()
                bbn = con.execute("select failure, failure_reason, Yp_BBN, DOverH from BBNData where model_serial=?",
                                  (serial,)).fetchone()
                con.close()
                return f, serial, rhs, np.array(vals), bbn
        con.close()
    raise SystemExit(f"no model beta={beta} M={M_Mp} phi={phi}")


def ratio_grid(vals, units, Tmin_MeV, Tmax_MeV):
    # the arithmetic of the ScalarModelValue factory and of compute_BBN_data, in the same order,
    # so the grid is bitwise the one production builds
    log_GeV = log(units.GeV); log_Mp = log(units.PlanckMass); log_MeV = log(units.MeV)
    CONST_3_MP_SQ = 3.0 * (units.PlanckMass * units.PlanckMass)
    Tlo, Thi = 0.2 * units.keV, 100 * units.MeV  # as compute_BBN_data forms them
    out_logT, out_r = [], []
    for raw_N, lT_GeV, H_Mp, lrho_Mp4, log_fm in vals:
        log_T_Jordan = float(lT_GeV) + log_GeV
        T_Jordan = exp(log_T_Jordan)
        if Tlo <= T_Jordan <= Thi:
            rhorad = exp(float(lrho_Mp4) + 4.0 * log_Mp)
            H = float(H_Mp) * units.PlanckMass
            LHS = (H * H) * CONST_3_MP_SQ
            fm = exp(float(log_fm))
            dens = LHS - rhorad * (1.0 + fm)
            out_logT.append(log_T_Jordan - log_MeV); out_r.append(dens / rhorad)
    return np.array(out_logT), np.array(out_r)


def make_callback(kind, logT, r, rho_SM, Tmin, Tmax, label):
    if kind == "prod":
        return B.build_rho_NP_callback(list(logT), list(r), rho_SM, Tmin, Tmax, label)
    if kind.startswith("pert"):  # production callback on r * (1 + 10^-E)
        eps = 10.0 ** (-int(kind[4:]))
        return B.build_rho_NP_callback(list(logT), [x * (1.0 + eps) for x in r], rho_SM, Tmin, Tmax, label)
    x, y = logT[::-1], r[::-1]
    f = (lambda u: np.interp(u, x, y)) if kind == "linear" else PchipInterpolator(x, y, extrapolate=False)
    def cb(T):
        if T <= 0:
            return 0.0
        if T < Tmin or T > Tmax:
            raise B.ComputationFailureError(f"T_in_MeV={T:.5g} outside [{Tmin}, {Tmax}]")
        return float(f(log(T))) * rho_SM(T)
    return cb


def main():
    stem, beta, M = sys.argv[1], float(sys.argv[2]), float(sys.argv[3])
    phi = float(sys.argv[4]) if len(sys.argv) > 4 else 5.0
    variants = sys.argv[5:] or ["prod"]
    units = Planck_units(); cosmology = QCD_Cosmology(0, units, Planck2018())
    shard, serial, rhs, vals, bbn = find_model(stem, beta, M, phi)
    Tmin, Tmax = 0.2e-3, 100.0
    logT, r = ratio_grid(vals, units, Tmin, Tmax)
    rho_SM = B.thermodynamic_rho_SM(cosmology, units)
    print(f"store beta={beta:g} M={M:g} phi={phi:g}: shard={shard[-7:-3]} serial={serial} RHS={rhs} "
          f"window_samples={len(r)} stored_bbn_failure={bbn[0]} stored_Yp={bbn[2]} stored_DoH={bbn[3]} "
          f"reason={(bbn[1] or '')[:90]}", flush=True)
    for kind in variants:
        t0 = time.time()
        cb = make_callback(kind, logT, r, rho_SM, Tmin, Tmax, f"store-{beta}-{M}")
        out = B._run_PRyMordial(cb, False, 600.0)
        if out.get("failure"):
            print(f"  {kind:6s} FAILURE {time.time()-t0:5.1f}s {out.get('failure_reason','')[:200]}", flush=True)
        else:
            print(f"  {kind:6s} Yp={out['Yp_BBN']:.10g} DoH={out['DOverH']:.10g} {time.time()-t0:5.1f}s", flush=True)


if __name__ == "__main__":
    main()
