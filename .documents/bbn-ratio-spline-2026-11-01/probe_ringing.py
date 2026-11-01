"""
Probe: does the cubic spline of r = rho_NP/rho_R,J that BBN reads ring at the ratio's jumps,
and does it move Yp and D/H?  Covers [05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]
(science-readiness) and [11-the-ratio-steps-at-injections-and-parking-in-the-10-100-keV-window]
(inelastic-reflection).

One history is tuned on main.py's axis (250 per decade), then integrated once more at its tuned
normalization on that axis plus a fine axis (FINE per decade) inside BBN's window. The integration
does not depend on the axis (integrate_history samples the dense output afterwards), which the
probe checks: the production points of the second run must equal the tuned run's bitwise.

Run from the repository root:
  PYTHONPATH=. ./venv/bin/python .documents/bbn-ratio-spline-2026-11-01/probe_ringing.py BETA M [ALPHA [FINE]]
"""

import sys
import time
from math import exp, log

import numpy as np
from scipy.interpolate import PchipInterpolator, make_interp_spline

from ComputeTargets.BBNData import (
    _run_PRyMordial,
    build_rho_NP_callback,
    thermodynamic_rho_SM,
)
from ComputeTargets.ScalarHistory import integrate_history, tune_history
from CosmologyConcepts import M_value, beta_value, redshift, redshift_array, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
from CosmologyConcepts.redshift import log_one_plus_z_grid
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units

SAMPLES_PER_LOG10_Z = 250
LOG10_HIGH = 300
FINE = int(sys.argv[4]) if len(sys.argv) > 4 else 25000  # per decade inside the window
FINE_ID0 = 10**8
T_MIN_KEV, T_MAX_MEV = 0.2, 100.0
WINDOWS_KEV = (
    (0.3, 1),
    (1, 3),
    (3, 10),
    (10, 100),
    (100, 1000),
    (1000, 10000),
    (10000, 100000),
)

beta = float(sys.argv[1])
M = float(sys.argv[2])
alpha = float(sys.argv[3]) if len(sys.argv) > 3 else 0.0
label = f"beta={beta:g} M={M:g} alpha={alpha:g}"

units = Planck_units()
cosmology = QCD_Cosmology(0, units, Planck2018())
model = ExponentialModel(
    0, M_value(0, M * units.PlanckMass), 1, beta_value(0, beta), units
)
T_init = temperature(0, 2.0e4 * units.GeV)
T_stop = temperature(1, cosmology.T_CMB_Kelvin * units.Kelvin)
phi_init = phi_value(0, 5.0 * units.PlanckMass)
pi_init = pi_value(0, 0.0)
keV, MeV = units.keV, units.MeV
M_P2 = units.PlanckMass**2

prod_pts = log_one_plus_z_grid(SAMPLES_PER_LOG10_Z, LOG10_HIGH)
prod_grid = redshift_array([redshift(i, x) for i, x in enumerate(prod_pts)])


def ratio(s):
    rho_R = exp(s.log_rhorad_Jordan)
    return (
        3.0 * M_P2 * s.H_Jordan * s.H_Jordan - rho_R * (1.0 + exp(s.log_fm))
    ) / rho_R


def in_window(s):
    T = exp(s.log_T_Jordan)
    return T_MIN_KEV * keV <= T <= T_MAX_MEV * MeV


t0 = time.perf_counter()
tuned = tune_history(
    cosmology,
    model,
    T_init,
    phi_init,
    pi_init,
    prod_grid,
    task_label="probe-tune",
    alpha=alpha,
)
assert not tuned.get("failure", False), tuned
print(
    f"[{label}] tuned in {time.perf_counter() - t0:.1f} s, {tuned['tuning_iterations']} integrations"
)

# the production samples in BBN's window, in the solver's order (T_J decreasing)
prod = [(z, s) for z, s in zip(tuned["z_grid"], tuned["sample"]) if in_window(s)]
Nb = [z.log_one_plus_z for z, _ in prod]
step = log(10.0) / SAMPLES_PER_LOG10_Z
lo_N, hi_N = min(Nb) - 2 * step, max(Nb) + 2 * step
d_f = log(10.0) / FINE
fine_pts = [k * d_f for k in range(int(lo_N / d_f), int(hi_N / d_f) + 2)]
grid = redshift_array(
    [redshift(i, x) for i, x in enumerate(prod_pts)]
    + [redshift(FINE_ID0 + j, x) for j, x in enumerate(fine_pts)]
)

t0 = time.perf_counter()
realized = model.realize(tuned["normalization"], tuned["phi_ref"])
again = integrate_history(
    cosmology,
    T_init,
    T_stop,
    phi_init,
    pi_init,
    grid,
    realized,
    task_label="probe-fine",
    alpha=alpha,
)
assert not again.get("failure", False), again
print(
    f"[{label}] fine integration in {time.perf_counter() - t0:.1f} s, {len(again['sample'])} samples"
)

# the integration is the same: production points bitwise
tuned_by_id = {z.store_id: s for z, s in zip(tuned["z_grid"], tuned["sample"])}
n_eq = n_ne = 0
for z, s in zip(again["z_grid"], again["sample"]):
    if z.store_id < FINE_ID0:
        if tuple(s) == tuple(tuned_by_id[z.store_id]):
            n_eq += 1
        else:
            n_ne += 1
print(
    f"[{label}] production samples equal to the tuned run's: {n_eq}, different: {n_ne}"
)

fine = [
    (z, s)
    for z, s in zip(again["z_grid"], again["sample"])
    if z.store_id >= FINE_ID0 and in_window(s)
]
xf = np.array(
    [s.log_T_Jordan - log(MeV) for _, s in fine]
)  # ln(T/MeV), decreasing in solver order
rf = np.array([ratio(s) for _, s in fine])
xp = np.array([s.log_T_Jordan - log(MeV) for _, s in prod])
rp = np.array([ratio(s) for _, s in prod])

nonmono = int(np.sum(np.diff(xf) >= 0.0))
print(
    f"[{label}] window: {len(xp)} production samples, {len(xf)} fine; fine T_J non-decreasing pairs: {nonmono}"
)

# interpolants on the production samples, on increasing abscissae as build_rho_NP_callback
cub = make_interp_spline(xp[::-1], rp[::-1], k=3)
lin = lambda x: np.interp(x, xp[::-1], rp[::-1])
pch = PchipInterpolator(xp[::-1], rp[::-1])

# jumps: production intervals that contain a fine-sample step much larger than the local drift
inside = (xf <= xp.max()) & (xf >= xp.min())
xfi, rfi = xf[inside], rf[inside]
dfine = np.abs(np.diff(rfi))
thresh = 1.0e-3  # an absolute step in r between fine samples 9e-5 e-folds apart
jump_at = 0.5 * (xfi[1:] + xfi[:-1])[dfine > thresh]
xp_inc = xp[::-1]
jump_intervals = set(np.searchsorted(xp_inc, jump_at) - 1)
interval_of = np.searchsorted(xp_inc, xfi) - 1
dist = np.array(
    [min((abs(int(i) - j) for j in jump_intervals), default=10**9) for i in interval_of]
)

print(
    f"[{label}] jumps found (fine step > {thresh:g}; median fine step {np.median(dfine):.3g}): {len(jump_at)} in {len(jump_intervals)} production intervals"
)
ib = int(np.argmax(np.abs(np.diff(rp))))
print(
    f"[{label}] production rms_step / max |step|: {np.sqrt(np.mean(np.diff(rp) ** 2)):.4g} / {np.max(np.abs(np.diff(rp))):.4g} at T={exp(xp[ib]) * 1e3:.4g} keV (r {rp[ib]:.4g} -> {rp[ib + 1]:.4g})"
)
e_c = cub(xfi) - rfi
iw = int(interval_of[int(np.argmax(np.abs(e_c)))])
m = interval_of == iw
print(
    f"[{label}] worst cubic interval: T={exp(xp_inc[iw]) * 1e3:.5g}-{exp(xp_inc[iw + 1]) * 1e3:.5g} keV, production r {rp[::-1][iw]:.4g} -> {rp[::-1][iw + 1]:.4g}, fine r in it min {rfi[m].min():.4g} max {rfi[m].max():.4g} over {m.sum()} points"
)
for lo, hi in WINDOWS_KEV:
    mm = (xp_inc >= log(lo * 1e-3)) & (xp_inc < log(hi * 1e-3))
    ji = [
        j
        for j in jump_intervals
        if 0 <= j < len(xp_inc) and log(lo * 1e-3) <= xp_inc[j] < log(hi * 1e-3)
    ]
    print(
        f"  > [{lo:g},{hi:g}) keV: {mm.sum()} production samples, {len(ji)} with a jump"
    )

for name, f in (("cubic", cub), ("linear", lin), ("pchip", pch)):
    e = f(xfi) - rfi
    parts = []
    for tag, mask in (
        ("in jump", dist == 0),
        ("next to jump", dist == 1),
        ("2 away", dist == 2),
        ("further", dist >= 3),
    ):
        parts.append(
            f"{tag}: max {np.max(np.abs(e[mask])) if mask.any() else float('nan'):.3g}"
        )
    print(
        f"[{label}] {name:6s} |interp - fine| "
        + "; ".join(parts)
        + f"; mean {np.mean(e):+.3g}"
    )
    for lo, hi in WINDOWS_KEV:
        m = (xfi >= log(lo * 1e-3)) & (xfi < log(hi * 1e-3))
        if m.any():
            print(
                f"  > {name:6s} [{lo:g},{hi:g}) keV: n={m.sum()} max|e|={np.max(np.abs(e[m])):.3g} "
                f"rms e={np.sqrt(np.mean(e[m] ** 2)):.3g} mean e={np.mean(e[m]):+.3g} "
                f"max|r|={np.max(np.abs(rfi[m])):.3g}"
            )

# BBN on each interpolant
rho_SM = thermodynamic_rho_SM(cosmology, units)


def callback(f):
    def rho_NP(T):
        if T < 0:
            return 0.0
        if not (T_MIN_KEV * 1e-3 <= T <= T_MAX_MEV):
            raise ValueError(f"T={T} MeV outside the window")
        return float(f(log(T))) * rho_SM(T)

    return rho_NP


# the fine samples in decreasing T order; drop any that would break strict monotonicity
keep = np.concatenate([[True], np.diff(xf) < 0.0])
xk, rk = xf[keep], rf[keep]
cases = (
    (
        "production cubic (the code's route)",
        build_rho_NP_callback(xp, rp, rho_SM, T_MIN_KEV * 1e-3, T_MAX_MEV, "probe"),
    ),
    ("production linear", callback(lin)),
    ("production pchip", callback(pch)),
    (
        "fine cubic",
        build_rho_NP_callback(
            xk, rk, rho_SM, T_MIN_KEV * 1e-3, T_MAX_MEV, "probe-fine"
        ),
    ),
    ("fine linear", callback(lambda x: np.interp(x, xk[::-1], rk[::-1]))),
)
for name, cb in cases:
    t0 = time.perf_counter()
    res = _run_PRyMordial(cb, True, None)
    if res.get("failure", False):
        print(f"[{label}] bbn {name}: FAILURE {res['failure_reason']}")
    else:
        print(
            f"[{label}] bbn {name}: Yp={res['Yp_BBN']:.10g} DoH={res['DOverH']:.10g} "
            f"({time.perf_counter() - t0:.1f} s)"
        )
