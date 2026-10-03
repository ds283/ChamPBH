# Brief: PRyMordial low-temperature network failures in the 2026.6.0 science run

**For:** a Claude Code agent working in the ChamPBH repository, under `CLAUDE.md` and the campaign
conventions in `prompts/`. **Written:** 2026-10-03, from a Claude Science session. **Tree:** `main` at
`4ae25b4` (`VERSION_LABEL` 2026.6.0, `PRYM_VERSION` `bf24c3d+ri02+sr01`). Treat this brief as
evidence to check, not as a specification (`CLAUDE.md`, invariant 7). It is meant to seed either a
single investigative prompt or the planning of a small campaign; the user decides which.

## 1. What happened

The user's science run wrote one store, `~/ChamPBH-stores/science-2026.6.0.db` (16 shards, 1.2 GB,
710 histories, all `ScalarModel` rows successful with 0 reflections). It was made by
`full_run_2026.6.0.sh` (blocks C1, C4, L, C5 at φ* = 1, 2, 5, C3 and C2), and `plot_by_beta.py`
outputs are in `~/ChamPBH-stores/science-out-phi{5,2,1}/`. BBN did not return for 21 of the 684
φ* = 5 histories:

- 9 hit the 0.2 keV spline floor (β = 0.50–0.90 at M = 10⁻³, non-surfing, ρ_NP/ρ_R,J = 18–48 at
  70 keV). These are expected and physically excluded; not part of this brief.
- 1 failed the output check (β = 0.95, M = 10⁻³: Yp = 0.512 > 0.5). Also excluded; not part of
  this brief.
- **11 failed inside PRyMordial's low-temperature nuclear network** with
  `PRyMSolverFailureError: solve_ivp failed in stage 'low-T nuclear network (full)': status=-1,
  message='Required step size is less than spacing between numbers.'` These are the subject here.

| β | M / M_P | shard | `ScalarModel.serial` | T at first bounce | ρ_NP/ρ_R,J at 70 keV | t reached / t target (s) |
|---|---|---|---|---|---|---|
| 1.6 | 1e-05 | 0011 | 2022 | 420.8 MeV | -0.006242 | 1.283e+06 / 1.316e+06 |
| 2.09 | 1e-05 | 0012 | 2130 | 845.8 MeV | 0.01361 | 1.257e+06 / 1.3e+06 |
| 2.12 | 1e-05 | 0015 | 1940 | 880.6 MeV | 0.03071 | 1.264e+06 / 1.317e+06 |
| 2.4 | 1e-05 | 0011 | 2076 | 1222 MeV | 0.04581 | 1.284e+06 / 1.315e+06 |
| 1.345 | 0.001 | 0000 | 660 | 283.1 MeV | 0.1776 | 1.207e+06 / 1.237e+06 |
| 2.89 | 0.001 | 0012 | 357 | 1841 MeV | 0.005482 | 1.309e+06 / 1.318e+06 |
| 1.05 | 0.01 | 0004 | 1675 | 0.0935 MeV | 0.4878 | 1.074e+06 / 1.084e+06 |
| 1.1 | 0.03 | 0009 | 1684 | 143.4 MeV | 0.1391 | 1.225e+06 / 1.254e+06 |
| 1.7 | 0.03 | 0005 | 1666 | 487 MeV | 0.04652 | 1.288e+06 / 1.304e+06 |
| 2.1 | 0.1 | 0013 | 1595 | 857.3 MeV | 0.05059 | 1.271e+06 / 1.299e+06 |
| 1.05 | 0.5 | 0004 | 1670 | 0.09529 MeV | 0.5083 | 1.057e+06 / 1.077e+06 |

The two at β = 1.05 are non-surfing (first bounce at about 94 keV); the other nine are surfing
histories. Every failure is at 96–99 % of the network's final time `t_end = t(T_end = 1 keV)`, i.e.
at T_J just above 1 keV, long after Yp and D/H have frozen. Their neighbours in β and M complete
normally. β = 1.6, M = 10⁻⁵ also failed on 2026-10-01 (a different harness) and completed in the
`science-readiness` close-out roster (verification §4.10 point 5, z-high 35): the outcome depends on
details of the input that do not change the physics.

## 2. What has already been measured (Claude Science, 2026-10-03, local Mac, repo venv)

All measurements used `bbn_from_store.py` (Appendix A). It reads one history's `ScalarModelValue`
rows from the store **read-only** (`sqlite3` URI `mode=ro`), rebuilds the ratio grid with **the same
arithmetic, in the same order, as `sqla_ScalarModelValue_factory.build` and `compute_BBN_data`**
(`log_T_Jordan = log_T_Jordan_GeV + log(units.GeV)`, `H_Jordan = H_Jordan_Mp · units.PlanckMass`,
`ρ_NP = 3 M_P² H_J² − ρ_R,J (1 + f_m)`, `math.exp`), builds the callback with the production
`build_rho_NP_callback` on [0.2 keV, 100 MeV], and calls `_run_PRyMordial(cb, small_network=False,
wall_clock_limit=600)`. Raw results are in `lt_failure_diagnostics.csv`.

1. **Bitwise reproduction.** A successful control (β = 1.6, M = 10⁻³) reproduces the stored
   Yp = 0.246894839 and D/H ×10⁵ = 2.461511946 to every printed digit. **All 11 failures
   reproduce**, with the same `t reached` as stored. An earlier reconstruction that differed only at
   the ulp level (it formed ln T in MeV as `log_T_GeV + log(GeV/MeV)`) succeeded on β = 1.6,
   M = 10⁻⁵ and moved the control's D/H by 3.7×10⁻⁴.
2. **Any tiny change to the input cures every failure.** All 11 complete when the ratio is scaled
   by (1 + 10⁻¹²) or (1 + 10⁻⁹), or splined linearly in ln T, or by PCHIP instead of the production
   cubic. The production cubic itself succeeds after the 10⁻¹² perturbation, so the interpolant is
   not needed to explain the failures (it may still contribute noise; see
   `[05-the-ratio-spline-may-ring-at-resolved-bounce-jumps]`).
3. **The same perturbations move D/H by up to 0.22 %.** Between the 10⁻¹² and 10⁻⁹ perturbations
   alone, D/H changes by a median 8.2×10⁻⁴ and a maximum 2.2×10⁻³ relative over the 11 cases
   (β = 2.4, M = 10⁻⁵: 2.460804 against 2.466193). Yp changes by ≤ 3×10⁻⁵. This is the
   `review-remediation` issue `[03-prymordial-output-moves-1e-5-under-1e-9-changes-in-rho-np]`,
   measured larger than its "1e-5 to 7e-4".
4. **Candidate root cause: the low-T stage runs at `solve_ivp`'s default `rtol = 1e-3`.** In
   `PRyM/PRyM_main.py` the low-T `solve_ivp(..., method="BDF", jac=...)` calls pass only `atol`
   (`1e-15` for the full network, `1e-11` for the small one) and no `rtol`. The mid-T calls pass
   `rtol = 1e-6, atol = 1e-9`; the thermodynamic and a(T) solves pass `rtol = 1e-6, atol = 1e-9`.
5. **Test patch: `rtol = 1e-6` on the full low-T call only** (a workspace copy of `PRyM/`, put first
   on `PYTHONPATH`; one added line). On four failures (β = 1.345 at 10⁻³, 1.6 at 10⁻⁵, 2.4 at 10⁻⁵,
   2.89 at 10⁻³) and the control:
   - every case completes on the exact production input;
   - the spread of D/H over {exact, ×(1+10⁻¹²), ×(1+10⁻⁹)} falls to 1.6×10⁻⁵–8.7×10⁻⁵ relative,
     against 2.9×10⁻⁴–2.2×10⁻³ at the default;
   - Yp's spread is unchanged at about 2×10⁻⁵, so Yp's floor is set elsewhere;
   - each solve took about 49–57 s against about 16–23 s at the default, but under different
     concurrent load (5 against 11 processes on 10 cores). **The cost has not been measured
     properly.**

   D/H at rtol = 1e-6 (×10⁵): β = 1.345 → 2.756420; 1.6 (10⁻⁵) → 2.461751; 2.4 (10⁻⁵) → 2.464413;
   2.89 → 2.478123; control 1.6 (10⁻³) → 2.461663 (stored, default rtol: 2.461512).

## 3. What the agent should establish

1. **Reproduce** the 11 failures and the control from the store, using Appendix A or a port of it
   into `tools/` (if ported, it is a new tool and belongs in a prompt's allowed files). Open the
   store read-only; never write to it, and never use it from a test (`CLAUDE.md`, Tests).
2. **Confirm the mechanism.** On two failures, instrument the full low-T solve: the step-size
   history near the failure time, which component's error estimate collapses the step, and whether
   any abundance goes negative or hits the `atol = 1e-15` floor. Compare `sol.y` at the failure time
   with a completed neighbour to show Yp and D/H are frozen by then.
3. **Scan the low-T tolerances.** Try rtol ∈ {1e-4, 1e-5, 1e-6, 1e-8} and a sensible `atol`
   (including a per-species vector), for both the full and the small network. Run on the SM baseline
   (`tools/bbn_baseline.py`), the 11 failures, and five successful controls spread over β and M. For
   each setting record:
   - the failure count;
   - the D/H and Yp spreads under the perturbations of §2.2;
   - the wall time per solve **on an unloaded machine**;
   - the SM baseline values.
4. **Look for Yp's residual floor** (about 2×10⁻⁵ relative) in the other stages (thermodynamics
   LSODA, a(T), high-T n↔p, mid-T BDF). Report it; do not change those stages unless the user asks.
5. **Check upstream.** Note whether upstream PRyMordial at `bf24c3d` has the same defaults. Any patch
   to `PRyM/` follows the vendored-patch rule: a comment naming the campaign and prompt, recorded in
   the log, `PRYM_VERSION` bumped.
6. **Say how to refresh BBN without recomputing histories.** The 710 `ScalarModel` histories took
   most of the run's compute and do not depend on PRyMordial. Find out:
   - whether `BBNData` rows are keyed on `PRYM_VERSION`, on `VERSION_LABEL`, or neither;
   - what `--drop bbn-data` and `--retry-failed-bbn` do on an existing store;
   - whether a `VERSION_LABEL` bump would invalidate the `ScalarModel` rows.

   Propose the smallest mechanism that lets the user recompute only the BBN stage under the new
   `PRYM_VERSION`. **Do not run `main.py` against a store**; production runs are the user's
   (`CLAUDE.md`, Long-running jobs).

## 4. Stop and ask if

- tightening the low-T tolerance does not cure all 11 failures at some setting;
- the SM baseline moves by more than 10⁻³ relative in D/H or 10⁻⁴ in Yp;
- the cost per solve rises by more than 3× at the setting that cures the failures and brings the
  D/H spread below 10⁻⁴;
- the mechanism of §3 point 2 points somewhere other than the low-T stage (for example a
  discontinuity in `T_of_t`, which is a linear `interp1d` of the thermodynamic solution).

## 5. Out of scope

- Changing the ratio interpolant, unless §3 shows it is needed.
- The 9 spline-floor and 1 output-check rows. They are excluded, and their classification belongs to
  the analysis.
- The adiabaticity issues, and `plot_by_beta.py` cosmetics: the missing `f` on line 218, and
  `--shards` not passed to `ShardedPool`. Record these as observations if met.

## 6. Why it matters for the science

The 11 rows are "not assessed": about 1.6 % of the surfing histories, including β = 1.345 inside
the threshold zoom and five points of the convergence-in-M comparison. The larger effect is the
noise in item 3 of §2. At the default tolerance every stored D/H carries a numerical scatter of
order 10⁻³, up to 0.2 %. That is negligible against the observational 1.2 %, but it is the same
size as the M = 10⁻³ against 10⁻⁵ differences (median 0.09 points) used to argue M-independence,
and as the shifts at β ≳ 1.6. A tolerance fix would make those comparisons physical rather than
solver-limited.

## Appendix A — `bbn_from_store.py`

Run from the repository root:
`PYTHONPATH=. ./venv/bin/python bbn_from_store.py ~/ChamPBH-stores/science-2026.6.0 1.6 1e-5 5 prod pert12 pert9 linear pchip`.
To test a PRyMordial patch, put a patched copy of `PRyM/` first on `PYTHONPATH`.

```python
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

```
