"""
Probe harness for the integrator audit (2026-09-30). No Ray, no datastore.

Builds ODEPolicy/ODERHS/supervisor exactly as compute_scalar_model does and drives
solve_ivp from a mid-history state with a pluggable step-control strategy:

  * "regions"   -- the shipped scheme: L1/L2 boundaries and per-region max_step, fragment
                   restarts at each boundary crossing, hard reflection at phi = 0.
  * "none"      -- no regions, max_step = inf, hard reflection only.
  * "velocity"  -- a state-dependent cap implemented with a custom Radau step loop:
                   h <= frac * (phi - phi_wall) / |pi|, with phi_wall from the RHS balance.
  * "fixedcap"  -- a single global max_step (no regions).

Every strategy records: RHS count, accepted steps, fragments, hard reflections, turning
points (pi sign changes on accepted steps), first bounce (N, phi_min, T_J), energy per bounce.
"""

import os, sys, time
from math import log, exp, sqrt, isnan, isinf
import numpy as np

REPO = os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
)
sys.path.insert(0, REPO)
os.chdir(REPO)

import importlib

SM = importlib.import_module("ComputeTargets.ScalarModel")
from Quadrature.supervisors.ScalarField import StateVector
from CosmologyConcepts import beta_value, M_value, Lambda_value, temperature
from CosmologyConcepts.ConformalCouplings.ExponentialCoupling import ExponentialCoupling
from CosmologyConcepts.Potentials.ExponentialPotential import ExponentialPotential
from CosmologyModels.GenericEOS.QCD_Cosmology import QCD_Cosmology
from CosmologyModels.LambdaCDM import Planck2018
from Units import Planck_units
from scipy.integrate import solve_ivp
from scipy.integrate._ivp.radau import Radau

units = Planck_units()
params = Planck2018()
cosmology = QCD_Cosmology(0, units, params)
GeV = units.GeV
LOG_GEV = log(GeV)
T_init = temperature(0, 2.0e4 * GeV)
T_stop = temperature(1, params.T_CMB_Kelvin * units.Kelvin)
log_T_stop = log(float(params.T_CMB_Kelvin * units.Kelvin))


def build(beta, M_Mp):
    potential = ExponentialPotential(
        0,
        M_value(0, M_Mp * units.PlanckMass),
        Lambda_value(0, 1e-3 * units.eV),
        1,
        units,
    )
    coupling = ExponentialCoupling(0, beta_value(0, beta), units)
    policy = SM.ODEPolicy("probe", cosmology, potential, coupling)
    rhs = SM.ODERHS("probe", policy)
    sup = SM.ScalarFieldIntegrationSupervisor(
        units, T_init, T_stop, np.inf, label="probe", collect_full_statistics=False
    )
    sup.__enter__()
    return potential, coupling, policy, rhs, sup


class Counter:
    """Wrap the RHS to count evaluations. With total=True a ComputationFailureError raised on a
    trial state is converted to a NaN vector, which Radau/BDF treat as a failed Newton iteration
    (the step is rejected and h halved) instead of aborting the whole integration."""

    def __init__(self, rhs, sup, total=False):
        self.rhs, self.sup, self.n, self.total, self.n_caught = rhs, sup, 0, total, 0

    def __call__(self, N, s):
        self.n += 1
        if not self.total:
            return self.rhs(N, s, self.sup)
        try:
            return self.rhs(N, s, self.sup)
        except SM.ComputationFailureError:
            self.n_caught += 1
            return np.full(5, np.nan)


def phi_wall(policy, state):
    """phi at which V' balances the conformal force, for the current rho, fm (n = 1).
    Balance: V0 M/phi^2 e^{M/phi} = 3 Mp^2 H^2 E beta/Mp R  ~  rho_rad (1+fm)-ish * beta R.
    Solve in log form by a few Newton steps on g(phi) = log(V M/phi^2) - log(force)."""
    pot = policy.potential
    M = pot._M_float
    beta = policy.coupling.d_logOmega_dphi(state.phi_Einstein)  # beta/Mp
    fm = exp(state.log_fm)
    T_J = exp(state.log_T_Jordan)
    Sigma = 1.0 - 3.0 * policy.cosmology.w(T_J)
    R = (Sigma + fm) / (1.0 + fm)
    rho = exp(state.log_rhorad_Einstein) * (1.0 + fm)  # approx 3 Mp^2 H^2 E
    log_force = log(abs(beta * R * rho)) if beta * R * rho > 0 else None
    if log_force is None:
        return 0.0
    # g(phi) = log_V0 + M/phi + log(M) - 2 log(phi) - log_force = 0, monotone decreasing in phi
    lo, hi = 1e-12, 100.0 * M
    for _ in range(200):
        mid = sqrt(lo * hi)
        g = pot._log_Lambda_4 + M / mid + log(M) - 2.0 * log(mid) - log_force
        if g > 0:
            lo = mid
        else:
            hi = mid
        if hi / lo < 1.0 + 1e-10:
            break
    return sqrt(lo * hi)


def energy(policy, state):
    """Field 'energy' proxy: pi^2/2 + V/(3 H^2 Mp^2) scaled; we use pi^2/2 + log-potential term
    relative to rho. Return (KE, V_over_rho) so bounce-to-bounce comparisons are meaningful.
    """
    d = policy(0.0, state)
    return 0.5 * state.pi_Einstein**2, d.V_over_3H2Mp2


def make_events(potential, in_l1, in_l2, use_regions):
    b1, b2, hr = (
        potential.bounce_region_level1_boundary,
        potential.bounce_region_level2_boundary,
        potential.hard_reflection_point,
    )

    def term(N, s):
        return s[4] - log_T_stop

    term.terminal, term.direction = True, -1.0

    def refl(N, s):
        return s[0] - hr

    refl.terminal, refl.direction = True, -1.0

    def dummy(N, s):
        return 1.0

    def e1(N, s):
        return s[0] - b1

    def x1(N, s):
        return s[0] - b1

    def e2(N, s):
        return s[0] - b2

    def x2(N, s):
        return s[0] - b2

    for f, d in ((e1, -1.0), (x1, 1.0), (e2, -1.0), (x2, 1.0)):
        f.terminal, f.direction = True, d
    if not use_regions:
        return (term, refl, dummy, dummy, dummy, dummy)
    return (
        term,
        refl,
        e1 if not in_l1 else dummy,
        x1 if in_l1 else dummy,
        e2 if not in_l2 else dummy,
        x2 if in_l2 else dummy,
    )


def run_fragment_loop(
    beta,
    M_Mp,
    N0,
    state0,
    N_end,
    strategy="regions",
    atol=1e-8,
    rtol=1e-8,
    global_cap=np.inf,
    frag_cap=100000,
    verbose=False,
    record_steps=False,
    method="Radau",
    total_rhs=False,
):
    """Reproduce compute_scalar_model's fragment loop between N0 and N_end (or T_stop)."""
    potential, coupling, policy, rhs, sup = build(beta, M_Mp)
    cnt = Counter(rhs, sup, total=total_rhs)
    b1, b2 = (
        potential.bounce_region_level1_boundary,
        potential.bounce_region_level2_boundary,
    )
    use_regions = strategy == "regions"
    in_l1 = use_regions and state0[0] < b1
    in_l2 = use_regions and state0[0] < b2
    if use_regions:
        max_step = (
            potential.bounce_region_level2_max_step
            if in_l2
            else (potential.bounce_region_level1_max_step if in_l1 else np.inf)
        )
    else:
        max_step = global_cap
    N_start, y0 = N0, np.array(state0, dtype=float)
    frags, hard, l1e, l1x, l2e, l2x = 0, 0, 0, 0, 0, 0
    nsteps = 0
    turns = []  # (N, phi, T_J, pi_before) at pi sign change of accepted steps
    steps_N, steps_phi, steps_pi, steps_h = [], [], [], []
    last_pi = y0[1]
    t0 = time.perf_counter()
    entries_N = []
    frag_nfev, frag_h0 = [], []
    n_before = 0
    failed = None
    while True:
        try:
            sol = solve_ivp(
                cnt,
                method=method,
                t_span=(N_start, N_end),
                y0=y0,
                atol=atol,
                rtol=rtol,
                events=make_events(potential, in_l1, in_l2, use_regions),
                dense_output=False,
                max_step=max_step,
            )
        except SM.ComputationFailureError as e:
            failed = dict(
                message=f"ComputationFailureError: {str(e)[:120]}", N=None, state=None
            )

            class _S:
                pass

            sol = _S()
            sol.t = np.array([N_start])
            sol.y = y0.reshape(-1, 1)
            sol.status = -1
            sol.t_events = [[]] * 6
            frags += 1
            break
        else:
            failed = None
            if not sol.success:
                failed = dict(
                    message=sol.message, N=float(sol.t[-1]), state=sol.y[:, -1].copy()
                )
            frags += 1
            nsteps += len(sol.t) - 1
            frag_nfev.append(cnt.n - n_before)
            n_before = cnt.n
            frag_h0.append(tuple(np.diff(sol.t)[:3]))
            # sign changes of pi on accepted steps
            pis = sol.y[1]
            for i in range(len(sol.t)):
                if (pis[i] < 0) != (last_pi < 0):
                    turns.append(
                        (sol.t[i], sol.y[0][i], exp(sol.y[4][i]) / GeV, last_pi, pis[i])
                    )
                last_pi = pis[i]
            if record_steps:
                steps_N.append(sol.t)
                steps_phi.append(sol.y[0])
                steps_pi.append(sol.y[1])
                steps_h.append(np.diff(sol.t))
            if sol.status == 0 or failed is not None:
                break
            nev = [len(t) for t in sol.t_events]
            if sum(nev) != 1:
                raise RuntimeError(f"multiple events {nev} at N={sol.t[-1]}")
            N_start = sol.t[-1]
            y0 = sol.y[:, -1].copy()
            if nev[0]:
                break
            if nev[1]:
                hard += 1
                if y0[1] < 0:
                    y0[1] = -y0[1]
            if nev[2]:
                l1e += 1
                in_l1, in_l2 = True, False
                max_step = potential.bounce_region_level1_max_step
            if nev[3]:
                l1x += 1
                in_l1, in_l2 = False, False
                max_step = np.inf
            if nev[4]:
                l2e += 1
                in_l1, in_l2 = True, True
                max_step = potential.bounce_region_level2_max_step
                entries_N.append(N_start)
            if nev[5]:
                l2x += 1
                in_l1, in_l2 = True, False
                max_step = potential.bounce_region_level1_max_step
            if frags >= frag_cap:
                raise RuntimeError(f"fragment cap {frag_cap} at N={N_start}")
    wall = time.perf_counter() - t0
    out = dict(
        strategy=strategy,
        atol=atol,
        rtol=rtol,
        N_final=sol.t[-1],
        state_final=sol.y[:, -1].copy(),
        failed=failed,
        nfev=cnt.n,
        nsteps=nsteps,
        fragments=frags,
        hard=hard,
        l1_entries=l1e,
        l1_exits=l1x,
        l2_entries=l2e,
        l2_exits=l2x,
        l2_entry_N=entries_N,
        turns=turns,
        wall=wall,
        policy=policy,
        potential=potential,
        frag_nfev=frag_nfev,
        frag_h0=frag_h0,
        n_caught=cnt.n_caught,
    )
    if record_steps and steps_N:
        out["steps"] = (
            np.concatenate(steps_N),
            np.concatenate(steps_phi),
            np.concatenate(steps_pi),
            np.concatenate(steps_h),
        )
    return out


# ---------------------------------------------------------------------------------------
# Velocity-aware cap: a custom step loop around scipy's Radau class, adjusting
# solver.max_step before every step from the current state.
# ---------------------------------------------------------------------------------------
def run_velocity_cap(
    beta,
    M_Mp,
    N0,
    state0,
    N_end,
    atol=1e-8,
    rtol=1e-8,
    frac=0.1,
    h_floor=1e-9,
    hard_reflect=True,
    record_steps=False,
    cap_kind="wall",
    h_max_global=np.inf,
    total_rhs=False,
    jac_factor_max=None,
    stop_when=None,
    reflect_at_floor=False,
):
    """
    cap_kind = "wall":  h <= frac * (phi - phi_wall)/|pi|   (distance to the balance point)
    cap_kind = "phi":   h <= frac * phi/|pi|                (distance to the origin; needs no wall estimate)
    cap_kind = "dphi":  h <= frac * M/|pi|                  (field moves at most frac*M per step)
    Applied only when pi < 0 (approaching the wall); when receding, h is unconstrained.
    Hard reflection at phi <= 0 as in the shipped code (should never fire if the cap works).
    """
    potential, coupling, policy, rhs, sup = build(beta, M_Mp)
    cnt = Counter(rhs, sup, total=total_rhs)
    M = potential._M_float
    y = np.array(state0, dtype=float)
    N = N0
    t0 = time.perf_counter()
    nsteps, hard, restarts = 0, 0, 0
    n_rejected_by_exception = 0
    n_reflect, reflect_N = 0, []
    failed = None
    turns = []
    last_pi = y[1]
    sN, sphi, spi, sh = [N], [y[0]], [y[1]], []
    n_capped = 0
    while N < N_end:
        solver = Radau(cnt, N, y, N_end, max_step=h_max_global, rtol=rtol, atol=atol)
        restarts += 1
        while solver.status == "running":
            yy = solver.y
            phi, pi_ = yy[0], yy[1]
            cap = h_max_global
            if cap_kind == "kin":
                # kinematic cap: the inward displacement within the step is bounded by
                # |pi_in| h + 0.5 |a_in| h^2, where a_in is the inward acceleration at the step start
                # (the wall force is purely repulsive, so the inward acceleration cannot grow inside
                # the step). Require that bound to be <= frac * phi.
                a_in = (
                    -solver.f[1] if solver.f is not None else 0.0
                )  # inward (negative-phi) acceleration
                if pi_ < 0.0:
                    cap = min(cap, max(frac * phi / (-pi_), h_floor))
                if a_in > 0.0:
                    cap = min(cap, max(sqrt(2.0 * frac * phi / a_in), h_floor))
            elif pi_ < 0.0 and cap_kind in ("wall", "phi", "dphi"):
                if cap_kind == "wall":
                    st = StateVector._make(yy)
                    pw = phi_wall(policy, st)
                    dist = max(
                        phi - pw, 0.05 * phi
                    )  # never let the cap collapse to zero
                elif cap_kind == "phi":
                    dist = phi
                else:
                    dist = M
                cap = min(cap, max(frac * dist / (-pi_), h_floor))
            if reflect_at_floor and pi_ < 0.0 and frac * phi / (-pi_) < h_floor:
                # The next resolved step would need h < h_floor: the wall is thinner than the
                # representable step. Reflect instantaneously and elastically here: the remaining
                # flight (in to the turning point and back) lasts < 2 h_floor / frac e-folds.
                n_reflect += 1
                reflect_N.append((solver.t, phi, pi_))
                y = solver.y.copy()
                y[1] = -y[1]
                N = solver.t
                turns.append((N, phi, exp(y[4]) / GeV, pi_, -pi_))
                last_pi = -pi_
                break  # restart the solver from the reflected state
            if cap < solver.max_step or cap > solver.max_step:
                solver.max_step = cap
            if solver.h_abs > cap:
                solver.h_abs = cap
                n_capped += 1
            try:
                msg = solver.step()
            except SM.ComputationFailureError as e:
                # a trial state (Newton iterate or Jacobian probe) was unphysical: treat as a rejected step
                n_rejected_by_exception += 1
                if solver.h_abs < 1e-13:
                    msg = f"step size collapsed after trial-state exceptions: {str(e)[:100]}"
                else:
                    solver.h_abs *= 0.5
                    continue
            if msg is not None:
                failed = dict(message=msg, N=solver.t, state=solver.y.copy())
                break
            nsteps += 1
            if jac_factor_max is not None and solver.jac_factor is not None:
                # scipy's num_jac multiplies the perturbation factor of a state component by 10 whenever
                # its Jacobian column is (nearly) zero, with no upper bound; clamp it.
                np.minimum(solver.jac_factor, jac_factor_max, out=solver.jac_factor)
            N, y = solver.t, solver.y.copy()
            if (y[1] < 0) != (last_pi < 0):
                turns.append((N, y[0], exp(y[4]) / GeV, last_pi, y[1]))
            last_pi = y[1]
            if record_steps:
                sN.append(N)
                sphi.append(y[0])
                spi.append(y[1])
                sh.append(solver.t - solver.t_old)
            if y[4] < log_T_stop or (stop_when is not None and stop_when(N, y)):
                N_end = N
                break
            if y[0] <= 0.0:
                hard += 1
                if not hard_reflect:
                    raise RuntimeError(f"phi<0 at N={N}")
                y[1] = abs(y[1])
                y[0] = abs(y[0])
                break  # restart the solver from the reflected state
        if solver.status == "finished" or failed is not None:
            break
    wall = time.perf_counter() - t0
    out = dict(
        strategy=f"velocity[{cap_kind},frac={frac},hmax={h_max_global},jfmax={jac_factor_max}]",
        atol=atol,
        rtol=rtol,
        N_final=N,
        state_final=y,
        failed=failed,
        nfev=cnt.n,
        nsteps=nsteps,
        fragments=restarts,
        hard=hard,
        turns=turns,
        wall=wall,
        n_capped=n_capped,
        policy=policy,
        potential=potential,
        n_caught=cnt.n_caught,
        n_rejected_by_exception=n_rejected_by_exception,
        n_reflect=n_reflect,
        reflect_N=reflect_N,
    )
    if record_steps:
        out["steps"] = (np.array(sN), np.array(sphi), np.array(spi), np.array(sh))
    return out


def first_wall_bounce(res, M_Mp):
    """First turning point with phi < 1.5 M and pi going from negative to positive."""
    for t in res["turns"]:
        N, phi, TJ, pib, pia = t
        if phi < 1.5 * M_Mp and pib < 0 < pia:
            return dict(N=N, phi=phi, T_J_GeV=TJ)
    return None


def wall_bounces(res, M_Mp):
    return [t for t in res["turns"] if t[1] < 1.5 * M_Mp and t[3] < 0 < t[4]]


def summarize(res, M_Mp, label=None):
    fb = first_wall_bounce(res, M_Mp)
    nb = len(wall_bounces(res, M_Mp))
    s = res["state_final"]
    return (
        f"{label or res['strategy']:<28s} tol={np.min(res['atol']):.0e} nfev={res['nfev']:>9d} steps={res['nsteps']:>8d} "
        f"frags={res['fragments']:>4d} hard={res['hard']:>2d} bounces={nb:>3d} "
        f"N_end={res['N_final']:.6f} phi_end={s[0]:.6e} pi_end={s[1]:+.4e} wall={res['wall']:.1f}s"
        + (
            f" | first bounce N={fb['N']:.6f} phi_min={fb['phi']:.5e} T_J={fb['T_J_GeV']*1e3:.4f} MeV"
            if fb
            else " | no wall bounce"
        )
        + (f" | FAILED: {res['failed']['message']}" if res.get("failed") else "")
        + (
            f" | trial RHS exceptions caught={res['n_caught']}"
            if res.get("n_caught")
            else ""
        )
        + (
            f" | steps rejected by exception={res['n_rejected_by_exception']}"
            if res.get("n_rejected_by_exception")
            else ""
        )
        + (
            f" | instantaneous reflections={res['n_reflect']}"
            if res.get("n_reflect")
            else ""
        )
    )


P1 = dict(
    beta=2.0,
    M=0.5,
    N0=20.0016270506,
    state=(
        0.174417541178,
        -0.497741179951,
        -166.040957866,
        -20.8145686243,
        -42.6274655299,
    ),
)
P2 = dict(
    beta=2.0,
    M=0.5,
    N0=25.003077235,
    state=(
        0.0242924190654,
        0.00184365317107,
        -185.456771851,
        -16.7033554358,
        -46.7288490575,
    ),
)
P3 = dict(
    beta=1.2,
    M=0.01,
    N0=32.8965084954,
    state=(
        0.00437706545159,
        -0.0116641233111,
        -232.843822983,
        -5.0399304446,
        -58.2427706427,
    ),
)


# main.py initial data (brief section 2), Planck units
LN_RHO_RAD_J_STAR = -126.1992161962
LN_FM_STAR = -31.0002642242
LN_T_STAR = -32.4334014712


def initial_state(beta, phi_star=5.0, pi_star=0.0):
    return (
        phi_star,
        pi_star,
        LN_RHO_RAD_J_STAR + 4.0 * beta * phi_star,
        LN_FM_STAR,
        LN_T_STAR,
    )
