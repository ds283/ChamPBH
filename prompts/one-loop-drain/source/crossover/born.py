"""
crossover side-campaign, stage 1b: the Born-regime one-loop scheme of wall_backreaction.tex ("The scheme solved in
the Born regime", scripts/return_fraction.py), generalized to any wall shape L and loop strength R, and the
production on its trajectory computed two ways.

Units are walls.py's: M = 1, omega_t = 1, U = c(e^{1/phi} - 1). The physical one-loop drag
D = -(V'''/16 pi^2) int dt' (dV''/dt') Ci[2 k_IR (t - t')] becomes, in these units,

    phi'' = -U'(phi) + G V'''(phi) I(t),     G = R E / 2,     R = omega_t^4 / (8 pi^2 E_phys),

since omega_t^2/M^2 = 8 pi^2 R E in units where E = c(e^L - 1) is the kinetic energy. The kernel integral I is
return_fraction.py's, with the logarithmic endpoint subtracted and integrated in closed form. k_IR is the infrared
end; the note puts it at the mode mass, and run_self_consistent iterates k_IR = sqrt(V''_max) until it holds to 1e-3.

On the trajectory, the production is computed
  - as the scheme gives it: E_fl = E - phi'^2/2 - U at the end (the note's "exact in the scheme");
  - by the Born formula, (1/8 pi^2) int_{k_IR} k |FT[V''](2k)|^2 dk, massless modes (the note's cross-check);
  - with massive mode functions, x'' + (k^2 + V''(phi(t))) x = 0 exactly, first-order adiabatic in and out, and
    rho = (1/2 pi^2) int k^2 omega_out |beta_k|^2 dk over every k (no infrared cutoff: the mass regulates it).
Each, as a fraction of E_phys, is the quantity times 8 pi^2 R for the two mode integrals (energy density in units
omega_t^4), and E_fl/E for the first.
"""

from math import log, pi, sqrt

import numpy as np
from scipy.integrate import solve_ivp
from scipy.interpolate import CubicSpline
from scipy.special import sici

from walls import RTOL, ATOL, Wall, wkb_state

EULER = 0.5772156649015329


def Ci(x):
    x = np.asarray(x, dtype=float)
    out = np.empty_like(x)
    small = x < 1e-6
    out[~small] = sici(x[~small])[1]
    out[small] = EULER + np.log(np.maximum(x[small], 1e-300)) - x[small] ** 2 / 4
    return out


def chain(w: Wall, R: float):
    """The one-loop chain at the wall's kinetic energy (AbstractModel.one_loop_turnaround, in these units):
    m^4 ell = 32 pi^2 E_phys -> in units omega_t = 1, m^4 ell = 4/R; V''(phi_ta) = m^2; k_ex = lambda sqrt(E)/2
    with lambda = -d ln V/dphi on the full V = V0 + U; returns (phi_ta, m_ta, k_ex, ell) or None.
    """
    E = w.E
    V0 = w.c

    def lnV(p):
        return np.log(V0 + w.U(p))

    def lam(p):
        return -w.dU(p) / (V0 + w.U(p))

    ell = 32.0
    for _ in range(200):
        m = (4.0 / (R * ell)) ** 0.25
        # phi_ta: V''(phi) = m^2, searched outwards of the classical turning point (V'' falls outwards)
        from scipy.optimize import brentq

        f = lambda p: np.log(w.V2(p)) - 2.0 * np.log(m)  # noqa: E731
        if f(w.phi_t) <= 0:
            return None
        hi = w.phi_t
        while f(hi) > 0:
            hi *= 1.5
        p = brentq(f, w.phi_t, hi, xtol=1e-15, rtol=1e-14)
        kex = lam(p) * sqrt(E) / 2.0
        new = log(kex / m)
        if not new > 0:
            return None
        if abs(new - ell) <= 1e-12 * new:
            return p, m, kex, new
        ell = new
    return None


def run(w: Wall, R: float, kIR: float, phi0=None, steps_per_dt=200, span=3.0):
    """The scheme on the wall at loop strength R and infrared end kIR (velocity Verlet, explicit memory, as in
    return_fraction.py). Starts at phi0 (default: where V'' = 1e-6 of the chain's m_ta^2) with phi' from energy
    conservation, and runs until the field is back out at phi0, or span times the inbound time.
    """
    ch = chain(w, R)
    if ch is None:
        raise ValueError(f"no one-loop chain at L={w.L}, R={R:g}")
    phi_ta, m_ta, kex, ell = ch
    if phi0 is None:
        from scipy.optimize import brentq

        target = 1e-6 * m_ta * m_ta
        hi = phi_ta
        while w.V2(hi) > target:
            hi *= 1.5
        phi0 = brentq(lambda p: np.log(w.V2(p)) - np.log(target), phi_ta, hi)
    G = R * w.E / 2.0
    v = float(w.phi_dot(phi0))
    dt = 1.0 / (kex * steps_per_dt)
    T_in = (phi0 - phi_ta) / v
    N = int(span * T_in / dt) + 2
    t = np.arange(N) * dt
    phi = np.empty(N)
    pd = np.empty(N)
    vdot = np.empty(N)
    phi[0], pd[0] = phi0, -v
    vdot[0] = w.V3(phi0) * pd[0]
    a = 2.0 * kIR

    def memory(n):
        if n == 0:
            return 0.0
        tau = t[n] - t[: n + 1]
        ci = Ci(a * tau[:-1])
        f = np.append((vdot[:n] - vdot[n]) * ci, 0.0)
        Isub = np.trapezoid(f, t[: n + 1])
        x = a * tau[0]
        Ilin = vdot[n] * (x * Ci(np.array([x]))[0] - np.sin(x)) / a
        return Isub + Ilin

    def accel(n):
        return -w.dU(phi[n]) + G * w.V3(phi[n]) * memory(n)

    an = accel(0)
    n_end = N - 1
    for n in range(N - 1):
        phi[n + 1] = phi[n] + pd[n] * dt + 0.5 * an * dt * dt
        if phi[n + 1] <= 0:
            raise RuntimeError("field reached phi <= 0")
        pp = pd[n] + an * dt
        vdot[n + 1] = w.V3(phi[n + 1]) * pp
        an1 = accel(n + 1)
        pd[n + 1] = pd[n] + 0.5 * (an + an1) * dt
        vdot[n + 1] = w.V3(phi[n + 1]) * pd[n + 1]
        an = an1
        if pd[n + 1] > 0 and phi[n + 1] >= phi0:
            n_end = n + 1
            break
    sl = slice(0, n_end + 1)
    t, phi, pd = t[sl], phi[sl], pd[sl]
    Efl = w.E - 0.5 * pd**2 - w.U(phi)
    ita = int(np.argmin(phi))
    return dict(
        L=w.L,
        R=R,
        kIR=kIR,
        chain=dict(phi_ta=phi_ta, m_ta=m_ta, kex=kex, ell=ell),
        t=t,
        phi=phi,
        pd=pd,
        phi_turn=phi[ita],
        m_turn=sqrt(w.V2(phi[ita])),
        Efl_final=Efl[-1] / w.E,
        vout=pd[-1] / v,
        returned=bool(pd[-1] > 0 and phi[-1] >= phi0),
    )


def run_self_consistent(w: Wall, R: float, tol=1e-3, **kw):
    """Iterate k_IR = sqrt(V''_max) from the chain's m_ta."""
    kIR = chain(w, R)[1]
    for _ in range(20):
        res = run(w, R, kIR, **kw)
        new = res["m_turn"]
        if abs(new - kIR) <= tol * kIR:
            res["iterations"] = _ + 1
            return res
        kIR = new
    res["iterations"] = None
    return res


def born_production(w: Wall, res, nk=400):
    """(1/8 pi^2) int_{k_IR}^inf k |FT[V''](2k)|^2 dk on the trajectory, in units omega_t^4."""
    t, vpp = res["t"], w.V2(res["phi"])
    kex = res["chain"]["kex"]
    ks = np.geomspace(res["kIR"], 30.0 * kex, nk)
    spec = np.array([abs(np.trapezoid(vpp * np.exp(-2j * k * t), t)) ** 2 for k in ks])
    return np.trapezoid(ks * spec, ks) / (8.0 * pi**2)


class Trajectory:
    """A tabulated background, splined, for the massive mode functions."""

    def __init__(self, w: Wall, res):
        self.w = w
        self.t0, self.t1 = res["t"][0], res["t"][-1]
        self.phi = CubicSpline(res["t"], res["phi"])
        self.pd = CubicSpline(res["t"], res["pd"])


def massive_beta2(tr: Trajectory, k: float) -> float:
    w = tr.w

    def om(t):
        p, pd = float(tr.phi(t)), float(tr.pd(t))
        ww = sqrt(k * k + w.V2(p))
        return ww, w.V3(p) * pd / (2.0 * ww)

    w0, wd0 = om(tr.t0)
    x0, xd0 = wkb_state(w0, wd0, -1.0)

    def rhs(t, y):
        m2 = k * k + w.V2(float(tr.phi(t)))
        return [y[2], y[3], -m2 * y[0], -m2 * y[1]]

    sol = solve_ivp(
        rhs,
        (tr.t0, tr.t1),
        [x0.real, x0.imag, xd0.real, xd0.imag],
        method="DOP853",
        rtol=RTOL,
        atol=ATOL,
    )
    x = sol.y[0, -1] + 1j * sol.y[1, -1]
    xd = sol.y[2, -1] + 1j * sol.y[3, -1]
    w1, wd1 = om(tr.t1)
    up, upd = wkb_state(w1, wd1, -1.0)
    um, umd = wkb_state(w1, wd1, +1.0)
    beta = (up * xd - upd * x) / (up * umd - upd * um)
    return float(abs(beta) ** 2), w1


def massive_production(w: Wall, res, nk=160, kmin=0.3):
    """(1/2 pi^2) int k^2 omega_out |beta_k|^2 dk, in units omega_t^4, over k from kmin m_ta to 30 k_ex.

    The lower end is where the mode's start is adiabatic: the trajectory starts where V'' = 1e-6 m_ta^2, and a mode
    with k << m_ta is not yet adiabatic there (|omega'|/omega^2 = 1e5 at k = 1e-3 m_ta for ell = 7.4). Below
    k = 0.3 m_ta the modes add nothing measurable once started adiabatically: with kmin from 1e-1 to 0.3 the
    fraction changes in the fourth digit at ell = 3.3, 7.4 and 9.5 (out/stage1b_kmin.txt).
    """
    tr = Trajectory(w, res)
    m, kex = res["m_turn"], res["chain"]["kex"]
    ks = np.geomspace(kmin * m, 30.0 * kex, nk)
    b2, wo = zip(*[massive_beta2(tr, k) for k in ks])
    b2, wo = np.array(b2), np.array(wo)
    return np.trapezoid(ks**2 * wo * b2, ks) / (2.0 * pi**2), ks, b2
