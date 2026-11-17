"""
power-law-wall prompt 07b (P74, P75): the one-loop chain with either definition of k_ex, the closed forms of the change,
and a scratch evaluation of the new form through the production hooks.

**The two forms** (README §6.12; prompt §1.1). k_ex = rate(phi) E^{1/2}/2, at the point the estimate is taken:
  - "lambda": rate = -d ln V/dphi on the full V = V0 + U, as born.chain, stage2.lam and production take it today
    (AbstractModel.classical_turning_point and one_loop_turnaround);
  - "v3": rate = |V'''/V''| (R13-R15), Erickcek et al. (arXiv:1310.5149) Eq. (64) with the dressing V_D ~ (V'')^2.
V'' and V''' are those of U (the floor V0 is a constant). The walls are walls.py's (Wall, WallN, PowerWall), duck-typed:
U, dU, V2, V3, c (the floor), E and phi_t. Three forms exist only for the breakage record (prompt §3) and are never used
for a measurement: "v3_over_V1" (the rate taken as |V'''/V'|), "v3_double" (the factor 1/2 dropped from k_ex) and
"v3_at_phi_t" (the rate evaluated at phi_t inside the one-loop chain).

**chain(w, R, form)** is born.chain (crossover's born.py at 9da5162, unchanged since; copied in crossover/ and wall/)
with the rate made a choice, every other expression and constant unchanged, so that with form "lambda" it returns
born.chain's values bit for bit (rechain.py shows it at every re-chained row). chain_reason gives the same values and,
where there is no chain, which test ended it: "inside" (V''(phi_t) <= m^2 at an iterate, born.chain's first None), "ell"
(an iterate with ell <= 0), or "iterations" (no convergence in 200).

**The closed forms** of new over old, |V'''/V''| / (-d ln V/dphi), at u = (M/phi)^n:
  - exponential, V = V0 e^u: [n^2 u^2 + 3n(n + 1)u + (n + 1)(n + 2)] / [n u (n u + n + 1)] = 1 + O(1/u);
  - power wall, V = V0 (1 + u): (n + 2)(1 + u) / (n u), exactly.

**The production hooks** (prompt §1.2): production_v3_turnaround(model, normalization, phi, ln E) is
AbstractModel.one_loop_turnaround's loop, its search and its constants, with ln k_ex = ln|V'''/V''| + ln E/2 - ln 2,
|V'''/V''| = |(ln V)''' + 3 (ln V)'(ln V)'' + (ln V)'^3| / ((ln V)'' + (ln V)'^2) from the hooks _d_logV_dphi and
_d2_logV_dphi2, and (ln V)''' by a centred difference of _d2_logV_dphi2 with step h = 1e-5 phi / (1 + |(ln V)'| phi).
That step keeps the truncation (h^2 (ln V)^(5)/6 (ln V)''', about 1e-11 relative, since (ln V)'' varies on
phi/(1 + |(ln V)'| phi)) and the rounding (eps (ln V)''/(h (ln V)'''), about 2e-11) both near 1e-11. It is a scratch
evaluation for 07c, not production code. production_v3_classical(model, normalization, phi_t, ln E) gives the classical
turning point's rate likewise.
"""

import importlib
import importlib.util
import os
import sys
from math import exp, log, sqrt

import numpy as np
from scipy.optimize import brentq

HERE = os.path.dirname(os.path.abspath(__file__))
FORMS = ("lambda", "v3")
BREAKAGES = ("v3_over_V1", "v3_double", "v3_at_phi_t")


def rate(w, p, form):
    """The pulse rate at phi = p in the form's definition (k_ex = rate E^{1/2}/2)."""
    if form == "lambda":
        return -w.dU(p) / (w.c + w.U(p))
    if form in ("v3", "v3_double", "v3_at_phi_t"):
        return abs(w.V3(p) / w.V2(p))
    if form == "v3_over_V1":
        return abs(w.V3(p) / w.dU(p))
    raise ValueError(form)


def kex_of(w, p, form):
    """k_ex at phi = p: rate(p) E^{1/2}/2 (the breakage "v3_double" drops the 1/2)."""
    if form == "v3_double":
        return rate(w, p, form) * sqrt(w.E)
    if form == "v3_at_phi_t":
        return rate(w, w.phi_t, form) * sqrt(w.E) / 2.0
    if form == "lambda":
        # born.chain's expression, lam(p) * sqrt(E) / 2.0, kept in its order of operations
        lam = -w.dU(p) / (w.c + w.U(p))
        return lam * sqrt(w.E) / 2.0
    return rate(w, p, form) * sqrt(w.E) / 2.0


def chain_reason(w, R: float, form: str):
    """born.chain with the form's k_ex: ((phi_ta, m_ta, k_ex, ell) or None, reason, iterations)."""
    E = w.E
    ell = 32.0
    for it in range(200):
        m = (4.0 / (R * ell)) ** 0.25
        f = lambda p: np.log(w.V2(p)) - 2.0 * np.log(m)  # noqa: E731
        if f(w.phi_t) <= 0:
            return None, "inside", it + 1
        hi = w.phi_t
        while f(hi) > 0:
            hi *= 1.5
        p = brentq(f, w.phi_t, hi, xtol=1e-15, rtol=1e-14)
        kex = kex_of(w, p, form)
        new = log(kex / m)
        if not new > 0:
            return None, "ell", it + 1
        if abs(new - ell) <= 1e-12 * new:
            return (p, m, kex, new), "ok", it + 1
        ell = new
    return None, "iterations", 200


def chain(w, R: float, form: str):
    """(phi_ta, m_ta, k_ex, ell) or None, as born.chain returns them."""
    return chain_reason(w, R, form)[0]


def classical_kex(w, form):
    """k_ex at the classical turning point phi_t (classical_turning_point's), in the wall's units."""
    return kex_of(w, w.phi_t, form)


def ratio_exponential(u: float, n: int) -> float:
    """|V'''/V''| / (-d ln V/dphi) on V0 e^u, u = (M/phi)^n."""
    return (n * n * u * u + 3.0 * n * (n + 1) * u + (n + 1) * (n + 2)) / (
        n * u * (n * u + n + 1)
    )


def ratio_power(u: float, n: int) -> float:
    """|V'''/V''| / (-d ln V/dphi) on V0 (1 + u), u = (M/phi)^n."""
    return (n + 2) * (1.0 + u) / (n * u)


def regime(lnR: float, ch):
    """drain_regime's classification as the prompt states it (§1.3): classical at ln R <= 0; one-loop where the chain
    has a solution with ell > 0; gap otherwise."""
    if lnR <= 0.0:
        return "classical"
    return "one_loop" if ch is not None else "gap"


def load(directory: str, names):
    """Import the copies `names` (in dependency order) from source/kex/<directory>/ under the module names the copies
    themselves import (walls, born, stage2, ...), with that directory alone first on sys.path. Returns {name: module}.
    One process imports one directory's copies only: the two directories hold modules of the same names.
    """
    path = os.path.join(HERE, directory)
    if path not in sys.path:
        sys.path.insert(0, path)
    mods = {}
    for name in names:
        mod = sys.modules.get(name)
        if mod is not None:
            if os.path.dirname(os.path.abspath(mod.__file__)) != path:
                raise RuntimeError(
                    f"{name} already imported from {mod.__file__}, not from {path}"
                )
        else:
            mod = __import__(name)
        mods[name] = mod
    return mods


def load_aside(directory: str, names, prefix: str):
    """Import copies from source/kex/<directory>/ under prefixed names (prefix_walls, ...), resolving the copies' own
    `import walls` / `import born` to these modules while they load, and restoring sys.modules after. For a check that
    needs both directories' walls in one process (rechain.py); never used with multiprocessing.
    """
    path = os.path.join(HERE, directory)
    saved = {n: sys.modules.get(n) for n in names}
    mods = {}
    try:
        for name in names:
            spec = importlib.util.spec_from_file_location(
                f"{prefix}_{name}", os.path.join(path, f"{name}.py")
            )
            mod = importlib.util.module_from_spec(spec)
            sys.modules[name] = mod
            sys.modules[f"{prefix}_{name}"] = mod
            spec.loader.exec_module(mod)
            mods[name] = mod
    finally:
        for n, m in saved.items():
            if m is None:
                sys.modules.pop(n, None)
            else:
                sys.modules[n] = m
    return mods


# ---- the production hooks (scratch; prompt §1.2) ---------------------------------------------------------------------


def hook_rate(model, normalization, p: float):
    """(|V'''/V''|, -d ln V/dphi) at phi = p from the model's hooks; (ln V)''' by a centred difference of
    _d2_logV_dphi2 (module docstring)."""
    d1 = model._d_logV_dphi(normalization, p)
    d2 = model._d2_logV_dphi2(normalization, p)
    h = 1e-5 * p / (1.0 + abs(d1) * p)
    d3 = (
        model._d2_logV_dphi2(normalization, p + h)
        - model._d2_logV_dphi2(normalization, p - h)
    ) / (2.0 * h)
    q2 = d2 + d1 * d1
    q3 = d3 + 3.0 * d1 * d2 + d1 * d1 * d1
    return abs(q3 / q2), -d1


def production_v3_classical(model, normalization, phi: float, log_E: float):
    """AbstractModel.classical_turning_point's body, its search and its expressions, with ln k_ex from |V'''/V''|
    (hook_rate) in place of -d ln V/dphi. Returns (phi_t, ln omega_t^2, ln R, |V'''/V''|, ln k_ex, ell_t) or None where
    production returns None. Only the rate differs; phi_t, ln omega_t^2 and ln R are computed as production computes
    them (R16), so that they can be compared bit for bit."""
    AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")

    context = model._wall_context(
        "production_v3_classical", normalization, phi=phi, log_E=log_E
    )
    log_V_floor = model._wall_floor(context, normalization, phi, log_E)

    def residual(p: float) -> float:
        return (
            AM._log_V_minus_floor(model._log_V(normalization, p), log_V_floor) - log_E
        )

    x_0 = log(phi)
    r_0 = AM._evaluate(residual, x_0, context)
    if r_0 >= 0.0:
        return None
    if AM._turning_point_unresolvable(log_V_floor, log_E):
        return None
    phi_t = exp(AM._search_log_field(residual, x_0, r_0, -1.0, context))
    d1 = model._d_logV_dphi(normalization, phi_t)
    q = model._d2_logV_dphi2(normalization, phi_t) + d1 * d1
    log_omega_t_squared = model._log_V(normalization, phi_t) + log(q)
    v3, _ = hook_rate(model, normalization, phi_t)
    log_k_ex = log(v3) + 0.5 * log_E - AM._LOG_2
    return (
        phi_t,
        log_omega_t_squared,
        2.0 * log_omega_t_squared - AM._LOG_8_PI_SQUARED - log_E,
        v3,
        log_k_ex,
        log_k_ex - 0.5 * log_omega_t_squared,
    )


def production_v3_turnaround(model, normalization, phi: float, log_E: float):
    """AbstractModel.one_loop_turnaround with ln k_ex from |V'''/V''| (hook_rate). Returns (phi_ta, ln m_ta, ln k_ex,
    ell, iterations) or None where production returns None; raises where production raises.
    """
    AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")

    context = model._wall_context(
        "production_v3_turnaround", normalization, phi=phi, log_E=log_E
    )
    model._wall_floor(context, normalization, phi, log_E)

    def log_V2(p: float) -> float:
        d1 = model._d_logV_dphi(normalization, p)
        q = model._d2_logV_dphi2(normalization, p) + d1 * d1
        if not q > 0.0:
            raise ValueError(f"V'' <= 0 ((ln V)'' + (ln V)'^2 = {q!r})")
        return model._log_V(normalization, p) + log(q)

    x_0 = log(phi)
    log_V2_0 = AM._evaluate(log_V2, x_0, context)
    ell = AM.ONE_LOOP_ELL_START
    for iteration in range(1, AM.ONE_LOOP_MAX_ITERATIONS + 1):
        log_m_ta = 0.25 * (AM._LOG_32_PI_SQUARED + log_E - log(ell))
        target = 2.0 * log_m_ta
        r_0 = log_V2_0 - target
        if r_0 >= 0.0:
            return None

        def residual(p: float) -> float:
            return log_V2(p) - target

        phi_ta = exp(AM._search_log_field(residual, x_0, r_0, -1.0, context))
        v3, _ = hook_rate(model, normalization, phi_ta)
        log_k_ex = log(v3) + 0.5 * log_E - AM._LOG_2
        ell_new = log_k_ex - log_m_ta
        if not ell_new > 0.0:
            raise AM._failure(f"{context}: ell fell to {ell_new!r}")
        if abs(ell_new - ell) <= AM.ONE_LOOP_ELL_RTOL * ell_new:
            return phi_ta, log_m_ta, log_k_ex, ell_new, iteration
        ell = ell_new
    raise AM._failure(f"{context}: no convergence")
