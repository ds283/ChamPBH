"""
crossover side-campaign, after stage 2: where a history parks if the one-loop law is replaced by stage 2's measured
fraction (README "Stage 2", and the qualitative law discussed with the user on 2026-11-04).

cascade_map.py is run unchanged, with two functions of ComputeTargets.ScalarHistory patched in this process only
(both are looked up by module name at call time; no production code changes):
  - drain_regime: unchanged, but it keeps the candidate's assessment (phi, ln E, ClassicalTurningPoint) so that
    the drain can read ln R and L = M/phi_t;
  - drain_alpha_effective: min(DRAIN_ALPHA_CAP, A f) w_gate, with f stage 2's fraction:
      one-loop, ell >= 1:   f = 1 - exp(-2.69/(ell + 0.12))          (the fit to stage 2's curve rows, ell 1-33)
      one-loop, ell < 1:    f = 0.9                                   (the plateau, stage 2: 0.83-0.97)
      gap:                  f interpolated in ln(R S(L)) on 2197's gap rows of out/stage2_scan.txt
                            (0.826 at ln RS = 8.35 and above, to 0.0027 at -4.67);
      classical (R <= 1):   f = R S(L) g(ln R), g from V6 at L = 0.678 (0.29 at R = 1, 1 for R << 1);
      unassessed:           0.
    S(L) is out/stage1a_S_of_L.txt, log-interpolated, held at its end values outside L = 0.3-134.

The gap and classical pieces were measured on the laboratory-M curve (ln(V0/M^4) = 3.345); stage 2 found the fraction
depends on V0/M^4 too, so on another curve they are an approximation. The normalization is the task's own tuning
under the old law (or another A's, for a failed tuning); nothing is re-tuned, so the field need not sit at its
minimum today.

    PYTHONPATH=. ./venv/bin/python prompts/one-loop-drain/source/crossover/measured_law_map.py \\
        var/one-loop-drain/02/tasks/P-2197-A1-zero.result.json [TUNING_FROM.result.json] [--law=adopted] > .../out/...

--law=adopted replaces the law above by the one the user adopted on 2026-11-05 (fraction_adopted).
"""

import os
import sys
from math import exp, log

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ".")
sys.path.insert(0, HERE)

import cascade_map  # noqa: E402

SM = cascade_map.SM

_a = np.loadtxt(
    os.path.join(HERE, "out/stage1a_S_of_L.txt"), comments="#", usecols=(0, 5)
)
_lL, _lS = np.log(_a[:, 0]), np.log(_a[:, 1])

# 2197's gap rows (out/stage2_scan.txt): ln(R S(L)) and the fraction
_GAP_LNRS = np.log([9.36e-03, 1.00e-01, 9.99e-01, 8.57e00, 6.99e01, 8.15e02, 4.22e03])
_GAP_F = np.array([0.0027, 0.0591, 0.1594, 0.2819, 0.5223, 0.7734, 0.8264])
# V6: frac/(R S) at L = 0.678 against ln R
_V6_LNR = np.array([-6.0, -4.0, -2.0, 0.0])
_V6_G = np.array([9.3291, 9.1634, 8.0156, 2.7275]) / 9.3552

M_MP = None
_last = {}
_orig_regime = SM.drain_regime


def S_of(L):
    return float(np.exp(np.interp(log(L), _lL, _lS)))


def fraction(regime, ell, turning_point):
    if regime == SM.REGIME_ONE_LOOP:
        return 1.0 - exp(-2.69 / (ell + 0.12)) if ell >= 1.0 else 0.9
    if turning_point is None:
        return 0.0
    L = M_MP / turning_point.phi_t
    lnR = turning_point.log_R
    if regime == SM.REGIME_GAP:
        return float(np.interp(lnR + log(S_of(L)), _GAP_LNRS, _GAP_F))
    if regime == SM.REGIME_CLASSICAL:
        return exp(lnR) * S_of(L) * float(np.interp(lnR, _V6_LNR, _V6_G))
    return 0.0


def fraction_adopted(regime, ell, turning_point):
    """The law the user adopted on 2026-11-05 (README "Other M"; fit_law.py): one-loop
    min(0.95, 1 - exp(-2.69/(ell + 0.969 - 0.41 ln L))) for ell >= 1 and 0.9 below; gap
    1 - exp(-0.0198 (ln R - 1.5601)^1.5472 (1 + 0.841 ln L)) for ln R > 1.5601, else 0; classical R S(L).
    """
    if turning_point is None:
        return 0.9 if regime == SM.REGIME_ONE_LOOP and ell < 1.0 else 0.0
    L = M_MP / turning_point.phi_t
    lnR = turning_point.log_R
    if regime == SM.REGIME_ONE_LOOP:
        if ell < 1.0:
            return 0.9
        d = ell + 0.9691 - 0.41 * log(L)
        return min(0.95, 1.0 - exp(-2.69 / d)) if d > 0.0 else 0.95
    if regime == SM.REGIME_GAP:
        if lnR <= 1.5601:
            return 0.0
        return 1.0 - exp(-0.0198 * (lnR - 1.5601) ** 1.5472 * (1.0 + 0.841 * log(L)))
    if regime == SM.REGIME_CLASSICAL:
        return exp(lnR) * S_of(L)
    return 0.0


LAW = fraction


def drain_regime(model, assessment):
    _last["tp"] = None if assessment is None else assessment[2]
    return _orig_regime(model, assessment)


def drain_alpha_effective(regime, ell, *args):
    # (regime, ell, A, w_gate) before one-loop-drain prompt 02c, (regime, ell, L, ln R, A, w_gate) after it; the
    # law applied is LAW's either way, with the turning point kept by drain_regime
    A, w_gate = args[-2], args[-1]
    return min(SM.DRAIN_ALPHA_CAP, A * LAW(regime, ell, _last.get("tp"))) * w_gate


if __name__ == "__main__":
    import json

    adopted = "--law=adopted" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--law=adopted"]
    M_MP = float(json.load(open(args[0]))["argv"][1])
    SM.drain_regime = drain_regime
    SM.drain_alpha_effective = drain_alpha_effective
    if adopted:
        LAW = fraction_adopted
        print(
            "# adopted-law map: one-loop min(0.95, 1 - exp(-2.69/(ell + 0.969 - 0.41 ln L))) (ell >= 1), 0.9 "
            "(ell < 1); gap 1 - exp(-0.0198 (ln R - 1.56)^1.547 (1 + 0.841 ln L)); classical R S(L); "
            "alpha_eff = min(0.99, A f) w_gate"
        )
    else:
        print(
            "# measured-law map: one-loop 1 - exp(-2.69/(ell + 0.12)) (ell >= 1), 0.9 (ell < 1); gap by ln(R S) on "
            "2197's stage 2 rows; classical R S(L) g(ln R); alpha_eff = min(0.99, A f) w_gate"
        )
    sys.exit(cascade_map.main(*args[:2]))
