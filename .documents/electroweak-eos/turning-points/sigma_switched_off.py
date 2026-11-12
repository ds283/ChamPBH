"""
Probe (electroweak-eos prompt 01a): the early pair made on the old table, for the integration test of
test_resolved_turning_points. The cosmology's w is patched (mock.patch.object on the instance) so that
Sigma = 1 - 3w is the table's less a ramp, linear in ln T, from 0 at T_lo to Sigma(T_cut) at T_cut, and
zero above T_cut: continuous, with a kink at T_cut, as prompt 01's table has at its top row (386.7 GeV).
Histories at the golden normalization, phi_ref = 0, phi* = 5 M_P, pi* = 0, T* = 2e4 GeV, to 100 GeV,
alpha = 0. For each: the turning points with |phi - phi*| < 1e-6 M_P, the count of dropped pairs where
the field exists, and on the step where ln T_J crosses ln T_cut, pi at the nodes, the largest pi > 0
over the ten samples and tau = 1e-8 + 1e-8 max(|pi_low|, |pi_high|).

  PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/turning-points/sigma_switched_off.py \
      T_CUT_GEV T_LO_GEV BETA,M [BETA,M ...]
No store, no Ray; about 0.1 s a history at M = 1e-3 M_P.
"""

import contextlib
import importlib
import io
import sys
import time
from math import exp, log
from unittest import mock

import numpy as np

from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import golden_tuned_history as gth
from CosmologyConcepts import M_value, beta_value, temperature
from CosmologyConcepts.FieldValues import phi_value, pi_value
from CosmologyConcepts.Models.ExponentialModel import ExponentialModel

SM = importlib.import_module("ComputeTargets.ScalarHistory")


def main():
    cos = gth.make_cosmology()
    u = cos.units
    GeV, Mp = u.GeV, u.PlanckMass
    w0 = cos.w
    T_cut = float(sys.argv[1]) * GeV
    T_lo = float(sys.argv[2]) * GeV
    S_cut = 1 - 3 * w0(T_cut)

    def w_patched(T):
        T = float(T)
        if T >= T_cut:
            return 1.0 / 3.0
        if T <= T_lo:
            return w0(T)
        x = (log(T) - log(T_lo)) / (log(T_cut) - log(T_lo))
        return (1 - ((1 - 3 * w0(T)) - S_cut * x)) / 3

    print(f"has resolved_sign_changes: {hasattr(SM, 'resolved_sign_changes')}")
    for arg in sys.argv[3:]:
        beta, M = (float(x) for x in arg.split(","))
        model = ExponentialModel(0, M_value(0, M * Mp), 1, beta_value(0, beta), u)
        rm = model.realize(gh.normalization(u), 0.0)
        cap = []
        real = SM.integrate_scalar_history

        def c(*a, **k):
            r = real(*a, **k)
            cap.append(r)
            return r

        t0 = time.time()
        with mock.patch.object(cos, "w", w_patched), mock.patch.object(
            SM, "integrate_scalar_history", c
        ), contextlib.redirect_stdout(io.StringIO()):
            d = SM.integrate_history(
                cos,
                temperature(0, 2e4 * GeV),
                temperature(1, 100 * GeV),
                phi_value(0, 5 * Mp),
                pi_value(0, 0.0),
                gh.make_z_grid(),
                rm,
                task_label="probe",
                compute_sample_masses=False,
            )
        dt = time.time() - t0
        r = cap[-1]
        early = [
            (round(t.N, 5), t.kind, round(exp(t.log_T_Jordan) / GeV, 2))
            for t in r.turning_points
            if abs(t.phi_Einstein / Mp - 5) < 1e-6
        ]
        print(
            f"beta={beta} M={M}: {'FAIL ' if d.get('failure') else ''}steps {r.accepted_steps}, "
            f"early TPs {early}, unresolved {getattr(r, 'unresolved_turning_point_pairs', None)}, "
            f"dropped_drains {r.dropped_drains}, {dt:.2f} s"
        )
        ts = list(r.solution.ts)
        for k, it in enumerate(r.solution.interpolants):
            lo, hi = ts[k], ts[k + 1]
            if it(lo)[4] >= log(T_cut) > it(hi)[4]:
                Nin = np.linspace(lo, hi, 10)[1:-1]
                pis = [it(lo)[1]] + list(it(Nin)[1]) + [it(hi)[1]]
                tau = 1e-8 + 1e-8 * max(abs(pis[0]), abs(pis[-1]))
                pos = [p for p in pis if p > 0]
                print(
                    f"   step [{lo:.6f}, {hi:.6f}], T_J {exp(it(lo)[4]) / GeV:.2f} -> "
                    f"{exp(it(hi)[4]) / GeV:.2f} GeV: pi nodes {pis[0]:.4g}, {pis[-1]:.4g}; "
                    f"largest pi > 0 {max(pos) if pos else None}; tau {tau!r}"
                )
                break


if __name__ == "__main__":
    main()
