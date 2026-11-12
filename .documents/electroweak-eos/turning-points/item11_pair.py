"""
Probe (electroweak-eos prompt 01a): test_injection's item (11) (beta = 10, M = 0.5 M_P, alpha = 0.1,
PARKING_EPSILON = 0.0), integrated through test_injection.integrate_captured: every step holding two or
more turning points, with its ten samples of pi (as observe_turning_points reads them), the excursion
of the pair and tau = 1e-8 + 1e-8 max(|pi_low|, |pi_high|).

  PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/turning-points/item11_pair.py
One integration, about 9 s.
"""

import importlib
import time
from bisect import bisect_right
from unittest import mock

import numpy as np

import ComputeTargets.tests.test_injection as ti

SM = importlib.import_module("ComputeTargets.ScalarHistory")


def main():
    t0 = time.time()
    with mock.patch.object(SM, "PARKING_EPSILON", 0.0):
        data, res, _ = ti.integrate_captured(10.0, 0.5, 0.1)
    print(
        f"integration {time.time() - t0:.2f} s; turning points {len(res.turning_points)}; "
        f"unresolved {getattr(res, 'unresolved_turning_point_pairs', None)}"
    )
    ts = list(res.solution.ts)
    its = res.solution.interpolants
    per = {}
    for t in res.turning_points:
        k = min(bisect_right(ts, t.N) - 1, len(its) - 1)
        per.setdefault(k, []).append(t)
    for k, v in per.items():
        if len(v) >= 2:
            it = its[k]
            lo, hi = ts[k], ts[k + 1]
            Nin = np.linspace(lo, hi, 10)[1:-1]
            pis = (
                [float(it(lo)[1])] + [float(x) for x in it(Nin)[1]] + [float(it(hi)[1])]
            )
            tau = 1e-8 + 1e-8 * max(abs(pis[0]), abs(pis[-1]))
            exc = max(-p for p in pis if p < 0)
            print(
                f"step {k} [{lo!r}, {hi!r}]: {[t.kind for t in v]} at {[t.N for t in v]}"
            )
            print(f"  pi samples {pis}")
            print(f"  excursion {exc!r}, tau {tau!r}, excursion/tau {exc / tau:.4g}")


if __name__ == "__main__":
    main()
