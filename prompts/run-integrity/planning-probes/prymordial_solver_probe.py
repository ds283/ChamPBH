"""
Planning probe for the run-integrity campaign: what PRyMordial does when one of
its solve_ivp integrations fails, and when a new-physics callback returns NaN.

It wraps PRyM_main.solve_ivp (the name PRyM_main imported) to record every call's
source line, method, status and end point. From the repository root:

    PYTHONPATH=. ./venv/bin/python -u -W ignore prompts/run-integrity/planning-probes/prymordial_solver_probe.py [sm] [truncated] [nan]

  sm         the SM baseline (rho_NP = 0), full network. About 8 s.
  truncated  the SM baseline, but the last nuclear solve (the full network's
             low-temperature BDF solve) integrates over only the first 1 % of
             its span and is then marked failed, as solve_ivp marks a solver
             that gives up. About 8 s.
  nan        rho_NP = NaN below 0.1 MeV. Stopped by an alarm after 60 s.

With no argument it runs all three.
"""

import inspect
import signal
import sys
import time

from ComputeTargets.BBNData import _configure_PRyMordial

PRyMmain = _configure_PRyMordial(False)
_solve_ivp = PRyMmain.solve_ivp

calls = []
truncate_line = None


def spy(fun, t_span, y0, **kw):
    line = inspect.stack()[1].lineno
    print(f"   entering solve_ivp at PRyM_main.py:{line}, t_span={t_span}", flush=True)
    if line == truncate_line:
        t0, t1 = float(t_span[0]), float(t_span[1])
        sol = _solve_ivp(fun, [t0, t0 + 0.01 * (t1 - t0)], y0, **kw)
        sol.status, sol.success = -1, False
        sol.message = "forced failure (probe)"
    else:
        sol = _solve_ivp(fun, t_span, y0, **kw)
    calls.append(
        (
            line,
            kw.get("method"),
            sol.status,
            sol.success,
            float(sol.t[-1]),
            float(t_span[1]),
        )
    )
    return sol


PRyMmain.solve_ivp = spy


class Alarm(Exception):
    pass


def _alarm(signum, frame):
    raise Alarm()


signal.signal(signal.SIGALRM, _alarm)


def run(name, rho, p, drho, timeout=None):
    calls.clear()
    t0 = time.time()
    if timeout is not None:
        signal.alarm(timeout)
    try:
        res = PRyMmain.PRyMclass(rho, p, drho).PRyMresults()
        out = f"returned Yp={res[4]:.10g}, D/H x1e5={res[5]:.10g}, 7Li/H x1e10={res[7]:.10g}"
    except Alarm:
        out = f"NO RETURN within {timeout} s (stopped by the probe's alarm)"
    except Exception as e:
        out = f"raised {type(e).__name__}: {str(e)[:100]}"
    finally:
        signal.alarm(0)
    print(f"== {name} ({time.time() - t0:.1f} s): {out}")
    for line, method, status, success, t_end, t_target in calls:
        print(
            f"   PRyM_main.py:{line} {method}: status={status} success={success} "
            f"t reached {t_end:.4g} of {t_target:.4g}"
        )


def zero(T):
    return 0.0


which = sys.argv[1:] or ["sm", "truncated", "nan"]

if "sm" in which:
    run("SM baseline, full network", zero, zero, zero)

if "truncated" in which:
    truncate_line = 1252
    run("SM baseline, last solve fails after 1 % of its span", zero, zero, zero)
    truncate_line = None

if "nan" in which:
    run(
        "rho_NP = NaN below 0.1 MeV",
        lambda T: float("nan") if T < 0.1 else 0.0,
        zero,
        zero,
        timeout=60,
    )
