"""
The reflected state from which the diagnosis's first failure takes its bad step (diagnosis §3 step 3:
beta = 1.5, M = 1e-12 M_P, alpha = 0.05), as float.hex, for the probe's tests
(ComputeTargets/tests/test_probe_outward_fix.py, reflection-restart prompt 01 §1.2).

It also prints the state the reflection was made from (the end of the last accepted step before it,
at the same N), and the ledger before the reflection's own drain, so that a test can start its window
there and let the loop make the reflection and the restart itself.

The history is run tuned under the production code, as tools/history_and_bbn.py runs it (it fails in its
seed integration), observed by tools/outward_step_scan.py. At the Radau instance whose first outward
step is flagged, the frame of integrate_scalar_history that builds it is read: the restarted state and
its N, the realized model's normalization and phi_ref, and the dark-radiation ledger as it stands (its
K_0 and every (N_j, Delta K_j) in order), which a test re-adds to a new ledger in the same order. Printed
as Python literals.

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/measure/restart_state.py \\
        > .documents/reflection-restart/out/restart_state.txt

Needs no Ray and no store. About 5 s.
"""

import contextlib
import importlib
import io
import sys

sys.path.insert(0, ".")
from tools import outward_step_scan as oss  # noqa: E402

SM = importlib.import_module("ComputeTargets.ScalarHistory")
BETA, M, ALPHA = 1.5, 1.0e-12, 0.05

starts = {}


class Capture(oss.Observer):
    def new_instance(self, solver, reason, first_step):
        index = super().new_instance(solver, reason, first_step)
        frame = sys._getframe(
            3
        )  # new_instance <- ObservedRadau.__init__ <- new_solver <- loop
        assert frame.f_code.co_name == "integrate_scalar_history", frame.f_code.co_name
        local = frame.f_locals
        ledger = local["ledger"]
        model = local["policy"].model
        starts[index] = dict(
            N=float(solver.t),
            y=[float(v) for v in solver.y],
            K_initial=float(ledger.K_initial),
            injections=[(float(n), float(k)) for n, k in ledger.injections],
            normalization=float(model.normalization),
            phi_ref=float(model.phi_ref),
            reason=reason,
            # the loop's `solver` is still the instance being replaced: its state is the one the
            # reflection was made from
            pre_y=[float(v) for v in local["solver"].y] if "solver" in local else None,
            pre_N=float(local["solver"].t) if "solver" in local else None,
        )
        return index


observer = Capture(0.1, M, "flagged")
radau = SM.Radau
SM.Radau = oss._observed_radau_class(radau, observer)
try:
    with contextlib.redirect_stdout(io.StringIO()):
        data, _ = oss.run_history(
            {"mode": "tuned", "beta": BETA, "M": M, "alpha": ALPHA}
        )
finally:
    SM.Radau = radau

bad = [r for r in observer.rows if r["flagged"] and r["first"]]
assert len(bad) == 1, bad
row = bad[0]
start = starts[row["instance"]]
print(
    f"# beta={BETA} M={M} alpha={ALPHA}: {data.get('failure_reason', 'success')[:80]}"
)
print(
    f"# the flagged step: instance {row['instance']} ({start['reason']}), N0={row['N0']!r}, "
    f"h={row['h']!r}, h*omega={row['h_omega']:.6g}, pi_end/pi_start={row['pi_ratio']:.6g}"
)
print(f"RESTART_N = float.fromhex({start['N'].hex()!r})")
print("RESTART_STATE = tuple(float.fromhex(x) for x in (")
for v in start["y"]:
    print(f"    {v.hex()!r},")
print("))")
print(f"NORMALIZATION = float.fromhex({start['normalization'].hex()!r})")
print(f"PHI_REF = float.fromhex({start['phi_ref'].hex()!r})")
print(f"K_INITIAL = float.fromhex({start['K_initial'].hex()!r})")
print("LEDGER = tuple((float.fromhex(n), float.fromhex(k)) for n, k in (")
for n, k in start["injections"]:
    print(f"    ({n.hex()!r}, {k.hex()!r}),")
print("))")
print(f"BAD_STEP_H = float.fromhex({row['h'].hex()!r})")
assert start["pre_N"] == start["N"] and start["injections"][-1][0] == start["N"]
print(
    "# the state the reflection was made from, at RESTART_N; the ledger before the reflection's"
)
print("# drain is LEDGER[:-1]")
print("PRE_REFLECTION_STATE = tuple(float.fromhex(x) for x in (")
for v in start["pre_y"]:
    print(f"    {v.hex()!r},")
print("))")
