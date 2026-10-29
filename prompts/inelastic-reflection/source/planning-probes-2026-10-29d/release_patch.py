# The orchestrator's probe of a release (prompt 07): integrate_scalar_history at 7b75ae7, edited by string
# replacement (each edit asserted to match once) and exec'd in the module's namespace.
#  - in run_parked_phase's check, a tracker below the threshold releases the field at the root of
#    m_eff/H_E = threshold on the step's ParkedSegment (brentq), instead of failing;
#  - cut() returns ("release", N_rel, y_rel); the loop starts a new Radau there, resets the gate's
#    half-oscillation and the infall, and goes on; no second parking (parking is not None).
import importlib, inspect

SM = importlib.import_module("ComputeTargets.ScalarHistory")
src = inspect.getsource(SM.integrate_scalar_history)

EDITS = [
    # the release at the root on the segment
    (
        """            if not tracker_adiabatic(m_eff_over_H_):
                raise failure(""",
        """            if not tracker_adiabatic(m_eff_over_H_) and PROBE["release"]:
                def g_(N__):
                    return guarded(tracker_m_eff_over_H, policy, hubble, N__, segment(N__))[0] - PARKING_ADIABATICITY_THRESHOLD
                t0_ = ts[-2]
                N_rel_ = brentq(g_, t0_, N_) if g_(t0_) > 0.0 else t0_
                ts[-1] = N_rel_
                PROBE["release_m_over_H_at_step_end"] = m_eff_over_H_
                PROBE["release_step"] = (t0_, N_)
                raise _Release(N_rel_, segment(N_rel_), parked_steps, min_m_eff_over_H)
            if not tracker_adiabatic(m_eff_over_H_):
                raise failure(""",
    ),
    (
        """                N_end, final_state_, parked_steps, min_m_eff_over_H = run_parked_phase(
                    parking, tracker.phi_root, y_parked
                )""",
        """                try:
                    N_end, final_state_, parked_steps, min_m_eff_over_H = run_parked_phase(
                        parking, tracker.phi_root, y_parked
                    )
                except _Release as rel_:
                    N_rel_, y_rel_, parked_steps, min_m_eff_over_H = rel_.args
                    parking = parking._replace(parked_steps=parked_steps, min_m_eff_over_H=min_m_eff_over_H)
                    PROBE["release"] = (N_rel_, y_rel_)
                    PROBE["release_record"] = (N_rel_, tuple(y_rel_), parked_steps, min_m_eff_over_H, accepted_steps)
                    return ("release", N_rel_, y_rel_)""",
    ),
    # never park again
    (
        "        if parking_ratio_test(index, ratio):",
        "        if parking is None and parking_ratio_test(index, ratio):",
    ),
    # the reflection branch
    (
        """                    parked = cut(N, y_reflected, kind, pi_)
                    if parked is not None:""",
        """                    parked = cut(N, y_reflected, kind, pi_)
                    if parked is not None and parked[0] == "release":
                        solver = new_solver(parked[1], np.array(parked[2], dtype=float))
                        boundary_N = float(parked[1]); running_max = None; infall_assessment = None
                        continue
                    if parked is not None:""",
    ),
    # the drain-point branch
    (
        """                        if parked is not None:
                            N_final, final_state = parked
                            break
                    if y_drained is not None:
                        solver = new_solver(solver.t, y_drained)""",
        """                        if parked is not None and parked[0] == "release":
                            solver = new_solver(parked[1], np.array(parked[2], dtype=float))
                            boundary_N = float(parked[1]); running_max = None; infall_assessment = None
                            y_drained = None
                        elif parked is not None:
                            N_final, final_state = parked
                            break
                    if y_drained is not None:
                        solver = new_solver(solver.t, y_drained)""",
    ),
]
for old, new in EDITS:
    assert src.count(old) == 1, old[:80]
    src = src.replace(old, new)


class _Release(Exception):
    pass


PROBE = {"release": True}
ns = dict(vars(SM))
ns["_Release"] = _Release
ns["PROBE"] = PROBE
exec(compile(src, "integrate_scalar_history(orch07)", "exec"), ns)
patched = ns["integrate_scalar_history"]
