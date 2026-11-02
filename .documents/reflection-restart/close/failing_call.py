"""
reflection-restart prompt 04 §1.2 (c): the failing call alone. For 2197 and 2188 (the inputs the drain probe's logs
print, prompt §0.2) and for the nine of prompt §0.2 (their stored failure_reason, read read-only from the 2026.8.0
store; phi, ln E and the normalization are complete there, the probes are truncated), call the model's
classical_turning_point(normalization, phi, ln E) with exactly those three, on a model built as the scanner builds
it (tools/outward_step_scan.py run_history: ExponentialModel(M = M/M_P x PlanckMass, n = 1, beta)), and report:

- whether it raises, and with what message (compared with the probe's printed message for 2197 and 2188, which is
  complete, and with the stored prefix for the nine), and how long the call takes;
- each probe of the search decoded: ln V - ln V_floor there, in quanta of ulp(max(|ln V|, |ln V_floor|)), with
  -inf where _log_V_minus_floor returns -inf (<= WALL_ROUNDING_QUANTA quanta);
- the root's margin: ln V - ln V_floor at the target ln(1 + E/V_floor), in the same quanta, against
  WALL_ROUNDING_QUANTA (ruling 7's bound is WALL_ROUNDING_QUANTA + 1);
- the spacing of the bracket at the raise, in units of 2^-WALL_SEARCH_HALVINGS e-folds.

Runs on either tree (check_tree, ruling 3). Run from the repository root (or the 89a0760 worktree's):

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/close/failing_call.py
"""

import math
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import check_tree  # noqa: E402

STORE = "/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0"
NORMALIZATION = -276.63072859401797

# the drain probe's logs (one-loop-drain:prompts/one-loop-drain/source/probe/drain/results/*-base-stored-alpha.log)
PROBE = {
    2197: dict(
        beta=1.185,
        M=4e-31,
        phi=2.950090845531173e-07,
        log_E=-306.7166549210319,
        bracket=(-42.28625968598544, -42.34875968598544),
    ),
    2188: dict(
        beta=1.06,
        M=4e-31,
        phi=2.647733963294608e-07,
        log_E=-306.6698578858307,
        bracket=(-42.269391484964466, -42.331891484964466),
    ),
}
NINE = (
    ("shard0002", 1592),
    ("shard0003", 2208),
    ("shard0004", 2199),
    ("shard0000", 1001),
    ("shard0001", 766),
    ("shard0015", 403),
    ("shard0000", 2735),
    ("shard0015", 2480),
    ("shard0015", 2386),
)
PATTERN = re.compile(
    r"normalization = (?P<norm>[-0-9.e+]+), phi = (?P<phi>[-0-9.e+]+), log_E = (?P<logE>[-0-9.e+]+)\)"
)
PROBE_PATTERN = re.compile(r"\(ln phi = (?P<x>[-0-9.e+]+), (?P<r>[-0-9.e+inf]+)\)")


def quanta(d, log_V, log_V_floor):
    return d / math.ulp(max(abs(log_V), abs(log_V_floor)))


def run_call(AM, model, normalization, phi, log_E):
    start = time.perf_counter()
    try:
        result = model.classical_turning_point(normalization, phi, log_E)
        message = None
    except Exception as e:  # ComputationFailureError
        result = None
        message = getattr(e, "message", str(e))
    return result, message, time.perf_counter() - start


def report(
    AM,
    units,
    model,
    label,
    normalization,
    phi,
    log_E,
    expected=None,
    stored_prefix=None,
):
    K, H = AM.WALL_ROUNDING_QUANTA, AM.WALL_SEARCH_HALVINGS
    log_V_floor = model._log_V_floor(normalization)
    result, message, dt = run_call(AM, model, normalization, phi, log_E)
    d_root = math.log1p(math.exp(log_E - log_V_floor))
    root_q = quanta(d_root, log_V_floor + d_root, log_V_floor)
    print(
        f"== {label}: phi = {phi!r} ({phi / units.PlanckMass:.6g} M_P), log_E = {log_E!r}, "
        f"normalization = {normalization!r}"
    )
    print(f"   call time {dt * 1e3:.2f} ms; raises: {message is not None}")
    if message is None:
        print(f"   returned {result!r}")
    else:
        print(f"   message: {message}")
        if expected is not None:
            print(f"   equal to the probe's printed message: {message == expected}")
        if stored_prefix is not None:
            print(
                f"   the stored failure_reason ({len(stored_prefix)} chars) is a prefix of it: "
                f"{message.startswith(stored_prefix)}"
            )
    print(
        f"   root: ln V - ln V_floor = ln(1 + E/V_floor) = {d_root:.6g}; ulp = "
        f"{math.ulp(abs(log_V_floor)):.6g}; margin = {root_q:.4f} quanta "
        f"(WALL_ROUNDING_QUANTA = {K}; within K + 1 = {K + 1}: {root_q <= K + 1})"
    )
    if message is not None:
        probes = [
            (float(m["x"]), float(m["r"])) for m in PROBE_PATTERN.finditer(message)
        ]
        print(
            f"   {len(probes)} probes, decoded (ln phi, residual, ln V - ln V_floor in quanta):"
        )
        for x, r in probes:
            p = math.exp(x)
            log_V = model._log_V(normalization, p)
            q = quanta(log_V - log_V_floor, log_V, log_V_floor)
            print(f"     ln phi = {x:.17g}  residual = {r!r:>22}  quanta = {q:.4f}")
        br = re.search(r"bracket \[(?P<a>[-0-9.e+]+), (?P<b>[-0-9.e+]+)\]", message)
        if br:
            a, b = float(br["a"]), float(br["b"])
            print(
                f"   the raise's bracket width {abs(a - b)!r} = 2^{math.log2(abs(a - b)):.3f} e-folds; its "
                f"midpoint is 2^{math.log2(abs(a - b) / 2):.3f} from each end; 07b's derivation orders probes "
                f"2^-{H} apart"
            )
    return message, root_q, dt


def main():
    SM, scan, head = check_tree()
    import importlib

    AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")
    from CosmologyConcepts import M_value, beta_value
    from CosmologyConcepts.Models.ExponentialModel import ExponentialModel
    from Units import Planck_units

    units = Planck_units()
    print(
        f"WALL_ROUNDING_QUANTA = {AM.WALL_ROUNDING_QUANTA}, WALL_SEARCH_HALVINGS = {AM.WALL_SEARCH_HALVINGS}, "
        f"WALL_SEARCH_MAX_DOUBLINGS = {AM.WALL_SEARCH_MAX_DOUBLINGS}"
    )

    def model_for(beta, M):
        return ExponentialModel(
            0, M_value(0, M * units.PlanckMass), 1, beta_value(0, beta), units
        )

    # the probe's printed messages, for an exact comparison
    probe_logs = {}
    for serial in PROBE:
        path = Path(f"var/reflection-restart/04/probe-{serial}-base-stored-alpha.log")
        if path.exists():
            for line in path.read_text().splitlines():
                if line.strip().startswith("classical_turning_point"):
                    probe_logs[serial] = line.strip()

    rows = []
    for serial, p in PROBE.items():
        msg, q, dt = report(
            AM,
            units,
            model_for(p["beta"], p["M"]),
            f"{serial} (beta {p['beta']}, the probe)",
            NORMALIZATION,
            p["phi"],
            p["log_E"],
            expected=probe_logs.get(serial),
        )
        rows.append(
            (
                serial,
                p["beta"],
                p["M"],
                None,
                p["phi"],
                p["log_E"],
                q,
                msg is not None,
                dt,
            )
        )

    from tools.outward_step_scan import connect_ro

    for shard, serial in NINE:
        conn = connect_ro(f"{STORE}-{shard}.db")
        r = conn.execute(
            """select h.failure_reason, b.value as beta, m.value_eV as M_eV, a.value as alpha
               from ScalarHistory h join ExponentialModel e on h.model_serial = e.serial
               join beta_value b on e.beta_serial = b.serial join M_value m on e.M_serial = m.serial
               join alpha_value a on h.alpha_serial = a.serial where h.serial = ?""",
            (serial,),
        ).fetchone()
        conn.close()
        reason = r["failure_reason"]
        m = PATTERN.search(reason)
        norm, phi, log_E = float(m["norm"]), float(m["phi"]), float(m["logE"])
        M_Mp = scan._M_over_Mp(r["M_eV"], units)
        M_stored = r["M_eV"] * units.eV
        M_tool = M_Mp * units.PlanckMass
        print(
            f"-- {shard}#{serial}: stored M = {r['M_eV']!r} eV; as the tool builds it, M/M_P = {M_Mp!r}; "
            f"bitwise equal: {M_stored == M_tool} (relative difference {abs(M_stored - M_tool) / M_tool:.3g})"
        )
        msg, q, dt = report(
            AM,
            units,
            model_for(r["beta"], M_Mp),
            f"{shard}#{serial} (beta {r['beta']}, " f"alpha {r['alpha']})",
            norm,
            phi,
            log_E,
            stored_prefix=reason,
        )
        if M_stored != M_tool:
            model_s = ExponentialModel(
                0, M_value(0, M_stored), 1, beta_value(0, r["beta"]), units
            )
            _, msg_s, _ = run_call(AM, model_s, norm, phi, log_E)
            print(
                f"   with the stored M instead: raises {msg_s is not None}; "
                f"prefix of the stored reason: {msg_s is not None and msg_s.startswith(reason)}"
            )
        rows.append(
            (serial, r["beta"], M_Mp, r["alpha"], phi, log_E, q, msg is not None, dt)
        )

    print()
    print(
        "serial  beta     M/M_P   alpha   phi/M_P        ln E                 root margin (quanta)  raises  ms"
    )
    for serial, beta, M, alpha, phi, log_E, q, raises, dt in rows:
        print(
            f"{serial:>6}  {beta:<7} {M:<7g} {'' if alpha is None else alpha:<7} {phi:<14.6g} {log_E!r:<20} "
            f"{q:>10.4f}            {str(raises):<6}  {dt * 1e3:.2f}"
        )
    worst = max(r[6] for r in rows)
    print(
        f"largest root margin {worst:.4f} quanta; every root within WALL_ROUNDING_QUANTA + 1 = "
        f"{AM.WALL_ROUNDING_QUANTA + 1}: {worst <= AM.WALL_ROUNDING_QUANTA + 1}; every call raises: "
        f"{all(r[7] for r in rows)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
