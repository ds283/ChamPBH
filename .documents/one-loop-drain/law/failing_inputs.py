"""
one-loop-drain prompt 01 §2.1.3 (orchestrator note 01, ruling 6): the wall-search fix answers the 19 failing
inputs, and the predicate holds at each.

The inputs are the test module's literals (ComputeTargets/tests/test_unresolvable_turning_points.py,
FAILING_INPUTS). This script shows that they are their sources' floats:
  - "probe 2197", "probe 2188": the drain probe's logs (var/reflection-restart/04/probe-*-base-stored-alpha.log,
    whose classical_turning_point line is the whole message);
  - "stored ...": the stored failure_reason of the nine of reflection-restart 04 §0.2, read read-only (sqlite
    mode=ro) from the 2026.8.0 store, with M/M_P as the scanner computes it from the stored M in eV;
  - "fixed tree ...": the failure_reason of the eight failures of var/reflection-restart/04/the-nine.jsonl.
For each it reports: the least finite value of _log_V_minus_floor at the input's ln V_floor, ln E's distance
below it, the predicate, what classical_turning_point returns now, and what the pre-fix copy (the test
module's classical_turning_point_before_fix) raises, whole, with whether the source's message (or stored
prefix) equals it (or is a prefix of it). Then the boundary at the two normalizations of the tests.

Run from the repository root:

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/failing_inputs.py > .documents/one-loop-drain/law/out/failing_inputs.txt
"""

import importlib
import json
import re
import subprocess
import sys
import time
from math import exp, inf, log1p, nextafter, ulp
from pathlib import Path

STORE = "/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0"
RR04 = Path("/Users/ds283/Documents/Code/ChamPBH/var/reflection-restart/04")
PATTERN = re.compile(
    r"normalization = (?P<norm>[-0-9.e+]+), phi = (?P<phi>[-0-9.e+]+), log_E = (?P<logE>[-0-9.e+]+)\)"
)


def tree():
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True
    ).stdout.strip()
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        capture_output=True,
        text=True,
    ).stdout.strip()
    print(
        f"tree at {head}; tracked changes: {'none' if not status else status.replace(chr(10), '; ')}"
    )


def sources(units):
    """{source label: (beta, M/M_P, normalization, phi/M_P, ln E, message or prefix, whole?)} from the sources."""
    from tools.outward_step_scan import _M_over_Mp, connect_ro

    out = {}
    for serial in (2197, 2188):
        for line in (
            (RR04 / f"probe-{serial}-base-stored-alpha.log").read_text().splitlines()
        ):
            if line.strip().startswith("classical_turning_point"):
                m = PATTERN.search(line)
                out[f"probe {serial}"] = (
                    None,
                    None,
                    float(m["norm"]),
                    float(m["phi"]),
                    float(m["logE"]),
                    line.strip(),
                    True,
                )
    nine = (
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
    for shard, serial in nine:
        conn = connect_ro(f"{STORE}-{shard}.db")
        r = conn.execute(
            """select h.failure_reason, b.value as beta, m.value_eV as M_eV
               from ScalarHistory h join ExponentialModel e on h.model_serial = e.serial
               join beta_value b on e.beta_serial = b.serial join M_value m on e.M_serial = m.serial
               where h.serial = ?""",
            (serial,),
        ).fetchone()
        conn.close()
        m = PATTERN.search(r["failure_reason"])
        out[f"stored {shard}#{serial}"] = (
            r["beta"],
            _M_over_Mp(r["M_eV"], units),
            float(m["norm"]),
            float(m["phi"]),
            float(m["logE"]),
            r["failure_reason"],
            False,
        )
    for line in (RR04 / "the-nine.jsonl").read_text().splitlines():
        d = json.loads(line)
        if d["outcome"] == "success":
            continue
        m = PATTERN.search(d["failure_reason"])
        key = d["key"].split(" ")[0]
        out[f"fixed tree {key}"] = (
            d["spec"]["beta"],
            d["spec"]["M"],
            float(m["norm"]),
            float(m["phi"]),
            float(m["logE"]),
            d["failure_reason"],
            False,
        )
    return out


def main():
    tree()
    AM = importlib.import_module("CosmologyConcepts.Models.AbstractModel")
    T = importlib.import_module("ComputeTargets.tests.test_unresolvable_turning_points")
    from ComputeTargets.exceptions import ComputationFailureError

    units = T.UNITS
    src = sources(units)
    print(
        f"WALL_ROUNDING_QUANTA = {AM.WALL_ROUNDING_QUANTA}; {len(T.FAILING_INPUTS)} inputs; "
        f"{len(src)} read from their sources"
    )
    print()
    rows = []
    all_ok = True
    for source, beta, M, normalization, phi, log_E in T.FAILING_INPUTS:
        s = src[source]
        literal_ok = (
            (s[0] is None or s[0] == beta)
            and (s[1] is None or s[1] == M)
            and s[2] == normalization
            and s[3] == phi
            and s[4] == log_E
        )
        model = T.make_model(beta, M)
        log_V_floor = model._log_V_floor(normalization)
        least = AM._least_finite_log_V_minus_floor(log_V_floor)
        holds = AM._turning_point_unresolvable(log_V_floor, log_E)
        t0 = time.perf_counter()
        now = model.classical_turning_point(
            normalization, phi * units.PlanckMass, log_E
        )
        dt_now = time.perf_counter() - t0
        t0 = time.perf_counter()
        try:
            T.classical_turning_point_before_fix(
                model, normalization, phi * units.PlanckMass, log_E
            )
            before = None
        except ComputationFailureError as e:
            before = e.message
        dt_before = time.perf_counter() - t0
        if s[6]:
            source_ok = before == s[5]
            source_text = "equal to the probe's printed message"
        else:
            source_ok = before is not None and before.startswith(s[5])
            source_text = f"the source's {len(s[5])}-character reason is a prefix"
        ok = literal_ok and holds and now is None and before is not None and source_ok
        all_ok = all_ok and ok
        # the root's margin as reflection-restart's failing_call.py states it: ln V - ln V_floor at the
        # root, ln(1 + E/V_floor), in quanta of ulp(|ln V_floor|)
        quanta = log1p(exp(log_E - log_V_floor)) / ulp(abs(log_V_floor))
        print(f"== {source}: beta {beta}, M/M_P {M!r}, normalization {normalization!r}")
        print(
            f"   phi = {phi!r} M_P, ln E = {log_E!r}; literal equal to its source: {literal_ok}"
        )
        print(
            f"   least finite ln(V - V_floor) = {least!r}; ln E below it by {least - log_E:.6g} "
            f"(the root at {quanta:.4f} quanta of ln V_floor, against WALL_ROUNDING_QUANTA + 1 = "
            f"{AM.WALL_ROUNDING_QUANTA + 1}); predicate: {holds}"
        )
        print(f"   now: {now!r} ({dt_now * 1e3:.3f} ms)")
        print(
            f"   before the fix: raises {before is not None} ({dt_before * 1e3:.3f} ms); {source_text}: {source_ok}"
        )
        print(f"   message: {before}")
        rows.append(
            (source, least - log_E, holds, now is None, before is not None, source_ok)
        )
    print()
    print(
        "| input | ln(V-V_floor)_min - ln E | predicate | now None | pre-fix raises | message as its source |"
    )
    print("|---|---|---|---|---|---|")
    for source, d, holds, none, raises, same in rows:
        print(f"| {source} | {d:.6g} | {holds} | {none} | {raises} | {same} |")
    print()
    print(
        f"every input: literal from its source, predicate holds, None now, pre-fix copy raises as recorded: {all_ok}"
    )
    print()

    print(
        "the boundary (prompt §1.8): the last ln E with the predicate True and the first with it False"
    )
    for label, log_V_floor in (
        ("laboratory M (2197's stored normalization)", T.LAB_NORMALIZATION),
        ("M = 1e-12 M_P (3233's stored normalization)", T.LARGE_M_NORMALIZATION),
    ):
        least, log_V, steps = T.least_finite(log_V_floor)
        last_true = nextafter(least, -inf)
        print(
            f"   {label}: ln V_floor = {log_V_floor!r}; first ln V with a finite value {log_V!r} "
            f"({steps} steps, ln V - ln V_floor = {(log_V - log_V_floor) / ulp(abs(log_V_floor)):.4g} ulp); "
            f"least value {least!r}; last True {last_true!r} -> "
            f"{AM._turning_point_unresolvable(log_V_floor, last_true)}; first False {least!r} -> "
            f"{AM._turning_point_unresolvable(log_V_floor, least)}"
        )
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.path.insert(0, ".")
    sys.exit(main())
