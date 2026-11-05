"""
one-loop-drain prompt 02c §2.3, the cause of the header differences (orchestrator note 02c, ruling 4's exception).

compare_maps.py finds every candidate row of the four maps equal to crossover's at the printed precision, and the
control byte-identical, but parking_N and N_final, printed at full precision (repr) in the header, differ in their
last digits for P-2197 (N_final), W-2187 and L-M5e-20 (both). The production law takes L = ln(1 + E/V0)
(drain_law_L, U15 (b)); the reference takes L = M/phi_t. At n = 1 they are equal but for the root's accuracy.

For one history (a 02b task's result.json, or the 02c input copy), this runs cascade_map.main twice in this process,
capturing each IntegrationResult:
  (a) unpatched: the production law;
  (b) with drain_law_inputs replaced by one returning L = M/phi_t (the reference's L, cascade_map's own expression)
      and the same ln R: everything else production.
It prints:
  - the header line of each run, and whether (b)'s header equals crossover's file's (the id and gap= aside, as
    compare_maps.py compares it: parking_N, N_final, candidates) and (b)'s rows equal crossover's rows;
  - over run (a)'s candidates, the largest |L/(M/phi_t) - 1| and the largest relative difference between the
    production alpha_eff and min(0.99, A f(M/phi_t)) w_gate on the same inputs (ruling 4 (ii): at most 1e-10),
    with the first candidate at which the two alpha_eff differ at all.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law-02c/l_form_probe.py RESULT_JSON CROSSOVER_MAP
"""

import contextlib
import io
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "prompts/one-loop-drain/source/crossover"))

import cascade_map  # noqa: E402

SM = cascade_map.SM
ROW = re.compile(r"^\s*\d+\s")


def run(result_json: str):
    captured = []
    inner = SM.integrate_scalar_history

    def capture(*a, **kw):
        res = inner(*a, **kw)
        captured[:] = [res]
        return res

    SM.integrate_scalar_history = capture
    out = io.StringIO()
    try:
        with contextlib.redirect_stdout(out):
            cascade_map.main(result_json)
    finally:
        SM.integrate_scalar_history = inner
    lines = out.getvalue().splitlines()
    header = next(l for l in lines if l.startswith("# ") and "parking_N=" in l)
    rows = [l for l in lines if ROW.match(l)]
    return captured[-1], header, rows


def fields(header: str) -> dict:
    return dict(re.findall(r"(parking_N|N_final|candidates)=(\S+)", header))


def main(result_json: str, crossover_map: str) -> int:
    record = json.load(open(result_json))
    M = float(record["argv"][1])
    A = float(record["argv"][record["argv"].index("--alpha") + 1])
    theirs = Path(crossover_map).read_text().splitlines()
    their_header = next(l for l in theirs if l.startswith("# ") and "parking_N=" in l)
    their_rows = [l for l in theirs if ROW.match(l)]

    res_a, header_a, rows_a = run(result_json)

    production_inputs = SM.drain_law_inputs

    def reference_inputs(model, assessment):
        if assessment is None:
            return None, None
        _, _, tp = assessment
        return M / tp.phi_t, tp.log_R

    SM.drain_law_inputs = reference_inputs
    try:
        res_b, header_b, rows_b = run(result_json)
    finally:
        SM.drain_law_inputs = production_inputs

    print(f"== {record['id']} (A = {A}, M = {M} M_P)")
    print(f"   (a) production:        {fields(header_a)}")
    print(f"   (b) L = M/phi_t:       {fields(header_b)}")
    print(f"   crossover's:           {fields(their_header)}")
    print(
        f"   (b) header equals crossover's: {fields(header_b) == fields(their_header)}; "
        f"(b) rows equal crossover's: {rows_b == their_rows}; (a) rows equal crossover's: {rows_a == their_rows}"
    )

    worst_L = worst_alpha = 0.0
    first = None
    for i, (c, inj) in enumerate(zip(res_a.drain.candidates, res_a.injections)):
        if c.regime == SM.REGIME_UNASSESSED:
            continue
        ct = inj.classical_turning_point
        L_ref = M / ct.phi_t
        worst_L = max(worst_L, abs(c.L / L_ref - 1.0))
        alt = SM.drain_alpha_effective(c.regime, c.ell, L_ref, c.log_R, A, c.w_gate)
        if alt != c.alpha_eff:
            rel = abs(c.alpha_eff / alt - 1.0) if alt != 0.0 else float("inf")
            worst_alpha = max(worst_alpha, rel)
            if first is None:
                first = (i, c.regime, c.alpha_eff, alt, rel)
    print(f"   (a) largest |L/(M/phi_t) - 1| over its candidates: {worst_L:.3e}")
    print(
        f"   (a) largest relative difference of alpha_eff, production L against M/phi_t on the same inputs: "
        f"{worst_alpha:.3e} (ruling 4 (ii): at most 1e-10)"
    )
    if first is not None:
        i, regime, a, b, rel = first
        print(
            f"   first candidate where they differ: {i} ({regime}): {a!r} against {b!r} (relative {rel:.3e})"
        )
    else:
        print("   no candidate where they differ")
    return 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:3]))
