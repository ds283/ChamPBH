"""
power-law-wall prompt 07b2 §1.8 (P98): the reference implementation against the fits, bit for bit.

minform.fraction(ell, L, ln_R, n, params) is the reference: one function of (ell, or None where the chain has no
solution, L, ln R > 0, n) and the constants of a variant. Every fit of minform_fits.py stored, in
.documents/power-law-wall/kex-07b2/minform.json, its constants at full precision and, at each of its points, the value
its own model (minform.model_fn, the function curve_fit minimised) gave at the optimum. Here fraction, called with the
constants read back from the JSON, is compared with each stored value by ==, at every fitted point of every variant on
every selection, without and with the new points. Also: fraction refuses ln R <= 0 (the classical region is R S(L),
not this law's), and prediction.json's predicted values are re-evaluated from its constants (minform_prediction.py
asserts the same at the measured points).

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B minform_ref.py \\
        > ../../../../.documents/power-law-wall/kex-07b2/reference.txt
"""

import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import minform as mf  # noqa: E402


def main():
    print(f"# minform_ref.py; start: {os.popen('uptime').read().strip()}")
    J = json.load(open(os.path.join(mf.OUT, "minform.json")))
    total, bad = 0, 0
    for section in ("without_new", "with_new"):
        for key, fit in sorted(J[section].items()):
            n_ok = 0
            for p in fit["points"]:
                v = mf.fraction(p["ell"], p["L"], p["ln_R"], p["n"], fit["params"])
                total += 1
                if v == p["f_fit"]:
                    n_ok += 1
                else:
                    bad += 1
                    print(f"    MISMATCH {section} {key} {p}: fraction {v!r}")
            print(
                f"  {section:12s} {key:14s} ({fit['params']['variant']}): {n_ok} of {len(fit['points'])} points bitwise"
            )
    print(f"# {total} fitted values, {bad} not bitwise equal to the reference")
    try:
        mf.fraction(1.0, 2.0, 0.0, 1, J["with_new"]["n1/S3/v"]["params"])
        print("# fraction at ln R = 0: returned a value (NOT REFUSED)")
    except ValueError as e:
        print(f"# fraction at ln R = 0: refused ({e})")
    pred = json.load(open(os.path.join(mf.OUT, "prediction.json")))
    same = sum(
        mf.fraction(p["ell_new"], p["L"], p["lnR"], p["n"], pred["params"])
        == p["f_predicted"]
        for p in pred["points"]
    )
    print(
        f"# prediction.json: {same} of {len(pred['points'])} predicted values bitwise from its constants"
    )
    same_c = (
        pred["params"]["n1"] == J["without_new"]["n1/S3/v"]["params"]["n1"]
        and pred["params"]["n2"] == J["without_new"]["n2/S3/v"]["params"]["n2"]
    )
    print(
        f"# prediction.json's constants equal minform.json's (v) on S3 without the new points, bitwise: {same_c}"
    )
    print(f"# minform_ref.py end: {os.popen('uptime').read().strip()}")


if __name__ == "__main__":
    main()
