"""
power-law-wall prompt 07b2 §1.5 (P95), the scoring: the new points against the prediction fixed before they ran.

It reads prediction.json (the constants and the predicted f) and stage2_points.txt (the measured f), and reports, per
group (each curve, the held-out set; each n) and for the points with the field back out and without: the residual
f_measured - f_predicted, max |res| / rms / mean (count). Every point is listed with its validation-relevant fields
(|dE/E|, back out, both regimes). The sharpness check (prompt §3): the same scoring with the committed constants
perturbed by +10% and -10% in A, at each n, beside the unperturbed.

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B minform_prediction.py \\
        > ../../../../.documents/power-law-wall/kex-07b2/prediction_scored.txt
"""

import copy
import json
import os
import sys

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import minform as mf  # noqa: E402

PRED = os.path.join(mf.OUT, "prediction.json")


def groups_of(p):
    g = p["set"].replace("_", " ")
    return [
        f"n = {p['n']}: {g}",
        f"n = {p['n']}: every new point",
        f"n = {p['n']}: predicted on the {p['branch']}",
        "every new point",
    ]


def score(rows, value):
    """{group: {'all': stats, 'back': stats, 'not back': stats}} of f_measured - value(row)."""
    out = {}
    for r in rows:
        res = r["frac"] - value(r)
        for g in groups_of(r):
            d = out.setdefault(g, {"all": [], "back": [], "not back": []})
            d["all"].append(res)
            d["back" if r["back"] else "not back"].append(res)
    return {g: {k: mf.stats(v) for k, v in d.items()} for g, d in out.items()}


def print_score(title, sc):
    print(f"  {title}")
    print(f"    {'group':40s} {'all':>26s} {'back out':>26s} {'not back out':>26s}")
    for g in sorted(sc):
        print(
            f"    {g:40s} "
            + " ".join(
                f"{mf.fmt_stats(sc[g][k]):>26s}" for k in ("all", "back", "not back")
            )
        )


def main():
    print(f"# minform_prediction.py; start: {os.popen('uptime').read().strip()}")
    doc = json.load(open(PRED))
    prm = doc["params"]
    pred = {p["id"]: p for p in doc["points"]}
    rows = mf.new_points_07b2()
    print(
        f"# prediction.json written {doc['written']}; {len(doc['points'])} points predicted, {len(rows)} measured "
        f"(stage2_points.txt)"
    )
    for r in rows:
        r["branch"] = pred[r["idx"]]["branch"]
    missing = sorted(set(pred) - {r["idx"] for r in rows})
    print(f"# predicted but not measured: {missing if missing else 'none'}")
    print(
        "# per point: id, label, n, L, ln R, ell_new (07b2's chain re-run here), regime old -> new, f measured, back "
        "out, predicted f (prediction.json) and branch, residual f - f_pred"
    )
    for r in sorted(rows, key=lambda r: r["idx"]):
        p = pred[r["idx"]]
        # the prediction's f is re-evaluated from its constants and checked against the stored value
        again = mf.fraction(r["ell_new"], r["L"], r["lnR"], r["n"], prm)
        assert again == p["f_predicted"], (r, p)
        e = "none" if r["ell_new"] is None else f"{r['ell_new']:.4f}"
        print(
            f"  {r['idx']:2d} {p['label']:40s} n={r['n']} L {r['L']:10.5f} ln R {r['lnR']:9.4f} ell_new {e:>7s} "
            f"{r['reg_old']:>8s} -> {r['reg_new']:<8s} f {r['frac']:.4f} back {str(r['back']):5s} | pred "
            f"{p['f_predicted']:.4f} ({p['branch']}) res {r['frac'] - p['f_predicted']:+.4f}"
        )
    print(
        "\n# the residuals f_measured - f_predicted, max |res| / rms / mean (count), by group and by whether the field "
        "came back out"
    )
    print_score(
        "the prediction (prediction.json's constants)",
        score(rows, lambda r: pred[r["idx"]]["f_predicted"]),
    )
    print(
        "\n# sharpness (prompt §3): the same points scored with prediction.json's constants perturbed in A, at each n"
    )
    for fac in (1.1, 0.9):
        q = copy.deepcopy(prm)
        for k in ("n1", "n2"):
            q[k]["A"] = prm[k]["A"] * fac
        sc = score(
            rows, lambda r, q=q: mf.fraction(r["ell_new"], r["L"], r["lnR"], r["n"], q)
        )
        print_score(f"A x {fac:g}", sc)
    # the points where the perturbation moves the prediction by more than the unperturbed residual
    base = {r["idx"]: r["frac"] - pred[r["idx"]]["f_predicted"] for r in rows}
    q = copy.deepcopy(prm)
    for k in ("n1", "n2"):
        q[k]["A"] = prm[k]["A"] * 1.1
    moved = [
        (
            r["idx"],
            mf.fraction(r["ell_new"], r["L"], r["lnR"], r["n"], q)
            - pred[r["idx"]]["f_predicted"],
        )
        for r in rows
    ]
    fall_moved = [abs(d) for i, d in moved if pred[i]["branch"] == "fall"]
    print(
        f"  A x 1.1 moves the prediction at the {len(fall_moved)} fall points by {min(fall_moved):.4f} .. "
        f"{max(fall_moved):.4f} (the rise and gap points not at all); the unperturbed residuals there: "
        + (
            f"{np.max([abs(base[i]) for i, d in moved if pred[i]['branch'] == 'fall']):.4f} at most"
            if fall_moved
            else "-"
        )
    )
    print(f"# minform_prediction.py end: {os.popen('uptime').read().strip()}")


if __name__ == "__main__":
    main()
