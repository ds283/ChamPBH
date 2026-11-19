"""
power-law-wall prompt 07b2 §1.5 (P95), the first step: fix the prediction before any new point runs.

It reads (v)'s S3 constants at n = 1 and n = 2 from .documents/power-law-wall/kex-07b2/minform.json ("without_new",
written by `minform_fits.py base`), chooses the new points, predicts f at each with those constants, and writes
.documents/power-law-wall/kex-07b2/prediction.json with the time it was written. It refuses to overwrite that file
(ruling 4: the prediction is not changed after it is written).

**The points** (at most 40; prompt §1.5):
  - n = 1, the laboratory curve (ln(V0/M^4) = 3.345, stage2_kex.CURVES["lab"], crossover's 2197): at target ell_new
    1.0, 1.6, 1.75, 1.95, 2.1, 2.5 and 3.4, from ell_new = 1 to the predicted crossing (ell_new 1.95 there) + 1.5,
    denser across the peak;
  - n = 1, the low-M curve L-M1e-16 (ln(V0/M^4) = -129.27): at 1.0, 1.25, 1.6, 1.8, 1.95, 2.2, 2.6 and 3.4 (its
    crossing is at ell_new 1.9). L-M1e-16 rather than L-M8e-18: its region ell_new 1-3.4 holds three measured rows
    (1.44, 2.03, 3.17), none with the field back out, where L-M8e-18's holds four;
  - n = 2, the laboratory curve at the n = 2 laboratory value (crossover's U15 formula, stage2_kex.lnR_n2 at 3.345):
    at 0.5, 0.9, 1.3, 1.65, 1.9, 2.1, 2.5, 3.2 and 4.0, between 07b's four (0.3, 0.7, 1.1, 1.45) and above them;
  - the held-out set: twelve points inside S3 (ln(V0/M^4) <= 10, ln R > 0.3), seven at n = 1 and five at n = 2, on
    the rise, at the crossing and on the fall as (v) predicts them, each at a distance d >= 1 from every measured row
    at its n and from every other new point, d = sqrt((d ln R / 2.5)^2 + (d ln L / 0.2)^2): half the grids' spacing
    in ln R (5) and about half their spacing in ln L (0.29-0.69).
On each curve L is the root of ell_new(L, ln R(L)) = target (brentq, xtol 1e-6), as stage2_kex.on_curve finds it.

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B minform_predict.py \\
        > ../../../../.documents/power-law-wall/kex-07b2/prediction.txt
"""

import datetime
import json
import os
import subprocess
import sys
import warnings
from math import exp, log, sqrt

from scipy.optimize import brentq

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import kexchain as kc  # noqa: E402
import minform as mf  # noqa: E402
import rechain  # noqa: E402

warnings.simplefilter("ignore")
PRED = os.path.join(mf.OUT, "prediction.json")
LV = {"lab": 3.345, "L-M1e-16": -129.27}
CURVE_TARGETS = (
    ("07b2 lab n=1", 1, "lab", (1.0, 1.6, 1.75, 1.95, 2.1, 2.5, 3.4)),
    ("07b2 L-M1e-16", 1, "L-M1e-16", (1.0, 1.25, 1.6, 1.8, 1.95, 2.2, 2.6, 3.4)),
    ("07b2 lab n=2", 2, "lab", (0.5, 0.9, 1.3, 1.65, 1.9, 2.1, 2.5, 3.2, 4.0)),
)
# (n, L, ln R, where (v) puts it)
HELD_OUT = (
    (1, 1.6, 11.0, "rise"),
    (1, 12.0, 7.0, "rise"),
    (1, 40.0, 6.0, "rise"),
    (1, 16.0, 13.46, "crossing"),
    (1, 45.0, 12.0, "crossing"),
    (1, 20.0, 22.0, "fall"),
    (1, 40.0, 20.0, "fall"),
    (2, 3.0, 6.5, "rise"),
    (2, 11.0, 6.0, "rise"),
    (2, 11.0, 13.41, "crossing"),
    (2, 22.0, 24.0, "fall"),
    (2, 45.0, 20.0, "fall"),
)
VALIDATE = (
    "07b2 lab n=1 ell_new~1.95",
    "07b2 L-M1e-16 ell_new~2.6",
    "07b2 lab n=2 ell_new~1.3",
)
D_LNR, D_LNL = 2.5, 0.2


def lnR_on(n, curve, L):
    lv = LV[curve]
    if n == 1:
        return rechain.lnR_on(L, lv)
    return (
        lv
        + 2 * L
        - log(exp(L) - 1)
        + 2 * log(4 * L**2 + 6 * L)
        + 2 * log(L)
        - log(8 * 3.141592653589793**2)
    )


def ell_new(n, L, lnR):
    ch = kc.chain(rechain.wall_of(dict(n=n, L=L)), exp(lnR), "v3")
    return None if ch is None else ch[3]


def on_curve(n, curve, target):
    def g(L):
        e = ell_new(n, L, lnR_on(n, curve, L))
        return (e if e is not None else 0.0) - target

    lo, hi = (1.0, 12.0) if curve == "lab" else (100.0, 125.0)
    return brentq(g, lo, hi, xtol=1e-6)


def dist(a, b):
    return sqrt(
        ((a["lnR"] - b["lnR"]) / D_LNR) ** 2
        + ((log(a["L"]) - log(b["L"])) / D_LNL) ** 2
    )


def main():
    if os.path.exists(PRED):
        raise SystemExit(f"{PRED} exists: the prediction is fixed (ruling 4)")
    J = json.load(open(os.path.join(mf.OUT, "minform.json")))["without_new"]
    params = {
        "variant": "v",
        "n1": J["n1/S3/v"]["params"]["n1"],
        "n2": J["n2/S3/v"]["params"]["n2"],
    }
    measured = [p for p in mf.all_points() if p["file"] != "new (07b2)"]
    pts = []
    for label, n, curve, targets in CURVE_TARGETS:
        for t in targets:
            L = on_curve(n, curve, t)
            pts.append(
                dict(
                    label=f"{label} ell_new~{t:g}",
                    set=label,
                    kind="curve",
                    n=n,
                    L=L,
                    lnR=lnR_on(n, curve, L),
                    target_ell=t,
                )
            )
    for i, (n, L, lnR, where) in enumerate(HELD_OUT):
        pts.append(
            dict(
                label=f"07b2 held-out #{i + 1} n={n} ({where})",
                set="07b2 held-out",
                kind=f"held-out ({where})",
                n=n,
                L=L,
                lnR=lnR,
            )
        )
    print(f"# minform_predict.py; start: {os.popen('uptime').read().strip()}")
    print(
        "# (v)'s S3 constants (minform.json without_new): n = 1 "
        + ", ".join(f"{k} {v!r}" for k, v in params["n1"].items())
        + "; n = 2 "
        + ", ".join(f"{k} {v!r}" for k, v in params["n2"].items())
    )
    print(
        "# id, label, n, L, ln R, ln(V0/M^4), ell_new, y_rise, y_fall, branch, predicted f, d to the nearest measured row"
    )
    for i, p in enumerate(pts):
        p["id"] = i + 1
        e = ell_new(p["n"], p["L"], p["lnR"])
        y, yr, yf = mf.y_of(e, p["L"], p["lnR"], p["n"], params)
        p["ell_new"] = e
        p["lv"] = mf.fits.ln_v0m4(p["L"], p["lnR"], p["n"])
        p["y_rise"] = yr
        p["y_fall"] = None if yf == float("inf") else yf
        p["branch"] = mf.active_branch(e, p["L"], p["lnR"], p["n"], params)
        p["f_predicted"] = mf.fraction(e, p["L"], p["lnR"], p["n"], params)
        same_n = [q for q in measured if q["n"] == p["n"]]
        p["d_measured"] = min(dist(p, q) for q in same_n)
        others = [q for q in pts if q is not p and q["n"] == p["n"]]
        p["d_new"] = min(dist(p, q) for q in others)
        if p["kind"].startswith("held-out"):
            assert p["lv"] <= mf.LV_MAX and p["lnR"] > mf.LNR_MIN, p
            assert p["d_measured"] >= 1.0 and p["d_new"] >= 1.0, p
        print(
            f"  {p['id']:2d} {p['label']:40s} n={p['n']} L {p['L']:10.5f} ln R {p['lnR']:9.4f} ln(V0/M^4) {p['lv']:8.2f} "
            f"ell_new {'none' if e is None else format(e, '.4f'):>7s} y_rise {yr:8.4f} y_fall "
            f"{'inf' if p['y_fall'] is None else format(p['y_fall'], '.4f'):>8s} {p['branch']:5s} f_pred "
            f"{p['f_predicted']:.4f} d {p['d_measured']:.2f}"
        )
    assert len(pts) <= 40
    now = datetime.datetime.now().astimezone()
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=mf.ROOT
    ).stdout.strip()
    doc = dict(
        written=now.isoformat(timespec="seconds"),
        written_unix=now.timestamp(),
        git_head=head,
        about=(
            "power-law-wall 07b2 §1.5: (v)'s S3 constants (fitted on 07b's points alone) and the new points, each with "
            "the f those constants predict, written before any new point ran (ruling 4); not changed after"
        ),
        params=params,
        distance=dict(
            formula="sqrt((d ln R / 2.5)^2 + (d ln L / 0.2)^2)",
            held_out_minimum=1.0,
        ),
        validate=list(VALIDATE),
        points=[
            {
                k: p.get(k)
                for k in (
                    "id",
                    "label",
                    "set",
                    "kind",
                    "n",
                    "L",
                    "lnR",
                    "lv",
                    "target_ell",
                    "ell_new",
                    "y_rise",
                    "y_fall",
                    "branch",
                    "f_predicted",
                    "d_measured",
                    "d_new",
                )
            }
            for p in pts
        ],
    )
    with open(PRED, "x") as fh:
        json.dump(doc, fh, indent=1)
    print(
        f"# {len(pts)} points; prediction.json written {doc['written']} at HEAD {head}"
    )


if __name__ == "__main__":
    main()
