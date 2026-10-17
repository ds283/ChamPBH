"""Compare notebook (NDSolve) and pipeline (Radau) histories pairwise.

Args: work dir. Reads work/nbrun/<tag>_b<beta>_<var>.csv and <tag>_log.txt (nb_common.wls), and
work/<pipe dir>/b<beta>_<var>.csv, summary_*.json (pipe_runs.py). Writes work/compare.json.
"""

import glob
import json
import re
import sys

import numpy as np
import pandas as pd

W = sys.argv[1]

# pairings: (label, notebook tag, notebook variant, pipeline dir, pipeline variant)
SCAN = [
    2.99,
    2.995,
    3.0,
    3.0028,
    3.0029,
    3.005,
    3.01,
    3.015,
    3.0194,
    3.0195,
    3.02,
    3.025,
    3.03,
]
PAIRS = [
    ("notebook thermodynamics in both (resampled)", "scan", "nb", "pipe", "nbthermo"),
    ("notebook thermodynamics in both (exact)", "scan", "nb", "pipe", "nbexact"),
    ("pipeline thermodynamics in both", "scan", "common", "pipe", "pipe"),
    ("pipeline Sigma only (first audit)", "scan", "pipeSig", "pipe", "pipe"),
]


def nb_log(tag):
    """{(beta, var): dict(N_end, T_end, tps)} from an nb_common.wls log."""
    out = {}
    lines = open(f"{W}/nbrun/{tag}_log.txt").read().splitlines()
    for i, line in enumerate(lines):
        m = re.match(r"^([\d.]+) \| (\w+) \| [\d.]+ \| ([\d.]+) \| ([\d.]+)", line)
        if not m:
            continue
        tp_txt = lines[i + 1].split("TP:", 1)[1].strip()
        tps = re.findall(
            r'\{([-\d.e*^]+), ([-\d.e*^]+), ([-\d.e*^]+), "(min|max)"\}', tp_txt
        )
        num = lambda s: float(s.replace("*^", "e"))  # noqa: E731
        out[(round(float(m[1]), 6), m[2])] = dict(
            N_end=float(m[3]),
            T_end=float(m[4]),
            tps=[
                dict(N=num(a), T_J_GeV=num(b), phi_Mp=num(c), type=d)
                for a, b, c, d in tps
            ],
        )
    return out


def pipe_summ(d):
    out = {}
    for f in glob.glob(f"{W}/{d}/summary_*_rtol1e-08.json"):
        for r in json.load(open(f)):
            out[(round(r["beta"], 6), r["variant"])] = r
    return out


def hist(path):
    h = pd.read_csv(path)
    h["N"] = h["N"].round(6)
    return h


def compare(nbh, ph):
    m = nbh.merge(ph, on="N", suffixes=("_n", "_p"))
    return dict(
        N_common=float(m.N.max()),
        dlnT=float(abs(m.lnTJ_n - m.lnTJ_p).max()),
        dphi=float(abs(m.phi_n - m.phi_p).max()),
        dy=float(abs(m.phiprime_n - m.phiprime_p).max()),
    )


def tp_diff(a, b):
    n = min(len(a), len(b))
    if n == 0:
        return dict(n_nb=len(a), n_pipe=len(b), dN=None, dlnT=None)
    return dict(
        n_nb=len(a),
        n_pipe=len(b),
        dN=max(abs(a[i]["N"] - b[i]["N"]) for i in range(n)),
        dlnT=max(abs(np.log(a[i]["T_J_GeV"] / b[i]["T_J_GeV"])) for i in range(n)),
        same_types=all(a[i]["type"] == b[i]["type"] for i in range(n)),
    )


result = {"pairs": []}
nbs = nb_log("scan")
ps = pipe_summ("pipe")
for label, tag, nv, pd_, pv in PAIRS:
    rows = []
    for b in SCAN:
        key = round(b, 6)
        nbh = hist(f"{W}/nbrun/{tag}_b{b:.5f}_{nv}.csv")
        ph = hist(f"{W}/{pd_}/b{b:.5f}_{pv}.csv")
        c = compare(nbh, ph)
        n, p = nbs[(key, nv)], ps[(key, pv)]
        first = lambda tps: (
            (tps[0]["type"], tps[0]["N"], tps[0]["T_J_GeV"]) if tps else None
        )  # noqa: E731
        row = dict(
            beta=b,
            **c,
            N_end_nb=n["N_end"],
            N_end_pipe=p["N_end"],
            dN_end=abs(n["N_end"] - p["N_end"]),
            first_nb=first(n["tps"]),
            first_pipe=first(p["turning_points"]),
            tp=tp_diff(n["tps"], p["turning_points"]),
        )
        rows.append(row)
    result["pairs"].append(dict(label=label, nb_variant=nv, pipe_variant=pv, rows=rows))
    print(f"\n== {label}: notebook '{nv}' vs pipeline '{pv}'")
    print(
        " beta     max|dlnT|  max|dphi|  max|dy|   |dN_end|   first TP (nb)              first TP (pipe)            TP: n_nb/n_p  max dN   max dlnT"
    )
    for r in rows:
        fn = r["first_nb"]
        fp = r["first_pipe"]
        f = lambda x: (
            f"{x[0]} N={x[1]:8.4f} T={x[2]:8.4f}" if x else "none".ljust(25)
        )  # noqa: E731
        tp = r["tp"]
        print(
            f" {r['beta']:.4f}  {r['dlnT']:9.2e}  {r['dphi']:9.2e}  {r['dy']:8.2e}  {r['dN_end']:8.2e}   {f(fn)}  {f(fp)}  "
            f"{tp['n_nb']}/{tp['n_pipe']}  "
            + (f"{tp['dN']:8.2e} {tp['dlnT']:8.2e}" if tp["dN"] is not None else "")
        )

# phi_i sweep (overflow test), beta = 3.005, notebook vs pipeline and vs the phi_i = 10 history
print(
    "\n== phi_i sweep at beta = 3.005 (notebook variants vs the pipeline, and vs phi_i = 10)"
)
sweep = []
ref = {v: hist(f"{W}/nbrun/scan_b3.00500_{v}.csv") for v in ("nb", "pipeSig", "common")}
pref = {"nb": "nbthermo", "pipeSig": "pipe", "common": "pipe"}
for phi in (50, 58, 60, 100):
    log = nb_log(f"phi{phi}")
    for v in ("nb", "pipeSig", "common"):
        h = hist(f"{W}/nbrun/phi{phi}_b3.00500_{v}.csv")
        pdir = f"pipephi{phi}" if phi != 100 else "pipe100"
        ph = hist(f"{W}/{pdir}/b3.00500_{pref[v]}.csv")
        # shift: compare the phi_i history with the phi_i=10 one in (y, T_J), which should be identical
        r10 = h.merge(ref[v], on="N", suffixes=("_n", "_p"))
        d10 = float(abs(r10.lnTJ_n - r10.lnTJ_p).max())
        cp = compare(h, ph)
        tps = log[(3.005, v)]["tps"]
        first = (
            (tps[0]["type"], round(tps[0]["N"], 4), round(tps[0]["T_J_GeV"], 4))
            if tps
            else None
        )
        sweep.append(
            dict(
                phi_i=phi,
                variant=v,
                dlnT_vs_phi10=d10,
                dlnT_vs_pipe=cp["dlnT"],
                first=first,
            )
        )
        print(
            f" phi_i={phi:3d} {v:8s} max|dlnT| vs phi_i=10: {d10:9.2e}   vs pipeline: {cp['dlnT']:9.2e}  first TP {first}"
        )
result["phi_sweep"] = sweep
json.dump(result, open(f"{W}/compare.json", "w"), indent=1)
