"""
power-law-wall prompt 07b2 §1.7 (P97): the paths. Measured, not judged.

On every exponential history of 07b's path map (paths.py's inputs: crossover's six cascade maps and 02's three
exponential maps, all n = 1; the power wall's law reads no ell, so its histories are not repeated here), each candidate
re-chained in both forms by 07b's paths.chains, and f under:
  - v1 (production's drain_fraction, in the old form), and 07b's (i)-(iv) at fits.json's constants under fit_law's
    selection and note §0.3's, by 07b's paths.law, as paths.txt gives them (re-computed here; each history's shares are
    printed beside the values paths.txt records);
  - each variant of this prompt at its S3 constants with the new points (minform.json "with_new"; (v-J) its n = 1
    part), by minform.fraction in the new form; the classical candidates (ln R <= 0) R S(L) under every law, as 07b.
Per history the first-order drained share, 07b's proxy (paths.py, its D8): K_X = sum_c alpha_X,c eps_c, eps_c =
(E_c/V0) e^{4 N_c} from the recorded ln(E/V0) and N, the trajectory and so the recorded Delta K held fixed,
alpha_X = min(0.99, A f_X) w_gate with the history's A (1 for 02's fixed-law maps) and w_gate = 1; reported as K_X/K_v1.
Every non-classical candidate's f under each law is written to .documents/power-law-wall/kex-07b2/paths_candidates.txt.

    PYTHONPATH=. ./venv/bin/python -B prompts/power-law-wall/source/kex/minform_paths.py \\
        > .documents/power-law-wall/kex-07b2/paths.txt
"""

import glob
import json
import os
import sys
import warnings
from math import exp

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import minform as mf  # noqa: E402
import paths as P07  # noqa: E402  (07b's; imported, never changed)

from ComputeTargets.ScalarHistory import drain_fraction, drain_S_of_L  # noqa: E402

warnings.simplefilter("ignore")
ROOT = mf.ROOT
VARIANTS = ("v", "v-L", "v-A", "v-J", "vi")
KEY = (
    sys.argv[1] if len(sys.argv) > 1 else "with_new"
)  # "without_new" only to test the script before the new points


def variant_params():
    J = json.load(open(os.path.join(mf.OUT, "minform.json")))[KEY]
    out = {}
    for v in VARIANTS:
        key = "n12/S3/v-J" if v == "v-J" else f"n1/S3/{v}"
        prm = J[key]["params"]
        out[v] = {"variant": prm["variant"], "n1": prm["n1"]}
    return out


def recorded_shares():
    """07b's paths.txt: {history: 'shares text'}."""
    out = {}
    for line in open(os.path.join(ROOT, ".documents/power-law-wall/kex-07b/paths.txt")):
        if not line.startswith("#") and "; drained share " in line:
            out[line.split()[0]] = line.split("drained share ")[1].strip()
    return out


def main():
    print(f"# minform_paths.py; start: {os.popen('uptime').read().strip()}")
    VP = variant_params()
    rec = recorded_shares()
    print(
        f"# the variants' S3 constants (minform.json {KEY}), n = 1: "
        + "; ".join(
            f"({v}) " + ", ".join(f"{a} {b:.4f}" for a, b in prm["n1"].items())
            for v, prm in VP.items()
        )
    )
    hist = [
        P07.read_cascade(p)
        for p in sorted(
            glob.glob(
                os.path.join(
                    ROOT, "prompts/one-loop-drain/source/crossover/out/cascade-*.txt"
                )
            )
        )
    ]
    hist += [
        P07.read_map02(p)
        for p in sorted(
            glob.glob(
                os.path.join(ROOT, ".documents/power-law-wall/wall-02/map/exp-*.txt")
            )
        )
    ]
    hist = [h for h in hist if h["rows"] and h["wall"] == "exponential"]
    laws = (
        ["v1"]
        + [f"{s}:{c}" for s in ("fit_law", "note") for c in P07.CANDS]
        + [f"({v})" for v in VARIANTS]
    )
    cand = [
        "# paths_candidates.txt (minform_paths.py): every non-classical candidate of the exponential histories. Columns: "
        "history, idx, N, ln(E/V0), L, ln R, regime and ell old | new; then f under "
        + ", ".join(laws)
    ]
    print(
        "# per history: candidates (non-classical); K_X/K_v1 for 07b's (i)-(iv) under fit_law's | note §0.3's "
        "selection (recomputed here; paths.txt's beside) and for this prompt's variants on S3 with the new points; "
        "then the candidates where (v)'s f differs from v1's by more than 0.1, counted"
    )
    for h in hist:
        K = {k: 0.0 for k in laws}
        eps_all = [r["lnEV0"] + 4.0 * r["N"] for r in h["rows"]]
        e0 = max(eps_all)
        ncl, big = 0, 0
        for r in h["rows"]:
            c, L = P07.chains(h, r)
            ro, rn = c["lambda"], c["v3"]
            lnR = r["lnR"]
            fs = {}
            if rn[0] == "classical":
                for k in laws:
                    fs[k] = exp(lnR) * drain_S_of_L(L)
                ncl += 1
            else:
                fs["v1"] = drain_fraction(ro[0], ro[1], L, lnR)
                for sname, prms in P07.SETS:
                    for cn in P07.CANDS:
                        fs[f"{sname}:{cn}"] = P07.law(
                            cn, prms[cn], rn[0], rn[1], L, lnR
                        )
                for v in VARIANTS:
                    fs[f"({v})"] = mf.fraction(rn[1], L, lnR, 1, VP[v])
                if abs(fs["(v)"] - fs["v1"]) > 0.1:
                    big += 1
                cand.append(
                    f"{h['name']:36s} {r['idx']:5d} {r['N']:9.4f} {r['lnEV0']:10.4f} {L:9.4f} {lnR:9.3f} "
                    f"{ro[0]:>9s} {'none' if ro[1] is None else format(ro[1], '.4f'):>8s} | {rn[0]:>9s} "
                    f"{'none' if rn[1] is None else format(rn[1], '.4f'):>8s} | "
                    + " ".join(f"{fs[k]:.4f}" for k in laws)
                )
            eps = r["lnEV0"] + 4.0 * r["N"]
            for k in laws:
                K[k] += min(0.99, h["A"] * fs[k]) * exp(eps - e0)
        base = K["v1"]
        mine07 = (
            " ".join(f"{K[f'fit_law:{c}'] / base:.3f}" for c in P07.CANDS)
            + " | "
            + " ".join(f"{K[f'note:{c}'] / base:.3f}" for c in P07.CANDS)
        )
        print(
            f"{h['name']:36s} A={h['A']} n={h['n']}: {len(h['rows'])} candidates ({len(h['rows']) - ncl} non-classical)"
        )
        print(f"    07b (i)-(iv): {mine07}   [paths.txt: {rec.get(h['name'], '-')}]")
        print(
            "    this prompt: "
            + "  ".join(f"({v}) {K[f'({v})'] / base:.3f}" for v in VARIANTS)
            + f"; candidates with |f(v) - f(v1)| > 0.1: {big}"
        )
    with open(os.path.join(mf.OUT, "paths_candidates.txt"), "w") as fh:
        fh.write("\n".join(cand) + "\n")
    print(f"# minform_paths.py end: {os.popen('uptime').read().strip()}")


if __name__ == "__main__":
    main()
