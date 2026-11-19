"""
power-law-wall prompt 02 §1.4: the map's summary, from path_map.py's outputs in .documents/power-law-wall/wall-02/map/:
  - per history: the candidates, their regimes, the ranges of ln x = ln(E/V0), ln R and ell reached, parking, and the
    path formula's largest residual;
  - the Planck-scale table: the first impact's N and T_J, ln(E/V0), ln(k_ex/M_P) at the first impact from the
    classical turning point and from the one-loop turnaround, the largest of each over all candidates, the number of
    candidates with the classical ln(k_ex/M_P) > 0, and the planner's estimate at the first impact
    (power wall: k_ex ~ (n/2)(sqrt E/M) x^{1/n}; exponential: (n/2)(sqrt E/M) L^{1+1/n}, L = ln(1 + x)) with the
    difference in ln;
  - ln A(phi_t) at the first impact (the Jordan-frame k differs from the Einstein-frame one by A(phi_t)).

    cd prompts/power-law-wall/source/wall && ../../../../venv/bin/python map_summary.py \\
        > ../../../../.documents/power-law-wall/wall-02/map_summary.txt
"""

import glob
import os
import re
from math import log, log1p

MAP = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "../../../../.documents/power-law-wall/wall-02/map",
)


def read(path):
    head, rows = {}, []
    for line in open(path):
        if line.startswith("# beta=") and "tuned normalization" in line:
            head["norm"] = float(re.search(r"ln V0 = (\S+),", line).group(1))
            head["integrations"] = int(
                re.search(r"integrations = (\d+)", line).group(1)
            )
            m = re.match(r"# beta=(\S+) M=(\S+) model=(\S+) n=(\d+) alpha=(\S+):", line)
            head.update(
                beta=float(m[1]),
                M=float(m[2]),
                model=m[3],
                n=int(m[4]),
                alpha=float(m[5]),
            )
        elif "FAILURE" in line:
            head["failure"] = line.strip()
        elif line.startswith("# ln(V0/M^4)"):
            head["lnV0M4"] = float(re.search(r"= (\S+);", line).group(1))
            pk = re.search(r"parking: N = (\S+) at candidate (\d+)", line)
            head["parking"] = None if pk is None else (float(pk[1]), int(pk[2]))
        elif line.startswith("# first bounce"):
            m = re.search(r"N = (\S+), T_J = (\S+) MeV", line)
            head["bounce"] = (float(m[1]), float(m[2]))
        elif line[:1] == " " and not line.startswith(" @@"):
            f = line.split()
            if len(f) < 15:
                continue
            ell = None if f[7] in ("nochain", "None(inside)") else float(f[7])
            rows.append(
                dict(
                    idx=int(f[0]),
                    N=float(f[1]),
                    kind=f[2],
                    lnx=float(f[3]),
                    lnR=float(f[4]),
                    phi_t=float(f[5]),
                    ell_t=float(f[6]),
                    ell=ell,
                    ell_note=f[7],
                    regime=f[8],
                    lkt=float(f[9]),
                    lkta=None if f[10] == "-" else float(f[10]),
                    resid=float(f[13]),
                    lnA=float(f[14]),
                )
            )
    return head, rows


def estimate(h, r):
    """the planner's estimate of ln(k_ex/M_P) at a candidate (M_P = 1 in the run's units)."""
    n = h["n"]
    lnE = r["lnx"] + h["norm"]
    base = log(n / 2.0) + 0.5 * lnE - log(h["M"])
    if h["model"] == "inverse-power":
        return base + r["lnx"] / n
    L = r["lnx"] + log1p(2.718281828459045 ** (-r["lnx"]))
    return base + (1.0 + 1.0 / n) * log(L)


def main():
    files = sorted(glob.glob(os.path.join(MAP, "*.txt")))
    data = [(os.path.basename(p)[:-4], *read(p)) for p in files]
    print(
        "# power-law-wall 02 §1.4: the map, from path_map.py's outputs (fixed law, tuned by the tool route)"
    )
    print(
        "# history | ok | integr. | candidates (one_loop/gap/classical) | ln x range | ln R range | ell range | "
        "parking N (candidate) | max |path resid|"
    )
    worst = 0.0
    for name, h, rows in data:
        if "failure" in h:
            print(f"{name} | FAILURE | {h['failure']}")
            continue
        reg = {
            k: sum(1 for r in rows if r["regime"] == k)
            for k in ("one_loop", "gap", "classical")
        }
        ells = [r["ell"] for r in rows if r["ell"] is not None]
        res = max(abs(r["resid"]) for r in rows)
        if h["model"] == "inverse-power":
            worst = max(worst, res)
        pk = h["parking"]
        print(
            f"{name} | ok | {h['integrations']} | {len(rows)} ({reg['one_loop']}/{reg['gap']}/{reg['classical']}) | "
            f"{min(r['lnx'] for r in rows):.3f} to {max(r['lnx'] for r in rows):.3f} | "
            f"{min(r['lnR'] for r in rows):.3f} to {max(r['lnR'] for r in rows):.3f} | "
            f"{min(ells):.3f} to {max(ells):.3f} | "
            f"{'none' if pk is None else f'{pk[0]:.4f} ({pk[1]})'} | {res:.2e}"
        )
    print(
        f"# largest |path formula residual| over every power-wall candidate: {worst:.3e}"
    )
    print()
    print(
        "# the Planck scale (Einstein frame; ln(k_ex/M_P) = log_k_ex - ln units.PlanckMass)"
    )
    print(
        "# history | first impact N | T_J (MeV) | ln(E/V0) | ln(k_ex/M_P) classical | turnaround | "
        "max classical | max turnaround | # classical > 0 | estimate | measured - estimate | ln A(phi_t)"
    )
    for name, h, rows in data:
        if "failure" in h:
            continue
        r0 = rows[0]
        est = estimate(h, r0)
        n_pos = sum(1 for r in rows if r["lkt"] > 0.0)
        lktas = [r["lkta"] for r in rows if r["lkta"] is not None]
        print(
            f"{name} | {r0['N']:.5f} | {h['bounce'][1]:.3f} | {r0['lnx']:.4f} | {r0['lkt']:.4f} | "
            f"{'-' if r0['lkta'] is None else format(r0['lkta'], '.4f')} | {max(r['lkt'] for r in rows):.4f} | "
            f"{'-' if not lktas else format(max(lktas), '.4f')} | {n_pos}/{len(rows)} | {est:.4f} | "
            f"{r0['lkt'] - est:+.2e} | {r0['lnA']:.3e}"
        )
        if abs(r0["N"] - h["bounce"][0]) > 1e-4:
            print(
                f"#  !! the first candidate's N {r0['N']} is not the first bounce's {h['bounce'][0]}"
            )


if __name__ == "__main__":
    main()
