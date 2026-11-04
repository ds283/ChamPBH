"""
crossover side-campaign, stage 1b: the Born scheme on the cascade's own bounces, with the production computed with
massless (Born) and massive mode functions on the same trajectory.

The (L, ln R) of each point are read from a cascade map (cascade_map.py's output), one-loop rows only, at the ell
nearest each target. For each: the chain's ell in these units (a check against the code's), the scheme run with
k_IR = sqrt(V''_max) self-consistently, and three production fractions of E_phys:
  - Efl:     E_fl/E at the end, as the scheme gives it;
  - born:    (1/8 pi^2) int_{k_IR} k |FT V''(2k)|^2 dk x 8 pi^2 R, massless, cut at k_IR;
  - massive: (1/2 pi^2) int k^2 omega |beta_k|^2 dk x 8 pi^2 R, exact massive modes on the trajectory, no cutoff;
beside 2.7/ell, the law. A first row at the note's worked example (L = 105.4) checks the physical-cutoff value
(the note: about 0.09).

    ../../../../venv/bin/python stage1b_born.py out/cascade-P-2197-A0.05.txt > out/stage1b_born.txt
"""

import sys
import time
from math import exp, log, pi

import numpy as np

sys.path.insert(0, ".")
import born  # noqa: E402
from walls import Wall  # noqa: E402

TARGETS = (32.0, 16.0, 10.0, 7.4, 5.8, 4.9, 4.3, 3.7, 3.3, 2.8, 2.4, 2.0, 1.5, 1.0)


def rows(path):
    out = []
    for line in open(path):
        if line.startswith("#") or line.startswith("@@") or not line.strip():
            continue
        f = line.split()
        if f[2] != "one_loop":
            continue
        out.append(dict(idx=int(f[0]), ell=float(f[3]), lnR=float(f[6]), L=float(f[7])))
    return out


def main(path):
    cands = rows(path)
    pts = []
    for tgt in TARGETS:
        r = min(cands, key=lambda c: abs(c["ell"] - tgt))
        if r not in pts:
            pts.append(r)
    print(
        f"# stage 1b on {path}: Born scheme with k_IR = sqrt(V''_max), production fractions of E"
    )
    print(
        "#  idx       L     ln R   ell(code)  ell(chain)  kIR/kex   M/phi_ta    Efl     born   massive   2.7/ell  v_out/v_in  back?  s"
    )
    for r in pts:
        t0 = time.time()
        w = Wall(r["L"])
        R = exp(r["lnR"])
        ch = born.chain(w, R)
        if ch is None:
            print(
                f"{r['idx']:5d} {r['L']:8.3f} {r['lnR']:8.3f} {r['ell']:10.4f}   no chain"
            )
            continue
        try:
            res = born.run_self_consistent(w, R)
        except Exception as e:  # noqa: BLE001
            print(
                f"{r['idx']:5d} {r['L']:8.3f} {r['lnR']:8.3f} {r['ell']:10.4f} {ch[3]:11.4f}   scheme failed: {e}"
            )
            continue
        fb = born.born_production(w, res) * 8.0 * pi**2 * R
        # the massive modes are not computed above ell = 12, where the hierarchy k_ex/m_ta > e^12 is beyond what
        # the mode grid resolves and the Born regime holds; nor where the field did not come back out
        fm = (
            born.massive_production(w, res)[0] * 8.0 * pi**2 * R
            if res["returned"] and ch[3] <= 12.0
            else float("nan")
        )
        print(
            f"{r['idx']:5d} {r['L']:8.3f} {r['lnR']:8.3f} {r['ell']:10.4f} {ch[3]:11.4f} "
            f"{res['kIR']/ch[2]:8.4f} {1/res['phi_turn']:10.3f} {res['Efl_final']:7.4f} {fb:8.4f} {fm:9.4f} "
            f"{2.7/ch[3]:9.4f} {res['vout']:10.4f} {str(res['returned']):>6s} {time.time()-t0:5.0f}",
            flush=True,
        )


if __name__ == "__main__":
    main(sys.argv[1])
