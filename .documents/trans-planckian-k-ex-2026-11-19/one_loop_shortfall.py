"""
ln(k_t/k_ta) at every one-loop bounce of the 07d-r2 rehearsal exports (trans-planckian-k-ex note, 2026-11-19, §3).

k_t, the classical k_ex the bounce would have had, is rebuilt from the stored log_E_Mp4 and log_R through the closed
forms: ln omega_t = (ln 8 pi^2 + ln E + ln R)/4, and k_t/omega_t = (n + 2)/(2 sqrt(n(n + 1))) on the inverse power,
c(L)/(2 sqrt(n(n + 1)L + n^2 L^2)) sqrt(1 - e^{-L}) on the exponential. The rebuild is checked on the classical-regime
bounces, where the stored log_k_ex_Mp is k_t. Reads var/power-law-wall/07d-r2 (git-ignored; on the power-law-wall
worktree). Run from the repository root: ./venv/bin/python <this file>.
"""

import csv
import glob
import math
import re
from collections import defaultdict
from math import log, pi, sqrt

ROOT = "var/power-law-wall/07d-r2"
by_group = defaultdict(list)
by_beta = defaultdict(list)
check = []
for f in glob.glob(
    ROOT + "/export-[pe][lx]*/QCD_Cosmology/*/*/A=*/csv/*/*/bounces.csv"
):
    if "resumed" in f:
        continue
    wall = "ipl" if "inverse-power" in f else "exp"
    A, beta, M, n = re.search(
        r"A=([^/]+)/csv/beta=([^/]+)/M=([^/]+)eV_n=(\d)", f
    ).groups()
    n = int(n)
    for r in csv.DictReader(open(f)):
        try:
            lnE = float(r["log_E_Mp4"])
            lnR = float(r["log_R"])
            lnk = float(r["log_k_ex_Mp"])
        except ValueError:
            continue
        ln_om = 0.25 * (log(8 * pi * pi) + lnE + lnR)
        if wall == "ipl":
            ln_kt = ln_om + log((n + 2) / (2 * sqrt(n * (n + 1))))
        else:
            L = float(r["L"])
            c = ((n + 1) * (n + 2) + 3 * n * (n + 1) * L + n * n * L * L) / (
                (n + 1) + n * L
            )
            ln_kt = (
                ln_om
                + log(c / (2 * sqrt(n * (n + 1) * L + n * n * L * L)))
                + 0.5 * log(-math.expm1(-L))
            )
        if r["regime"] == "classical":
            check.append(abs(ln_kt - lnk))
        if r["regime"] == "one_loop":
            by_group[(wall, n, float(M))].append(ln_kt - lnk)
            if M == "0.00097413":
                by_beta[(wall, n, float(beta))].append(ln_kt - lnk)

print(
    f"check on {len(check)} classical-regime bounces: max |ln k_t(rebuilt) - ln k_ex(stored)| = {max(check):.2e}"
)
print()
print("by (wall, n, M/eV), all A and beta:")
for k in sorted(by_group):
    v = sorted(by_group[k])
    print(
        f"  {k}  N={len(v):4d}  ln(k_t/k_ta): min {v[0]:6.1f}  median {v[len(v) // 2]:6.1f}  max {v[-1]:6.1f}"
    )
print()
print("by (wall, n, beta) at M = 9.7413e-4 eV, all A:")
for k in sorted(by_beta):
    v = sorted(by_beta[k])
    print(
        f"  {k}  N={len(v):4d}  ln(k_t/k_ta): min {v[0]:6.1f}  median {v[len(v) // 2]:6.1f}  max {v[-1]:6.1f}"
    )
