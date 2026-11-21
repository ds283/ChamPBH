"""P4's counts: the exponential's one-loop candidates at block B's masses outside law v2's fit domain ln(V0/M^4) <= 10.

Reads one store's shards read-only through sqlite3 and writes stdout. ln(V0/M^4) of each candidate is taken
directly, ln E - ln(e^L - 1) - 4 ln M with E, L and M in units of M_P, and checked against 07b's formula from
(L, ln R). Run where the store is, from the repository root (the probe's on the Mac Studio, 2026-11-21):

    ./venv/bin/python science-runs/plan-science-2026.14.0/probe/p4_counts.py var/datastores/<store stem>
"""

import glob
import sqlite3
import statistics as st
import sys
from collections import defaultdict
from math import expm1, log, pi

MP = 2.4353235955263046e27  # REDUCED_PLANCK_MASS_EV
STEM = sys.argv[1]


def f07b(L, lnR):  # 07b's fits.ln_v0m4 at n = 1
    return lnR + log(8 * pi**2) + log(expm1(L)) - 2 * L - 2 * log(L**4 + 2 * L**3)


Q = """select b.history_serial, b.regime, b.L, b.log_R, b.log_E_Mp4, mv.value_eV, bv.value, a.drain_model
       from ScalarHistoryBounce b join ScalarHistory h on h.serial = b.history_serial
       join ExponentialModel m on m.serial = h.model_serial and h.model_type = 1
       join M_value mv on mv.serial = m.M_serial join beta_value bv on bv.serial = m.beta_serial
       join alpha_value a on a.serial = h.alpha_serial
       where b.source = 'candidate'"""

rows = []
for path in sorted(glob.glob(STEM + "-shard*.db")):
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    rows += [(path,) + tuple(r) for r in c.execute(Q)]
print(
    f"{STEM}: {len(rows)} candidate rows; drain models {sorted({r[8] for r in rows})}"
)

diffs, per_hist = [], defaultdict(set)
by_M = defaultdict(list)
for path, hs, regime, L, lnR, lnE, M_eV, beta, dm in rows:
    if not (L and L > 0) or lnE is None:
        continue
    lnM = log(M_eV / MP)
    direct = lnE - log(expm1(L)) - 4 * lnM
    if lnR is not None:
        diffs.append(abs(direct - f07b(L, lnR)))
    per_hist[(path, hs)].add(round(direct, 6))
    by_M[M_eV / MP].append((direct, beta, (path, hs), regime))

print(f"max |direct - 07b's formula| = {max(diffs):.3g} over {len(diffs)} candidates")
print(
    f"histories whose candidates give more than one value (to 1e-6): "
    f"{sum(1 for s in per_hist.values() if len(s) > 1)} of {len(per_hist)}"
)
print(
    f"{'M/M_P':>8} {'cands':>6} {'outside':>8} {'share':>6} {'hists':>5} {'out':>4} {'min':>7} {'median':>7} {'max':>7}"
)
for M in sorted(by_M):
    v = by_M[M]
    vals = [x[0] for x in v]
    out = [x for x in v if x[0] > 10]
    hists = {x[2] for x in v}
    hout = {x[2] for x in out}
    print(
        f"{M:8.3g} {len(v):6} {len(out):8} {100 * len(out) / len(v):5.1f}% {len(hists):5} {len(hout):4} "
        f"{min(vals):7.2f} {st.median(vals):7.2f} {max(vals):7.2f}"
    )
    bb = defaultdict(list)
    for x in v:
        bb[x[1]].append(x[0])
    print(
        "          by beta, median ln(V0/M^4): "
        + ", ".join(f"{b:g}: {st.median(z):.1f}" for b, z in sorted(bb.items()))
    )
