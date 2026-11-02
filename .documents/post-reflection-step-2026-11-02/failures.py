"""
The m_eff^2 failures of the 2026.8.0 run's main store, read-only: for each, M, beta, alpha, phi_E/M at the
failure, and T_J there, read off a successful history at the same (M, beta) (the nearest alpha) by matching
ln rho_R,E, with that history's parking temperature.

    ./venv/bin/python .documents/post-reflection-step-2026-11-02/failures.py > .documents/post-reflection-step-2026-11-02/out/failures.txt

Opens each shard with sqlite's mode=ro. Needs no Ray.
"""

import glob
import re
import sqlite3

import numpy as np

STORE = "var/datastores/science-2026.8.0-shard*.db"
MP_GEV = 2.435324e18


def connect(path):
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


rows = []
for f in sorted(glob.glob(STORE)):
    c = connect(f)
    for r in c.execute(
        """select h.serial, m.value_eV, b.value, a.value, h.failure, h.failure_reason, h.parking_log_T_Jordan
           from ScalarHistory h join ExponentialModel e on h.model_serial = e.serial
           join M_value m on e.M_serial = m.serial join beta_value b on e.beta_serial = b.serial
           join alpha_value a on h.alpha_serial = a.serial"""
    ):
        rows.append((f,) + r)

fails = [r for r in rows if r[6] and "m_eff" in r[6]]
print(f"{len(rows)} histories; {len(fails)} failed with m_eff^2 <= 0")
print(f"stored samples of the failed histories: ", end="")
n = 0
for f, s, *_ in fails:
    n += (
        connect(f)
        .execute(
            "select count(*) from ScalarHistoryValue where history_serial = ?", (s,)
        )
        .fetchone()[0]
    )
print(n)
neg = sum(
    connect(f)
    .execute("select count(*) from ScalarHistoryValue where phi_Einstein_Mp < 0")
    .fetchone()[0]
    for f in sorted(glob.glob(STORE))
)
print(f"stored samples anywhere in the store with phi_E < 0: {neg}")
print(f"longest stored failure_reason: {max(len(r[6]) for r in fails)} characters")
print()
print(
    f"{'M/Mp':>8} {'beta':>5} {'alpha':>6} {'phi_E/M':>10} {'T_J [eV]':>10} {'sibling park T_J [eV]':>22} sibling_alpha"
)
out = []
for f, s, M, b, a, _, reason, _ in fails:
    lr = float(re.search(r"ln rho_R,E = ([-0-9.e+]+)", reason).group(1))
    phi = float(re.search(r"phi_E = ([-0-9.e+]+)", reason).group(1))
    sib = [r for r in rows if r[2] == M and r[3] == b and not r[5] and r[4] > 0]
    sb = min(sib, key=lambda r: abs(r[4] - a))
    v = np.array(
        connect(sb[0])
        .execute(
            "select log_rhorad_Einstein_Mp4, log_T_Jordan_GeV from ScalarHistoryValue where history_serial = ? order by raw_N",
            (sb[1],),
        )
        .fetchall()
    )
    k = np.argmin(abs(v[:, 0] - lr))
    Mmp = M / 1e9 / MP_GEV
    park = np.exp(sb[7]) * 1e9 if sb[7] is not None else float("nan")
    out.append((Mmp, b, a, phi / Mmp, np.exp(v[k, 1]) * 1e9, park, sb[4]))
for Mmp, b, a, p, T, park, sa in sorted(out):
    print(f"{Mmp:8.0e} {b:5.2f} {a:6.3f} {p:10.3g} {T:10.4g} {park:22.4g} {sa}")
