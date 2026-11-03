"""
one-loop-drain prompt 02 §1.3, "The fixed-law references": for each stored (M, beta) of §1.1, every validated success
at that (M, beta) in the 2026.8.0 store, read-only from the main store's shards only (`science-2026.8.0-shard*.db`,
mode=ro; orchestrator note 02, ruling 8; the two sibling stores are not read): its serial and shard, alpha,
phi* and T* (so that a reference at other initial data would show), Delta N_eff at 1 MeV and today, r_chi today,
parking N, the stored count of injections (drains), log_R_first_impact, and its BBN row's Y_p and D/H (DOverH as
stored, in units of 10^-5) with the network. The ladder has no reference. A stored history whose BBN row is
missing or failed is listed with None there.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure/fixed_law.py var/one-loop-drain/02/tasks.json \\
        var/one-loop-drain/02/fixed_law.json > .documents/one-loop-drain/measure/out/fixed_law.txt
"""

import glob
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, ".")

from Units.base import REDUCED_PLANCK_MASS_EV  # noqa: E402

STORE_GLOB = (
    "/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0-shard*.db"
)

QUERY = """
select h.serial, m.value_eV, b.value, a.value, p.value_PlanckMass, t.value_GeV,
       h.Delta_N_eff_1MeV, h.Delta_N_eff_today, h.rho_chi_ratio_today, h.parking_N, h.injections,
       h.log_R_first_impact, h.tuning_iterations,
       d.Yp_BBN, d.DOverH, d.small_network, d.failure
from ScalarHistory h
join ExponentialModel e on h.model_serial = e.serial
join M_value m on e.M_serial = m.serial
join beta_value b on e.beta_serial = b.serial
join alpha_value a on h.alpha_serial = a.serial
join phi_value p on h.phi_Einstein_init_serial = p.serial
join temperature t on h.T_Jordan_init_serial = t.serial
left join BBNData d on d.scalar_history_serial = h.serial
where h.failure = 0 and h.validated = 1 and b.value = ?
"""

FIELDS = (
    "serial",
    "M_eV",
    "beta",
    "alpha",
    "phi_init_Mp",
    "T_init_GeV",
    "Delta_N_eff_1MeV",
    "Delta_N_eff_today",
    "rho_chi_ratio_today",
    "parking_N",
    "injections",
    "log_R_first_impact",
    "tuning_iterations",
    "Yp",
    "DoH",
    "small_network",
    "bbn_failure",
)


def main():
    tasks = json.loads(Path(sys.argv[1]).read_text())
    out = Path(sys.argv[2])
    keys = []
    for t in tasks:
        if t["serial"] is None:
            continue
        k = (t["history"], t["M"], t["beta"])
        if k not in keys:
            keys.append(k)

    shards = sorted(glob.glob(STORE_GLOB))
    refs = {}
    for hid, M, beta in keys:
        rows = []
        for f in shards:
            c = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
            try:
                for r in c.execute(QUERY, (beta,)):
                    d = dict(zip(FIELDS, r))
                    if abs(d["M_eV"] / REDUCED_PLANCK_MASS_EV / M - 1.0) < 1e-12:
                        d["shard"] = Path(f).name
                        rows.append(d)
            finally:
                c.close()
        rows.sort(key=lambda d: (d["alpha"], d["serial"]))
        refs[hid] = {"M": M, "beta": beta, "references": rows}

    out.write_text(json.dumps(refs, indent=1) + "\n")

    print(
        f"# fixed-law references from {len(shards)} shards of science-2026.8.0 (mode=ro), {len(keys)} stored (M, beta)"
    )
    print(
        "# DoH is DOverH as stored (10^-5); dN = Delta N_eff; r_chi today; inj = stored injections (drains)"
    )
    total = 0
    for hid, M, beta in keys:
        rows = refs[hid]["references"]
        total += len(rows)
        inits = sorted({(d["phi_init_Mp"], d["T_init_GeV"]) for d in rows})
        print(
            f"\n{hid}  M={M:g}  beta={beta!r}  successes={len(rows)}  (phi*, T*) = {inits}"
        )
        print(
            "   serial  alpha    dN_1MeV       dN_today      r_chi_today   parking_N   inj   lnR_first   Yp          DoH       net"
        )
        for d in rows:

            def g(x, fmt):
                return "None" if x is None else format(x, fmt)

            print(
                f"   {d['serial']:6d}  {d['alpha']:<7g}  {g(d['Delta_N_eff_1MeV'], '.6e')}  "
                f"{g(d['Delta_N_eff_today'], '.6e')}  {g(d['rho_chi_ratio_today'], '.6e')}  "
                f"{g(d['parking_N'], '.4f'):>10s}  {g(d['injections'], 'd'):>5s}  "
                f"{g(d['log_R_first_impact'], '.3f'):>9s}  {g(d['Yp'], '.8f')}  {g(d['DoH'], '.6f')}  "
                f"{'small' if d['small_network'] else ('full' if d['small_network'] is not None else 'None')}"
            )
    print(f"\n{total} references written to {out}")


if __name__ == "__main__":
    main()
