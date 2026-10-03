"""
bbn-tolerance prompt 01c, README section 2 (c'): enumerate T2's breadth sample
read-only, through the tool's connect_ro, and write it to breadth.csv.

    ./venv/bin/python prompts/bbn-tolerance/logs/01c-probes/breadth.py

The sample is
  part "phi5":   every 10th of the phi* = 5 histories in (M, beta) order,
                 starting with the first (indices 0, 10, 20, ...);
  part "phiN5":  every history whose phi* != 5, in (phi*, M, beta) order.
For each it records the shard, serial, beta, M / M_P, phi*, and the stored
BBNData row's failure flag and the start of its reason, and checks that the
tool's find_model would match exactly that one history (beta and phi* to
1e-9, M to 1e-3 relative). Prints the counts.
"""

import csv
import glob
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

import tools.bbn_from_store as T  # noqa: E402
from Units import Planck_units  # noqa: E402

STORE = os.path.expanduser("~/ChamPBH-stores/science-2026.6.0")
HERE = Path(__file__).resolve().parent

QUERY = """
    select sm.serial, b.value, mv.value_eV, ph.value_PlanckMass,
           bd.failure, bd.failure_reason
    from ScalarModel sm
    join ExponentialCoupling c on c.serial = sm.coupling_serial
    join beta_value b on b.serial = c.beta_serial
    join ExponentialPotential p on p.serial = sm.potential_serial
    join M_value mv on mv.serial = p.M_serial
    join phi_value ph on ph.serial = sm.phi_Einstein_init_serial
    left join BBNData bd on bd.model_serial = sm.serial
"""


def main():
    units = Planck_units()
    Mp_eV = units.PlanckMass / units.eV
    hist = []
    for shard in sorted(glob.glob(STORE + "-shard*.db")):
        con = T.connect_ro(shard)
        try:
            for serial, beta, M_eV, phi, fail, reason in con.execute(QUERY):
                hist.append(
                    dict(
                        shard=Path(shard).name[-7:-3],
                        serial=serial,
                        beta=beta,
                        M_Mp=M_eV / Mp_eV,
                        phi_init_Mp=phi,
                        stored_failure=fail,
                        stored_reason=(reason or "")[:90],
                    )
                )
        finally:
            con.close()

    # find_model's matching rule must single out each history
    for h in hist:
        n = sum(
            1
            for g in hist
            if abs(g["beta"] - h["beta"]) < 1e-9
            and abs(g["phi_init_Mp"] - h["phi_init_Mp"]) < 1e-9
            and abs(g["M_Mp"] / h["M_Mp"] - 1.0) < 1e-3
        )
        if n != 1:
            raise SystemExit(f"{n} histories match {h}")

    phi5 = sorted(
        (h for h in hist if abs(h["phi_init_Mp"] - 5.0) < 1e-9),
        key=lambda h: (h["M_Mp"], h["beta"]),
    )
    other = sorted(
        (h for h in hist if abs(h["phi_init_Mp"] - 5.0) >= 1e-9),
        key=lambda h: (h["phi_init_Mp"], h["M_Mp"], h["beta"]),
    )
    sample = [dict(h, part="phi5", index=i) for i, h in enumerate(phi5) if i % 10 == 0]
    sample += [dict(h, part="phiN5", index=i) for i, h in enumerate(other)]

    fields = [
        "part",
        "index",
        "beta",
        "M_Mp",
        "phi_init_Mp",
        "shard",
        "serial",
        "stored_failure",
        "stored_reason",
    ]
    with open(HERE / "breadth.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for h in sample:
            w.writerow(
                {
                    **{k: h[k] for k in fields},
                    "beta": f"{h['beta']:.10g}",
                    "M_Mp": f"{h['M_Mp']:.6g}",
                    "phi_init_Mp": f"{h['phi_init_Mp']:.10g}",
                }
            )
    n5 = sum(1 for h in sample if h["part"] == "phi5")
    print(
        f"histories in the store: {len(hist)}; phi* = 5: {len(phi5)}; phi* != 5: {len(other)}"
    )
    print(f"breadth sample: {n5} (every 10th phi* = 5) + {len(other)} (phi* != 5)")
    print(
        f"stored BBN failures in the sample: "
        f"{[(h['beta'], round(h['M_Mp'], 6), h['phi_init_Mp'], h['stored_reason'][:60]) for h in sample if h['stored_failure']]}"
    )


if __name__ == "__main__":
    main()
