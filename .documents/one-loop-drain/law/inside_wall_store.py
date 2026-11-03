"""
one-loop-drain prompt 01 §1.7: the histories of the 2026.8.0 store (read-only) that failed with "reflection
requested inside the wall" (the guard G2 that 3873 meets under the one-loop law), with M/M_P, beta, alpha and the
reported W/(pi^2/2), N and phi. They were made under the fixed law, before reflection-restart.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/inside_wall_store.py > .documents/one-loop-drain/law/out/inside_wall_store.txt
"""

import glob
import re
import sqlite3
import sys

sys.path.insert(0, ".")
from tools.outward_step_scan import _M_over_Mp  # noqa: E402
from Units import Planck_units  # noqa: E402

STORE_GLOB = (
    "/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0-shard*.db"
)
P = re.compile(
    r"W/\(pi\^2/2\) = (?P<ratio>[0-9.e+-]+) at N=(?P<N>[0-9.e+-]+), phi_E=(?P<phi>[0-9.e+-]+)"
)


def main():
    units = Planck_units()
    rows, total, failures = [], 0, 0
    for f in sorted(glob.glob(STORE_GLOB)):
        c = sqlite3.connect(f"file:{f}?mode=ro", uri=True)
        for serial, fail, reason, M_eV, beta, alpha in c.execute(
            """select h.serial, h.failure, h.failure_reason, m.value_eV, b.value, a.value
               from ScalarHistory h join ExponentialModel e on h.model_serial=e.serial
               join M_value m on e.M_serial=m.serial join beta_value b on e.beta_serial=b.serial
               join alpha_value a on h.alpha_serial=a.serial"""
        ):
            total += 1
            failures += bool(fail)
            if fail and "inside the wall" in (reason or ""):
                m = P.search(reason)
                rows.append(
                    (
                        f.split("-")[-1][:-3],
                        serial,
                        _M_over_Mp(M_eV, units),
                        beta,
                        alpha,
                        m["ratio"] if m else "?",
                        m["N"] if m else "?",
                        m["phi"] if m else "?",
                    )
                )
        c.close()
    print(
        f"{total} histories, {failures} failures, {len(rows)} of them 'reflection requested inside the wall'"
    )
    print()
    print("| shard#serial | M/M_P | beta | alpha | W/(pi^2/2) | N | phi_E/M_P |")
    print("|---|---|---|---|---|---|---|")
    for shard, serial, M, beta, alpha, ratio, N, phi in sorted(
        rows, key=lambda r: (r[2], r[3], r[4])
    ):
        print(
            f"| {shard}#{serial} | {M:g} | {beta} | {alpha} | {ratio} | {N} | {phi} |"
        )


if __name__ == "__main__":
    main()
