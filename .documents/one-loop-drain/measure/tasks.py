"""
one-loop-drain prompt 02 §1.1, §1.3: the sample's task list, written from §1.1's table and the 2026.8.0 store
(the main store's shards, `science-2026.8.0-shard*.db`, opened mode=ro; orchestrator note 02, ruling 8).

For each of the thirty histories of §1.1 it checks the Gate's last store item: the serial is in the shard the
table names, a validated success, with the table's M (to 1e-12 relative, M/M_P from the stored M in eV and
Units.base.REDUCED_PLANCK_MASS_EV), beta and alpha. A mismatch is printed as `!! GATE` and the script exits 1
without writing. It then writes `tasks.json`: one task per (history, A, gap prescription), A in {1, 0.1, 0.01},
gap in {zero, edge}, 180 in all, each with its id, group, M (in M_P, the table's figure, as the tool takes it),
beta (the store's float; the ladder's 2.0), A, gap, the store's serial and shard (None for the ladder), the
store's alpha, and the tool's argv (`BETA M --drain-law one_loop --alpha A --gap-prescription G`). Which of
them are run, and which are derived by S1 or S2, is the driver's decision (run_sample.py), not this list's.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure/tasks.py var/one-loop-drain/02/tasks.json \\
        > .documents/one-loop-drain/measure/out/tasks.txt
"""

import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, ".")

from Units.base import REDUCED_PLANCK_MASS_EV  # noqa: E402

STORE = (
    "/Users/ds283/Documents/Code/ChamPBH/var/datastores/science-2026.8.0-shard{:04d}.db"
)

LAB_M = 4e-31
# §1.1's table: (group, M / M_P, beta, shard, serial, alpha). The ladder has no store entry
HISTORIES = [
    ("P", LAB_M, 1.185, 5, 2197, 1e-3),
    ("P", LAB_M, 1.06, 3, 2188, 1e-3),
    ("W", LAB_M, 1.0775, 10, 2187, 1e-3),
    ("W", LAB_M, 1.07, 7, 1631, 1e-3),
    ("W", LAB_M, 1.0925, 0, 2179, 1e-3),
    ("W", LAB_M, 1.175, 1, 2078, 1e-3),
    ("W", LAB_M, 1.105, 5, 2192, 1e-3),
    ("W", LAB_M, 1.1875, 6, 2209, 1e-3),
    ("W", LAB_M, 1.125, 13, 2183, 1e-3),
    ("W", LAB_M, 1.1225, 12, 2172, 1e-3),
    ("W", LAB_M, 1.1025, 4, 2178, 1e-3),
    ("W", LAB_M, 1.1725, 0, 2185, 1e-3),
    ("R", LAB_M, 1.0, 2, 632, 0.01),
    ("R", LAB_M, 1.15, 7, 1193, 0.01),
    ("R", LAB_M, 2.0, 2, 865, 0.01),
    ("R", LAB_M, 10.0, 6, 611, 0.01),
    ("B", 1e-33, 1.2, 11, 2819, 0.01),
    ("B", 1e-33, 2.0, 2, 2496, 0.01),
    ("B", 1e-33, 10.0, 6, 2772, 0.01),
    ("B", 1e-30, 1.2, 11, 2821, 0.01),
    ("B", 1e-30, 2.0, 2, 2498, 0.01),
    ("B", 1e-30, 10.0, 6, 2774, 0.01),
    ("L", 5e-20, 2.0, None, None, None),
    ("L", 8e-18, 2.0, None, None, None),
    ("L", 1e-16, 2.0, None, None, None),
    ("L", 5e-16, 2.0, None, None, None),
    ("L", 4e-15, 2.0, None, None, None),
    ("C", 1e-12, 4.0, 2, 3016, 0.1),
    ("C", 1e-12, 1.0, 2, 3233, 0.15),
    ("C", 1e-12, 1.11, 7, 3873, 0.03),
]
A_VALUES = (1.0, 0.1, 0.01)
GAPS = ("zero", "edge")


def history_id(group, M, serial):
    return f"{group}-{serial}" if serial is not None else f"{group}-M{M:g}"


def a_label(A):
    return f"A{A:g}"


def task_id(hid, A, gap):
    return f"{hid}-{a_label(A)}-{gap}"


def store_row(shard, serial):
    c = sqlite3.connect(f"file:{STORE.format(shard)}?mode=ro", uri=True)
    try:
        return c.execute(
            """select m.value_eV, b.value, a.value, h.failure, h.validated
               from ScalarHistory h join ExponentialModel e on h.model_serial=e.serial
               join M_value m on e.M_serial=m.serial join beta_value b on e.beta_serial=b.serial
               join alpha_value a on h.alpha_serial=a.serial where h.serial=?""",
            (serial,),
        ).fetchone()
    finally:
        c.close()


def main():
    out = Path(sys.argv[1])
    bad = 0
    tasks = []
    print("history  group  M/M_P  beta  shard#serial  stored alpha  store check")
    for group, M, beta, shard, serial, alpha in HISTORIES:
        hid = history_id(group, M, serial)
        if serial is not None:
            row = store_row(shard, serial)
            if row is None:
                print(f"!! GATE {hid}: serial {serial} not in shard {shard:04d}")
                bad += 1
                continue
            M_eV, beta_s, alpha_s, failure, validated = row
            M_rel = M_eV / REDUCED_PLANCK_MASS_EV / M - 1.0
            ok = (
                abs(M_rel) < 1e-12
                and beta_s == beta
                and alpha_s == alpha
                and failure == 0
                and validated == 1
            )
            check = (
                f"M rel {M_rel:+.1e}, beta {beta_s!r}, alpha {alpha_s!r}, failure {failure}, "
                f"validated {validated}: {'ok' if ok else 'MISMATCH'}"
            )
            if not ok:
                print(f"!! GATE {hid}: {check}")
                bad += 1
            beta = beta_s
        else:
            check = "(no store entry)"
        where = f"{shard:04d}#{serial}" if serial is not None else "-"
        print(f"{hid:12s} {group}  {M:g}  {beta!r}  {where}  {alpha!r}  {check}")
        for A in A_VALUES:
            for gap in GAPS:
                tasks.append(
                    {
                        "id": task_id(hid, A, gap),
                        "history": hid,
                        "group": group,
                        "M": M,
                        "beta": beta,
                        "A": A,
                        "gap": gap,
                        "shard": shard,
                        "serial": serial,
                        "stored_alpha": alpha,
                        "argv": [
                            repr(beta),
                            repr(M),
                            "--drain-law",
                            "one_loop",
                            "--alpha",
                            repr(A),
                            "--gap-prescription",
                            gap,
                        ],
                    }
                )
    if bad:
        print(f"!! GATE: {bad} histories do not match; tasks.json not written")
        sys.exit(1)
    out.write_text(json.dumps(tasks, indent=1) + "\n")
    print(f"{len(HISTORIES)} histories, {len(tasks)} tasks written to {out}")


if __name__ == "__main__":
    main()
