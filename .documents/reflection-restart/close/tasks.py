"""
reflection-restart prompt 04 (the close): write the three task lists of the close, from the 2026.8.0 store,
read-only (orchestrator note 04, ruling 1). Run from the repository root:

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/close/tasks.py

Writes, under var/reflection-restart/04/:
  - tasks-42.json: the 42 histories of block C whose stored failure_reason contains log_m_eff_squared, in
    mode "tuned", with normalization, phi_ref_Mp, store and failure dropped, beta, M, alpha, phi_init_Mp,
    T_init_GeV, atol and rtol kept (log 03 §8; prompt §1.1);
  - tasks-pair.json: 2197 (shard0005) and 2188 (shard0003), in mode "store", as select_from_store gives
    them, with absolute shard paths (prompt §1.2 (a));
  - tasks-nine.json: the nine "not monotone" classical_turning_point failures of prompt §0.2, keyed on their
    (shard, serial) pairs, tuned as the 42 are, the three at alpha = 1e-3 first (ruling 2).
Each task carries an explicit "key" naming its shard and serial, so that a record can be traced to its stored
row. Prints the counts the Gate and the stop conditions name, and each task's stored failure_reason.

The 2026.8.0 store is opened with sqlite mode=ro only (tools/outward_step_scan.py's connect_ro).
"""

import json
import sys
from collections import Counter
from pathlib import Path

from tools.outward_step_scan import connect_ro, select_from_store, store_shards

STORE = str(Path("var/datastores/science-2026.8.0").resolve())
OUT = Path("var/reflection-restart/04")
KEEP = ("beta", "M", "alpha", "phi_init_Mp", "T_init_GeV", "atol", "rtol")

PAIR = (("shard0005", 2197), ("shard0003", 2188))
# prompt §0.2's nine (shards from orchestrator note 04, §0); the three at alpha = 1e-3 first (ruling 2)
NINE = (
    ("shard0002", 1592),
    ("shard0003", 2208),
    ("shard0004", 2199),
    ("shard0000", 1001),
    ("shard0001", 766),
    ("shard0015", 403),
    ("shard0000", 2735),
    ("shard0015", 2480),
    ("shard0015", 2386),
)


def shard_name(spec):
    return Path(spec["store"]["shard"]).name.rsplit("-", 1)[1].removesuffix(".db")


def failure_reason(spec):
    conn = connect_ro(spec["store"]["shard"])
    row = conn.execute(
        "select failure_reason from ScalarHistory where serial = ?",
        (spec["store"]["serial"],),
    ).fetchone()
    conn.close()
    return None if row is None else row["failure_reason"]


def tuned(spec):
    task = {"mode": "tuned"}
    task.update({k: spec[k] for k in KEEP})
    task["key"] = (
        f"{shard_name(spec)}#{spec['store']['serial']} beta={spec['beta']!r} "
        f"M={spec['M']!r} alpha={spec['alpha']!r} tuned"
    )
    return task


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    print(f"store {STORE}: {len(store_shards(STORE))} shards")

    every = select_from_store(STORE)
    print(f"histories in the store: {len(every)}")
    meff_all = [s for s in every if "log_m_eff_squared" in (failure_reason(s) or "")]
    tcp_all = [
        s
        for s in every
        if (failure_reason(s) or "").startswith("classical_turning_point")
        and "not monotone" in failure_reason(s)
    ]
    print(
        f"over all shards: log_m_eff_squared failures {len(meff_all)}, "
        f"classical_turning_point 'not monotone' failures {len(tcp_all)}"
    )

    # the 42
    block_c = select_from_store(STORE, blocks=["C"])
    the_42 = [s for s in block_c if "log_m_eff_squared" in (failure_reason(s) or "")]
    by_M = Counter(s["M"] for s in the_42)
    print(
        f"block C: {len(block_c)} histories; log_m_eff_squared failures {len(the_42)}; "
        f"by M: "
        + ", ".join(f"{m:g}: {n}" for m, n in sorted(by_M.items(), reverse=True))
    )
    print(
        "by alpha: "
        + ", ".join(
            f"{a:g}: {n}"
            for a, n in sorted(Counter(s["alpha"] for s in the_42).items())
        )
    )
    print("shards: " + ", ".join(sorted({shard_name(s) for s in the_42})))
    tasks_42 = [tuned(s) for s in the_42]
    (OUT / "tasks-42.json").write_text(json.dumps(tasks_42, indent=1) + "\n")

    # the pair, in mode "store"
    pair = []
    for shard, serial in PAIR:
        hit = [
            s
            for s in every
            if shard_name(s) == shard and s["store"]["serial"] == serial
        ]
        assert len(hit) == 1, (shard, serial, len(hit))
        s = hit[0]
        assert not s["failure"] and s["normalization"] is not None
        pair.append(s)
    (OUT / "tasks-pair.json").write_text(json.dumps(pair, indent=1) + "\n")

    # the nine, tuned
    nine = []
    for shard, serial in NINE:
        hit = [
            s
            for s in tcp_all
            if shard_name(s) == shard and s["store"]["serial"] == serial
        ]
        assert len(hit) == 1, (shard, serial, len(hit))
        nine.append(hit[0])
    assert {(shard_name(s), s["store"]["serial"]) for s in nine} == {
        (shard_name(s), s["store"]["serial"]) for s in tcp_all
    }
    (OUT / "tasks-nine.json").write_text(
        json.dumps([tuned(s) for s in nine], indent=1) + "\n"
    )

    print()
    print("the pair (mode store):")
    for s in pair:
        print(
            f"  {shard_name(s)}#{s['store']['serial']} beta={s['beta']!r} M={s['M']!r} "
            f"alpha={s['alpha']!r} normalization={s['normalization']!r} phi_ref_Mp={s['phi_ref_Mp']!r}"
        )
    print()
    print(
        "the nine (tuned), with their stored failure_reason (truncated in the store):"
    )
    for s in nine:
        print(
            f"  {shard_name(s)}#{s['store']['serial']} beta={s['beta']!r} M={s['M']!r} "
            f"alpha={s['alpha']!r} normalization={s['normalization']!r}"
        )
        print(f"    {failure_reason(s)}")
    print()
    print("the 42 (tuned), with their stored failure_reason:")
    for s in the_42:
        print(
            f"  {shard_name(s)}#{s['store']['serial']} beta={s['beta']!r} M={s['M']!r} alpha={s['alpha']!r}"
        )
        print(f"    {failure_reason(s)}")
    print()
    print(
        f"wrote {OUT}/tasks-42.json ({len(tasks_42)}), tasks-pair.json ({len(pair)}), "
        f"tasks-nine.json ({len(nine)})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
