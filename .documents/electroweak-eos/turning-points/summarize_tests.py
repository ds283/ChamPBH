"""
Probe (electroweak-eos prompt 01a): the 22 tests on the new table, before and after 01a's change, from
two outputs of run_tests_new_table.py. Prints, per test, the status before and after and, where it still
fails, its first failing assertion (the last test-file frame and the exception); then the integrations
with the early pair before, and the pairs dropped after.

  PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/turning-points/summarize_tests.py BEFORE.json AFTER.json
"""

import json
import sys


def short(test_id):
    for a, b in (
        ("ComputeTargets.tests.", "CT."),
        ("ObjectFactories.tests.", "OF."),
        ("tools.tests.", "tools."),
    ):
        test_id = test_id.replace(a, b)
    return test_id


def main():
    b = json.load(open(sys.argv[1]))
    a = json.load(open(sys.argv[2]))
    print(f"table: {a['table']}")
    print(f"before: resolved_sign_changes present = {b['has_resolved_sign_changes']}")
    print(f"after:  resolved_sign_changes present = {a['has_resolved_sign_changes']}\n")
    counts = {}
    for k, va in a["outcome"].items():
        vb = b["outcome"][k]
        counts[(vb["status"], va["status"])] = (
            counts.get((vb["status"], va["status"]), 0) + 1
        )
        print(f"{vb['status']:5} -> {va['status']:5}  {short(k)}")
        if va["status"] != "pass":
            print(f"      at {va.get('frame')}")
            print(f"      {va.get('exception')}")
            if va.get("subtests"):
                print(f"      and {len(va['subtests'])} failing subtests")
    print()
    for (sb, sa), n in sorted(counts.items()):
        print(f"{n:3} tests: {sb} -> {sa}")
    bi, ai = b["integrations"], a["integrations"]
    print(
        f"\nintegrations: before {len(bi)}, {sum(1 for x in bi if x.get('early'))} with the early pair; "
        f"after {len(ai)}, {sum(1 for x in ai if x.get('early'))} with an early TurningPoint, "
        f"{sum(1 for x in ai if (x.get('unresolved_turning_point_pairs') or 0) > 0)} dropping a pair, "
        f"{sum(x.get('unresolved_turning_point_pairs') or 0 for x in ai)} pairs dropped in all"
    )
    agree = all(
        x.get("model") == y.get("model")
        and bool(x.get("early")) == ((y.get("unresolved_turning_point_pairs") or 0) > 0)
        for x, y in zip(bi, ai)
    )
    print(
        f"in call order, the integrations that dropped a pair are exactly those that had it: {agree}"
    )
    for x, y in zip(bi, ai):
        print(
            f"  {str(y.get('model'))[:62]:62} alpha={y.get('alpha')}: early pair before "
            f"{len(x.get('early', []))} TPs, dropped_drains {x.get('dropped_drains')} -> "
            f"{y.get('dropped_drains')}, unresolved {y.get('unresolved_turning_point_pairs')}"
        )


if __name__ == "__main__":
    main()
