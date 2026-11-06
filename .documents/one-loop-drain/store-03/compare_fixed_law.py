"""
one-loop-drain prompt 03, Store gate §2.1: compare two outputs of fixed_law_payloads.py, key by key.

    python compare_fixed_law.py BASE.json TIP.json

Prints, per run, every key of each and whether its repr is equal; the keys only the tip has, and their values'
summary. Exit 0 iff every key the base has is equal in the tip and the tip's only new keys are "bounces" (None),
"log_m_eff" and "log_m_tracker".
"""

import json
import sys

NEW = {"bounces", "log_m_eff", "log_m_tracker"}


def main() -> int:
    base = json.load(open(sys.argv[1]))
    tip = json.load(open(sys.argv[2]))
    ok = True
    for run in sorted(base):
        b, t = base[run], tip[run]
        print(f"== {run}: {len(b)} keys at the base, {len(t)} at the tip")
        for key in sorted(b):
            equal = key in t and t[key] == b[key]
            ok &= equal
            print(f"   {key:32s} {'equal' if equal else 'DIFFERS'}")
        for key in sorted(set(t) - set(b)):
            value = t[key]
            print(f"   {key:32s} new: {value[:60]}{'...' if len(value) > 60 else ''}")
            ok &= key in NEW
        ok &= t.get("bounces") == "None"
    print(
        "RESULT:",
        (
            "every base key equal; new keys only bounces (None) and the masses"
            if ok
            else "FAIL"
        ),
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
