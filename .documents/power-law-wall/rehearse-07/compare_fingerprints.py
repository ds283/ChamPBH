"""
power-law-wall prompt 07 §2.1 (P64): two store sidecars' fingerprints, class by class.

    ./venv/bin/python .documents/power-law-wall/rehearse-07/compare_fingerprints.py SIDECAR_A SIDECAR_B

SIDECAR_A and SIDECAR_B are `<stem>.manifest.json` files whose `fingerprint` field `runregistrykit store fingerprint
--write` has recorded. Stdlib only; it reads the two JSON files and writes nothing but stdout (prompt 07 sends it to a
file under var/power-law-wall/07/). It opens no store.

It prints each sidecar's fingerprint format and top digest, then every class either names: its count and digest in
each, and whether they are equal; a class one sidecar names and the other does not is listed with the side that names
it and its count there.

Exit 0 when every class both name is equal in count and digest and every class named on one side only has count 0;
1 otherwise. The top digests and formats are printed, not judged: they differ whenever the formats do.
"""

import json
import sys


def load(path):
    d = json.load(open(path))
    fp = d.get("fingerprint")
    if not fp:
        raise SystemExit(f"{path}: no fingerprint recorded in the sidecar")
    return d, fp


def main(argv):
    pa, pb = argv
    da, fa = load(pa)
    db, fb = load(pb)
    print(f"A: {pa} (store {da.get('name')}, store_id {da.get('store_id')})")
    print(f"B: {pb} (store {db.get('name')}, store_id {db.get('store_id')})")
    for tag, fp in (("A", fa), ("B", fb)):
        print(
            f"{tag}: format {fp.get('fingerprint_format')}, digest {fp.get('digest')}, "
            f"{len(fp['classes'])} classes"
        )
    ca, cb = fa["classes"], fb["classes"]
    ok = True
    print()
    print(f"{'class':24s} {'count A':>8s} {'count B':>8s}  digest A / digest B")
    for name in sorted(set(ca) | set(cb)):
        a, b = ca.get(name), cb.get(name)
        if a is not None and b is not None:
            eq = a["count"] == b["count"] and a["digest"] == b["digest"]
            ok &= eq
            print(
                f"{name:24s} {a['count']:8d} {b['count']:8d}  {a['digest'][:16]}… / {b['digest'][:16]}…  "
                f"{'equal' if eq else 'DIFFER'}"
            )
        else:
            side, x = ("A", a) if a is not None else ("B", b)
            print(
                f"{name:24s} {'-' if a is None else a['count']:>8} {'-' if b is None else b['count']:>8}  "
                f"named by {side} only, count {x['count']}, digest {x['digest'][:16]}…"
            )
            ok &= x["count"] == 0
    print()
    print(
        f"RESULT: {'every class both name equal in count and digest; every one-sided class empty' if ok else 'NOT EQUAL (see above)'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
