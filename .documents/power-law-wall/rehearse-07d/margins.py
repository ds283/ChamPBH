"""
power-law-wall prompt 07d §3 (P119; P88): the relativistic margins of each history of a store of 07's tree (A, the old
k_ex) beside the same history of a store of 07d's (B, one k_ex). New in 07d. MARGIN_THRESHOLD is not read or changed;
this script reports what is stored.

    ./venv/bin/python .documents/power-law-wall/rehearse-07d/margins.py A_STEM B_STEM

A_STEM and B_STEM are the stores' paths without `.db`. Stdlib only, and compare_histories.py of this directory for its
readers. Every shard (`<stem>-shard*.db`) is read through sqlite3 with `file:...?mode=ro` URIs only; no primary is
opened, neither datastorekit nor Ray is used, and nothing is written but stdout (prompt 07d sends it to a file under
var/power-law-wall/07d-r2/).

- **Matched** by (potential, M, n, beta, phi*, alpha, drain model), floats by float.hex, A's drain model through the map
  of 07's names to 07d's (`one-loop-measured-pl-v1` -> `one-loop-inverse-power-v1`, U11; `one-loop-measured-v1` ->
  `one-loop-exponential-v2`, R14), every history of both stores.
- **Per history:** birth_failing_share, reflection_failing_share, tracker_failing_share and unassessed_share, and
  worst_<kind>_margin for birth, reflection and tracker, A beside B and the change B - A.
- **Then**, per group (potential, n, B's drain model) and over all: for each failing share and unassessed_share the
  count of histories in which it rose, fell or held (or is NULL on a side), and for each worst margin the same with the
  largest rise and fall.

Exit 0 when every history is matched exactly once each way; 1 otherwise (a measurement: no value is judged).
"""

import argparse
import sys
from collections import Counter, defaultdict

from compare_histories import MAP_EXPONENTIAL, MAP_POWER, histories, hkey

M_P_EV = 2.4353235955263046e27
KINDS = ("birth", "reflection", "tracker")
SHARES = tuple(f"{k}_failing_share" for k in KINDS) + ("unassessed_share",)
WORST = tuple(f"worst_{k}_margin" for k in KINDS)


def fmt(v):
    return "NULL" if v is None else f"{v:.6g}"


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("a")
    p.add_argument("b")
    args = p.parse_args(argv)
    dmap = dict(MAP_POWER)
    dmap.update(MAP_EXPONENTIAL)
    print(f"A store (07): {args.a}")
    print(f"B store (07d): {args.b}")
    print(
        "A's drain models read through "
        + ", ".join(f"{k} -> {v}" for k, v in dmap.items())
    )
    _, a_h = histories(args.a, None, None)
    _, b_h = histories(args.b, None, None)
    for h in a_h:
        h.model = dmap.get(h.model, h.model)
    print(f"A {len(a_h)} histories, B {len(b_h)}")
    ok = True
    seen = Counter()
    tallies = defaultdict(lambda: defaultdict(Counter))
    extremes = defaultdict(dict)
    print()
    head = "  ".join(f"{c:>34s}" for c in SHARES + WORST)
    print(f"{'history':58s}  {head}")
    for a in sorted(
        a_h, key=lambda h: (h.potential, h.n, h.model, h.alpha, h.M, h.beta)
    ):
        cands = [b for b in b_h if hkey(b) == hkey(a)]
        tag = f"{a.potential} n={a.n} {a.model} a={a.alpha:g} M={a.M / M_P_EV:.3g} b={a.beta:g}"
        if len(cands) != 1:
            print(f"{tag:58s}  MATCH FAILED ({len(cands)} in B)")
            ok = False
            continue
        b = cands[0]
        seen[id(b)] += 1
        group = (a.potential, a.n, b.model)
        cells = []
        for c in SHARES + WORST:
            x, y = a.row.get(c), b.row.get(c)
            d = "" if x is None or y is None else f" ({y - x:+.3g})"
            cells.append(f"{fmt(x) + ' -> ' + fmt(y) + d:>34s}")
            for g in (group, ("all", "", "")):
                t = tallies[g][c]
                if x is None or y is None:
                    t["NULL" if not (x is None and y is None) else "NULL both"] += 1
                elif y > x:
                    t["rose"] += 1
                elif y < x:
                    t["fell"] += 1
                else:
                    t["held"] += 1
                if x is not None and y is not None and y != x:
                    e = extremes[g].setdefault(c, [None, None])
                    if y > x and (e[0] is None or y - x > e[0][0]):
                        e[0] = (y - x, tag)
                    if y < x and (e[1] is None or y - x < e[1][0]):
                        e[1] = (y - x, tag)
        if a.row["failure"] or b.row["failure"]:
            cells = [f"failure A {bool(a.row['failure'])} B {bool(b.row['failure'])}"]
        print(f"{tag:58s}  " + "  ".join(cells))
    for b in b_h:
        if seen[id(b)] != 1:
            print(
                f"   !! unmatched B history {b.potential} n={b.n} {b.model} a={b.alpha:g} M={b.M:g} b={b.beta:g}"
            )
            ok = False
    print()
    print(
        "Per group (potential, n, B's drain model): rose / fell / held / NULL on one side / NULL on both"
    )
    for g in sorted(tallies, key=lambda x: (x[0] == "all", str(x))):
        print(f"  [{' '.join(str(x) for x in g if x != '')}]")
        for c in SHARES + WORST:
            t = tallies[g][c]
            line = f"    {c:26s} rose {t['rose']}, fell {t['fell']}, held {t['held']}, NULL {t['NULL']}, NULL both {t['NULL both']}"
            e = extremes[g].get(c)
            if e:
                if e[0]:
                    line += f"; largest rise {e[0][0]:+.4g} ({e[0][1]})"
                if e[1]:
                    line += f"; largest fall {e[1][0]:+.4g} ({e[1][1]})"
            print(line)
    print(
        f"RESULT: {'every history matched once each way; measured' if ok else 'MATCH FAILED (see above)'}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
