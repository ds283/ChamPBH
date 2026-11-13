"""
electroweak-eos prompt 02 §1.2 (README §9.2 P13; the Threshold gate): the capture edges of the three scans, read from
their CSVs. Measurements, not bounds.

    ./venv/bin/python .documents/electroweak-eos/threshold/edges.py range [--s1-new PATH]
    ./venv/bin/python .documents/electroweak-eos/threshold/edges.py report [--out DIR]

`range` prints F-new's grid as read from S1-new's CSV: the largest beta whose capture_kick is QCD and the smallest
whose capture_kick is EW, and the uncaptured windows between a QCD capture and an EW capture (a maximal run of rows
with captured = false whose neighbours are a QCD row below and an EW row above). It exits 3 if there is not exactly
one such window (prompt §1.2: a stop), and 0 otherwise.

`report` writes out/edges.txt (beside the CSVs, by default .documents/electroweak-eos/threshold/out/):
  - F-new's range from S1-new, as `range` prints it;
  - for each fine grid (F-new, F-old): every row (beta, captured, capture_kick, n_crossings, T_capture, T_max,
    T_deliver, N_deliver, accepted_steps), the edges (the largest beta QCD captures, the smallest EW captures, the
    uncaptured window between them), whether the grid shows more than one window or a capture between uncaptured
    beta (a stop), the first bounce across the window and the ratios at its edges;
  - the EW threshold on each fine grid beside the untuned 3.020 (surfing-temperature verification §1.8), the
    notebook's 3.00285 (README §0.1) and the reachability prediction from the new peak, 2.99918 (log 01 §3.4);
  - S1-new against S1 (surfing-temperature verification §1.4; its CSV, untuned, at 2026.6.0): who captures, at
    which beta, and T_capture, T_max and T_deliver by kick, also as ratios to the capture kick's T_hot;
  - each scan's sum of wall_s.
Temperatures are in GeV and N in e-folds from T*, as the CSVs write them.
"""

import argparse
import csv
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
OUT = HERE / "out"
S1_OLD = (
    ROOT / ".documents" / "surfing-temperature" / "out" / "surfing_scan_phi5_M1e-3.csv"
)
S5_OLD = (
    ROOT
    / ".documents"
    / "surfing-temperature"
    / "out"
    / "surfing_scan_phi5_M1e-3_near3.csv"
)
REFERENCES = [
    (
        "untuned, old table, 2026.6.0 (surfing-temperature S5, §1.8)",
        "3.020",
        "(3.019, 3.020]",
    ),
    ("the notebook's integrator, merged table (README §0.1)", "3.00285", "bisection"),
    ("reachability from the new peak Sigma (log 01 §3.4)", "2.99918", "analytic"),
    (
        "reachability from the old peak Sigma (surfing-temperature §1.3)",
        "3.011779",
        "analytic",
    ),
]


def read(path: Path):
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    rows.sort(key=lambda r: float(r["beta"]))
    return rows


def f(r, c):
    s = r.get(c, "")
    return None if s in ("", None) else float(s)


def g6(x):
    return "—" if x is None else f"{x:.6g}"


def kick(r):
    return r["capture_kick"] if r["captured"] == "true" else ""


def windows(rows):
    """Maximal runs of uncaptured rows; each with the kick of the row below and above (None at the ends)."""
    out = []
    i = 0
    while i < len(rows):
        if kick(rows[i]) == "":
            j = i
            while j + 1 < len(rows) and kick(rows[j + 1]) == "":
                j += 1
            below = kick(rows[i - 1]) if i > 0 else None
            above = kick(rows[j + 1]) if j + 1 < len(rows) else None
            out.append((i, j, below, above))
            i = j + 1
        else:
            i += 1
    return out


def f_new_range(s1):
    qcd = [float(r["beta"]) for r in s1 if kick(r) == "QCD"]
    ew = [float(r["beta"]) for r in s1 if kick(r) == "EW"]
    ws = [w for w in windows(s1) if w[2] == "QCD" and w[3] == "EW"]
    return (max(qcd) if qcd else None), (min(ew) if ew else None), ws


def cmd_range(args) -> int:
    s1 = read(args.s1_new)
    lo, hi, ws = f_new_range(s1)
    print(
        f"S1-new: {len(s1)} rows; largest QCD-captured beta {lo}; smallest EW-captured beta {hi}"
    )
    for i, j, b, a in ws:
        print(
            f"  window between a QCD and an EW capture: beta {s1[i]['beta']} - {s1[j]['beta']} ({j - i + 1} rows)"
        )
    if len(ws) != 1:
        print(
            f"!! {len(ws)} windows between a QCD capture and an EW capture (prompt §1.2: a stop)"
        )
        return 3
    print(f"F-NEW {lo:.3f} {hi:.3f} ({round((hi - lo) / 0.001) + 1} beta)")
    return 0


def fine(say, name, rows):
    say(
        f"### {name}: {len(rows)} rows, wall_s summed {sum(f(r, 'wall_s') or 0 for r in rows):.1f} s"
    )
    say(
        "| β | captured | capture_kick | n_crossings | T_capture | T_max | T_deliver | N_deliver | accepted_steps |"
    )
    say("|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        say(
            f"| {r['beta']} | {r['captured']} | {r['capture_kick'] or '—'} | {r['n_crossings']} | "
            f"{g6(f(r, 'T_capture_GeV'))} | {g6(f(r, 'T_max_GeV'))} | {g6(f(r, 'T_deliver_GeV'))} | "
            f"{g6(f(r, 'N_deliver'))} | {r['accepted_steps']} |"
        )
    bad = [r for r in rows if r["status"] != "ok"]
    if bad:
        say(f"failures: {[(r['beta'], r['failure_reason']) for r in bad]}")
    qcd = [r for r in rows if kick(r) == "QCD"]
    ew = [r for r in rows if kick(r) == "EW"]
    other = [r for r in rows if kick(r) not in ("", "QCD", "EW")]
    ws = windows(rows)
    say("")
    say(
        f"- largest β QCD captures: {qcd[-1]['beta'] if qcd else '—'}; smallest β EW captures: {ew[0]['beta'] if ew else '—'}"
    )
    if other:
        say(f"- other kicks capture: {[(r['beta'], r['capture_kick']) for r in other]}")
    for i, j, b, a in ws:
        say(
            f"- uncaptured window: β {rows[i]['beta']} – {rows[j]['beta']} ({j - i + 1} β), {b or 'grid edge'} below, {a or 'grid edge'} above"
        )
    # pattern: QCD... then none... then EW..., one window, no capture inside it
    seq = [kick(r) for r in rows]
    ok_pattern = (
        len(ws) == 1
        and all(k == "QCD" for k in seq[: ws[0][0]])
        and all(k == "EW" for k in seq[ws[0][1] + 1 :])
    )
    say(
        "- one window, QCD below and EW above, no capture between uncaptured β: "
        + ("yes" if ok_pattern else "**NO (prompt §1.2: a stop)**")
    )
    edge = None
    if ws and ok_pattern:
        i, j, _, _ = ws[0]
        T = [f(r, "T_deliver_GeV") for r in rows]
        say(
            f"- first bounce across the window: {g6(T[i])} GeV (β {rows[i]['beta']}, N {g6(f(rows[i], 'N_deliver'))}) to "
            f"{g6(T[j])} GeV (β {rows[j]['beta']}, N {g6(f(rows[j], 'N_deliver'))})"
        )
        if i > 0:
            say(
                f"  - at the lower edge: {T[i] / T[i - 1]:.5f} times the last QCD delivery ({g6(T[i - 1])} GeV at β {rows[i - 1]['beta']})"
            )
        if j + 1 < len(rows):
            say(
                f"  - at the upper edge: {T[j] / T[j + 1]:.5f} times the first EW delivery ({g6(T[j + 1])} GeV at β {rows[j + 1]['beta']})"
            )
        ratios = [(rows[k]["beta"], T[k] / T[k - 1]) for k in range(i + 1, j + 1)]
        if ratios:
            big = sorted(ratios, key=lambda x: -x[1])[:4]
            say(
                "  - the largest ratios of adjacent deliveries inside the window: "
                + ", ".join(f"{x:.3f} at {b}" for b, x in big)
            )
        edge = (rows[j]["beta"], rows[j + 1]["beta"] if j + 1 < len(rows) else None)
    say("")
    return ok_pattern, edge


def by_kick(say, name, rows):
    for k in ("QCD", "EW"):
        rs = [r for r in rows if kick(r) == k]
        if not rs:
            say(f"- {name} {k}: none")
            continue

        def rng(c, ratio=False):
            vals = []
            for r in rs:
                x = f(r, c)
                if x is None:
                    continue
                if ratio:
                    th = f(r, "T_hot_GeV")
                    if not th:
                        continue
                    x = x / th
                vals.append((x, r["beta"]))
            if not vals:
                return "—"
            lo, hi = min(vals), max(vals)
            return f"{lo[0]:.6g} ({lo[1]}) – {hi[0]:.6g} ({hi[1]})"

        say(
            f"- {name} {k}: {len(rs)} β, {rs[0]['beta']}–{rs[-1]['beta']}; T_capture {rng('T_capture_GeV')}; "
            f"T_max {rng('T_max_GeV')}; T_deliver {rng('T_deliver_GeV')}"
        )
        say(
            f"  ratios to T_hot: T_capture {rng('T_capture_GeV', True)}; T_max {rng('T_max_GeV', True)}; "
            f"T_deliver {rng('T_deliver_GeV', True)}"
        )


def cmd_report(args) -> int:
    lines = []

    def say(s=""):
        print(s)
        lines.append(s)

    s1n = read(args.s1_new)
    fnew = read(args.f_new)
    fold = read(args.f_old)
    s1o = read(S1_OLD)
    s5o = read(S5_OLD)
    say("edges.py report (electroweak-eos prompt 02 §1.2)")
    say("")
    lo, hi, ws = f_new_range(s1n)
    say(f"## F-new's range from S1-new ({len(s1n)} rows)")
    say(
        f"- largest QCD-captured β {lo}; smallest EW-captured β {hi}; windows between them: {len(ws)}"
    )
    for i, j, b, a in ws:
        say(f"  - β {s1n[i]['beta']} – {s1n[j]['beta']} ({j - i + 1} rows uncaptured)")
    expect = (
        [round(lo + k * 0.001, 3) for k in range(round((hi - lo) / 0.001) + 1)]
        if lo and hi
        else []
    )
    got = [round(float(r["beta"]), 3) for r in fnew]
    say(
        f"- F-new's grid is {lo:.3f}–{hi:.3f} at 0.001 ({len(expect)} β); its CSV holds {len(fnew)} rows, "
        + ("the same β" if got == expect else "**NOT the same β**")
    )
    say("")
    say("## The fine grids")
    ok_new, edge_new = fine(say, "F-new (new tree, 2026.11.0, tuned)", fnew)
    ok_old, edge_old = fine(say, "F-old (old tree ec6bca8, 2026.10.0, tuned)", fold)
    say("## The EW threshold")
    say("| source | EW threshold |")
    say("|---|---|")
    if edge_new:
        say(
            f"| F-new, ChamPBH, new table, tuned (this prompt) | ({edge_new[0]}, {edge_new[1]}] |"
        )
    if edge_old:
        say(
            f"| F-old, ChamPBH, old table, tuned (this prompt) | ({edge_old[0]}, {edge_old[1]}] |"
        )
    for name, val, how in REFERENCES:
        say(f"| {name} | {val} ({how}) |")
    say("")
    say("## F-old against S5 (the same grid and table, untuned at 2026.6.0)")
    S5 = {round(float(r["beta"]), 3): r for r in s5o}
    changed = [
        r["beta"] for r in fold if kick(r) != kick(S5[round(float(r["beta"]), 3)])
    ]
    say(f"- β whose capture_kick differs from S5's: {changed if changed else 'none'}")
    for c in ("T_capture_GeV", "T_max_GeV", "T_deliver_GeV", "N_deliver"):
        rel = []
        for r in fold:
            x, y = f(S5[round(float(r["beta"]), 3)], c), f(r, c)
            if x is not None and y is not None:
                rel.append((abs(y / x - 1.0), r["beta"]))
        if rel:
            m = max(rel)
            say(
                f"- {c}: largest |F-old/S5 - 1| = {m[0]:.3g} (β {m[1]}), over {len(rel)} β"
            )
    say("")
    say("## S1-new against S1 (surfing-temperature §1.4: untuned, old table, 2026.6.0)")
    S1 = {round(float(r["beta"]), 2): r for r in s1o}
    diff = [
        (r["beta"], kick(S1[round(float(r["beta"]), 2)]) or "none", kick(r) or "none")
        for r in s1n
        if kick(r) != kick(S1[round(float(r["beta"]), 2)])
    ]
    say(f"- β whose capture_kick differs (β, S1, S1-new): {diff if diff else 'none'}")
    for name, rows in (("S1", s1o), ("S1-new", s1n)):
        cap = {}
        for r in rows:
            cap.setdefault(kick(r) or "none", []).append(r["beta"])
        say(
            f"- {name}: "
            + "; ".join(f"{k} {len(v)} β ({v[0]}–{v[-1]})" for k, v in cap.items())
        )
    for c in ("T_capture_GeV", "T_max_GeV", "T_deliver_GeV"):
        rel = []
        for r in s1n:
            x, y = f(S1[round(float(r["beta"]), 2)], c), f(r, c)
            if x is not None and y is not None:
                rel.append((y / x - 1.0, r["beta"], kick(r)))
        for k in ("QCD", "EW"):
            rk = [x for x in rel if x[2] == k]
            if rk:
                lo, hi = min(rk), max(rk)
                say(
                    f"- {c}, {k}: S1-new/S1 - 1 from {lo[0]:+.3g} (β {lo[1]}) to {hi[0]:+.3g} (β {hi[1]})"
                )
    by_kick(say, "S1", s1o)
    by_kick(say, "S1-new", s1n)
    say("")
    say("## wall_s summed")
    for name, rows in (("S1-new", s1n), ("F-new", fnew), ("F-old", fold)):
        say(
            f"- {name}: {len(rows)} rows, {sum(f(r, 'wall_s') or 0 for r in rows):.1f} s; tuning_iterations {sum(int(r['tuning_iterations'] or 0) for r in rows)}"
        )
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "edges.txt").write_text("\n".join(lines) + "\n")
    return 0 if (ok_new and ok_old) else 3


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    pr = sub.add_parser("range")
    pr.add_argument("--s1-new", type=Path, default=OUT / "s1_new.csv")
    pp = sub.add_parser("report")
    pp.add_argument("--s1-new", type=Path, default=OUT / "s1_new.csv")
    pp.add_argument("--f-new", type=Path, default=OUT / "f_new.csv")
    pp.add_argument("--f-old", type=Path, default=OUT / "f_old.csv")
    pp.add_argument("--out", type=Path, default=OUT)
    args = p.parse_args(argv)
    return cmd_range(args) if args.cmd == "range" else cmd_report(args)


if __name__ == "__main__":
    sys.exit(main())
