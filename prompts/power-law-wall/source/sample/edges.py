"""
power-law-wall prompt 04 §1.5 (U7; P29): the surfing edges, read from tools/surfing_scan.py's CSVs under
.documents/power-law-wall/sample-04/surfing/. Measurements, not bounds. Run from the repository root:

    ./venv/bin/python prompts/power-law-wall/source/sample/edges.py grids C_CSV
    ./venv/bin/python prompts/power-law-wall/source/sample/edges.py fnew
    ./venv/bin/python prompts/power-law-wall/source/sample/edges.py report

`grids` reads a coarse scan C-n<N> and prints the fine grids prompt §1.5 asks for, by capture_kick:
  - Q: 0.001 steps from C's largest beta below its first QCD capture to that capture, both included (1.050-1.100 if
    C's first row, 1.10, already captures QCD); the row below is the nearest *successful* row of C below the capture,
    so that failed rows between are spanned (prompt §1.5, "Failed rows are measurements");
  - E: 0.001 steps from C's largest QCD-captured beta to its smallest EW-captured beta, both included.
A failure row is neither captured nor uncaptured: it is kept with its reason and named. It says STOP (exit 3) where the
prompt stops: where C's rows succeed across the range a grid needs and the grid is wider than 101 beta, or where there
is more than one window, or none, between a QCD capture and an EW capture. Where failures stand in the way and the grid
would be wider than 101 beta, or no EW capture exists, it says so and names no grid (not a stop: the edge is reported
between the nearest successful rows).

`fnew` compares the three exponential rows of surfing/f_new_check.csv (2.975, 2.976, 3.003) with electroweak-eos 02's
f_new.csv, every column but wall_s (prompt §1.5).

`report` writes surfing/edges.txt: each scan's rows (beta, status, captured, capture_kick, n_crossings, T_capture,
T_max, T_deliver, N_deliver, reflections, accepted_steps, wall_s), its failures with their reasons, its edges (the
onset of QCD capture, the largest beta QCD captures, the smallest EW captures, the window between, each bracketed by the
nearest successful rows, with the failures between), its wall time summed and its load at the start (from the scan's
console file), beside the exponential's (electroweak-eos 02's s1_new.csv and f_new.csv, and Q-exp here).
"""

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SURF = ROOT / ".documents" / "power-law-wall" / "sample-04" / "surfing"
EWEOS = ROOT / ".documents" / "electroweak-eos" / "threshold" / "out"
STEP = 0.001
MAX_FINE = 101


def read(path):
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    rows.sort(key=lambda r: float(r["beta"]))
    return rows


def ok(r):
    return r["status"] == "ok"


def kick(r):
    """QCD, EW, another kick, "" (uncaptured) or None (a failure row)."""
    if not ok(r):
        return None
    return r["capture_kick"] if r["captured"] == "true" else ""


def nb(x):
    return round(x / STEP) + 1


def windows(rows):
    """
    Maximal runs of successful uncaptured rows (failure rows skipped over and listed), each with the kick of the
    nearest successful row below and above (None at the ends): (first, last, below, above, failures inside).
    """
    good = [r for r in rows if ok(r)]
    out = []
    i = 0
    while i < len(good):
        if kick(good[i]) == "":
            j = i
            while j + 1 < len(good) and kick(good[j + 1]) == "":
                j += 1
            below = kick(good[i - 1]) if i > 0 else None
            above = kick(good[j + 1]) if j + 1 < len(good) else None
            lo = float(good[i - 1]["beta"]) if i > 0 else float(good[i]["beta"])
            hi = (
                float(good[j + 1]["beta"])
                if j + 1 < len(good)
                else float(good[j]["beta"])
            )
            fails = [r for r in rows if not ok(r) and lo < float(r["beta"]) < hi]
            out.append((good[i], good[j], below, above, fails))
            i = j + 1
        else:
            i += 1
    return out


def fine_grids(rows):
    """(Q range or None, E range or None, notes, stop)."""
    notes, stop = [], False
    good = [r for r in rows if ok(r)]
    fails = [r for r in rows if not ok(r)]
    notes.append(
        f"{len(rows)} rows, {len(good)} succeeded, {len(fails)} failed"
        + (f" (beta {fails[0]['beta']} - {fails[-1]['beta']})" if fails else "")
    )
    qcd = [r for r in good if kick(r) == "QCD"]
    ew = [r for r in good if kick(r) == "EW"]
    other = [r for r in good if kick(r) not in ("", "QCD", "EW")]
    if other:
        notes.append(
            f"other kicks capture: {[(r['beta'], r['capture_kick']) for r in other]}"
        )
    q = None
    if not qcd:
        notes.append("no QCD capture on C")
    else:
        bq = float(qcd[0]["beta"])
        below = [r for r in good if float(r["beta"]) < bq]
        if float(rows[0]["beta"]) == bq:
            q = (1.050, 1.100)
            notes.append("C's first row captures QCD: Q is 1.050-1.100")
        elif below:
            lo = float(below[-1]["beta"])
            between = [r for r in fails if lo < float(r["beta"]) < bq]
            if nb(bq - lo) > MAX_FINE:
                notes.append(
                    f"Q would be {lo:.3f}-{bq:.3f}, {nb(bq - lo)} beta, wider than {MAX_FINE}"
                    + (
                        f", across failures {[r['beta'] for r in between]}: not run"
                        if between
                        else ": STOP"
                    )
                )
                stop = stop or not between
            else:
                q = (lo, bq)
                if between:
                    notes.append(f"Q spans failed rows {[r['beta'] for r in between]}")
        else:
            notes.append(
                f"no successful row below the first QCD capture {bq}: the onset is not bracketed"
            )
    e = None
    ws = [w for w in windows(rows) if w[2] == "QCD" and w[3] == "EW"]
    if not ew:
        notes.append("no EW capture on C: the EW edge is not bracketed")
    elif qcd:
        lo, hi = float(qcd[-1]["beta"]), float(ew[0]["beta"])
        between = [r for r in fails if lo < float(r["beta"]) < hi]
        if len(ws) != 1 and not between:
            notes.append(
                f"{len(ws)} windows between a QCD capture and an EW capture: STOP"
            )
            stop = True
        if lo > hi:
            notes.append(
                f"the largest QCD capture {lo} lies above the smallest EW capture {hi}: STOP"
            )
            stop = True
        elif nb(hi - lo) > MAX_FINE:
            notes.append(
                f"E would be {lo:.3f}-{hi:.3f}, {nb(hi - lo)} beta, wider than {MAX_FINE}"
                + (
                    f", across failures {[r['beta'] for r in between]}: not run"
                    if between
                    else ": STOP"
                )
            )
            stop = stop or not between
        else:
            e = (lo, hi)
            if between:
                notes.append(f"E spans failed rows {[r['beta'] for r in between]}")
    for first, last, b, a, fl in windows(rows):
        notes.append(
            f"uncaptured window beta {first['beta']} - {last['beta']}, {b or 'grid edge'} below, {a or 'grid edge'} "
            f"above" + (f", failures inside {[r['beta'] for r in fl]}" if fl else "")
        )
    return q, e, notes, stop


def cmd_grids(path):
    rows = read(path)
    q, e, notes, stop = fine_grids(rows)
    for n in notes:
        print(f"- {n}")
    print(
        "Q "
        + ("none" if q is None else f"{q[0]:.3f} {q[1]:.3f} ({nb(q[1] - q[0])} beta)")
    )
    print(
        "E "
        + ("none" if e is None else f"{e[0]:.3f} {e[1]:.3f} ({nb(e[1] - e[0])} beta)")
    )
    return 3 if stop else 0


def g6(r, c):
    s = r.get(c, "")
    return "—" if s in ("", None) else f"{float(s):.6g}"


def load_at_start(name):
    p = SURF / f"{name}.console.txt"
    if not p.exists():
        return "—"
    m = re.search(r"load averages?: ([0-9.]+) ([0-9.]+) ([0-9.]+)", p.read_text())
    return " / ".join(m.groups()) if m else "—"


def edges(rows):
    """The edges, each bracketed by the nearest successful rows: (onset, last QCD, first EW, windows)."""
    good = [r for r in rows if ok(r)]
    out = {}
    qcd = [r for r in good if kick(r) == "QCD"]
    ew = [r for r in good if kick(r) == "EW"]
    if qcd:
        bq = qcd[0]
        below = [r for r in good if float(r["beta"]) < float(bq["beta"])]
        out["onset"] = (below[-1]["beta"] if below else None, bq["beta"])
        nxt = [r for r in good if float(r["beta"]) > float(qcd[-1]["beta"])]
        out["last_QCD"] = (qcd[-1]["beta"], nxt[0]["beta"] if nxt else None)
    if ew:
        prev = [r for r in good if float(r["beta"]) < float(ew[0]["beta"])]
        out["first_EW"] = (prev[-1]["beta"] if prev else None, ew[0]["beta"])
    return out


def scan_section(say, name, path):
    rows = read(path)
    good = [r for r in rows if ok(r)]
    wall = sum(float(r["wall_s"]) for r in rows if r["wall_s"])
    say(
        f"### {name} ({path.relative_to(ROOT)}): {len(rows)} rows, {len(good)} succeeded; wall_s summed {wall:.1f} s;"
    )
    say(f"load (1, 5, 15 min) at the start: {load_at_start(name)}")
    say("")
    say(
        "| β | status | captured | kick | n_crossings | T_capture | T_max | T_deliver (first bounce) | N_deliver | "
        "reflections | accepted_steps | wall_s |"
    )
    say("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for r in rows:
        say(
            f"| {r['beta']} | {r['status']} | {r['captured'] or '—'} | {r['capture_kick'] or '—'} | "
            f"{r['n_crossings'] or '—'} | {g6(r, 'T_capture_GeV')} | {g6(r, 'T_max_GeV')} | {g6(r, 'T_deliver_GeV')} | "
            f"{g6(r, 'N_deliver')} | {r['reflections'] or '—'} | {r['accepted_steps'] or '—'} | {r['wall_s']} |"
        )
    fails = [r for r in rows if not ok(r)]
    if fails:
        say("")
        say("Failed rows (the payload's reason):")
        for r in fails:
            say(f"- β {r['beta']}: {r['failure_reason']}")
    say("")
    e = edges(rows)
    if "onset" in e:
        say(
            f"- onset of QCD capture: between β {e['onset'][0] or '(grid edge)'} (not captured) and {e['onset'][1]}"
        )
        say(
            f"- largest β QCD captures: {e['last_QCD'][0]}; next successful row {e['last_QCD'][1] or '(grid edge)'}"
        )
    else:
        say("- no QCD capture")
    if "first_EW" in e:
        say(
            f"- smallest β EW captures: {e['first_EW'][1]}; previous successful row {e['first_EW'][0] or '(grid edge)'}"
        )
    else:
        say("- no EW capture")
    for first, last, b, a, fl in windows(rows):
        say(
            f"- uncaptured window: β {first['beta']} – {last['beta']}, {b or 'grid edge'} below, {a or 'grid edge'} "
            f"above"
            + (f"; failed rows inside: {[r['beta'] for r in fl]}" if fl else "")
        )
    say("")
    return e


def cmd_report():
    lines = []
    say = lines.append
    say(
        "edges.txt — power-law-wall prompt 04 §1.5 (P29): the surfing edges at M = 1e-3 M_P, phi* = 5 M_P, tuned"
    )
    say(
        "Built by prompts/power-law-wall/source/sample/edges.py from the CSVs; measurements, not bounds."
    )
    say("")
    say("## The grids as read from the coarse scans")
    for n in (1, 2):
        p = SURF / f"c_n{n}.csv"
        if p.exists():
            q, e, notes, stop = fine_grids(read(p))
            say(f"### C-n{n}")
            for x in notes:
                say(f"- {x}")
            say(
                "- Q: "
                + (
                    "none"
                    if q is None
                    else f"{q[0]:.3f}–{q[1]:.3f} ({nb(q[1] - q[0])} β)"
                )
            )
            say(
                "- E: "
                + (
                    "none"
                    if e is None
                    else f"{e[0]:.3f}–{e[1]:.3f} ({nb(e[1] - e[0])} β)"
                )
            )
            say("")
    say("## The scans")
    summary = {}
    for name in (
        "c_n1",
        "q_n1",
        "e_n1",
        "c_n2",
        "q_n2",
        "e_n2",
        "q_exp",
        "f_new_check",
    ):
        p = SURF / f"{name}.csv"
        if p.exists():
            summary[name] = scan_section(say, name, p)
    say("## The exponential's reference (electroweak-eos 02, the same w table)")
    for name in ("s1_new", "f_new"):
        summary[name] = edges(read(EWEOS / f"{name}.csv"))
        say(f"- {name}.csv: {summary[name]}")
    say("")
    say("## Each power-law scan against the exponential's at the same β")
    say("")
    say(
        "Rows compared: the β both scans hold, where the power-law row succeeded. 'same capture' counts rows whose "
        "captured and capture_kick agree; the largest relative difference is over T_capture, T_max and T_deliver where "
        "both rows hold them; the last three columns count the rows whose n_crossings differ, whose T_max is blank on one "
        "side only, and whose deliver_reflected differs (the power wall's value first)."
    )
    say("")
    say(
        "| scan | exponential's | β in both (succeeded) | same capture | differing β | largest relative difference in T | "
        "n_crossings differ | T_max blank on one side | deliver_reflected differs |"
    )
    say("|---|---|---|---|---|---|---|---|---|")
    for name, ref in (
        ("c_n1", EWEOS / "s1_new.csv"),
        ("c_n2", EWEOS / "s1_new.csv"),
        ("q_n1", SURF / "q_exp.csv"),
        ("q_n2", SURF / "q_exp.csv"),
        ("e_n1", EWEOS / "f_new.csv"),
        ("e_n2", EWEOS / "f_new.csv"),
    ):
        p = SURF / f"{name}.csv"
        if not p.exists():
            continue
        refs = {f"{float(r['beta']):.3f}": r for r in read(ref)}
        both = same = 0
        differ, worst, ncross, tmax, refl = [], 0.0, 0, 0, {}
        for r in read(p):
            e = refs.get(f"{float(r['beta']):.3f}")
            if e is None or not ok(r):
                continue
            both += 1
            if (r["captured"], r["capture_kick"]) == (e["captured"], e["capture_kick"]):
                same += 1
            else:
                differ.append(r["beta"])
            ncross += r["n_crossings"] != e["n_crossings"]
            tmax += (r["T_max_GeV"] == "") != (e["T_max_GeV"] == "")
            if r["deliver_reflected"] != e["deliver_reflected"]:
                k = f"{r['deliver_reflected']} / {e['deliver_reflected']}"
                refl[k] = refl.get(k, 0) + 1
            for c in ("T_capture_GeV", "T_max_GeV", "T_deliver_GeV"):
                if r[c] and e[c]:
                    worst = max(worst, abs(float(r[c]) / float(e[c]) - 1.0))
        say(
            f"| {name} | {ref.name} | {both} | {same} | {', '.join(differ) or '—'} | {worst:.3g} | {ncross} | {tmax} | "
            + (", ".join(f"{v} ({k})" for k, v in refl.items()) or "0")
            + " |"
        )
    say("")
    say("## The edges per wall")
    say("")
    say(
        "| wall | QCD onset (coarse) | QCD onset (fine) | largest β QCD captures (fine) | smallest β EW captures (fine) |"
    )
    say("|---|---|---|---|---|")

    def br(e, k):
        if e is None or k not in e:
            return "—"
        a, b = e[k]
        return (
            f"({a or '…'}, {b or '…'}]" if k != "last_QCD" else f"{a} (next {b or '…'})"
        )

    for wall, c, q, f in (
        ("exponential", "s1_new", "q_exp", "f_new"),
        ("power n = 1", "c_n1", "q_n1", "e_n1"),
        ("power n = 2", "c_n2", "q_n2", "e_n2"),
    ):
        say(
            f"| {wall} | {br(summary.get(c), 'onset')} | {br(summary.get(q), 'onset')} | "
            f"{br(summary.get(f) or summary.get(c), 'last_QCD')} | {br(summary.get(f) or summary.get(c), 'first_EW')} |"
        )
    (SURF / "edges.txt").write_text("\n".join(lines) + "\n")
    print(f"wrote {(SURF / 'edges.txt').relative_to(ROOT)}")
    return 0


def cmd_fnew():
    """The three rows of surfing/f_new_check.csv against electroweak-eos 02's f_new.csv, every column but wall_s."""
    ref = {r["beta"]: r for r in read(EWEOS / "f_new.csv")}
    bad = 0
    for r in read(SURF / "f_new_check.csv"):
        f = ref[r["beta"]]
        diff = [(k, r[k], f[k]) for k in r if k != "wall_s" and r[k] != f[k]]
        bad += 1 if diff else 0
        print(
            f"beta {r['beta']}: "
            + (
                "equal to f_new.csv in every column but wall_s"
                if not diff
                else f"DIFFERENT {diff}"
            )
            + f" (wall_s {r['wall_s']} here, {f['wall_s']} there)"
        )
    return 0 if bad == 0 else 1


def main(argv):
    if argv and argv[0] == "fnew":
        return cmd_fnew()
    if not argv:
        print(__doc__)
        return 2
    if argv[0] == "grids":
        return cmd_grids(argv[1])
    if argv[0] == "report":
        return cmd_report()
    print(f"unknown command {argv[0]}")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
