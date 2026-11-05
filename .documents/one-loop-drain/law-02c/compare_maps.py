"""
one-loop-drain prompt 02c §2.3, the comparisons (orchestrator note 02c, ruling 4, and the user's ruling of
2026-11-05 on its ruling 9, "I think probably we set `f = 0` there").

For each of the four maps, this tree's unpatched `cascade_map.py` output (var/one-loop-drain/02c/maps/) against
crossover's file (prompts/one-loop-drain/source/crossover/out/):
  - from the `# <id>:` header line, parking_N, N_final and candidates, as strings;
  - every candidate row, as a string, with crossover's row of the same index;
  - not compared: crossover's first line (the adopted-law banner), the `@@` line, the header's id and `gap=`.
A row that differs only in alpha_eff, at a gap candidate where crossover prints a negative alpha_eff and this tree
0.0000e+00, is the user's ruling (expected, listed); any other difference is reported with the first such row.

The control (`measured_law_map.py --law=adopted` on P-2197 at A = 0.05, this tree) must equal crossover's file byte
for byte. 2197 at A = 1, unpatched against the patched reference, is compared the same way (rows and header), for
the re-pinned TestTheOneLoopLawOn2197.

Counts per map: candidates by regime; gap candidates with 1 + q ln L < 0 (q = 0.841, L the map's printed M/phi_t).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law-02c/compare_maps.py > .documents/one-loop-drain/law-02c/out/compare_maps.txt
"""

import re
from collections import Counter
from math import log
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CROSSOVER = ROOT / "prompts/one-loop-drain/source/crossover/out"
MAPS = ROOT / "var/one-loop-drain/02c/maps"
Q = 0.841

FOUR = (
    ("P-2197-A0.05", "adopted-A0.05/P-2197-A0.05-zero.txt"),
    ("W-2187-A0.05", "adopted-A0.05/W-2187-A0.05-zero.txt"),
    ("R-632-A0.5", "adopted-floor/R-632-A0.5.txt"),
    ("L-M5e-20-A0.2", "adopted-floor/L-M5e-20-A0.2.txt"),
)
ROW = re.compile(r"^\s*\d+\s")
COLUMNS = (
    "idx",
    "N",
    "regime",
    "ell",
    "alpha_eff",
    "ln(E/V0)",
    "ln R",
    "L",
    "ell_t",
    "ln(E/M^4)",
)


def parse(path: Path):
    header, rows = None, []
    for line in path.read_text().splitlines():
        if line.startswith("# ") and "parking_N=" in line:
            header = line
        elif ROW.match(line):
            rows.append(line)
    return header, rows


def header_fields(header: str) -> dict:
    return {
        k: v for k, v in re.findall(r"(parking_N|N_final|candidates)=(\S+)", header)
    }


def fields(row: str) -> dict:
    """The row's columns by their fixed widths (cascade_map.py's format); ell is blank off one_loop."""
    widths = (5, 9, 10, 10, 12, 11, 9, 10, 10, 11)
    out, pos = {}, 0
    for name, width in zip(COLUMNS, widths):
        out[name] = row[pos : pos + width].strip()
        pos += width
    return out


def compare(name: str, ours_path: Path, theirs_path: Path) -> bool:
    ours_h, ours = parse(ours_path)
    theirs_h, theirs = parse(theirs_path)
    ok = True
    print(
        f"== {name}: {ours_path.relative_to(ROOT)} against {theirs_path.relative_to(ROOT)}"
    )
    hf, tf = header_fields(ours_h), header_fields(theirs_h)
    for key in ("parking_N", "N_final", "candidates"):
        same = hf.get(key) == tf.get(key)
        ok &= same
        print(
            f"   header {key}: {'equal' if same else 'DIFFERENT'} ({hf.get(key)} | {tf.get(key)})"
        )
    print(f"   rows: {len(ours)} here, {len(theirs)} crossover's")
    if len(ours) != len(theirs):
        ok = False
    expected, first = [], None
    for i, (a, b) in enumerate(zip(ours, theirs)):
        if a == b:
            continue
        fa, fb = fields(a), fields(b)
        others = [c for c in COLUMNS if c != "alpha_eff" and fa[c] != fb[c]]
        if (
            not others
            and fa["regime"] == "gap"
            and float(fb["alpha_eff"]) < 0.0
            and float(fa["alpha_eff"]) == 0.0
        ):
            expected.append((i, a, b))
            continue
        if first is None:
            first = (i, a, b)
    for i, a, b in expected:
        print(f"   row {i}: the ruling's (gap, 1 + q ln L < 0): here  {a}")
        print(f"   row {i}:                                     there {b}")
    if first is not None:
        ok = False
        i, a, b = first
        print(f"   FIRST DIFFERENCE at row {i}:\n     here  {a}\n     there {b}")
    print(
        f"   rows equal but for the ruling's: {first is None and len(ours) == len(theirs)} ({len(expected)} by the ruling)"
    )
    return ok


def counts(name: str, path: Path) -> None:
    _, rows = parse(path)
    by_regime = Counter(fields(r)["regime"] for r in rows)
    negative = [
        fields(r)
        for r in rows
        if fields(r)["regime"] == "gap" and 1.0 + Q * log(float(fields(r)["L"])) < 0.0
    ]
    least_L_gap = min(
        (float(fields(r)["L"]) for r in rows if fields(r)["regime"] == "gap"),
        default=None,
    )
    print(
        f"   {name}: candidates={len(rows)} by regime {dict(sorted(by_regime.items()))}; gap with 1 + q ln L < 0: "
        f"{len(negative)}; least L at a gap candidate: {least_L_gap}"
    )


def main() -> int:
    all_ok = True
    for name, theirs in FOUR:
        all_ok &= compare(name, MAPS / f"{name}.txt", CROSSOVER / theirs)
    control = (MAPS / "control-P-2197-A0.05.txt").read_bytes()
    reference = (CROSSOVER / "adopted-A0.05/P-2197-A0.05-zero.txt").read_bytes()
    same = control == reference
    all_ok &= same
    print(
        f"== control: measured_law_map.py --law=adopted on P-2197 A = 0.05, this tree, against crossover's file: "
        f"{'byte-identical' if same else 'DIFFERENT'} ({len(control)} and {len(reference)} bytes)"
    )
    all_ok &= compare(
        "P-2197-A1 (unpatched against the patched reference)",
        MAPS / "P-2197-A1.txt",
        MAPS / "reference-P-2197-A1.txt",
    )
    print("== counts")
    for name, _ in FOUR:
        counts(name, MAPS / f"{name}.txt")
    counts("P-2197-A1", MAPS / "P-2197-A1.txt")
    print(f"== all equal (but for the ruling's rows): {all_ok}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
