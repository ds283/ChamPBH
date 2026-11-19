"""
Split a copy of a power-law export root into one root per n (power-law-wall prompt 08 §2 item 5; orchestrator note 08,
ruling 7; README §6.17 P143).

    ./venv/bin/python .documents/power-law-wall/close-08/split_by_n.py SRC DST N [--report FILE]

SRC is a potential's export root, `<output>/<label>/<model>/`, as a plot run writes it (here a scratch copy of 07d's
`export-pl/QCD_Cosmology/inverse-power/`); DST, which must not exist, receives the root of n = N:
  - every file whose path, below SRC, has a directory named `…_n=<k>` (the per-(M, n) directories
    `M=<M>eV_n=<k>`) is copied, with its directories, if k is N, and left out otherwise;
  - every CSV file directly in SRC (`histories.csv`, `quanta_histories.csv`) is copied with its header and those of
    its records whose `n` column is N, each record's text as it stands in SRC (a record that spans several lines,
    inside quotes, is kept whole), in SRC's order;
  - any other file is copied unchanged, and named in the report.
Then, as ruling 7 asks, the quanta files that a run of tools/quanta_measures.py writes (`quanta_histories*.csv` at the
root, `quanta_bounces*.csv` and `quanta_samples*.csv` beside each history) are removed from DST, so that DST's quanta
files are its own once quanta_measures has run on it. SRC is only read. The report lists what was kept, dropped and
removed, by count and by file; it goes to stdout, and to FILE with --report.

Exit 0 on success, 2 on a usage error (DST exists, SRC is not a directory, N is not a positive integer, a root CSV has
no `n` column). No store is opened and no Ray is started.
"""

import argparse
import csv
import re
import shutil
import sys
from pathlib import Path

N_DIRECTORY = re.compile(r"_n=(\d+)$")
QUANTA_FILE = re.compile(r"^quanta_(histories|bounces|samples).*\.csv$")


def records(text: str):
    """The CSV records of text, each as its own text: physical lines joined while a quote is open."""
    out, pending = [], ""
    for line in text.splitlines(keepends=True):
        pending += line
        if pending.count('"') % 2 == 0:
            out.append(pending)
            pending = ""
    if pending:
        out.append(pending)
    return out


def n_of_path(rel: Path):
    """The n of the first directory of rel named `…_n=<k>`, or None."""
    for part in rel.parts[:-1]:
        m = N_DIRECTORY.search(part)
        if m:
            return int(m.group(1))
    return None


def split_csv(src: Path, dst: Path, n: int):
    """Copy src's header and its records whose n is n to dst; return (kept, dropped)."""
    recs = records(src.read_text())
    header = next(csv.reader([recs[0]]))
    if "n" not in header:
        raise ValueError(f"{src} has no 'n' column")
    col = header.index("n")
    kept, dropped = [recs[0]], 0
    for rec in recs[1:]:
        row = next(csv.reader([rec]))
        if int(float(row[col])) == n:
            kept.append(rec)
        else:
            dropped += 1
    dst.write_text("".join(kept))
    return len(kept) - 1, dropped


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("src", type=Path)
    p.add_argument("dst", type=Path)
    p.add_argument("n", type=int)
    p.add_argument("--report", type=Path)
    a = p.parse_args(argv)
    if not a.src.is_dir() or a.dst.exists() or a.n < 1:
        print(
            "!! split_by_n: SRC must be a directory, DST must not exist, N must be >= 1",
            file=sys.stderr,
        )
        return 2

    lines = [f"split_by_n: {a.src} -> {a.dst}, n = {a.n}"]
    kept, dropped, other = [], [], []
    for path in sorted(a.src.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(a.src)
        target = a.dst / rel
        if len(rel.parts) == 1 and rel.suffix == ".csv":
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                k, d = split_csv(path, target, a.n)
            except ValueError as e:
                print(f"!! split_by_n: {e}", file=sys.stderr)
                return 2
            lines.append(f"root CSV {rel}: {k} records kept, {d} dropped")
            continue
        k = n_of_path(rel)
        if k is None:
            other.append(rel)
        elif k != a.n:
            dropped.append(rel)
            continue
        else:
            kept.append(rel)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)

    removed = []
    for path in sorted(a.dst.rglob("*")):
        if path.is_file() and QUANTA_FILE.match(path.name):
            removed.append(path.relative_to(a.dst))
            path.unlink()

    lines.append(
        f"files under a _n={a.n} directory kept: {len(kept)}; under another n dropped: {len(dropped)}; "
        f"with no n copied unchanged: {len(other)}"
    )
    lines.append(f"quanta files removed from {a.dst}: {len(removed)}")
    for title, items in (
        ("kept", kept),
        ("dropped", dropped),
        ("copied, no n", other),
        ("removed", removed),
    ):
        lines.append(f"-- {title} ({len(items)})")
        lines.extend(f"   {r}" for r in items)
    text = "\n".join(lines) + "\n"
    sys.stdout.write(text)
    if a.report:
        a.report.write_text(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
