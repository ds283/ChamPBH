"""
The quanta tools on the exponential root of each tree (power-law-wall prompt 06, §2.2; README §6.10 P54; orchestrator
note 06, ruling 2), as quanta_roots.sh ran them, and the tip's quanta_measures on the power-law root and on a root
without `model`.

    PYTHONPATH=. ./venv/bin/python .documents/power-law-wall/export-06/compare_quanta.py var/power-law-wall/06

- **the exponential root**: every file quanta_measures wrote (quanta_histories.csv, each history's quanta_bounces.csv
  and quanta_samples.csv) is byte-identical between the base's run on the base's root and the tip's on the tip's
  exponential root, and no file is written by one only; each tool's printed lines are identical once the timing fields
  are masked (`wall <x> s`, the only timing field any of them prints); the PNG figures are byte-identical; the PDFs are
  compared with matplotlib's CreationDate masked;
- **the power-law root** (the tip's `<label>/inverse-power/`, copied): quanta_measures measures each one-loop history
  with its own wall's spectrum (R8-power-n1-v1 at n = 1, R8-power-n2-v1 at n = 2): every row of every output names it;
- **a root without `model`** (a copy of the base's root, whose histories.csv has no `model` column): quanta_measures
  refuses it, exit 2, naming the column, and writes nothing.

A difference on the exponential root, or a failure of the other two, is a stop (exit 1). Writes out/quanta.txt.
"""

import csv
import hashlib
import io
import re
import shutil
import subprocess
import sys
from pathlib import Path

WALL = re.compile(r"wall [0-9.]+ s")
DATE = re.compile(rb"/CreationDate \(D:[0-9+\-'Z]*\)")


def files(root: Path, pattern="*"):
    return sorted(str(p.relative_to(root)) for p in root.rglob(pattern) if p.is_file())


def masked(text: str) -> str:
    return WALL.sub("wall <masked> s", text)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def rows(path: Path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def main(V: Path) -> int:
    lines = []

    def say(text=""):
        lines.append(text)
        print(text)

    stop = False

    # ---- the exponential root ------------------------------------------------------------------------------------
    say(
        "== the exponential root: the base's tools on the base's root, the tip's on the tip's exponential root"
    )
    for kind, a, b in (
        ("quanta file", V / "quanta-base", V / "quanta-tip"),
        ("figure", V / "figures-base", V / "figures-tip"),
    ):
        pattern = "quanta_*" if kind == "quanta file" else "*"
        fa, fb = files(a, pattern), files(b, pattern)
        if fa != fb:
            say(f"STOP: the {kind}s differ in their paths: {sorted(set(fa) ^ set(fb))}")
            stop = True
        for rel in fa:
            if rel not in fb:
                continue
            x, y = (a / rel).read_bytes(), (b / rel).read_bytes()
            if rel.endswith(".pdf"):
                same = DATE.sub(b"", x) == DATE.sub(b"", y)
                how = "with CreationDate masked"
            else:
                same = x == y
                how = "byte for byte"
            say(
                f"{kind} {rel}: {'IDENTICAL' if same else 'DIFFERS'} {how} ({len(x)} bytes, sha256 {sha(x)})"
            )
            stop |= not same
    for tool in ("quanta_measures", "plot_quanta_grid", "plot_quanta_history"):
        x = (V / "runs-base" / f"{tool}.stdout").read_text()
        y = (V / "runs-tip" / f"{tool}.stdout").read_text()
        x = x.replace(str(V / "figures-base"), "<figures>")
        y = y.replace(str(V / "figures-tip"), "<figures>")
        same = masked(x) == masked(y)
        say(
            f"{tool}: printed lines {'IDENTICAL' if same else 'DIFFER'} with the timing fields masked and the --output "
            f"directory named alike ({len(x.splitlines())} lines; raw text identical: {x == y})"
        )
        if not same:
            stop = True
            say(masked(x))
            say(masked(y))

    # ---- the power-law root --------------------------------------------------------------------------------------
    say()
    say(
        "== the power-law root (the tip's <label>/inverse-power/, copied to V/quanta-power)"
    )
    power = V / "quanta-power"
    if power.exists():
        shutil.rmtree(power)
    shutil.copytree(V / "export-tip" / "QCD_Cosmology" / "inverse-power", power)
    run = subprocess.run(
        [sys.executable, "tools/quanta_measures.py", str(power)],
        capture_output=True,
        text=True,
    )
    say(f"quanta_measures exit {run.returncode}")
    for line in run.stdout.splitlines():
        say("   " + masked(line))
    stop |= run.returncode != 0
    expected = {"1": "R8-power-n1-v1", "2": "R8-power-n2-v1"}
    hist = rows(power / "quanta_histories.csv")
    for h in hist:
        ok = h["spectrum_model"] == expected[h["n"]]
        say(
            f"quanta_histories.csv n={h['n']} {h['drain_model']} A={h['alpha']}: spectrum_model {h['spectrum_model']} ({'right' if ok else 'WRONG'})"
        )
        stop |= not ok
    for rel in files(power, "quanta_*.csv"):
        if rel == "quanta_histories.csv":
            continue
        n = re.search(r"_n=(\d+)/", rel).group(1)
        models = sorted({r["spectrum_model"] for r in rows(power / rel)})
        ok = models == [expected[n]]
        say(f"{rel}: spectrum_model {models} ({'right' if ok else 'WRONG'})")
        stop |= not ok
    measured = len(hist)
    say(f"histories measured: {measured} (the one-loop histories at n = 1 and n = 2)")
    stop |= measured != 2

    # ---- a root without model --------------------------------------------------------------------------------------
    say()
    say("== a root without model (a copy of the base's root)")
    nomodel = V / "nomodel-root"
    with open(nomodel / "histories.csv", newline="") as f:
        header = next(csv.reader(f))
    say(f"its histories.csv has model: {'model' in header}")
    before = files(nomodel)
    run = subprocess.run(
        [sys.executable, "tools/quanta_measures.py", str(nomodel)],
        capture_output=True,
        text=True,
    )
    after = files(nomodel)
    say(f"quanta_measures exit {run.returncode}; stderr: {run.stderr.strip()}")
    say(f"files written: {sorted(set(after) - set(before))}")
    ok = run.returncode == 2 and "'model'" in run.stderr and before == after
    stop |= not ok

    say()
    say(f"STOP CONDITION HIT: {stop}")
    out = Path(__file__).parent / "out"
    out.mkdir(exist_ok=True)
    (out / "quanta.txt").write_text("\n".join(lines) + "\n")
    return 1 if stop else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]).resolve()))
