"""
main.py's order and printed lines for an exponential-only run, the base against the tip (power-law-wall prompt 06 §1.4;
orchestrator note 06, rulings 2 and 3). main.py is not run against a store and starts no Ray: main_harness.py extracts
its `execute` by ast, with the module's own top-level imports, and runs it on a stood-in pool that records every
`object_get` in order (the class and its keywords, the sampling axis summarised by its length and ends), with
`run_pipeline`, `build_model_list` and `warn_super_planckian` stood in.

    PYTHONPATH=. ./venv/bin/python .documents/power-law-wall/export-06/main_order.py BASE_TREE TIP_TREE

For each command of main_cases.txt, the base's lookups, pipelines and printed lines against the tip's, for the command
as it is and with `--models exponential` added (the tip only; the base has no such option): any difference is a stop
(exit 1). Writes out/main_order.txt.
"""

import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def run(tree: Path, argv):
    p = subprocess.run(
        [sys.executable, str(HERE / "main_harness.py"), "main.py", *argv],
        cwd=tree,
        env=dict(os.environ, PYTHONPATH="."),
        capture_output=True,
        text=True,
    )
    assert p.returncode == 0, p.stderr
    return p.stdout


def main(base: Path, tip: Path) -> int:
    lines = []
    stop = False
    for k, case in enumerate((HERE / "main_cases.txt").read_text().splitlines(), 1):
        argv = case.split()
        b = run(base, argv)
        printed = b.split("\n----\n")[1].strip().splitlines()
        for extra in ([], ["--models", "exponential"]):
            t = run(tip, argv + extra)
            same = t == b
            stop |= not same
            lines.append(
                f"case {k} {' '.join(argv + extra)}: lookups, pipelines and printed lines "
                f"{'IDENTICAL' if same else 'DIFFER'} ({len(b)} characters of output)"
            )
        lines.extend(f"      | {l}" for l in printed)
    lines.append(f"STOP CONDITION HIT: {stop}")
    (HERE / "out").mkdir(exist_ok=True)
    (HERE / "out" / "main_order.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 1 if stop else 0


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()))
