# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
power-law-wall prompt 01 §1.5: the table of the twelve inverse-power-law runs and their exponential
counterparts, read from runs.sh's out/*.txt. Prints a Markdown table to stdout; run from the repository
root after runs.sh:

    ./venv/bin/python .documents/power-law-wall/model-01/summary.py > .documents/power-law-wall/model-01/out/summary.md

Each row: the run, its status (ok, or FAILURE and the reason, cut at 160 characters), the wall time of
the whole process (the footer's), the tuning's integrations and seed kind, the reflections, the
injections and log_R_first_impact (the dissipation line; the tool prints none at alpha = 0), Delta N_eff
today and at 1 MeV (likewise), Y_p and D/H (the bbn line), and the first bounce's N and T_J.
"""

import sys
from pathlib import Path

OUT = Path(__file__).resolve().parent / "out"


def fields(line: str) -> dict:
    body = line.split(": ", 1)[1] if ": " in line else ""
    return dict(item.split("=", 1) for item in body.split() if "=" in item)


def read(path: Path) -> dict:
    row = {"run": path.stem}
    for line in path.read_text().splitlines():
        if line.startswith("# end: "):
            row["process_wall"] = line.split("wall=", 1)[1].split()[0]
            row["exit"] = line.split("exit=", 1)[1].split()[0]
        elif line.startswith("history ") and ": FAILURE" in line:
            row["status"] = "FAILURE " + line.split("reason=", 1)[1][:160]
        elif line.startswith("history "):
            f = fields(line)
            row["status"] = "ok"
            row["reflections"] = f["reflections"]
            row["integrations"] = f["integrations"]
        elif line.startswith("tuning "):
            f = fields(line)
            row["seed_kind"] = f["seed_kind"]
        elif line.startswith("bounce ") and not line.endswith(": none"):
            f = fields(line)
            row["bounce"] = f"N={float(f['N']):.4f}, {float(f['T_J']):.1f} MeV"
        elif line.startswith("dissipation "):
            f = fields(line)
            row["injections"] = f["injections"]
            row["log_R_first_impact"] = f["log_R_first_impact"]
            row["dNeff_today"] = f["Delta_N_eff_today"]
            row["dNeff_1MeV"] = f["Delta_N_eff_1MeV"]
        elif line.startswith("bbn ") and "FAILURE" not in line:
            f = fields(line)
            row["Yp"] = f["Yp"]
            row["DoH"] = f["DoH"]
        elif line.startswith("bbn ") and "FAILURE" in line:
            row["status"] = row.get("status", "") + "; BBN FAILURE"
    if "status" not in row:
        row["status"] = "no history line (see the file)"
    return row


COLUMNS = (
    "run",
    "status",
    "exit",
    "process_wall",
    "integrations",
    "seed_kind",
    "reflections",
    "injections",
    "log_R_first_impact",
    "dNeff_today",
    "dNeff_1MeV",
    "Yp",
    "DoH",
    "bounce",
)


def main() -> int:
    rows = [read(p) for p in sorted(OUT.glob("*.txt"))]
    print("| " + " | ".join(COLUMNS) + " |")
    print("|" + "---|" * len(COLUMNS))
    for row in rows:
        print("| " + " | ".join(row.get(c, "—") for c in COLUMNS) + " |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
