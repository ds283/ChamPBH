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
Each pipeline stage's work-item counts from a main.py log, without the rates or times
(dk-and-rpk-migration README section 4.1, L2 and L4). Written for dk-and-rpk-migration
prompt 01; prompts 02, 06 and 10 run it unchanged.

    ./venv/bin/python .documents/dk-and-rpk-migration/rehearsal/stage_counts.py LOG... [--json OUT]

For each log, each stage banner (a line `** CALCULATE ...`, printed by the work pool) with:
- whether the stage completed (`-- ALL WORK ITEMS COMPLETE`);
- its queue-summary counts, without the rates: `Queue summary: 5 lookup (avg 61.32/hr),
  5 compute (avg ...), 5 store (...), 5 validation (...)` gives lookup 5, compute 5, store 5,
  validation 5 (and `available` where the pool has one);
- whether the pool said no compute item was evaluated;
- the "models skipped" lines that follow it (`-- AdiabaticHistory: N models skipped ...`,
  `-- BBNData: N models skipped ...; M computations skipped ...`), parsed and verbatim.
And the `** driver exit status N` line build.sh appends. The output carries no time, so two
logs of equal runs give equal output.
"""

import argparse
import json
import re
import sys
from pathlib import Path

BANNER = re.compile(r"^\*\* (CALCULATE .*?)\s*$")
COMPLETE = re.compile(r"^\s*-- ALL WORK ITEMS COMPLETE\b")
SUMMARY = re.compile(r"^\s*Queue summary: (.*)$")
SUMMARY_ITEM = re.compile(r"(\d+) (lookup|compute|store|available|validation)\b")
NO_COMPUTE = re.compile(r"^\s*\(no compute items were evaluated")
SKIPPED = re.compile(
    r"^-- (\w+): (\d+) models skipped because their ScalarModel failed"
    r"(?:; (\d+) computations skipped because of a stored failure)?"
)
EXIT_STATUS = re.compile(r"^\*\* driver exit status (-?\d+)\s*$")
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def parse_log(text: str) -> dict:
    """{"stages": [...], "exit_status": int or None} for one log's text."""
    stages = []
    exit_status = None
    current = None
    for raw in text.splitlines():
        line = ANSI.sub("", raw)

        match = EXIT_STATUS.match(line)
        if match:
            exit_status = int(match.group(1))
            continue

        match = BANNER.match(line)
        if match:
            current = {
                "stage": match.group(1),
                "complete": False,
                "counts": None,
                "no_compute": False,
                "skipped": [],
            }
            stages.append(current)
            continue

        if current is None:
            continue

        if COMPLETE.match(line):
            current["complete"] = True
            continue

        match = SUMMARY.match(line)
        if match:
            current["counts"] = {
                name: int(count) for count, name in SUMMARY_ITEM.findall(match.group(1))
            }
            continue

        if NO_COMPUTE.match(line):
            current["no_compute"] = True
            continue

        match = SKIPPED.match(line)
        if match:
            entry = {
                "table": match.group(1),
                "models_skipped": int(match.group(2)),
                "line": line.strip(),
            }
            if match.group(3) is not None:
                entry["stored_failures_skipped"] = int(match.group(3))
            current["skipped"].append(entry)

    return {"stages": stages, "exit_status": exit_status}


def format_counts(name: str, parsed: dict) -> str:
    lines = [f"LOG {name}"]
    if not parsed["stages"]:
        lines.append("  no stage banner")
    for stage in parsed["stages"]:
        state = "complete" if stage["complete"] else "INCOMPLETE"
        if stage["counts"] is None:
            counts = "no queue summary"
        else:
            counts = ", ".join(f"{k} {v}" for k, v in stage["counts"].items())
        extra = "; no compute items evaluated" if stage["no_compute"] else ""
        lines.append(f"  {stage['stage']}: {state}; {counts}{extra}")
        for skipped in stage["skipped"]:
            lines.append(f"    {skipped['line']}")
    status = parsed["exit_status"]
    lines.append(
        f"  driver exit status: {'not recorded' if status is None else status}"
    )
    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Each stage's queue-summary counts, without rates, from main.py logs."
    )
    parser.add_argument("logs", nargs="+", type=Path, metavar="LOG")
    parser.add_argument("--json", metavar="OUT", help="also write the result as JSON")
    args = parser.parse_args(argv)

    result = {}
    status = 0
    for log in args.logs:
        try:
            text = log.read_text(errors="replace")
        except OSError as e:
            print(f"!! stage_counts: cannot read {log}: {e}")
            status = 2
            continue
        parsed = parse_log(text)
        result[str(log)] = parsed
        print(format_counts(str(log), parsed))

    if args.json:
        Path(args.json).write_text(json.dumps(result, indent=2) + "\n")
    return status


if __name__ == "__main__":
    sys.exit(main())
