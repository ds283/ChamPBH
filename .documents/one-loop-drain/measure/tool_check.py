"""
one-loop-drain prompt 02 §1.3, "The driver is checked against the tool" (orchestrator note 02, ruling 9): for P-2197 at
A = 0.1 and the ladder's 10^-16 at A = 1 (gap zero), `tools/history_and_bbn.py` itself was run in a subprocess with the
driver task's argv (run_sample.py, `tool:` lines; stdout in var/one-loop-drain/02/tasks/spot-tool-<id>.log, the step-4 batch run with --tag spot-). Its stdout is
compared line by line with the driver task's (tasks/<id>.log): every line byte-equal once the fields `wall=...` and
`PRyM_time=...` are masked, in the tune lines and the stage lines. The `history`, `drain` and `bbn` lines are also
checked against the driver's JSON line (sample.jsonl): the tool's printed values against the driver's, formatted as the
tool formats them. A difference is a stop (exit 3).

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure/tool_check.py > .documents/one-loop-drain/measure/out/tool_check.txt
"""

import re
import sys

from common import TASK_DIR, SPOT, read_jsonl, run_lines

CHECKS = ("P-2197-A0.1-zero", "L-M1e-16-A1-zero")
MASK = re.compile(r"\b(wall|PRyM_time)=[^ ]+")


def masked(text):
    return [MASK.sub(r"\1=*", x) for x in text.splitlines()]


def fields(line):
    return dict(kv.split("=", 1) for kv in line.split(": ", 1)[1].split() if "=" in kv)


def main():
    runs = run_lines()
    tools = {l["tool_check"]: l for l in read_jsonl(SPOT) if "tool_check" in l}
    stop = False
    for tid in CHECKS:
        drv = (TASK_DIR / f"{tid}.log").read_text()
        tool = (TASK_DIR / f"spot-tool-{tid}.log").read_text()
        a, b = masked(drv), masked(tool)
        diffs = [(j, x, y) for j, (x, y) in enumerate(zip(a, b)) if x != y]
        print(
            f"{tid}: driver stdout {len(a)} lines, tool stdout {len(b)} lines, tool rc {tools[tid]['rc']}; "
            f"{len(a) - len(diffs) if len(a) == len(b) else 'n/a'} byte-equal after masking"
        )
        if len(a) != len(b) or diffs:
            stop = True
            print(f"   !! line counts {len(a)} and {len(b)}; {len(diffs)} lines differ")
            for j, x, y in diffs[:10]:
                print(f"   [{j}] driver: {x}\n   [{j}]   tool: {y}")
        line = runs[tid]
        acc = line["accepted"]
        bbn = line["bbn"]
        tl = {
            x.split(" ", 1)[0]: x
            for x in tool.splitlines()
            if x.split(" ", 1)[0] in ("history", "drain", "bbn")
        }
        h, d, bb = fields(tl["history"]), fields(tl["drain"]), fields(tl["bbn"])
        want = {
            ("history", "accepted_steps"): str(acc["accepted_steps"]),
            ("history", "reflections"): str(acc["reflections"]),
            ("history", "samples"): str(acc["n_samples"]),
            ("history", "RHS"): str(acc["RHS_evaluations"]),
            ("history", "integrations"): str(line["tuning"]["tuning_iterations"]),
            ("bbn", "Yp"): format(bbn["Yp"], ".10g"),
            ("bbn", "DoH"): format(bbn["DoH"], ".10g"),
            ("bbn", "He3oH"): format(bbn["He3oH"], ".10g"),
            ("bbn", "Li7oH"): format(bbn["Li7oH"], ".10g"),
        }
        for k, v in acc["drain_values"].items():
            want[("drain", k)] = "None" if v is None else format(v, ".6g")
        got = {"history": h, "drain": d, "bbn": bb}
        bad = [
            (s, k, v, got[s].get(k)) for (s, k), v in want.items() if got[s].get(k) != v
        ]
        print(
            f"   history, drain and bbn lines against the driver's JSON line: {len(want) - len(bad)} of {len(want)} "
            f"values equal"
        )
        for s, k, v, gv in bad:
            print(f"   !! {s}.{k}: driver {v} tool {gv}")
        if bad:
            stop = True
    if stop:
        print("!! STOP: the driver disagrees with the tool")
        sys.exit(3)
    print("the driver agrees with the tool on both tasks")


if __name__ == "__main__":
    main()
