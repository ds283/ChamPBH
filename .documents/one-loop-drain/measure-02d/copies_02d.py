"""
one-loop-drain prompt 02d §1.2 and orchestrator note 02d ruling 1: every copy of a function (or module-level name) of
02's or 02b's scripts in 02d's, with a unified diff against its original. The sources are read as text and parsed
with ast, not imported (they no longer run on this tree). A function's comment line above its `def` is not part of
its segment.

    ./venv/bin/python .documents/one-loop-drain/measure-02d/copies_02d.py \\
        > .documents/one-loop-drain/measure-02d/out/copies.txt
"""

import ast
import difflib
from pathlib import Path

HERE = Path(__file__).resolve().parent
M02 = HERE.parent / "measure"
M02B = HERE.parent / "measure-02b"

# (source file, name in source, 02d file, name in 02d)
COPIES = [
    (M02 / "tasks.py", n, HERE / "tasks_02d.py", n)
    for n in (
        "STORE",
        "LAB_M",
        "HISTORIES",
        "A_VALUES",
        "history_id",
        "a_label",
        "task_id",
        "store_row",
        "main",
    )
] + [
    (M02 / "run_sample.py", n, HERE / "run_02d.py", n)
    for n in (
        "OUT",
        "TASKS_JSON",
        "TASK_DIR",
        "BATCH_DIR",
        "SELF",
        "TIMEOUT_S",
        "now_utc",
        "loads",
        "load_tasks",
        "digest",
        "reduce_result",
        "accepted_values",
        "child",
        "read_partial",
        "TASK_FIELDS",
        "build_line",
        "batch",
        "main",
    )
]
COPIES += [(M02B / "run_wide.py", "first_impact", HERE / "run_02d.py", "first_impact")]
COPIES += [
    (M02 / "common.py", n, HERE / "checks_02d.py", n)
    for n in (
        "NOT_COMPARED",
        "INTEGRATION_NOT_COMPARED",
        "BBN_NOT_COMPARED",
        "read_jsonl",
        "_diff",
        "compare_lines",
        "compare_partials",
    )
]
COPIES += [
    (M02 / "tool_check.py", n, HERE / "checks_02d.py", n)
    for n in ("MASK", "masked", "fields")
]
COPIES += [(M02 / "tool_check.py", "main", HERE / "checks_02d.py", "tool")]
COPIES += [
    (M02 / "common.py", "read_jsonl", HERE / "analyse_02d.py", "read_jsonl"),
    (M02 / "common.py", "g", HERE / "analyse_02d.py", "g"),
    (M02 / "analyse.py", "short_reason", HERE / "analyse_02d.py", "short_reason"),
    (M02 / "analyse.py", "met_counts", HERE / "analyse_02d.py", "met_counts"),
]


def segment(path: Path, name: str) -> str:
    text = path.read_text()
    tree = ast.parse(text)
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name == name:
            return ast.get_source_segment(text, node)
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    return ast.get_source_segment(text, node)
    raise KeyError(f"{name} not in {path}")


def main():
    print(
        "# the copies of prompt 02d's scripts, each against its original (note 02d ruling 1)"
    )
    n_same = 0
    for src, sname, dst, dname in COPIES:
        a = segment(src, sname).splitlines()
        b = segment(dst, dname).splitlines()
        rel_s = src.relative_to(HERE.parent)
        rel_d = dst.relative_to(HERE.parent)
        d = list(
            difflib.unified_diff(
                a, b, f"{rel_s}:{sname}", f"{rel_d}:{dname}", lineterm="", n=1
            )
        )
        if not d:
            n_same += 1
            print(f"\n## {rel_d}:{dname} <- {rel_s}:{sname}: unchanged")
        else:
            print(f"\n## {rel_d}:{dname} <- {rel_s}:{sname}: changed")
            for x in d:
                print(x)
    print(
        f"\n# {len(COPIES)} copies, {n_same} unchanged, {len(COPIES) - n_same} changed"
    )


if __name__ == "__main__":
    main()
