"""
one-loop-drain prompt 08, §2 item 3 and orchestrator note 08 ruling 4: every command that
review-remediation-verification.md §4.20 gives runs with --help, and every option §4.20 names appears
in its own command's help, spelled as §4.20 spells it.

The option list is taken from §4.20's text: every token --name in the section is collected, and each
must be one of OPTIONS' entries below (the commands it belongs to), so that an option the text names
and this table does not is a failure too. Each command's --help is written to out/help-<name>.txt and
the result to out/check_help.txt. Nothing here opens a store, starts Ray or runs a build.

Run from the repository root:
    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/close-08/check_help.py
Exit 0 if every check holds, 1 otherwise.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
DOCUMENT = Path(".documents/review-remediation-verification.md")
SECTION_START = "### 4.20 "
SECTION_END = "\n---\n"

PY = "./venv/bin/python"
COMMANDS = {
    "main": [PY, "main.py", "--help"],
    "plot_by_beta": [PY, "plot_by_beta.py", "--help"],
    "plot_ScalarHistory": [PY, "plot_ScalarHistory.py", "--help"],
    "registered_run": [PY, "RunDrivers/registered_run.py", "--help"],
    "quanta_measures": [PY, "tools/quanta_measures.py", "--help"],
    "plot_quanta_grid": [PY, "tools/plot_quanta_grid.py", "--help"],
    "plot_quanta_history": [PY, "tools/plot_quanta_history.py", "--help"],
    "bbn_from_store": [PY, "tools/bbn_from_store.py", "--help"],
    "history_and_bbn": [PY, "tools/history_and_bbn.py", "--help"],
    "runregistrykit_list": [PY, "-m", "runregistrykit", "list", "--help"],
}

PLOT = ("plot_by_beta", "plot_ScalarHistory")
# each option §4.20 names, and the commands whose help must list it (§4.20's own attribution)
OPTIONS = {
    "--config": ("main",) + PLOT,
    "--database": ("main",) + PLOT,
    "--shards": ("main",) + PLOT,
    "--ray-address": ("main",) + PLOT,
    "--phi-init-Mp": ("main",) + PLOT + ("bbn_from_store",),
    "--alpha-values": ("main",) + PLOT,
    "--A-values": ("main",) + PLOT + ("plot_quanta_grid", "plot_quanta_history"),
    "--job-name": ("main",),
    "--bbn-wall-clock-limit": ("main",),
    "--M-values-Mp": ("main",),
    "--beta-values": ("main", "plot_quanta_history"),
    "--inventory": ("main",),
    "--tuning-max-integrations": ("main",),
    "--retry-failed-scalar": ("main",),
    "--output": PLOT + ("plot_quanta_grid", "plot_quanta_history"),
    "--campaign": ("registered_run",),
    "--prompt": ("registered_run",),
    "--register": ("registered_run",),
    "--purpose": ("registered_run",),
    "--assoc-scale": ("quanta_measures", "plot_quanta_grid", "plot_quanta_history"),
    "--M-Mp-values": ("plot_quanta_history",),
    "--beta": ("bbn_from_store",),
    "--M-Mp": ("bbn_from_store",),
    "--drain-law": ("bbn_from_store", "history_and_bbn"),
    "--alpha": ("bbn_from_store",),
    "--small-network": ("bbn_from_store",),
    "--variant": ("bbn_from_store",),
    "--stale-after": ("runregistrykit_list",),
    "--help": tuple(COMMANDS),
}

# §4.20 point 7 gives one git command, read-only; it is run as written and its exit recorded, since
# git's --help opens a manual page
GIT_COMMANDS = {
    "--is-ancestor": ["git", "merge-base", "--is-ancestor", "main", "one-loop-drain"],
}


def section_text() -> str:
    text = DOCUMENT.read_text(encoding="utf-8")
    start = text.index(SECTION_START)
    end = text.index(SECTION_END, start)
    return text[start:end]


def options_in(text: str):
    return sorted(set(re.findall(r"(?<![\w-])--[A-Za-z][A-Za-z0-9-]*", text)))


def listed(option: str, help_text: str) -> bool:
    return (
        re.search(r"(?<![\w-])" + re.escape(option) + r"(?![\w-])", help_text)
        is not None
    )


def main() -> int:
    OUT.mkdir(exist_ok=True)
    env = dict(os.environ, PYTHONPATH=".")
    lines = []
    failures = 0

    helps = {}
    lines.append("# the commands, each with --help")
    for name, argv in COMMANDS.items():
        p = subprocess.run(argv, capture_output=True, text=True, env=env, timeout=300)
        helps[name] = p.stdout + p.stderr
        (OUT / f"help-{name}.txt").write_text(
            "$ PYTHONPATH=. "
            + " ".join(argv)
            + f"\n# exit {p.returncode}\n"
            + helps[name],
            encoding="utf-8",
        )
        ok = p.returncode == 0
        failures += 0 if ok else 1
        lines.append(
            f"{'ok  ' if ok else 'FAIL'} {' '.join(argv)}: exit {p.returncode}"
        )

    named = options_in(section_text())
    lines.append("")
    lines.append(
        f"# the options §4.20 names ({len(named)}), each in its command's --help"
    )
    for option in named:
        if option in GIT_COMMANDS:
            argv = GIT_COMMANDS[option]
            p = subprocess.run(argv, capture_output=True, text=True)
            ok = p.returncode in (0, 1)
            failures += 0 if ok else 1
            lines.append(
                f"{'ok  ' if ok else 'FAIL'} {option}: {' '.join(argv)} runs as written, exit {p.returncode}"
            )
            continue
        commands = OPTIONS.get(option)
        if commands is None:
            failures += 1
            lines.append(
                f"FAIL {option}: named in §4.20 and in no command of this table"
            )
            continue
        for name in commands:
            ok = listed(option, helps[name])
            failures += 0 if ok else 1
            lines.append(f"{'ok  ' if ok else 'FAIL'} {option} in {name} --help")
    unused = sorted(set(OPTIONS) - set(named) - set(GIT_COMMANDS))
    lines.append("")
    lines.append(
        f"# options of the table that §4.20 does not name: {unused if unused else 'none'}"
    )
    lines.append(f"# failures: {failures}")
    (OUT / "check_help.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
