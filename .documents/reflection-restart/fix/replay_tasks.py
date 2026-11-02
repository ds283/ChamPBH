"""
The replay's task list of reflection-restart prompt 03 (§1.7 item 2; orchestrator note 03, ruling 1): the 66
task specs that prompt 01 ran under its probe's F3 (var/reflection-restart/01/tasks-main.json), with the
key "variant" removed and each "key" rewritten without " variant=F3". Everything else is unchanged: the nine
tuned histories of the diagnosis, the 54 at a stored normalization (with "store" and "failure"), and the three
goldens with their "post" hook (01's golden_post.py, which reads the golden paths through the modules, so it
compares each golden task with the records this prompt regenerates).

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/fix/replay_tasks.py

writes var/reflection-restart/03/tasks-replay.json and prints the count by mode. Run by

    PYTHONPATH=. ./venv/bin/python tools/outward_step_scan.py batch \\
        --tasks var/reflection-restart/03/tasks-replay.json \\
        --output var/reflection-restart/03/replay.jsonl --jobs 6 --timeout 1800

(detached), and compared with 01's F3 lines by replay_compare.py.
"""

import collections
import json
from pathlib import Path

SOURCE = Path("var/reflection-restart/01/tasks-main.json")
TARGET = Path("var/reflection-restart/03/tasks-replay.json")
SUFFIX = " variant=F3"


def replay_tasks():
    tasks = []
    for spec in json.loads(SOURCE.read_text()):
        if spec.get("variant") != "F3":
            continue
        spec = dict(spec)
        del spec["variant"]
        if SUFFIX not in spec["key"]:
            raise SystemExit(f"!! a key without '{SUFFIX}': {spec['key']}")
        spec["key"] = spec["key"].replace(SUFFIX, "", 1)
        tasks.append(spec)
    return tasks


def main():
    tasks = replay_tasks()
    keys = [t["key"] for t in tasks]
    if len(set(keys)) != len(keys):
        raise SystemExit("!! two replay tasks have the same key")
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(json.dumps(tasks, indent=1) + "\n")
    count = collections.Counter("golden" if t.get("post") else t["mode"] for t in tasks)
    print(f"wrote {TARGET}: {len(tasks)} tasks, {dict(sorted(count.items()))}")


if __name__ == "__main__":
    main()
