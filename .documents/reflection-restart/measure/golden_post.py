"""
The golden histories' comparison for reflection-restart prompt 01 §1.3 item 3, run by
tools/outward_step_scan.py in the worker on the history's payload (a task's "post"). Not run on its
own: tasks.py names it.

For the untuned golden histories (task "golden" = "beta=1.5" or "beta=5.5") the payload's record, by
ComputeTargets/tests/golden_history.record_of, is compared with data/golden_history_05.json; for the
tuned one ("tuned"), golden_tuned_history.build_record's with data/golden_history_06_tuned.json. The
result: "bitwise" (no difference), golden_history's own messages (the first differing excerpt and every
differing top-level key), and "moved": for each top-level key that differs, its recorded and current
values, and for each sample field the largest relative difference over the excerpts. Reads the
records, writes nothing.
"""

import json

from ComputeTargets.tests import golden_history as gh
from ComputeTargets.tests import golden_tuned_history as gth


def _rel(a, b):
    a, b = float.fromhex(a), float.fromhex(b)
    if a == b:
        return 0.0
    scale = max(abs(a), abs(b))
    return abs(a - b) / scale if scale > 0.0 else float("inf")


def _moved(stored: dict, current: dict) -> dict:
    out = {"keys": {}, "excerpt_max_rel": {}}
    for key in stored:
        if key == "excerpts":
            continue
        if stored[key] != current.get(key):
            out["keys"][key] = {"recorded": stored[key], "now": current.get(key)}
    s_ex = {e["index"]: e for e in stored["excerpts"]}
    c_ex = {e["index"]: e for e in current["excerpts"]}
    for index in sorted(set(s_ex) & set(c_ex)):
        if s_ex[index]["log_one_plus_z"] != c_ex[index]["log_one_plus_z"]:
            out.setdefault("excerpt_label_differs", []).append(index)
            continue
        for name, value in s_ex[index]["fields"].items():
            r = _rel(value, c_ex[index]["fields"][name])
            if r > out["excerpt_max_rel"].get(name, 0.0):
                out["excerpt_max_rel"][name] = r
    out["excerpts_compared"] = len(set(s_ex) & set(c_ex))
    out["excerpt_indices_only_in_one"] = sorted(set(s_ex) ^ set(c_ex))
    return out


def record(data: dict, spec: dict) -> dict:
    if data.get("failure", False):
        return {"bitwise": False, "failure": data.get("failure_reason")}
    if spec["golden"] == "tuned":
        golden = json.loads(gth.GOLDEN_TUNED_PATH.read_text())
        current = gth.build_record(data)
        messages = gth.differences(golden, current)
        moved = _moved(golden["history"], current["history"])
        moved["tuning"] = {
            k: {"recorded": golden["tuning"][k], "now": current["tuning"].get(k)}
            for k in golden["tuning"]
            if golden["tuning"][k] != current["tuning"].get(k)
        }
    else:
        golden = json.loads(gh.GOLDEN_PATH.read_text())["histories"][spec["golden"]]
        current = gh.record_of(data)
        messages = gh.first_difference(golden, current)
        moved = _moved(golden, current)
    return {"bitwise": not messages, "messages": messages[:12], "moved": moved}
