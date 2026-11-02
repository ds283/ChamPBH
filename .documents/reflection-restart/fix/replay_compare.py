"""
The replay's comparison of reflection-restart prompt 03 (§1.7 items 2 and 3; orchestrator note 03, ruling 1):
each line of var/reflection-restart/03/replay.jsonl (the production code after this prompt) against prompt 01's
line for the same task under its probe's F3 (var/reflection-restart/01/main.jsonl: the line whose key holds
" variant=F3", which the replay's key is without; replay_tasks.py removed it).

For each task, equal or not: `outcome`, `samples_sha256`, `log_one_plus_z_sha256`, `n_samples`,
`accepted_steps`, `RHS_evaluations`, `compute_steps` and `reflections`; for the 54 store tasks, the store
comparison (`comparison`, the whole dict: the same columns differing by the same amounts as 01's F3); for the
three goldens, the hook's verdict, which must be `bitwise` against the records this prompt regenerated. Then,
in every task: `restart_retakes` 0 (the record's and every integration's), and the ruled signature (P1's flag,
either condition, on the first accepted step of an instance begun by a reflection, a drain, a release or a
retake) on no step, kept or discarded. A failure (the three of the diagnosis under 01's None) compares its
`failure_reason` instead of the digests.

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/fix/replay_compare.py \\
        [REPLAY.jsonl] > .documents/reflection-restart/fix/out/replay.txt

Exit 0 if every task agrees in every field, 1 otherwise. Reads only.
"""

import json
import re
import sys
from pathlib import Path

OLD = Path("var/reflection-restart/01/main.jsonl")
NEW = Path("var/reflection-restart/03/replay.jsonl")
SUFFIX = " variant=F3"
# the token exactly: not " variant=F3+F4"
F3_KEY = re.compile(r" variant=F3(?= |$)")

FIELDS = (
    "outcome",
    "samples_sha256",
    "log_one_plus_z_sha256",
    "n_samples",
    "accepted_steps",
    "RHS_evaluations",
    "compute_steps",
    "reflections",
)
RESTARTED = ("reflection", "drain", "release", "retake")


def lines(path: Path) -> dict:
    out = {}
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                record = json.loads(line)
                out[record["key"]] = record
    return out


def signature(record: dict):
    """(kept, discarded) steps carrying the ruled signature, from the rows the scanner keeps (every
    flagged row is kept, so every signature row is there)."""
    kept = discarded = 0
    for row in record.get("rows", []):
        if row["flagged"] and row["first"] and row["reason"] in RESTARTED:
            if row.get("discarded"):
                discarded += 1
            else:
                kept += 1
    return kept, discarded


def kind(record: dict) -> str:
    spec = record["spec"]
    if spec.get("post"):
        return "golden"
    return spec["mode"]


def main(argv) -> int:
    new_path = Path(argv[1]) if len(argv) > 1 else NEW
    old = lines(OLD)
    # 01's F3 lines, by the replay's key: " variant=F3" removed wherever it stands (before
    # " golden=..." in the goldens' keys)
    old_f3 = {F3_KEY.sub("", k, count=1): r for k, r in old.items() if F3_KEY.search(k)}
    new = lines(new_path)
    differing = 0
    rows = []
    for key in sorted(new):
        n = new[key]
        o = old_f3.get(key)
        notes = []
        if o is None:
            notes.append("no line of 01's F3 for this task")
        elif n.get("status") != "done":
            notes.append(f"status {n.get('status')}")
        else:
            for field in FIELDS:
                if field == "outcome" or n.get("outcome") == "success":
                    if o.get(field) != n.get(field):
                        notes.append(f"{field}: {o.get(field)!r} -> {n.get(field)!r}")
            if n.get("outcome") == "failure" and o.get("failure_reason") != n.get(
                "failure_reason"
            ):
                notes.append("failure_reason differs")
            if n["spec"].get("store") is not None and n.get("outcome") == "success":
                if o.get("comparison") != n.get("comparison"):
                    notes.append("the store comparison differs from 01's F3")
            if n["spec"].get("post"):
                if not (n.get("post") or {}).get("bitwise"):
                    notes.append(
                        f"golden hook not bitwise: {(n.get('post') or {}).get('messages')}"
                    )
        retakes = [n.get("restart_retakes")] + [
            i.get("restart_retakes") for i in n.get("integrations", [])
        ]
        if any(r != 0 for r in retakes if n.get("outcome") == "success") or (
            n.get("outcome") == "success" and n.get("restart_retakes") is None
        ):
            notes.append(f"restart_retakes {retakes}")
        kept, discarded = signature(n)
        if kept or discarded:
            notes.append(f"signature steps kept {kept}, discarded {discarded}")
        scan = n.get("scan", {})
        if (
            scan.get("signature_kept", 0) != kept
            or scan.get("signature_discarded", 0) != discarded
        ):
            notes.append(
                f"the scanner's signature counts {scan.get('signature_kept')}/"
                f"{scan.get('signature_discarded')} against the rows' {kept}/{discarded}"
            )
        if notes:
            differing += 1
        rows.append(
            (
                kind(n),
                key,
                n.get("outcome"),
                n.get("accepted_steps"),
                n.get("restart_retakes"),
                kept,
                discarded,
                "equal" if not notes else "DIFFERS: " + "; ".join(notes),
            )
        )

    missing = sorted(old_f3)
    missing = [k for k in missing if k not in new]

    print(f"replay: {new_path}, {len(new)} lines; 01's F3 lines: {len(old_f3)}")
    print(
        "fields compared: "
        + ", ".join(FIELDS)
        + "; the store comparison (store tasks); the golden hook (goldens); restart_retakes; the signature"
    )
    print()
    print(
        "kind\toutcome\taccepted_steps\trestart_retakes\tsignature kept\tsignature discarded\tverdict\tkey"
    )
    for kind_, key, outcome, steps, retakes, kept, discarded, verdict in sorted(rows):
        print(
            f"{kind_}\t{outcome}\t{steps}\t{retakes}\t{kept}\t{discarded}\t{verdict}\t{key}"
        )
    print()
    by_kind = {}
    for r in rows:
        by_kind.setdefault(r[0], [0, 0])
        by_kind[r[0]][0] += 1
        by_kind[r[0]][1] += r[-1] == "equal"
    for k, (total, equal) in sorted(by_kind.items()):
        print(f"{k}: {equal} of {total} equal in every field")
    print(f"tasks of 01's F3 not in the replay: {len(missing)}")
    for k in missing:
        print(f"  {k}")
    total_kept = sum(r[5] for r in rows)
    total_discarded = sum(r[6] for r in rows)
    print(
        f"signature steps over the replay: kept {total_kept}, discarded {total_discarded}"
    )
    print(f"restart_retakes over the replay: {sum((r[4] or 0) for r in rows)}")
    ok = differing == 0 and not missing and len(rows) == 66
    print(
        f"RESULT: {'EQUAL' if ok else 'DIFFERENT'} ({len(rows) - differing} of {len(rows)} tasks equal)"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
