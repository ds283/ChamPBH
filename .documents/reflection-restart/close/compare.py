"""
reflection-restart prompt 04: the tables of the close, from the JSONL records under var/reflection-restart/04/
(prompt §1.1, §1.2 (a)-(d); orchestrator note 04, rulings 4-7). Reads only; runs nothing. Run from the
repository root:

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/close/compare.py SECTION

SECTION is one of:
  the-42      the 42 recomputed (the-42.jsonl): one row each, the counts by outcome, the stop conditions;
  pair        (a): 2197 and 2188 at 89a0760 (pair-89a0760.jsonl) and on this tree (pair-this.jsonl), the store
              comparison and the failure against the probe's;
  f3-off      (b): f3-off.jsonl against pair-89a0760.jsonl, field by field;
  candidates  (c): candidates-*.jsonl, the passivity check against (a)'s records, and the margins;
  nine        (d): the-nine.jsonl, one row each.

The signature at 89a0760, whose scanner does not count it, is counted from the record's rows as the scanner on
this tree counts it: a flagged row that is the first step of an instance begun by a restart (README §6.3).
"""

import json
import re
import sys
from collections import Counter
from pathlib import Path

D = Path("var/reflection-restart/04")
RESTART_REASONS = ("reflection", "drain", "release", "retake")
PROBE = {
    2197: dict(
        phi="2.950090845531173e-07",
        log_E="-306.7166549210319",
        bracket="[-42.28625968598544, -42.34875968598544]",
        wall=117.11052393913269,
    ),
    2188: dict(
        phi="2.647733963294608e-07",
        log_E="-306.6698578858307",
        bracket="[-42.269391484964466, -42.331891484964466]",
        wall=115.25966906547546,
    ),
}
FIELDS = (
    "outcome",
    "samples_sha256",
    "log_one_plus_z_sha256",
    "n_samples",
    "accepted_steps",
    "RHS_evaluations",
    "reflections",
)


def read(name):
    path = D / name
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def serial_of(rec):
    spec = rec.get("spec", {})
    if spec.get("store"):
        return spec["store"]["serial"]
    m = re.search(r"#(\d+) ", rec.get("key", ""))
    return int(m.group(1)) if m else None


def signature_counts(rec):
    """(kept, discarded): the scan's own counts where it has them, else counted from the rows."""
    scan = rec.get("scan") or {}
    if "signature_kept" in scan:
        return scan["signature_kept"], scan["signature_discarded"], "scan"
    rows = [
        r
        for r in rec.get("rows", [])
        if r["flagged"] and r["first"] and r["reason"] in RESTART_REASONS
    ]
    return (
        sum(1 for r in rows if not r.get("discarded")),
        sum(1 for r in rows if r.get("discarded")),
        "rows",
    )


def reflections_of(rec):
    if rec.get("outcome") == "success":
        return rec["reflections"]
    return (rec.get("scan") or {}).get("instance_reasons", {}).get("reflection")


def short_reason(reason, n=160):
    return reason if len(reason) <= n else reason[:n] + "..."


def the_42():
    recs = read("the-42.jsonl")
    tasks = json.loads((D / "tasks-42.json").read_text())
    print(f"the-42.jsonl: {len(recs)} records for {len(tasks)} tasks")
    print()
    print(
        "| # | store | beta | M/M_P | alpha | outcome | integrations | accepted steps | restart_retakes | reflections | "
        "signature kept / discarded | wall (s) |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    by_key = {r["key"]: r for r in recs}
    outcomes = Counter()
    stops = []
    retakes = []
    failures = []
    for i, t in enumerate(tasks, 1):
        r = by_key.get(t["key"])
        if r is None:
            print(f"| {i} | {t['key'].split()[0]} | — | — | — | MISSING | | | | | | |")
            stops.append(f"{t['key']}: no record")
            continue
        outcome = r.get("outcome", r.get("status"))
        outcomes[outcome] += 1
        kept, disc, _ = signature_counts(r)
        n_int = len(r.get("integrations", []))
        steps = r.get("accepted_steps", "—")
        rr = r.get("restart_retakes")
        if rr is None:
            rr = sum(e.get("restart_retakes", 0) for e in r.get("integrations", []))
        print(
            f"| {i} | {t['key'].split()[0]} | {t['beta']:g} | {t['M']:g} | {t['alpha']:g} | {outcome} | {n_int} | "
            f"{steps} | {rr} | {reflections_of(r)} | {kept} / {disc} | {r.get('wall', r.get('wall_batch', 0)):.1f} |"
        )
        if kept:
            stops.append(f"{t['key']}: {kept} kept signature step(s)")
        if outcome == "failure":
            failures.append((t["key"], r["failure_reason"]))
            if "log_m_eff_squared" in r["failure_reason"]:
                stops.append(f"{t['key']}: a log_m_eff_squared failure")
        if outcome not in ("success", "failure"):
            failures.append((t["key"], f"{outcome}: {r.get('traceback', '')[-300:]}"))
        if rr:
            retakes.append((t["key"], rr))
    print()
    print("outcomes: " + ", ".join(f"{k} {v}" for k, v in sorted(outcomes.items())))
    print(f"histories with restart_retakes > 0 (the guard working): {len(retakes)}")
    for k, n in retakes:
        print(f"  {k}: {n}")
    tot_kept = sum(signature_counts(r)[0] for r in recs)
    tot_disc = sum(signature_counts(r)[1] for r in recs)
    print(f"signature steps over the 42: kept {tot_kept}, discarded {tot_disc}")
    print(f"failures: {len(failures)}")
    for k, reason in failures:
        print(f"  {k}: {reason}")
    walls = [r.get("wall", 0) for r in recs]
    if walls:
        print(f"wall: sum {sum(walls):.1f} s, longest {max(walls):.1f} s")
    print(f"STOP conditions met: {len(stops)}")
    for s in stops:
        print(f"  {s}")


def show_pair_record(tree, r):
    s = serial_of(r)
    kept, disc, how = signature_counts(r)
    print(
        f"-- {s} at {tree}: {r.get('outcome', r.get('status'))}, wall {r.get('wall', r.get('wall_batch', 0)):.1f} s, "
        f"integrations {len(r.get('integrations', []))}"
    )
    if r.get("outcome") == "success":
        print(
            f"   accepted_steps {r['accepted_steps']}, RHS {r['RHS_evaluations']}, compute_steps {r['compute_steps']}, "
            f"reflections {r['reflections']}, n_samples {r['n_samples']}, restart_retakes {r.get('restart_retakes', 'n/a')}"
        )
        print(f"   samples_sha256 {r['samples_sha256']}")
        c = r.get("comparison")
        if c is not None:
            print(
                f"   store comparison: bitwise {c['bitwise']}; samples stored {c['stored_samples']} new {c['new_samples']}; "
                f"differing history columns {sorted(c['history_columns'])}, value columns {sorted(c['value_columns'])}, "
                f"extra_data {sorted(c['extra_data'])}; only in {c['extra_data_only_in']}; stored accepted_steps "
                f"{c['stored_accepted_steps']}, stored compute_time {c['stored_compute_time']}"
            )
    elif r.get("outcome") == "failure":
        print(f"   failure_reason: {r['failure_reason']}")
        integ = r.get("integrations", [])
        if integ:
            print(
                f"   integration entries: {[{k: v for k, v in e.items() if k != 'failure'} for e in integ]}"
            )
    sc = r.get("scan") or {}
    print(
        f"   scan: radau accepted {sc.get('radau_accepted_steps')}, instances {sc.get('instances')} "
        f"{sc.get('instance_reasons')}, flagged first {sc.get('flagged_first')} later {sc.get('flagged_later')}, "
        f"signature kept/discarded {kept}/{disc} (counted from {how})"
    )


def pair():
    old = {serial_of(r): r for r in read("pair-89a0760.jsonl")}
    new = {serial_of(r): r for r in read("pair-this.jsonl")}
    for s in (2197, 2188):
        for tree, recs in (("89a0760", old), ("this tree", new)):
            if s in recs:
                show_pair_record(tree, recs[s])
            else:
                print(f"-- {s} at {tree}: no record")
        r = new.get(s)
        if r is not None and r.get("outcome") == "failure":
            # the payload's failure_reason is cut at 256 characters, as the store's is; the whole message is the
            # integration's printed line, which the record's stdout_tail keeps
            full = [
                x.strip()
                for x in r.get("stdout_tail", [])
                if x.strip().startswith("classical_turning_point")
            ]
            print(
                f"   failure_reason is {len(r['failure_reason'])} characters, a prefix of the printed line: "
                f"{bool(full) and full[0].startswith(r['failure_reason'])}"
            )
            reason = full[0] if full else r["failure_reason"]
            p = PROBE[s]
            print(
                f"   against the probe: phi {('phi = ' + p['phi'] + ',') in reason}, "
                f"log_E {('log_E = ' + p['log_E'] + ')') in reason}, bracket {p['bracket'] in reason}"
            )
            line = Path(D / f"probe-{s}-base-stored-alpha.log")
            if line.exists():
                printed = [
                    x.strip()
                    for x in line.read_text().splitlines()
                    if x.strip().startswith("classical_turning_point")
                ]
                if printed:
                    print(
                        f"   the whole message equal to the probe's printed line: {reason == printed[0]} "
                        f"(lengths {len(reason)}, {len(printed[0])})"
                    )
            print(
                f"   probe wall {p['wall']:.1f} s (not compared, ruling 6); scanner wall {r.get('wall', 0):.1f} s"
            )
        print()


def f3_off():
    old = {serial_of(r): r for r in read("pair-89a0760.jsonl")}
    off = {serial_of(r): r for r in read("f3-off.jsonl")}
    for s in (2197, 2188):
        a, b = old.get(s), off.get(s)
        if a is None or b is None:
            print(
                f"-- {s}: records missing (89a0760 {a is not None}, f3-off {b is not None})"
            )
            continue
        print(f"-- {s}: F3 off (guard live) on this tree, against 89a0760")
        same = True
        for f in FIELDS:
            eq = a.get(f) == b.get(f)
            same &= eq
            print(
                f"   {f:<24} {'equal' if eq else 'DIFFERS'}: {a.get(f)!r} | {b.get(f)!r}"
            )
        kept, disc, _ = signature_counts(b)
        print(
            f"   restart_retakes {b.get('restart_retakes')}; signature kept {kept}, discarded {disc}"
        )
        print(
            f"   RESULT: {'bitwise equal' if same and not b.get('restart_retakes') and disc == 0 else 'NOT equal'}"
        )


def candidates():
    old = {serial_of(r): r for r in read("pair-89a0760.jsonl")}
    new = {serial_of(r): r for r in read("pair-this.jsonl")}
    for name, ref in (
        ("candidates-89a0760.jsonl", old),
        ("candidates-this.jsonl", new),
    ):
        for c in read(name):
            s = serial_of(c)
            a = ref.get(s)
            print(
                f"-- {s} ({name}, tree {c.get('tree', '')[:7]}): {c.get('outcome', c.get('status'))}, "
                f"wall {c.get('wall', 0):.1f} s"
            )
            if a is None:
                print("   no (a) record to compare")
            else:
                if c.get("outcome") == "success":
                    keys = ("outcome", "samples_sha256", "accepted_steps")
                else:
                    keys = ("outcome", "failure_reason")
                eq = {k: a.get(k) == c.get(k) for k in keys}
                print(f"   passive (ruling 5): {all(eq.values())} {eq}")
            sm = c.get("candidates", {})
            print(
                f"   calls {sm.get('calls')}: none {sm.get('n_none')}, point {sm.get('n_point')}, raise {sm.get('n_raise')}; "
                f"searched {sm.get('n_searched')}"
            )
            print(
                f"   least margin (quanta): searched {sm.get('min_margin_searched')}, point {sm.get('min_margin_point')}, "
                f"raise {sm.get('min_margin_raise')}, all calls {sm.get('min_margin_all')}"
            )
            print(
                "   searched calls with margin <= 17 / 32 / 100 / 1000: "
                f"{sm.get('n_searched_margin_le_17')} / {sm.get('n_searched_margin_le_32')} / "
                f"{sm.get('n_searched_margin_le_100')} / {sm.get('n_searched_margin_le_1000')}"
            )
            if "last_call" in sm:
                print(f"   the failing call: {sm['last_call']}")
            for x in sm.get("smallest_searched", [])[:5]:
                print(f"   smallest: {x}")


def nine():
    recs = read("the-nine.jsonl")
    tasks = json.loads((D / "tasks-nine.json").read_text())
    by_key = {r["key"]: r for r in recs}
    print(f"the-nine.jsonl: {len(recs)} records for {len(tasks)} tasks")
    print()
    print(
        "| store | beta | M/M_P | alpha | outcome | integrations | accepted steps | restart_retakes | reflections | "
        "signature kept / discarded | wall (s) |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|---|")
    pat = re.compile(r"phi = (?P<phi>[-0-9.e+]+), log_E = (?P<logE>[-0-9.e+]+)\)")
    detail = []
    for t in tasks:
        r = by_key.get(t["key"])
        if r is None:
            print(
                f"| {t['key'].split()[0]} | {t['beta']:g} | {t['M']:g} | {t['alpha']:g} | MISSING | | | | | | |"
            )
            continue
        outcome = r.get("outcome", r.get("status"))
        kept, disc, _ = signature_counts(r) if r.get("scan") else ("—", "—", "")
        rr = r.get("restart_retakes")
        if rr is None and r.get("integrations"):
            rr = sum(e.get("restart_retakes", 0) for e in r["integrations"])
        print(
            f"| {t['key'].split()[0]} | {t['beta']:g} | {t['M']:g} | {t['alpha']:g} | {outcome} | "
            f"{len(r.get('integrations', [])) or r.get('integration_reached', '—')} | {r.get('accepted_steps', '—')} | "
            f"{rr if rr is not None else '—'} | {reflections_of(r) if r.get('scan') else '—'} | {kept} / {disc} | "
            f"{r.get('wall', r.get('wall_batch', 0)):.1f} |"
        )
        if outcome == "failure":
            m = pat.search(r["failure_reason"])
            detail.append(
                f"{t['key']}: {short_reason(r['failure_reason'], 400)}"
                + (f"\n    phi = {m['phi']}, log_E = {m['logE']}" if m else "")
            )
        elif outcome == "timeout":
            detail.append(
                f"{t['key']}: timeout after {r.get('wall', 0):.0f} s; reached integration "
                f"{r.get('integration_reached')}, Radau accepted steps {r.get('radau_accepted_steps_reached')}, "
                f"N {r.get('N_reached')}"
            )
        elif outcome == "success":
            tu = r.get("tuning", {})
            detail.append(f"{t['key']}: success; tuning {tu}")
    print()
    for x in detail:
        print(x)


def main():
    {
        "the-42": the_42,
        "pair": pair,
        "f3-off": f3_off,
        "candidates": candidates,
        "nine": nine,
    }[sys.argv[1]]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
