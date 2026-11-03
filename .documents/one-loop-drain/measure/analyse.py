"""
one-loop-drain prompt 02 §2 (the Measure gate), from var/one-loop-drain/02/sample.jsonl (180 task lines, run or
derived by S1/S2), fixed_law.json and the batches' logs. One subcommand per item of §2; each prints a text table.

    analyse.py sample    §2.1  every task's outcome, by group, A and prescription, S1 and S2 marked; failures and
                               timeouts with their reasons
    analyse.py regimes   §2.2  per history and A: the K shares by regime and the parking leftover's; the gap's reach;
                               l_min, the drains, the R S(L) bound; U9's w_gate < 1 count and P19's unassessed count
    analyse.py spread    §2.3  edge - zero for each history and A run under both
    analyse.py fixed     §2.4  the one-loop values at each A beside the fixed law's at each stored alpha
    analyse.py cost      §2.5  wall time and integrations per tuning, by group and A; the batches
    analyse.py u14       §2.6  for U14: each observable against A, per history, beside the fixed law

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure/analyse.py SUB > .documents/one-loop-drain/measure/out/SUB.txt

Numbers are printed to 4-7 significant figures; sample.jsonl holds every value exactly (json floats are reprs).
"""

import json
import re
import sys
from pathlib import Path

from common import (
    A_VALUES,
    GROUP_ORDER,
    OUT,
    all_lines,
    g,
    histories,
    load_tasks,
    read_jsonl,
    task_id,
)

REGIMES = ("one_loop", "gap", "classical", "unassessed")
SHORT = {"one_loop": "1L", "gap": "gap", "classical": "cl", "unassessed": "un"}


def ordered_histories():
    hs = histories(load_tasks())
    return sorted(hs, key=lambda h: GROUP_ORDER.index(h["group"]))


def line_of(lines, hid, A, gap):
    return lines[task_id(hid, A, gap)]


def met_counts(line):
    """The candidates of each regime over every integration of a line, finished or not (the partial records)."""
    out = {r: 0 for r in REGIMES}
    for i in line.get("integrations") or []:
        for r, n in (i.get("counts") or {}).items():
            out[r] += n
    return out


def short_reason(line):
    r = line.get("reason") or ""
    if "step budget" in r:
        m = re.search(r"at N=([0-9.]+)", r)
        return f"step budget (2e6 steps) at N={m.group(1) if m else '?'}"
    if "effective minimum" in r:
        return "the effective minimum was not found (after integration 0)"
    return r[:100]


def lnR_first(line):
    acc = line.get("accepted")
    if acc is None:
        return None
    return acc["dissipation"].get("log_R_first_impact")


def sample():
    lines = all_lines()
    hs = ordered_histories()
    print(f"# §2.1 the sample: {len(lines)} task lines")
    counts = {}
    for l in lines.values():
        key = (l["obtained"], l["outcome"])
        counts[key] = counts.get(key, 0) + 1
    for (how, outcome), n in sorted(counts.items()):
        print(f"#   obtained {how:3s} outcome {outcome:8s}: {n}")
    print(
        "# each cell: the outcome (ok = success), /S1 or /S2 where derived, not run (sample.jsonl's `equals` names the task)"
    )
    head = "history        M        beta    " + "  ".join(
        f"{a_g:>16s}"
        for a_g in [f"A{A:g} {gap}" for A in A_VALUES for gap in ("zero", "edge")]
    )
    print(head)
    for h in hs:
        cells = []
        for A in A_VALUES:
            for gap in ("zero", "edge"):
                l = line_of(lines, h["history"], A, gap)
                o = "ok" if l["outcome"] == "success" else l["outcome"]
                if l["obtained"] == "S1":
                    o += "/S1"
                elif l["obtained"] == "S2":
                    o += "/S2"
                cells.append(f"{o:>16s}")
        print(f"{h['history']:13s}  {h['M']:<7g}  {h['beta']:<6g}  " + "  ".join(cells))
    print("\n# failures, errors and timeouts (run lines), with their reasons")
    n = 0
    for l in lines.values():
        if l["obtained"] == "run" and l["outcome"] != "success":
            n += 1
            ints = l.get("integrations") or []
            print(
                f"{l['id']}: {l['outcome']} stage={l.get('stage')} after {len(ints)} integrations, "
                f"wall {l['wall']:.0f} s: {l.get('reason')}"
            )
    if n == 0:
        print("none")


def regimes():
    lines = all_lines()
    hs = ordered_histories()
    print("# §2.2 the accepted integration of each tuning, by regime (zero and edge)")
    print(
        "# cand: candidates 1L/gap/cl/un; K shares of K(N_final) drained in each regime and the parking leftover's;"
    )
    print(
        "# drains (delta_K > 0); l_min; least ln R at a drain; w<1: one-loop candidates with w_gate < 1 (U9)"
    )
    for h in hs:
        print(
            f"\n{h['history']}  M={h['M']:g} beta={h['beta']:g}  "
            f"ln R_first={g(next((lnR_first(line_of(lines, h['history'], A, 'zero')) for A in A_VALUES if lnR_first(line_of(lines, h['history'], A, 'zero')) is not None), None), '.3f')}"
        )
        for A in A_VALUES:
            for gap in ("zero", "edge"):
                l = line_of(lines, h["history"], A, gap)
                tag = l["obtained"] if l["obtained"] != "run" else ""
                if l.get("accepted") is None:
                    met = met_counts(l)
                    print(
                        f"   A={A:<5g} {gap:4s} {tag:3s} {l['outcome']}; met in its {len(l.get('integrations') or [])} "
                        f"integration(s): cand={'/'.join(str(met[r]) for r in REGIMES)}; {short_reason(l)}"
                    )
                    continue
                acc = l["accepted"]
                dv = acc["drain_values"]
                cand = "/".join(str(dv[r]) for r in REGIMES)
                shares = (
                    " ".join(
                        f"{SHORT[r]}={g(dv['K_share_' + r], '.4g')}" for r in REGIMES
                    )
                    + f" park={g(dv['K_share_parking'], '.3g')}"
                )
                drains = "/".join(str(acc["drains"][r]) for r in REGIMES)
                print(
                    f"   A={A:<5g} {gap:4s} {tag:3s} cand={cand:18s} {shares}  drains={drains} "
                    f"l_min={g(dv['ell_min'], '.4g')} lnR_drain_min={g(acc['least_log_R_at_drain'], '.4g')} "
                    f"w<1={acc['one_loop_w_gate_below_1']} unresolvable={dv['unresolvable_assessments']} "
                    f"declined={dv['unresolved_declined']}"
                )
    print(
        "\n# the regimes met, by history and A: the zero task's candidates over every integration of its tuning,"
    )
    print(
        "# failures included (1L/gap/cl/un); G: the cascade enters the gap; C: it reaches R <= 1; x: the tuning failed"
    )
    print(
        f"{'history':13s} {'ln R_first':>10s}  "
        + "  ".join(f"{'A=' + format(A, 'g'):>24s}" for A in A_VALUES)
    )
    for h in hs:
        cells = []
        for A in A_VALUES:
            l = line_of(lines, h["history"], A, "zero")
            m = met_counts(l)
            flag = (
                ("G" if m["gap"] else "-")
                + ("C" if m["classical"] else "-")
                + ("x" if l["outcome"] != "success" else " ")
            )
            cells.append(f"{'/'.join(str(m[r]) for r in REGIMES) + ' ' + flag:>24s}")
        lr = next(
            (
                lnR_first(line_of(lines, h["history"], A, g_))
                for A in A_VALUES
                for g_ in ("zero", "edge")
                if lnR_first(line_of(lines, h["history"], A, g_)) is not None
            ),
            None,
        )
        print(f"{h['history']:13s} {g(lr, '.3f'):>10s}  " + "  ".join(cells))

    print("\n# the gap's reach (zero; edge where it was run)")
    print(
        "# first gap: index, N, ln R, l of the one-loop candidate before it; first classical: index, N, ln R;"
    )
    print(
        "# 1L after: one-loop candidates after the first gap / after the first classical"
    )
    for h in hs:
        for A in A_VALUES:
            for gap in ("zero", "edge"):
                l = line_of(lines, h["history"], A, gap)
                if gap == "edge" and l["obtained"] != "run":
                    continue
                if l.get("accepted") is None:
                    continue
                r = l["accepted"]["reach"]
                gp, cl = r["gap"], r["classical"]
                gs = (
                    "no gap"
                    if gp is None
                    else f"gap #{gp['index']} N={gp['N']:.4f} lnR={g(gp['log_R'], '.4g')} l_before={g(gp['ell_one_loop_before'], '.4g')} 1L_after={gp['one_loop_after']}"
                )
                cs = (
                    "no classical"
                    if cl is None
                    else f"classical #{cl['index']} N={cl['N']:.4f} lnR={g(cl['log_R'], '.4g')} 1L_after={cl['one_loop_after']} gap_after={cl['gap_after']}"
                )
                print(
                    f"{h['history']:13s} A={A:<5g} {gap:4s} lnR_first={g(lnR_first(l), '.4g'):>8s}  "
                    f"enters_gap={r['enters_gap']!s:5s} reaches_R<=1={r['reaches_classical']!s:5s}  {gs}; {cs}"
                )
    print("\n# the R S(L) bound on the classical candidates (ruling 7)")
    print(
        "# n: classical candidates; in/out: with L in / outside [40, 134]; L range; sum of the bound as a share of"
    )
    print("# K(N_final) (None where K = 0) and as an r_chi today")
    for h in hs:
        for A in A_VALUES:
            for gap in ("zero", "edge"):
                l = line_of(lines, h["history"], A, gap)
                if gap == "edge" and l["obtained"] != "run":
                    continue
                if l.get("accepted") is None:
                    continue
                b = l["accepted"]["RSL"]
                print(
                    f"{h['history']:13s} A={A:<5g} {gap:4s} n={b['classical']:6d} in={b['in_range']:6d} "
                    f"out={b['outside_range']:6d} L=[{g(b['L_min'], '.4g')}, {g(b['L_max'], '.4g')}] "
                    f"share={g(b['share_of_K'], '.4g')} r_chi_bound={g(b['r_chi_today'], '.4g')} "
                    f"r_chi_today={g(l['accepted']['dissipation']['rho_chi_ratio_today'], '.4g')}"
                )
    print("\n# U9 and P19 over the sample's run lines (accepted integrations)")
    runs = [l for l in lines.values() if l["obtained"] == "run" and l.get("accepted")]
    w1 = sum(l["accepted"]["one_loop_w_gate_below_1"] for l in runs)
    n1 = sum(l["accepted"]["drain_values"]["one_loop"] for l in runs)
    un = sum(l["accepted"]["drain_values"]["unassessed"] for l in runs)
    nc = sum(l["accepted"]["drain_values"]["candidates"] for l in runs)
    wg = sum(l["accepted"]["gap_w_gate_below_1"] for l in runs)
    ng = sum(l["accepted"]["drain_values"]["gap"] for l in runs)
    print(
        f"one-loop candidates with w_gate < 1: {w1} of {n1}; gap candidates with w_gate < 1: {wg} of {ng}; "
        f"unassessed candidates: {un} of {nc} (over {len(runs)} run lines)"
    )
    for l in runs:
        if (
            l["accepted"]["one_loop_w_gate_below_1"]
            or l["accepted"]["drain_values"]["unassessed"]
        ):
            print(
                f"   {l['id']}: w<1 {l['accepted']['one_loop_w_gate_below_1']}, unassessed "
                f"{l['accepted']['drain_values']['unassessed']}"
            )
    print(
        "\n# ruling 7's check of the r_chi form: K(N_final) e^{-4 N_final} / rho_R,E(last) against rho_chi_ratio_today"
    )
    for l in runs:
        c = l["accepted"]["r_chi_check"]
        if c["rho_chi_ratio_today"]:
            rel = c["K_form"] / c["rho_chi_ratio_today"] - 1.0
            print(
                f"{l['id']}: K form {c['K_form']!r}, rho_chi_ratio_today {c['rho_chi_ratio_today']!r}, "
                f"relative difference {rel:.3e}, last raw_N - N_final {c['last_raw_N_minus_N_final']!r}"
            )
            break


OBS = (
    ("Yp", lambda l: l["bbn"]["Yp"]),
    ("DoH", lambda l: l["bbn"]["DoH"]),
    ("dNeff_1MeV", lambda l: l["accepted"]["dissipation"]["Delta_N_eff_1MeV"]),
    ("dNeff_today", lambda l: l["accepted"]["dissipation"]["Delta_N_eff_today"]),
    ("r_chi_today", lambda l: l["accepted"]["dissipation"]["rho_chi_ratio_today"]),
)


def ok(l):
    return l["outcome"] == "success"


def spread():
    lines = all_lines()
    hs = ordered_histories()
    print(
        "# §2.3 edge - zero, for each history and A where edge was run (S1 elsewhere: equal by construction)"
    )
    print(
        "# DoH in units of 10^-5 (as compute_BBN_data returns it); share_gap: K share drained in the gap (edge)"
    )
    print(
        "history        A      "
        + "  ".join(f"{n:>13s}" for n, _ in OBS)
        + "   share_gap"
    )
    largest = {n: (0.0, None) for n, _ in OBS}
    rel_largest = {n: (0.0, None) for n, _ in OBS}
    for h in hs:
        for A in A_VALUES:
            e = line_of(lines, h["history"], A, "edge")
            z = line_of(lines, h["history"], A, "zero")
            if e["obtained"] != "run":
                continue
            if not (ok(e) and ok(z)):
                print(
                    f"{h['history']:13s}  {A:<5g}  edge {e['outcome']}, zero {z['outcome']}"
                )
                continue
            cells = []
            for n, f in OBS:
                d = f(e) - f(z)
                cells.append(f"{d:+13.4e}")
                if abs(d) > abs(largest[n][0]):
                    largest[n] = (d, f"{h['history']} A={A:g}")
                if f(z) != 0.0 and abs(d / f(z)) > abs(rel_largest[n][0]):
                    rel_largest[n] = (d / f(z), f"{h['history']} A={A:g}")
            sg = e["accepted"]["drain_values"]["K_share_gap"]
            print(
                f"{h['history']:13s}  {A:<5g}  "
                + "  ".join(cells)
                + f"   {g(sg, '.4g'):>9s}"
            )
    print(
        "\n# the largest |edge - zero| across the sample, and the largest relative to zero's value"
    )
    for n, _ in OBS:
        print(
            f"{n:12s} {largest[n][0]:+.4e} at {largest[n][1]};  relative {rel_largest[n][0]:+.4e} at {rel_largest[n][1]}"
        )


def fixed():
    lines = all_lines()
    hs = ordered_histories()
    refs = json.loads((OUT / "fixed_law.json").read_text())
    print(
        "# §2.4 the one-loop law (zero and edge) beside the fixed law at each stored alpha (2026.8.0 store, mode=ro)"
    )
    print(
        "# the store's values were measured on the 2026.8.0 tree: a measurement, not a bound. DoH in 10^-5"
    )
    print(
        "#              law/alpha        dNeff_1MeV     dNeff_today    r_chi_today    Yp           DoH        parking_N"
    )
    for h in hs:
        print(f"\n{h['history']}  M={h['M']:g} beta={h['beta']:g}")
        for A in A_VALUES:
            for gap in ("zero", "edge"):
                l = line_of(lines, h["history"], A, gap)
                tag = "" if l["obtained"] == "run" else f" ({l['obtained']})"
                if not ok(l):
                    print(f"   one-loop A={A:<5g} {gap:4s}{tag}: {l['outcome']}")
                    continue
                d = l["accepted"]["dissipation"]
                print(
                    f"   one-loop A={A:<5g} {gap:4s}  {g(d['Delta_N_eff_1MeV'], '.6e'):>13s}  "
                    f"{g(d['Delta_N_eff_today'], '.6e'):>13s}  {g(d['rho_chi_ratio_today'], '.6e'):>13s}  "
                    f"{l['bbn']['Yp']:.8f}  {l['bbn']['DoH']:.6f}  {g(d['parking_N'], '.4f'):>9s}{tag}"
                )
        if h["serial"] is None:
            print("   (no fixed-law reference)")
            continue
        for r in refs[h["history"]]["references"]:
            print(
                f"   fixed a={r['alpha']:<9g}     {g(r['Delta_N_eff_1MeV'], '.6e'):>13s}  "
                f"{g(r['Delta_N_eff_today'], '.6e'):>13s}  {g(r['rho_chi_ratio_today'], '.6e'):>13s}  "
                f"{g(r['Yp'], '.8f')}  {g(r['DoH'], '.6f')}  {g(r['parking_N'], '.4f'):>9s}  (serial {r['serial']})"
            )


def cost():
    lines = all_lines()
    runs = [l for l in lines.values() if l["obtained"] == "run"]
    print("# §2.5 the cost: run lines only (derived lines cost nothing)")
    print(
        "# wall: the task's wall time (s), tuning and BBN; integ: integrations of the tuning; rss: peak RSS (MiB)"
    )
    print(
        "group A      gap   n   wall min/median/max           integ min/max  rss max (MiB)"
    )
    for group in GROUP_ORDER:
        for A in A_VALUES:
            for gap in ("zero", "edge"):
                sel = [
                    l
                    for l in runs
                    if l["group"] == group and l["A"] == A and l["gap"] == gap
                ]
                if not sel:
                    continue
                w = sorted(l["wall"] for l in sel)
                it = [len(l.get("integrations") or []) for l in sel]
                rss = max(l["peak_rss"] for l in sel) / 2**20
                print(
                    f"{group:5s} {A:<6g} {gap:4s} {len(sel):3d}  {w[0]:8.1f} {w[len(w) // 2]:8.1f} {w[-1]:8.1f}"
                    f"     {min(it):3d} {max(it):3d}       {rss:8.0f}"
                )
    print("\n# integrations per tuning, over every run line")
    hist = {}
    for l in runs:
        n = len(l.get("integrations") or [])
        hist[n] = hist.get(n, 0) + 1
    print("   " + ", ".join(f"{n}: {c}" for n, c in sorted(hist.items())))
    print(
        f"   run lines: {len(runs)}; total task wall {sum(l['wall'] for l in runs):.0f} s (CPU-bound, one core each)"
    )
    print("\n# the batches (batch.log: first and last lines)")
    for d in sorted(x for x in (OUT / "batches").iterdir() if x.is_dir()):
        log = (
            (d / "batch.log").read_text().splitlines()
            if (d / "batch.log").exists()
            else []
        )
        first = next((x for x in log if "** batch" in x and "start" in x), None)
        last = next((x for x in log if "** batch" in x and " end " in x), None)
        rss = [
            int(m.group(1))
            for x in log
            for m in [re.search(r"rss=([0-9]+) MiB", x)]
            if m
        ]
        print(
            f"{d.name}: peak RSS of a task {max(rss) if rss else None} MiB\n   {first}\n   {last}"
        )


def u14():
    """
    §2.6's handover for U14, as a table: per history, at each A, the zero task's Delta N_eff today, Y_p, D/H (and the
    edge task's where it differs), the regimes met (G: the gap; C: R <= 1; x: the tuning failed), beside the fixed law
    at the stored alpha of the serial named in §1.1.
    """
    lines = all_lines()
    hs = ordered_histories()
    refs = json.loads((OUT / "fixed_law.json").read_text())
    print(
        "# §2.6, for U14: how each observable moves with A (zero; edge in brackets where it differs), and the fixed"
    )
    print(
        "# law at the stored alpha of the serial named. dN: Delta N_eff today; DoH in 10^-5; flags as in regimes.txt"
    )
    for h in hs:
        ref = None
        if h["serial"] is not None:
            ref = next(
                r
                for r in refs[h["history"]]["references"]
                if r["serial"] == h["serial"]
            )
        head = f"{h['history']:12s} M={h['M']:<7g} beta={h['beta']:<7g}"
        if ref is not None:
            head += (
                f" fixed alpha={ref['alpha']:g}: dN={g(ref['Delta_N_eff_today'], '.4g')} Yp={g(ref['Yp'], '.5f')} "
                f"DoH={g(ref['DoH'], '.4f')}"
            )
        print(head)
        for A in A_VALUES:
            z = line_of(lines, h["history"], A, "zero")
            e = line_of(lines, h["history"], A, "edge")
            m = met_counts(z)
            flag = ("G" if m["gap"] else "-") + ("C" if m["classical"] else "-")
            parts = []
            for l in (z, e):
                if l["outcome"] != "success":
                    parts.append(f"{l['outcome']} ({short_reason(l)})")
                else:
                    parts.append(
                        f"dN={l['accepted']['dissipation']['Delta_N_eff_today']:.4g} Yp={l['bbn']['Yp']:.5f} "
                        f"DoH={l['bbn']['DoH']:.4f}"
                    )
            same = e["obtained"] == "S1"
            print(
                f"   A={A:<5g} {flag}{'x' if z['outcome'] != 'success' else ' '} zero: {parts[0]}"
                + ("" if same else f"   [edge: {parts[1]}]")
            )


def main():
    {
        "sample": sample,
        "regimes": regimes,
        "spread": spread,
        "fixed": fixed,
        "cost": cost,
        "u14": u14,
    }[sys.argv[1]]()


if __name__ == "__main__":
    main()
