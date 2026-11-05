"""
one-loop-drain prompt 02d §2 (the Remeasure gate), from var/one-loop-drain/02d/sample.jsonl (240 task lines, every task
run), its partial files, checks.jsonl, 02's and 02b's sample.jsonl (read as they stand), 02's fixed_law.json and the
batches' logs. One subcommand per item of §2; each prints a text table.

    analyse_02d.py sample       §2 item 1  every task's outcome by history and A; failures and timeouts with reasons
    analyse_02d.py lawcheck     note 02d ruling 6's checks over every integration of 02d (sample and checks): the
                                record's model and alpha_eff recomputed at every candidate; the cap checked on one run
    analyse_02d.py regimes      §2 item 2  per history and A: K shares by regime and parking; drains by regime; l_min;
                                the gap's entry and R <= 1's; the law's edges; U9's and P19's counts
    analyse_02d.py parking      §2 item 3  per history and A: parking N; the first impact; drains before parking; the
                                cap by regime
    analyse_02d.py observables  §2 item 4  Delta N_eff at 1 MeV and today, r_chi today, Y_p, D/H; laboratory M first
    analyse_02d.py oldlaw       §2 item 5  each line beside the old law's `zero` line (02 at A = 1, 0.1; 02b else)
    analyse_02d.py fixed        §2 item 6  beside the fixed law at each stored alpha, at all eight A
    analyse_02d.py cost         §2 item 7  wall, integrations, steps, RHS per tuning by group and A, beside 02b's and
                                02's; the classical tail's steps; the batches
    analyse_02d.py summary      the headline numbers by A that verification §2d cites: per A, the outcomes, how many
                                histories enter the gap and reach R <= 1, park, and the ranges at laboratory M in the
                                transition window (P and W) of Delta N_eff, Y_p, D/H, parking N and the first impact
    analyse_02d.py handover     §2 item 8  for 03 (the bounce rows) and for the user (what cannot be built; the
                                observables against A)

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02d/analyse_02d.py SUB \\
        > .documents/one-loop-drain/measure-02d/out/SUB.txt

Numbers are printed to 4-7 significant figures; sample.jsonl holds every value exactly (json floats are reprs).
Copies (prompt 02d §1.2): read_jsonl, g, short_reason and met_counts from `.documents/one-loop-drain/measure/common.py`
and `measure/analyse.py`, changed as marked; the rest is 02d's own.
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path("/Users/ds283/Documents/Code/ChamPBH")
OUT = ROOT / "var/one-loop-drain/02d"
TASK_DIR = OUT / "tasks"
SAMPLE = OUT / "sample.jsonl"
CHECKS = OUT / "checks.jsonl"
TASKS_JSON = OUT / "tasks.json"
OUT02 = ROOT / "var/one-loop-drain/02"
OUT02B = ROOT / "var/one-loop-drain/02b"

GROUP_ORDER = ("P", "W", "R", "B", "L", "C")
A_VALUES = (10.0, 5.0, 2.0, 1.5, 1.0, 0.5, 0.1, 0.05)
REGIMES = ("one_loop", "gap", "classical", "unassessed")
SHORT = {"one_loop": "1L", "gap": "gap", "classical": "cl", "unassessed": "un"}
LAB_M = 4e-31


# source: measure/common.py, read_jsonl (unchanged)
def read_jsonl(path):
    path = Path(path)
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


# source: measure/common.py, g (unchanged)
def g(x, fmt=".4g"):
    if x is None:
        return "None"
    if isinstance(x, bool):
        return str(x)
    if isinstance(x, int):
        return str(x)
    return format(x, fmt)


def tasks():
    return json.loads(TASKS_JSON.read_text())


def histories():
    out = []
    for t in tasks():
        if t["history"] not in [h["history"] for h in out]:
            out.append(
                {
                    k: t[k]
                    for k in (
                        "history",
                        "group",
                        "M",
                        "beta",
                        "shard",
                        "serial",
                        "stored_alpha",
                    )
                }
            )
    # laboratory M first (Fig 6, README §8.0): P, W, R are at 4e-31; then B, L, C
    return sorted(out, key=lambda h: GROUP_ORDER.index(h["group"]))


def lines():
    return {
        l["id"]: l
        for l in read_jsonl(SAMPLE)
        if "tool_check" not in l and l.get("tag") == ""
    }


def tid(hid, A):
    return f"{hid}-A{A:g}"


def ok(l):
    return l is not None and l["outcome"] == "success"


# source: measure/analyse.py, short_reason; changed: the integrations counted from the line
def short_reason(line):
    r = line.get("reason") or ""
    n = len(line.get("integrations") or [])
    if "step budget" in r:
        m = re.search(r"at N=([0-9.]+)", r)
        return f"step budget (2e6 steps) at N={m.group(1) if m else '?'}"
    if "effective minimum" in r:
        return f"the effective minimum was not found (after {n} integration(s))"
    return r[:120]


# source: measure/analyse.py, met_counts (unchanged)
def met_counts(line):
    """The candidates of each regime over every integration of a line, finished or not (the partial records)."""
    out = {r: 0 for r in REGIMES}
    for i in line.get("integrations") or []:
        for r, n in (i.get("counts") or {}).items():
            out[r] += n
    return out


def accepted_integration(line):
    ints = [i for i in line.get("integrations") or [] if i["outcome"] == "returned"]
    return ints[-1] if ints else None


def last_integration(line):
    ints = line.get("integrations") or []
    return ints[-1] if ints else None


def old_line(hid, A):
    """The old law's `zero` line at (history, A): 02's at A = 1 and 0.1, 02b's at the other six (note 02d ruling 7)."""
    if A in (1.0, 0.1):
        src = OUT02 / "sample.jsonl"
        tag = "02"
    else:
        src = OUT02B / "sample.jsonl"
        tag = "02b"
    want = f"{hid}-A{A:g}-zero"
    for l in read_jsonl(src):
        if l.get("id") == want and "tool_check" not in l:
            return l, tag
    return None, tag


# ---- §2 item 1 ---------------------------------------------------------------------------------------


def sample():
    ls = lines()
    hs = histories()
    print(
        f"# §2 item 1, the sample: {len(ls)} task lines (every task run; no shortcut)"
    )
    counts = {}
    for l in ls.values():
        counts[l["outcome"]] = counts.get(l["outcome"], 0) + 1
    for k, n in sorted(counts.items()):
        print(f"#   outcome {k:8s}: {n}")
    expected = {t["id"] for t in tasks()}
    print(
        f"#   task ids: {len(expected)} expected; missing {sorted(expected - set(ls))}; "
        f"extra {sorted(set(ls) - expected)}; lines in sample.jsonl {len([l for l in read_jsonl(SAMPLE)])}"
    )
    print(
        "# each cell: ok (success), fail (failure), err, TIMEOUT or crash; the tuning's integrations in brackets"
    )
    print(
        "history        M        beta    "
        + "  ".join(f"{f'A{A:g}':>10s}" for A in A_VALUES)
    )
    for h in hs:
        cells = []
        for A in A_VALUES:
            l = ls.get(tid(h["history"], A))
            if l is None:
                cells.append(f"{'-':>10s}")
                continue
            o = {
                "success": "ok",
                "failure": "fail",
                "error": "err",
                "timeout": "TIMEOUT",
                "crashed": "crash",
            }[l["outcome"]]
            n = len(l.get("integrations") or [])
            cells.append(f"{o + f'({n})':>10s}")
        print(f"{h['history']:13s}  {h['M']:<7g}  {h['beta']:<6g}  " + "  ".join(cells))
    print("\n# failures, errors and timeouts, with their reasons")
    n = 0
    for h in hs:
        for A in A_VALUES:
            l = ls.get(tid(h["history"], A))
            if l is None or l["outcome"] == "success":
                continue
            n += 1
            ints = l.get("integrations") or []
            last = ints[-1] if ints else {}
            cnt = met_counts(l)
            print(
                f"{l['id']}: {l['outcome']} stage={l.get('stage')} after {len(ints)} integration(s), wall "
                f"{l['wall']:.0f} s; candidates over all integrations "
                + " ".join(f"{SHORT[r]}={cnt[r]}" for r in REGIMES)
                + f"; last integration {last.get('outcome')} {last.get('error_type', '')}"
            )
            print(f"    reason: {l.get('reason')}")
    if n == 0:
        print("none")


# ---- note 02d ruling 6's checks ------------------------------------------------------------------------


def lawcheck():
    import importlib

    sys.path.insert(0, str(ROOT))
    SM = importlib.import_module("ComputeTargets.ScalarHistory")
    all_l = [
        l for l in read_jsonl(SAMPLE) + read_jsonl(CHECKS) if "tool_check" not in l
    ]
    n_int = n_rec = n_model_ok = n_checked = n_diff = 0
    models = {}
    for l in all_l:
        for i in l.get("integrations") or []:
            n_int += 1
            lc = i.get("law_check")
            if lc is None:
                continue
            n_rec += 1
            models[lc["model"]] = models.get(lc["model"], 0) + 1
            n_model_ok += 1 if lc["model_ok"] else 0
            n_checked += lc["alpha_checked"]
            n_diff += lc["alpha_differs"]
    print(
        "# note 02d ruling 6, 'Checked', over every integration of 02d's lines (sample.jsonl and checks.jsonl):"
    )
    print(
        f"#   {len(all_l)} lines, {n_int} integrations, {n_rec} with a DrainRecord; models {models}; "
        f"model = {SM.DRAIN_MODEL_ONE_LOOP!r} on {n_model_ok} of {n_rec}"
    )
    print(
        f"#   alpha_eff recomputed by drain_alpha_effective(c.regime, c.ell, c.L, c.log_R, record.A, c.w_gate) at "
        f"{n_checked} candidates; differing by repr: {n_diff}"
    )
    # the same, independently, from the partial files' candidate lists (the line's counts are the child's)
    ls = lines()
    p_int = p_c = p_diff = 0
    for l in ls.values():
        for rec in read_jsonl(TASK_DIR / f"{l['id']}.partial.jsonl"):
            if rec.get("candidates") is None:
                continue
            p_int += 1
            A = rec["drain"]["A"]
            for c in rec["candidates"]:
                regime, ell, a_eff, w, _lE, _lH, L, lR = c
                p_c += 1
                if repr(SM.drain_alpha_effective(regime, ell, L, lR, A, w)) != repr(
                    a_eff
                ):
                    p_diff += 1
    print(
        f"#   recomputed again here from the {len(ls)} sample tasks' partial files: {p_int} integrations, {p_c} "
        f"candidates, {p_diff} differing"
    )
    # the cap on one run: every capped candidate's alpha_eff is DRAIN_ALPHA_CAP * w_gate exactly
    print(
        "\n# the cap, by regime, over the accepted integrations (A f >= DRAIN_ALPHA_CAP)"
    )
    tot = {r: 0 for r in REGIMES}
    runs = {r: 0 for r in REGIMES}
    for l in ls.values():
        if not ok(l):
            continue
        for r in REGIMES:
            tot[r] += l["accepted"]["cap"][r]
            runs[r] += 1 if l["accepted"]["cap"][r] else 0
    for r in REGIMES:
        print(f"   {r:10s}: {tot[r]} capped candidates, in {runs[r]} runs")
    pick = None
    for A in A_VALUES:
        for h in histories():
            l = ls.get(tid(h["history"], A))
            if ok(l) and sum(l["accepted"]["cap"].values()) > 0:
                pick = l
                break
        if pick:
            break
    if pick is not None:
        rec = [
            r
            for r in read_jsonl(TASK_DIR / f"{pick['id']}.partial.jsonl")
            if r["outcome"] == "returned"
        ][-1]
        A = rec["drain"]["A"]
        n_cap = n_eq = 0
        for c in rec["candidates"]:
            regime, ell, a_eff, w, _lE, _lH, L, lR = c
            if regime == "unassessed":
                continue
            if A * SM.drain_fraction(regime, ell, L, lR) >= SM.DRAIN_ALPHA_CAP:
                n_cap += 1
                n_eq += 1 if repr(a_eff) == repr(SM.DRAIN_ALPHA_CAP * w) else 0
        print(
            f"\n# one capped run, {pick['id']}: {n_cap} capped candidates in its accepted integration (the line says "
            f"{sum(pick['accepted']['cap'].values())}); alpha_eff == DRAIN_ALPHA_CAP * w_gate exactly at {n_eq}"
        )


# ---- §2 item 2 ---------------------------------------------------------------------------------------


def regimes():
    ls = lines()
    print(
        "# §2 item 2, the regime shares across the eight A, from each accepted integration (the payload's record)"
    )
    print(
        "# K: shares of K(N_final) drained by regime and by the parking leftover (drain_values'); n: candidates/drains"
    )
    print(
        "# by regime; l_min; gap: N and ln R at the first gap candidate; R<=1: N and ln R at the first classical one;"
    )
    print(
        "# edges: clamp (gap, 1 + q ln L <= 0), thr (gap, ln R <= r0), cont (classical, L > 134), Sheld (classical,"
    )
    print(
        "# L < 0.3), uflow (classical, alpha_eff = 0 with w_gate > 0); U9: drains with w_gate < 1; P19: unassessed"
    )
    tot_edges = {}
    for h in histories():
        print(f"\n{h['history']}  M={h['M']:g} beta={h['beta']:g}")
        for A in A_VALUES:
            l = ls.get(tid(h["history"], A))
            if not ok(l):
                m = met_counts(l) if l else {}
                last = last_integration(l) if l else None
                e = (last or {}).get("edges")
                print(
                    f"   A={A:<5g} {l['outcome'] if l else 'missing'}: over all integrations "
                    + " ".join(f"{SHORT[r]}={m.get(r, 0)}" for r in REGIMES)
                    + (f"; last integration's edges {e}" if e else "")
                )
                continue
            acc = l["accepted"]
            dv, br, re_ = acc["drain_values"], acc["by_regime"], acc["reach"]
            K = (
                " ".join(
                    f"{SHORT[r]}={g(dv[f'K_share_{r}'], '.3g')}" for r in REGIMES[:3]
                )
                + f" park={g(dv['K_share_parking'], '.3g')}"
            )
            n = " ".join(
                f"{SHORT[r]}={br[r]['count']}/{br[r]['drains']}" for r in REGIMES[:3]
            )
            gap = re_["gap"]
            cl = re_["classical"]
            gs = (
                "-" if gap is None else f"N={gap['N']:.3f} lnR={g(gap['log_R'], '.4g')}"
            )
            cs = "-" if cl is None else f"N={cl['N']:.3f} lnR={g(cl['log_R'], '.4g')}"
            e = acc["edges"]
            for k, v in e.items():
                tot_edges[k] = tot_edges.get(k, 0) + v
            es = " ".join(
                f"{k}={v}"
                for k, v in (
                    ("clamp", e["gap_clamp"]),
                    ("thr", e["gap_below_threshold"]),
                    ("cont", e["classical_continuation"]),
                    ("Sheld", e["classical_S_held"]),
                    ("uflow", e["classical_underflow"]),
                )
            )
            u9 = sum(br[r]["w_gate_below_1_at_drain"] for r in REGIMES)
            print(
                f"   A={A:<5g} K: {K}  n: {n}  l_min={g(dv['ell_min'], '.4g')}  gap: {gs}  R<=1: {cs}  "
                f"edges: {es}  U9={u9} P19={br['unassessed']['count']}"
            )
    print(
        "\n# the law's edges summed over the accepted integrations of every successful task"
    )
    for k, v in tot_edges.items():
        print(f"   {k}: {v}")
    # the ranges of L and ln R by regime over every accepted integration
    print("\n# the ranges of L and ln R by regime, over every accepted integration")
    for r in REGIMES[:3]:
        Ls, lRs = [], []
        for l in ls.values():
            if ok(l):
                b = l["accepted"]["by_regime"][r]
                if b["L_min"] is not None:
                    Ls += [b["L_min"], b["L_max"]]
                    lRs += [b["log_R_min"], b["log_R_max"]]
        if Ls:
            print(
                f"   {r:10s}: L {min(Ls):.4g} to {max(Ls):.4g}; ln R {min(lRs):.4g} to {max(lRs):.4g}"
            )
        else:
            print(f"   {r:10s}: none")
    # the edges over every integration (failed tunings included)
    print(
        "\n# the law's edges over every integration of every task (failed and timed-out tunings included)"
    )
    allE = {}
    for l in ls.values():
        for i in l.get("integrations") or []:
            for k, v in (i.get("edges") or {}).items():
                allE[k] = allE.get(k, 0) + v
    for k, v in allE.items():
        print(f"   {k}: {v}")


# ---- §2 item 3 ---------------------------------------------------------------------------------------


def parking():
    ls = lines()
    print(
        "# §2 item 3, parking and the first impact, from each accepted integration (first_impact)"
    )
    print(
        "# park: the parking N (None: does not park); first: regime, N, alpha_eff (the fraction of the first impact's"
    )
    print(
        "# kinetic energy drained), its share of K(N_final); before: drains before parking by regime (1L/gap/cl);"
    )
    print("# cap: capped candidates by regime (A f >= 0.99)")
    for h in histories():
        print(f"\n{h['history']}  M={h['M']:g} beta={h['beta']:g}")
        for A in A_VALUES:
            l = ls.get(tid(h["history"], A))
            if not ok(l):
                print(f"   A={A:<5g} {l['outcome'] if l else 'missing'}")
                continue
            fi = l["accepted"]["first_impact"]
            f0 = fi["first"]
            fs = (
                "none"
                if f0 is None
                else f"{SHORT[f0['regime']]} N={f0['N']:.3f} a_eff={f0['alpha_eff']:.4g} "
                f"share={g(f0['share_of_K'], '.3g')}"
            )
            b = fi["drains_before_parking_by_regime"]
            cap = fi["cap"]
            print(
                f"   A={A:<5g} park={g(fi['parking_N'], '.4f'):>8s}  first: {fs}  drains={fi['drains']} "
                f"before={fi['drains_before_parking']} ({b['one_loop']}/{b['gap']}/{b['classical']}) "
                f"after={fi['drains_after_parking']}  park_share={g(fi['parking_share_of_K'], '.3g')}  "
                f"cap: {cap['one_loop']}/{cap['gap']}/{cap['classical']}"
            )


# ---- §2 item 4 ---------------------------------------------------------------------------------------


def observables():
    ls = lines()
    print(
        "# §2 item 4, the observables per history and A, laboratory M (4e-31 M_P: P, W, R) first (Fig 6, README §8.0)"
    )
    print(
        "# Y_p and D/H are set beside bbn_constraints_explainer.tex's criteria as context only, never as a pass or fail."
    )
    print("# DoH in 10^-5")
    print("#          dNeff_1MeV     dNeff_today    r_chi_today    Yp           DoH")
    for h in histories():
        print(f"\n{h['history']}  M={h['M']:g} beta={h['beta']:g}")
        for A in A_VALUES:
            l = ls.get(tid(h["history"], A))
            if not ok(l):
                print(
                    f"   A={A:<5g} {l['outcome'] if l else 'missing'}: {short_reason(l) if l else ''}"
                )
                continue
            d = l["accepted"]["dissipation"]
            print(
                f"   A={A:<5g} {g(d['Delta_N_eff_1MeV'], '.6e'):>13s}  {g(d['Delta_N_eff_today'], '.6e'):>13s}  "
                f"{g(d['rho_chi_ratio_today'], '.6e'):>13s}  {l['bbn']['Yp']:.8f}  {l['bbn']['DoH']:.6f}"
            )


# ---- §2 item 5 ---------------------------------------------------------------------------------------


def oldlaw():
    ls = lines()
    print(
        "# §2 item 5, against the old law: each 02d line (new) beside the `zero` line of the same history and A (old):"
    )
    print(
        "# 02's at A = 1 and 0.1, 02b's at the other six; a 02b line obtained by S2 carries its source's values and"
    )
    print(
        "# is marked 'S2 of' its values_from (note 02d ruling 7). counts: candidates 1L/gap/cl of the accepted"
    )
    print(
        "# integration; dN1: Delta N_eff at 1 MeV; dN0: today; DoH in 10^-5. Nothing of the old lines is re-run."
    )
    rows = []
    for h in histories():
        print(f"\n{h['history']}  M={h['M']:g} beta={h['beta']:g}")
        for A in A_VALUES:
            l = ls.get(tid(h["history"], A))
            o, src = old_line(h["history"], A)
            for which, x in (("new", l), ("old", o)):
                tag = ""
                if which == "old":
                    tag = f" [{src}]"
                    if x is not None and x.get("obtained") == "S2":
                        tag = f" [{src} S2 of {x['values_from']}]"
                if x is None:
                    print(f"   A={A:<5g} {which}: missing{tag}")
                    continue
                if x["outcome"] != "success":
                    print(
                        f"   A={A:<5g} {which}: {x['outcome']} ({short_reason(x)}){tag}"
                    )
                    continue
                acc = x["accepted"]
                dv, d = acc["drain_values"], acc["dissipation"]
                print(
                    f"   A={A:<5g} {which}: park={g(d['parking_N'], '.4f'):>8s} counts "
                    f"{dv['one_loop']}/{dv['gap']}/{dv['classical']}  dN1={g(d['Delta_N_eff_1MeV'], '.5g'):>10s} "
                    f"dN0={g(d['Delta_N_eff_today'], '.5g'):>10s} Yp={x['bbn']['Yp']:.6f} DoH={x['bbn']['DoH']:.5f}{tag}"
                )
            rows.append((h, A, l, o))
    # where the two laws differ most, and where they agree
    print("\n# the differences new - old, over the (history, A) where both succeed")
    both = [(h, A, l, o) for h, A, l, o in rows if ok(l) and ok(o)]
    print(
        f"#   both succeed: {len(both)}; new succeeds, old fails: "
        f"{sum(1 for h, A, l, o in rows if ok(l) and not ok(o))}; new fails, old succeeds: "
        f"{sum(1 for h, A, l, o in rows if not ok(l) and ok(o))}; both fail: "
        f"{sum(1 for h, A, l, o in rows if not ok(l) and not ok(o))}"
    )
    for h, A, l, o in rows:
        if ok(l) != ok(o):
            print(
                f"#   outcome differs: {h['history']} A={A:g}: new {l['outcome'] if l else None}, old "
                f"{o['outcome'] if o else None}"
            )

    def key(name, get):
        out = []
        for h, A, l, o in both:
            a, b = get(l), get(o)
            if a is None or b is None:
                continue
            out.append((b - a if False else a - b, h["history"], A, a, b))
        return out

    for name, get in (
        (
            "Delta N_eff today",
            lambda x: x["accepted"]["dissipation"]["Delta_N_eff_today"],
        ),
        (
            "Delta N_eff at 1 MeV",
            lambda x: x["accepted"]["dissipation"]["Delta_N_eff_1MeV"],
        ),
        ("Y_p", lambda x: x["bbn"]["Yp"]),
        ("D/H (1e-5)", lambda x: x["bbn"]["DoH"]),
        ("parking N", lambda x: x["accepted"]["dissipation"]["parking_N"]),
    ):
        d = key(name, get)
        if not d:
            continue
        d.sort(key=lambda t: abs(t[0]))
        print(f"\n# {name}: new - old over {len(d)} pairs")
        print(
            "#   largest |difference|: "
            + "; ".join(
                f"{hid} A={A:g}: {nv:.5g} vs {ov:.5g} ({dd:+.4g})"
                for dd, hid, A, nv, ov in d[::-1][:6]
            )
        )
        eq = [t for t in d if t[0] == 0.0]
        print(
            f"#   equal exactly: {len(eq)} pairs"
            + (": " + ", ".join(f"{t[1]} A={t[2]:g}" for t in eq) if eq else "")
        )
        small = [t for t in d if t[4] != 0 and abs(t[0] / t[4]) < 1e-3]
        print(f"#   within 1e-3 relative: {len(small)} pairs")
        # by group
        for grp in GROUP_ORDER:
            gd = [t for t in d if t[1].startswith(grp + "-")]
            if gd:
                print(
                    f"#   {grp}: {len(gd)} pairs, |difference| {min(abs(t[0]) for t in gd):.3g} to "
                    f"{max(abs(t[0]) for t in gd):.3g}"
                )
    # parking: parks / does not, new against old
    print("\n# parking, new against old, over the pairs where both succeed")
    pp = {}
    for h, A, l, o in both:
        k = (
            l["accepted"]["dissipation"]["parking_N"] is not None,
            o["accepted"]["dissipation"]["parking_N"] is not None,
        )
        pp[k] = pp.get(k, 0) + 1
    for (a, b), n in sorted(pp.items()):
        print(
            f"#   new {'parks' if a else 'does not park'}, old {'parks' if b else 'does not park'}: {n}"
        )


# ---- §2 item 6 ---------------------------------------------------------------------------------------


def fixed():
    ls = lines()
    refs = json.loads((OUT02 / "fixed_law.json").read_text())
    print(
        "# §2 item 6, the measured law at each A beside the fixed law at each stored alpha (02's fixed_law.json, read"
    )
    print(
        "# as it stands: the 2026.8.0 store, mode=ro). The store's values were measured on the 2026.8.0 tree: a"
    )
    print("# measurement, not a bound. DoH in 10^-5")
    print(
        "#              law/alpha        dNeff_1MeV     dNeff_today    r_chi_today    Yp           DoH        parking_N"
    )
    for h in histories():
        print(f"\n{h['history']}  M={h['M']:g} beta={h['beta']:g}")
        for A in A_VALUES:
            l = ls.get(tid(h["history"], A))
            if not ok(l):
                print(f"   measured A={A:<5g}  {l['outcome'] if l else 'missing'}")
                continue
            d = l["accepted"]["dissipation"]
            print(
                f"   measured A={A:<5g}      {g(d['Delta_N_eff_1MeV'], '.6e'):>13s}  "
                f"{g(d['Delta_N_eff_today'], '.6e'):>13s}  {g(d['rho_chi_ratio_today'], '.6e'):>13s}  "
                f"{l['bbn']['Yp']:.8f}  {l['bbn']['DoH']:.6f}  {g(d['parking_N'], '.4f'):>9s}"
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


# ---- §2 item 7 ---------------------------------------------------------------------------------------


def cost():
    ls = lines()
    print(
        "# §2 item 7, the cost per tuning, by group and A, beside the old law's `zero` line (02 at A = 1, 0.1; 02b else)"
    )
    print(
        "# wall: the task's wall (s, tuning and BBN, under load); integ: integrations; steps and RHS: the accepted"
    )
    print(
        "# integration's accepted steps and RHS evaluations (a failure: its last integration's, if it returned);"
    )
    print(
        "# drains: alpha_eff > 0 in the accepted integration; cl_tail: accepted steps after the first classical"
    )
    print(
        "# candidate, and its share of the integration's steps. Old lines by S2 have no wall: '—' (note 02d ruling 7)"
    )
    print(
        "history      A      new: outcome  wall    integ  steps     RHS       drains  cl_drains cl_tail (share)   | old: wall    integ  steps     RHS"
    )
    for grp in GROUP_ORDER:
        for h in [x for x in histories() if x["group"] == grp]:
            for A in A_VALUES:
                l = ls.get(tid(h["history"], A))
                o, src = old_line(h["history"], A)
                ints = l.get("integrations") or []
                acc_i = accepted_integration(l)
                if ok(l):
                    steps = l["accepted"]["accepted_steps"]
                    rhs = l["accepted"]["RHS_evaluations"]
                    drains = l["accepted"]["first_impact"]["drains"]
                    cld = l["accepted"]["by_regime"]["classical"]["drains"]
                else:
                    steps = acc_i["accepted_steps"] if acc_i else None
                    rhs = acc_i["nfev"] if acc_i else None
                    drains = cld = None
                tail = acc_i.get("steps_after_first_classical") if acc_i else None
                share = (
                    None
                    if tail is None or not acc_i or not acc_i.get("accepted_steps")
                    else tail / acc_i["accepted_steps"]
                )
                if o is None:
                    os_ = "missing"
                elif o.get("obtained") == "S2":
                    oa = o["accepted"]
                    os_ = (
                        f"—  (S2 of {o['values_from']})  {len(o.get('integrations') or []):3d}  "
                        f"{g(oa['accepted_steps']):>8s}  {g(oa['RHS_evaluations']):>8s}"
                    )
                else:
                    oa = o.get("accepted") or {}
                    os_ = (
                        f"{o['wall']:7.1f}  {len(o.get('integrations') or []):3d}  "
                        f"{g(oa.get('accepted_steps')):>8s}  {g(oa.get('RHS_evaluations')):>8s}"
                        + ("" if o["outcome"] == "success" else f"  ({o['outcome']})")
                    )
                print(
                    f"{h['history']:12s} {A:<6g} {l['outcome']:8s} {l['wall']:7.1f}  {len(ints):3d}  "
                    f"{g(steps):>8s}  {g(rhs):>8s}  {g(drains):>6s}  {g(cld):>6s}    {g(tail):>8s} "
                    f"({g(share, '.3f')})   | {src}: {os_}"
                )
    # summaries by group and A
    print(
        "\n# by group and A: the median and largest wall (s), new and old (run lines only)"
    )
    for grp in GROUP_ORDER:
        for A in A_VALUES:
            sel = [ls[tid(h["history"], A)] for h in histories() if h["group"] == grp]
            w = sorted(x["wall"] for x in sel)
            olds = []
            for h in histories():
                if h["group"] != grp:
                    continue
                o, _ = old_line(h["history"], A)
                if o is not None and o.get("obtained") == "run":
                    olds.append(o["wall"])
            olds.sort()
            print(
                f"   {grp} A={A:<5g} new: n={len(w)} median {w[len(w) // 2]:7.1f} max {w[-1]:7.1f}   old run: "
                + (
                    f"n={len(olds)} median {olds[len(olds) // 2]:7.1f} max {olds[-1]:7.1f}"
                    if olds
                    else "none"
                )
            )
    runs = list(ls.values())
    print(
        f"\n# 02d: {len(runs)} tasks, summed task wall {sum(l['wall'] for l in runs):.0f} s = "
        f"{sum(l['wall'] for l in runs) / 3600:.2f} h (CPU-bound, one core each)"
    )
    hist = {}
    for l in runs:
        n = len(l.get("integrations") or [])
        hist[n] = hist.get(n, 0) + 1
    print(
        "#   integrations per tuning: "
        + ", ".join(f"{n}: {c}" for n, c in sorted(hist.items()))
    )
    print(
        f"#   peak RSS of a task: {max(l['peak_rss'] for l in runs) / 2**20:.0f} MiB "
        f"({max(runs, key=lambda l: l['peak_rss'])['id']})"
    )
    print("\n# the batches (batch.log: first and last lines; the largest task RSS)")
    for d in sorted(
        (x for x in (OUT / "batches").iterdir() if x.is_dir()),
        key=lambda x: (
            (x / "batch.pid").stat().st_mtime if (x / "batch.pid").exists() else 0
        ),
    ):
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


# ---- §2 item 8 ---------------------------------------------------------------------------------------


def handover():
    ls = lines()
    print(
        "# §2 item 8, the handover. Measurements and the options they bear on; no choice is made here."
    )
    print(
        "\n## for 03: the bounce rows a store must hold (one per candidate of the accepted integration)"
    )
    print(
        "history      "
        + "  ".join(f"{f'A{A:g}':>11s}" for A in A_VALUES)
        + "   (candidates/drains)"
    )
    allc, alld = [], []
    for h in histories():
        cells = []
        for A in A_VALUES:
            l = ls.get(tid(h["history"], A))
            if not ok(l):
                cells.append(f"{'x':>11s}")
                continue
            c = l["accepted"]["drain_values"]["candidates"]
            d = l["accepted"]["first_impact"]["drains"]
            allc.append((c, l["id"]))
            alld.append((d, l["id"]))
            cells.append(f"{f'{c}/{d}':>11s}")
        print(f"{h['history']:12s} " + "  ".join(cells))
    allc.sort()
    alld.sort()
    print(
        f"\n# candidates per accepted integration: {allc[0][0]} ({allc[0][1]}) to {allc[-1][0]} ({allc[-1][1]}); "
        f"summed over the {len(allc)} successful tasks {sum(c for c, _ in allc)}"
    )
    print(
        f"# drains per accepted integration: {alld[0][0]} ({alld[0][1]}) to {alld[-1][0]} ({alld[-1][1]}); "
        f"summed {sum(d for d, _ in alld)}"
    )
    for r in REGIMES:
        Ls, lRs, n = [], [], 0
        for l in ls.values():
            if ok(l):
                b = l["accepted"]["by_regime"][r]
                n += b["count"]
                if b["L_min"] is not None:
                    Ls += [b["L_min"], b["L_max"]]
                    lRs += [b["log_R_min"], b["log_R_max"]]
        print(
            f"# {r:10s}: {n} candidates; "
            + (
                f"L {min(Ls):.6g} to {max(Ls):.6g}; ln R {min(lRs):.6g} to {max(lRs):.6g}"
                if Ls
                else "no L or ln R"
            )
        )
    ells = [
        l["accepted"]["drain_values"]["ell_min"]
        for l in ls.values()
        if ok(l) and l["accepted"]["drain_values"]["ell_min"] is not None
    ]
    ellx = [
        l["accepted"]["ell_max"]
        for l in ls.values()
        if ok(l) and l["accepted"]["ell_max"] is not None
    ]
    print(f"# ell over one-loop candidates: {min(ells):.6g} to {max(ellx):.6g}")
    am = [
        l["accepted"]["alpha_eff_min"]
        for l in ls.values()
        if ok(l) and l["accepted"]["alpha_eff_min"] is not None
    ]
    print(f"# least alpha_eff > 0 at a drain: {min(am):.6g}")
    ua = sum(
        l["accepted"]["drain_values"]["unresolvable_assessments"]
        for l in ls.values()
        if ok(l)
    )
    ud = sum(
        l["accepted"]["drain_values"]["unresolved_declined"]
        for l in ls.values()
        if ok(l)
    )
    print(
        f"# unresolvable assessments over the accepted integrations: {ua}; unresolved reflections declined: {ud}"
    )
    print("\n## for the user: the (history, A) that cannot be built on this code")
    n = 0
    for h in histories():
        for A in A_VALUES:
            l = ls.get(tid(h["history"], A))
            if l is None:
                n += 1
                print(f"   {tid(h['history'], A)}: missing")
            elif not ok(l):
                n += 1
                print(f"   {l['id']}: {l['outcome']}: {short_reason(l)}")
    if n == 0:
        print("   none")
    print(
        "\n## for the user: how the observables move with A (Delta N_eff today; at 1 MeV; Y_p; D/H in 1e-5), "
        "laboratory M first"
    )
    for h in histories():
        print(f"{h['history']:12s} M={h['M']:<7g} beta={h['beta']:<7g}")
        for label, get, fmt in (
            (
                "dN today",
                lambda x: x["accepted"]["dissipation"]["Delta_N_eff_today"],
                ".4g",
            ),
            (
                "dN 1MeV",
                lambda x: x["accepted"]["dissipation"]["Delta_N_eff_1MeV"],
                ".4g",
            ),
            ("Yp", lambda x: x["bbn"]["Yp"], ".5f"),
            ("DoH", lambda x: x["bbn"]["DoH"], ".4f"),
        ):
            cells = []
            for A in A_VALUES:
                l = ls.get(tid(h["history"], A))
                cells.append(f"{(format(get(l), fmt) if ok(l) else 'x'):>9s}")
            print(f"   {label:8s} " + " ".join(cells))
    print("   (columns: A = " + ", ".join(f"{A:g}" for A in A_VALUES) + ")")


def summary():
    ls = lines()
    hs = histories()
    print(
        "# the headline numbers by A (verification §2d cites them); every value is from sample.jsonl"
    )
    for A in A_VALUES:
        sel = [ls[tid(h["history"], A)] for h in hs]
        okl = [l for l in sel if ok(l)]
        out = {}
        for l in sel:
            out[l["outcome"]] = out.get(l["outcome"], 0) + 1
        gap = [l["history"] for l in okl if l["accepted"]["reach"]["enters_gap"]]
        cl = [l["history"] for l in okl if l["accepted"]["reach"]["reaches_classical"]]
        npark = [
            l["history"]
            for l in okl
            if l["accepted"]["dissipation"]["parking_N"] is None
        ]
        print(f"\nA={A:g}: outcomes {out}")
        print(f"   enter the gap ({len(gap)}): {' '.join(gap)}")
        print(f"   reach R <= 1 ({len(cl)}): {' '.join(cl)}")
        print(f"   do not park ({len(npark)}): {' '.join(npark)}")
        pw = [l for l in okl if l["group"] in ("P", "W")]
        for name, get in (
            (
                "Delta N_eff today",
                lambda x: x["accepted"]["dissipation"]["Delta_N_eff_today"],
            ),
            (
                "Delta N_eff 1 MeV",
                lambda x: x["accepted"]["dissipation"]["Delta_N_eff_1MeV"],
            ),
            (
                "r_chi today",
                lambda x: x["accepted"]["dissipation"]["rho_chi_ratio_today"],
            ),
            ("Y_p", lambda x: x["bbn"]["Yp"]),
            ("D/H (1e-5)", lambda x: x["bbn"]["DoH"]),
            ("parking N", lambda x: x["accepted"]["dissipation"]["parking_N"]),
            (
                "first-impact alpha_eff",
                lambda x: x["accepted"]["first_impact"]["first"]["alpha_eff"],
            ),
            ("drains", lambda x: x["accepted"]["first_impact"]["drains"]),
            ("l_min", lambda x: x["accepted"]["drain_values"]["ell_min"]),
        ):
            v = [get(l) for l in pw if get(l) is not None]
            if v:
                print(f"   P and W ({len(pw)}), {name}: {min(v):.6g} to {max(v):.6g}")
        p = ls[tid("P-2197", A)]
        if ok(p):
            d = p["accepted"]["dissipation"]
            print(
                f"   P-2197: dNeff today {d['Delta_N_eff_today']:.6g}, 1 MeV {d['Delta_N_eff_1MeV']:.6g}, "
                f"r_chi {d['rho_chi_ratio_today']:.6g}, Yp {p['bbn']['Yp']:.6f}, DoH {p['bbn']['DoH']:.5f}, "
                f"parking N {g(d['parking_N'], '.4f')}"
            )


def main():
    {
        "sample": sample,
        "lawcheck": lawcheck,
        "regimes": regimes,
        "parking": parking,
        "observables": observables,
        "oldlaw": oldlaw,
        "fixed": fixed,
        "cost": cost,
        "handover": handover,
        "summary": summary,
    }[sys.argv[1]]()


if __name__ == "__main__":
    main()
