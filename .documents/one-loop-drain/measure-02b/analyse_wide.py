"""
one-loop-drain prompt 02b §2 (the Wide gate), over 02's lines at A = 1, 0.1, 0.01 (var/one-loop-drain/02/sample.jsonl)
and 02b's at A = 10, 5, 2, 1.5, 0.5, 0.05 (var/one-loop-drain/02b/sample.jsonl), 540 task lines in all, with the
partial files of both and 02's fixed-law references (var/one-loop-drain/02/fixed_law.json, read as it stands).

02's `measure/analyse.py` is imported and its subcommands are run unchanged, with these overrides of its module globals
before each call (orchestrator note 02b, ruling 3; each is read there at call time):
  all_lines -> the merged 540 lines (sample, regimes, fixed, u14) or 02b's 360 (spread, cost), since 02's all_lines
               takes 02's sample as a default argument bound at import;
  A_VALUES  -> the nine A (sample, regimes, fixed, u14) or 02b's six (spread, cost);
  OUT       -> 02's directory (fixed, u14: fixed_law.json) or 02b's (cost: its batches).
The subcommands of 02b's own:

    analyse_wide.py sample    §2.1  02's table over the nine A, S1 and S2 marked; failures with their reasons
    analyse_wide.py regimes   §2.2  02's tables over the nine A; then U9's and P19's counts at 02b's A
    analyse_wide.py first     §2.3  the first impact and the cap, per history and A (ruling 6); A = 5 and 10 beside
                                    A = 1 and 2; ruling 6's check of the two cap counts on 02b's runs
    analyse_wide.py spread    §2.4  02's spread at 02b's A
    analyse_wide.py fixed     §2.5  02's comparison with the fixed law at all nine A
    analyse_wide.py cost      §2.6  02's cost table at 02b's A, with 02b's batches
    analyse_wide.py u14       §2.7  02's per-history table at the nine A; then each observable as a history x A matrix

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/measure-02b/analyse_wide.py SUB \\
        > .documents/one-loop-drain/measure-02b/out/SUB.txt

The first impact (ruling 6), from the accepted integration, that is the last `returned` line of the partial file of
the run whose values the task carries (its own; for a derived task, its values_from run, 02's or 02b's):
  - the first candidate: the first entry of `regimes` and `injections`; its regime and N;
  - the fraction drained at the first impact: its alpha_eff, 0 where it drained nothing;
  - its share: its delta_K / K(N_final) (the line's accepted.K_final), None where K(N_final) = 0;
  - the drains before parking: injections with alpha_eff > 0 and N <= parking_N (all of them where the field does not
    park). The parking leftover is not among the injections (run_wide.first_impact): its share is counted apart, from
    the line's drain_values.K_share_parking;
  - the cap at a one-loop drain: by ell (A c_1/ell >= alpha_cap) where the partial file keeps ell (02b's runs), and by
    alpha_eff == alpha_cap exactly otherwise (02's partials; this relies on 02's count of no one-loop candidate with
    w_gate < 1, 0 of 7203, verification §2.5). Each row says which way it was counted.
"""

import importlib
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import wide  # noqa: E402
from wide import A_02B, A_ALL, g, read_jsonl, task_id  # noqa: E402

import analyse as an02  # noqa: E402  (02's analyse.py, by import)

sys.path.insert(0, str(wide.ROOT))
SM = importlib.import_module("ComputeTargets.ScalarHistory")
CAP = SM.DRAIN_ALPHA_CAP
C1 = SM.DRAIN_C1

LAB_GROUPS = ("P", "W", "R")


def use(lines, A_values, out):
    an02.all_lines = lambda: lines
    an02.A_VALUES = A_values
    an02.OUT = out


def merged():
    return wide.all_lines_merged()


def ordered_histories():
    return an02.ordered_histories()


# ---- the first impact ----------------------------------------------------------------------------------------------


def partial_path(line):
    """The partial file of the run whose values a line carries, and which sample's it is."""
    if line["obtained"] == "run":
        src, sample = line["id"], ("02b" if line["A"] in A_02B else "02")
    else:
        src = line["values_from"]
        sample = line.get("values_from_sample", "02")
    d = wide.TASK_DIR if sample == "02b" else wide.TASK_DIR_02
    return d / f"{src}.partial.jsonl", sample, src


def accepted_partial(line):
    path, sample, src = partial_path(line)
    recs = [r for r in read_jsonl(path) if r.get("outcome") == "returned"]
    return (recs[-1] if recs else None), sample, src


def first_impact(line):
    """The first impact and the cap from the partial file (ruling 6), or None where the tuning failed."""
    if line["outcome"] != "success":
        return None
    rec, sample, src = accepted_partial(line)
    acc = line["accepted"]
    K_final = acc["K_final"]
    regimes, inj = rec["regimes"], rec["injections"]
    assert len(regimes) == len(inj)
    pN = rec["parking_N"]
    drained = [j for j, x in enumerate(inj) if x[1] > 0.0]
    before = [j for j in drained if pN is None or inj[j][0] <= pN]
    out = {
        "source": f"{sample}:{src}",
        "candidates": len(regimes),
        "first_regime": regimes[0] if regimes else None,
        "first_N": inj[0][0] if inj else None,
        "first_alpha_eff": inj[0][1] if inj else None,
        "first_delta_K": inj[0][2] if inj else None,
        "first_share": (
            (inj[0][2] / K_final if K_final > 0.0 else None) if inj else None
        ),
        "drains": len(drained),
        "drains_before_parking": len(before),
        "drains_after_first_before_parking": len([j for j in before if j > 0]),
        "drains_after_parking": len(drained) - len(before),
        "parking_N": pN,
        "parking_share": acc["drain_values"]["K_share_parking"],
        "one_loop_drains": sum(
            1 for r, x in zip(regimes, inj) if r == "one_loop" and x[1] > 0.0
        ),
        "cap_by_alpha_eff": sum(
            1 for r, x in zip(regimes, inj) if r == "one_loop" and x[1] == CAP
        ),
        "cap_gap_by_alpha_eff": sum(
            1 for r, x in zip(regimes, inj) if r == "gap" and x[1] == CAP
        ),
    }
    if "ells" in rec:
        A = rec["drain"]["A"]
        out["cap_by_ell"] = sum(
            1
            for r, ell in zip(regimes, rec["ells"])
            if r == "one_loop" and A * C1 / ell >= CAP
        )
        out["cap_way"] = "ell"
        out["cap"] = out["cap_by_ell"]
    else:
        out["cap_by_ell"] = None
        out["cap_way"] = "alpha_eff"
        out["cap"] = out["cap_by_alpha_eff"]
    return out


def obs(line):
    d = line["accepted"]["dissipation"]
    return {
        "r1": d["rho_chi_ratio_1MeV"],
        "r0": d["rho_chi_ratio_today"],
        "n1": d["Delta_N_eff_1MeV"],
        "n0": d["Delta_N_eff_today"],
        "Yp": line["bbn"]["Yp"],
        "DoH": line["bbn"]["DoH"],
    }


def fi_row(line):
    tag = "" if line["obtained"] == "run" else f"/{line['obtained']}"
    if line["outcome"] != "success":
        return f"{line['outcome']}{tag} ({an02.short_reason(line)})"
    f = first_impact(line)
    o = obs(line)
    return (
        f"first={f['first_regime'] or '-':8s} N1={g(f['first_N'], '.4f'):>8s} a1={g(f['first_alpha_eff'], '.4g'):>8s} "
        f"share1={g(f['first_share'], '.4g'):>9s} drains={f['drains']:4d} after1<park={f['drains_after_first_before_parking']:4d} "
        f"after_park={f['drains_after_parking']:3d} park_N={g(f['parking_N'], '.4f'):>8s} park_share={g(f['parking_share'], '.3g'):>8s} "
        f"cap={f['cap']:3d}({f['cap_way']}) "
        f"r1MeV={g(o['r1'], '.4g'):>9s} r0={g(o['r0'], '.4g'):>9s} dN1MeV={g(o['n1'], '.4g'):>9s} dN0={g(o['n0'], '.4g'):>9s} "
        f"Yp={o['Yp']:.5f} DoH={o['DoH']:.4f}{tag}"
    )


def first():
    lines = merged()
    hs = ordered_histories()
    print(
        "# §2.3 the first impact and the cap (ruling 6), per history and A; zero, and edge where it was run"
    )
    print(
        "# first: the first candidate's regime; N1 its N; a1 its alpha_eff (the fraction of the first impact's kinetic"
    )
    print(
        "# energy drained); share1 its delta_K / K(N_final); drains: alpha_eff > 0, in all; after1<park: after the first"
    )
    print(
        "# impact, at N <= parking_N (all, where the field does not park); after_park: drains after the parking N (a"
    )
    print(
        "# released history); park_share: the parking leftover's share of K, which is not an injection; cap: one-loop"
    )
    print(
        "# drains where the cap acted, counted by ell (02b's runs) or by alpha_eff == 0.99 (02's partials)."
    )
    print(
        "# r1MeV, r0: r_chi at 1 MeV and today; dN1MeV, dN0: Delta N_eff at 1 MeV and today; DoH in 10^-5."
    )
    for h in hs:
        print(f"\n{h['history']}  M={h['M']:g} beta={h['beta']:g}")
        for A in A_ALL:
            for gap in ("zero", "edge"):
                l = lines[task_id(h["history"], A, gap)]
                if gap == "edge" and l["obtained"] == "S1":
                    continue
                print(f"   A={A:<5g} {gap:4s} {fi_row(l)}")

    print(
        "\n# A = 5 and 10 beside A = 1 and 2 (zero; edge in brackets where it was run), laboratory M and"
    )
    print(
        "# block B, and the ladder: a1, drains after the first impact before parking, parking N, cap, dN_eff 1 MeV"
    )
    print("# and today, Y_p, D/H")
    for h in hs:
        if h["group"] == "C":
            continue
        print(f"{h['history']}  M={h['M']:g} beta={h['beta']:g}")
        for A in (1.0, 2.0, 5.0, 10.0):
            cells = []
            for gap in ("zero", "edge"):
                l = lines[task_id(h["history"], A, gap)]
                if gap == "edge" and l["obtained"] == "S1":
                    continue
                if l["outcome"] != "success":
                    cells.append(f"{gap}: {l['outcome']} ({an02.short_reason(l)})")
                    continue
                f, o = first_impact(l), obs(l)
                cells.append(
                    f"{gap}: a1={g(f['first_alpha_eff'], '.4g')} ({f['first_regime']}) after1<park="
                    f"{f['drains_after_first_before_parking']} park_N={g(f['parking_N'], '.4f')} cap={f['cap']} "
                    f"dN1MeV={g(o['n1'], '.4g')} dN0={g(o['n0'], '.4g')} Yp={o['Yp']:.5f} DoH={o['DoH']:.4f}"
                )
            print(
                f"   A={A:<3g} " + "   [".join(cells) + ("]" if len(cells) > 1 else "")
            )

    print(
        "\n# by group and A (zero, successful tunings): the range of a1, of the drains after the first impact before"
    )
    print(
        "# parking, of the parking N, and the cap's count; n: successful tunings of the group at that A"
    )
    for grp in ("P", "W", "R", "B", "L", "C"):
        for A in A_ALL:
            sel = [
                lines[task_id(h["history"], A, "zero")] for h in hs if h["group"] == grp
            ]
            ok = [l for l in sel if l["outcome"] == "success"]
            if not ok:
                print(f"{grp} A={A:<5g} n=0 of {len(sel)}")
                continue
            fs = [first_impact(l) for l in ok]

            def rng(key, fmt):
                v = [f[key] for f in fs if f[key] is not None]
                return (
                    "None" if not v else f"{format(min(v), fmt)}-{format(max(v), fmt)}"
                )

            print(
                f"{grp} A={A:<5g} n={len(ok)} of {len(sel)}  a1={rng('first_alpha_eff', '.3g')}  "
                f"after1<park={rng('drains_after_first_before_parking', 'd')}  park_N={rng('parking_N', '.2f')}  "
                f"cap={rng('cap', 'd')} (capped drains in all {sum(f['cap'] for f in fs)}; parks {sum(1 for f in fs if f['parking_N'] is not None)})"
            )

    print(
        "\n# ruling 6's check of the two cap counts, on 02b's runs (by ell against by alpha_eff == 0.99), and of the"
    )
    print(
        "# partial file's first impact against the line's first_impact (run_wide.first_impact, from the payload)"
    )
    n, nd, ncap = 0, 0, 0
    nl, nld = 0, 0
    for l in lines.values():
        if l["obtained"] != "run" or l["A"] not in A_02B or l["outcome"] != "success":
            continue
        f = first_impact(l)
        n += 1
        if f["cap_by_ell"] > 0:
            ncap += 1
        if f["cap_by_ell"] != f["cap_by_alpha_eff"]:
            nd += 1
            print(
                f"   {l['id']}: by ell {f['cap_by_ell']}, by alpha_eff {f['cap_by_alpha_eff']} (reported, not a stop)"
            )
        lf = l["accepted"]["first_impact"]
        first_ = lf["first"] or {}
        pairs = (
            (first_.get("regime"), f["first_regime"]),
            (first_.get("N"), f["first_N"]),
            (first_.get("alpha_eff"), f["first_alpha_eff"]),
            (first_.get("share_of_K"), f["first_share"]),
            (lf["drains_before_parking"], f["drains_before_parking"]),
            (
                lf["drains_after_first_before_parking"],
                f["drains_after_first_before_parking"],
            ),
            (lf["cap_one_loop_by_ell"], f["cap_by_ell"]),
            (lf["cap_one_loop_by_alpha_eff"], f["cap_by_alpha_eff"]),
            (lf["parking_N"], f["parking_N"]),
        )
        nl += 1
        if any(repr(a) != repr(b) for a, b in pairs):
            nld += 1
            print(
                f"   {l['id']}: the line's first_impact differs from the partial file's: {pairs}"
            )
    print(
        f"   {n} successful 02b runs; {ncap} with a capped one-loop drain; the two cap counts differ in {nd}; "
        f"the line's first_impact differs from the partial file's in {nld} of {nl}"
    )
    cap_gap = [
        (l["id"], l["accepted"]["first_impact"]["cap_gap_by_alpha_eff"])
        for l in lines.values()
        if l["obtained"] == "run"
        and l["A"] in A_02B
        and l["outcome"] == "success"
        and l["accepted"]["first_impact"]["cap_gap_by_alpha_eff"]
    ]
    print(
        f"   gap drains capped under edge (alpha_eff == 0.99 at a gap candidate), 02b's runs: {cap_gap}"
    )
    w1 = [
        (l["id"], i["k"], i.get("cap_one_loop_by_ell"))
        for l in lines.values()
        if l["obtained"] == "run" and l["A"] in A_02B
        for i in (l.get("integrations") or [])
    ]
    print(
        f"   integrations of 02b's runs with a cap count recorded: {sum(1 for x in w1 if x[2] is not None)} of {len(w1)}"
    )


# ---- 02's subcommands, with the overrides -----------------------------------------------------------------------------


def sample():
    use(merged(), A_ALL, wide.OUT_02)
    an02.sample()


def regimes():
    lines = merged()
    use(lines, A_ALL, wide.OUT_02)
    an02.regimes()
    print(
        "\n# U9 and P19 at 02b's A (A = 10, 5, 2, 1.5, 0.5, 0.05), over 02b's run lines (accepted integrations)"
    )
    runs = [
        l
        for l in lines.values()
        if l["obtained"] == "run" and l["A"] in A_02B and l.get("accepted")
    ]
    for A in A_02B + (None,):
        sel = [l for l in runs if A is None or l["A"] == A]
        w1 = sum(l["accepted"]["one_loop_w_gate_below_1"] for l in sel)
        n1 = sum(l["accepted"]["drain_values"]["one_loop"] for l in sel)
        un = sum(l["accepted"]["drain_values"]["unassessed"] for l in sel)
        nc = sum(l["accepted"]["drain_values"]["candidates"] for l in sel)
        wg = sum(l["accepted"]["gap_w_gate_below_1"] for l in sel)
        ng = sum(l["accepted"]["drain_values"]["gap"] for l in sel)
        print(
            f"{'all six' if A is None else 'A=' + format(A, 'g'):8s}: one-loop candidates with w_gate < 1: {w1} of {n1}; "
            f"gap candidates with w_gate < 1: {wg} of {ng}; unassessed: {un} of {nc} (over {len(sel)} run lines)"
        )
    print(
        "\n# unresolvable assessments and declined reflections at 02b's A: run lines, accepted or not"
    )
    for l in lines.values():
        if l["obtained"] != "run" or l["A"] not in A_02B:
            continue
        ua = sum(
            (i.get("drain") or {}).get("unresolvable_assessments", 0) or 0
            for i in l.get("integrations") or []
        )
        ud = sum(
            (i.get("drain") or {}).get("unresolved_declined", 0) or 0
            for i in l.get("integrations") or []
        )
        if ua or ud:
            print(
                f"   {l['id']}: {l['outcome']}; unresolvable {ua}, declined {ud} (over its integrations)"
            )


def spread():
    use(wide.all_lines_02b(), A_02B, wide.OUT)
    an02.spread()


def fixed():
    use(merged(), A_ALL, wide.OUT_02)
    an02.fixed()


def cost():
    use(wide.all_lines_02b(), A_02B, wide.OUT)
    an02.cost()


MATRIX = (
    (
        "Delta N_eff today",
        lambda l: l["accepted"]["dissipation"]["Delta_N_eff_today"],
        ".4g",
    ),
    (
        "Delta N_eff at 1 MeV",
        lambda l: l["accepted"]["dissipation"]["Delta_N_eff_1MeV"],
        ".4g",
    ),
    (
        "r_chi today",
        lambda l: l["accepted"]["dissipation"]["rho_chi_ratio_today"],
        ".4g",
    ),
    ("Y_p", lambda l: l["bbn"]["Yp"], ".5f"),
    ("D/H (10^-5)", lambda l: l["bbn"]["DoH"], ".4f"),
    ("parking N", lambda l: l["accepted"]["dissipation"]["parking_N"], ".2f"),
    ("first impact alpha_eff", lambda l: first_impact(l)["first_alpha_eff"], ".3g"),
)


def u14():
    lines = merged()
    use(lines, A_ALL, wide.OUT_02)
    an02.u14()
    hs = ordered_histories()
    print(
        "\n# for U14: each observable as a history x A matrix, gap zero. x: the tuning failed; edge's value follows in"
    )
    print(
        "# brackets where edge was run ([x] where it failed); the regimes met are the last matrix but one"
    )
    for name, f, fmt in MATRIX:
        print(f"\n## {name}")
        print(
            f"{'history':12s} "
            + " ".join(f"{'A=' + format(A, 'g'):>22s}" for A in A_ALL)
        )
        for h in hs:
            cells = []
            for A in A_ALL:
                z = lines[task_id(h["history"], A, "zero")]
                e = lines[task_id(h["history"], A, "edge")]
                c = (
                    format(f(z), fmt)
                    if z["outcome"] == "success" and f(z) is not None
                    else ("None" if z["outcome"] == "success" else "x")
                )
                if e["obtained"] == "run":
                    c += f" [{format(f(e), fmt) if e['outcome'] == 'success' and f(e) is not None else 'x'}]"
                cells.append(f"{c:>22s}")
            print(f"{h['history']:12s} " + " ".join(cells))
    print(
        "\n## the regimes met by the zero tuning (any integration): G gap, C R <= 1, - neither; x the tuning failed"
    )
    print(f"{'history':12s} " + " ".join(f"{'A=' + format(A, 'g'):>8s}" for A in A_ALL))
    for h in hs:
        cells = []
        for A in A_ALL:
            z = lines[task_id(h["history"], A, "zero")]
            m = an02.met_counts(z)
            cells.append(
                f"{('G' if m['gap'] else '-') + ('C' if m['classical'] else '-') + ('x' if z['outcome'] != 'success' else ' '):>8s}"
            )
        print(f"{h['history']:12s} " + " ".join(cells))
    print(
        "\n## which (history, A) cannot be built on this code: the zero and edge tunings that fail"
    )
    for h in hs:
        bad = []
        for A in A_ALL:
            for gap in ("zero", "edge"):
                l = lines[task_id(h["history"], A, gap)]
                if l["outcome"] != "success":
                    bad.append(
                        f"A={A:g} {gap}{'' if l['obtained'] == 'run' else '/' + l['obtained']}"
                    )
        if bad:
            print(f"{h['history']:12s} {', '.join(bad)}")


def main():
    {
        "sample": sample,
        "regimes": regimes,
        "first": first,
        "spread": spread,
        "fixed": fixed,
        "cost": cost,
        "u14": u14,
    }[sys.argv[1]]()


if __name__ == "__main__":
    main()
