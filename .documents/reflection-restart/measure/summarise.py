"""
The tables of reflection-restart prompt 01's measurement (§1.3, §1.5), from the batch's output
var/reflection-restart/01/main.jsonl (tasks.py main, run by tools/outward_step_scan.py batch).

    PYTHONPATH=. ./venv/bin/python .documents/reflection-restart/measure/summarise.py

Writes .documents/reflection-restart/out/measure-summary.txt (the tables) and
.documents/reflection-restart/out/measure-tasks.tsv (one line per task: its key, status, outcome, wall,
counts and flags). Reads only; needs no Ray and no store.

Conventions. "flagged" is P1's flag on a step the loop kept (a step F4 discarded is counted apart);
"phi" counts flagged steps whose interpolant fell below (1 - f) phi_start (the diagnosis's bad steps
are all of this kind), "ratio-only" those flagged only by pi_end/pi_start < 1/2 with no sign change.
A cost ratio is the variant's total over the histories that both it and None completed, divided by
None's. "vs None" compares a history's per-history values (the dissipation values, the first bounce,
the fixed-temperature values, the tuning's normalization and phi_ref) with the same history under
None; "vs store" is outward_step_scan's comparison with the stored rows (reflection store and block B).
"""

import json
import math
from collections import defaultdict
from pathlib import Path

OUT = Path(".documents/reflection-restart/out")
SRC = Path("var/reflection-restart/01/main.jsonl")
VARIANTS = ["None", "F1", "F2", "F2(c=0.3)", "F2(c=3)", "F3", "F4", "F3+F4"]
ITEMS = {
    1: "the three failures (tuned)",
    2: "the six clean histories (tuned)",
    3: "the golden histories",
    4: "the reflection store's 42 (stored normalization)",
    5: "block B's twelve (stored normalization)",
}


def variant_of(spec):
    v = spec.get("variant") or "None"
    if spec.get("stiffness_c") is not None:
        v = f"{v}(c={spec['stiffness_c']:g})"
    return v


def history_of(spec):
    if spec.get("golden"):
        return f"golden {spec['golden']}"
    s = f"beta={spec['beta']:g} M={spec['M']:g} alpha={spec.get('alpha', 0.0):g}"
    if spec.get("store"):
        s += f" #{Path(spec['store']['shard']).name.split('-shard')[-1][:4]}/{spec['store']['serial']}"
    return s


def values_of(rec):
    """The per-history values compared against None."""
    out = {}
    for k, v in (rec.get("dissipation") or {}).items():
        if isinstance(v, (int, float)) and v is not None:
            out[k] = float(v)
    fb = rec.get("first_bounce")
    if fb:
        out["first_bounce_N"] = fb["N"]
    for k, v in (rec.get("fixed_T_values") or {}).items():
        if isinstance(v, (int, float)):
            out[k] = float(v)
    for k in ("normalization", "phi_ref"):
        v = (rec.get("tuning") or {}).get(k)
        if isinstance(v, float):
            out[k] = v
    return out


def rel(a, b):
    if a == b:
        return 0.0
    s = max(abs(a), abs(b))
    return abs(a - b) / s if s > 0 else math.inf


def g(x, fmt=".3g"):
    return "-" if x is None else format(x, fmt)


def main():
    recs = [json.loads(l) for l in SRC.read_text().splitlines() if l.strip()]
    by = {}
    for r in recs:
        by[(history_of(r["spec"]), variant_of(r["spec"]))] = r
    histories = defaultdict(list)
    for r in recs:
        h = history_of(r["spec"])
        if h not in histories[r["spec"]["item"]]:
            histories[r["spec"]["item"]].append(h)

    lines = []
    w = lines.append

    # the per-task table
    tsv = [
        "key\tstatus\toutcome\twall_s\taccepted_steps\tRHS\tflagged_first\tflagged_later\t"
        "flag_phi\tflag_ratio_only\tdiscarded\tmax_h_omega_outward\tbitwise_vs_store\tgolden_bitwise"
    ]
    for r in recs:
        s = r.get("scan", {})
        tsv.append(
            "\t".join(
                str(x)
                for x in (
                    r["key"],
                    r.get("status"),
                    r.get("outcome"),
                    g(r.get("wall"), ".1f"),
                    r.get("accepted_steps", r.get("radau_accepted_steps_reached")),
                    r.get("RHS_evaluations"),
                    s.get("flagged_first"),
                    s.get("flagged_later"),
                    (
                        (s.get("flag_phi_only", 0) or 0) + (s.get("flag_both", 0) or 0)
                        if s
                        else None
                    ),
                    s.get("flag_ratio_only"),
                    s.get("discarded_steps"),
                    g(s.get("max_h_omega_outward")),
                    (r.get("comparison") or {}).get("bitwise"),
                    (r.get("post") or {}).get("bitwise"),
                )
            )
        )
    (OUT / "measure-tasks.tsv").write_text("\n".join(tsv) + "\n")

    status = defaultdict(int)
    for r in recs:
        status[(r.get("status"), r.get("outcome"))] += 1
    w(f"tasks: {len(recs)}; by status and outcome: {dict(status)}")
    timeouts = [r for r in recs if r.get("status") == "timeout"]
    errors = [r for r in recs if r.get("status") == "error"]
    w(f"timeouts: {len(timeouts)}; errors: {len(errors)}")
    for r in timeouts:
        w(
            f"   timeout {r['key']}: wall {r['wall']:.0f} s, Radau steps reached "
            f"{r['radau_accepted_steps_reached']}, N reached {r['N_reached']:.6g}, integration "
            f"{r['integration_reached']}"
        )
    for r in errors:
        w(f"   error {r['key']}: {r['traceback'].strip().splitlines()[-1]}")
    w("")

    for item in sorted(histories):
        w(f"=== item {item}: {ITEMS[item]}")
        for v in VARIANTS:
            rows = [(h, by.get((h, v))) for h in histories[item]]
            rows = [(h, r) for h, r in rows if r is not None]
            if not rows:
                continue
            w(f"--- {v}")
            n_out = defaultdict(int)
            for _, r in rows:
                n_out[r.get("outcome") or r.get("status")] += 1
            fl_first = sum(r.get("scan", {}).get("flagged_first", 0) for _, r in rows)
            fl_later = sum(r.get("scan", {}).get("flagged_later", 0) for _, r in rows)
            fl_phi = sum(
                r.get("scan", {}).get("flag_phi_only", 0)
                + r.get("scan", {}).get("flag_both", 0)
                for _, r in rows
            )
            disc = sum(r.get("scan", {}).get("discarded_steps", 0) for _, r in rows)
            w(
                f"    outcomes {dict(n_out)}; flagged first {fl_first}, later {fl_later} "
                f"(of all flags made, phi condition {fl_phi}); discarded by F4 {disc}"
            )
            if item == 4 or item == 5:
                done = [r for _, r in rows if r.get("comparison") is not None]
                nb = sum(r["comparison"]["bitwise"] for r in done)
                w(f"    bitwise against the store: {nb} of {len(done)} compared")
                worst_h, worst_v = {}, {}
                for r in done:
                    for col, d in r["comparison"]["history_columns"].items():
                        if (
                            "max_rel" in d
                            and d["max_rel"] > worst_h.get(col, (0, ""))[0]
                        ):
                            worst_h[col] = (d["max_rel"], history_of(r["spec"]))
                    for col, d in r["comparison"]["value_columns"].items():
                        if (
                            "max_rel" in d
                            and d["max_rel"] > worst_v.get(col, (0, ""))[0]
                        ):
                            worst_v[col] = (d["max_rel"], history_of(r["spec"]))
                if worst_h:
                    w(
                        "    largest relative change against the store, ScalarHistory columns:"
                    )
                    for col, (x, h) in sorted(worst_h.items()):
                        w(f"      {col:28s} {x:.3g}  ({h})")
                if worst_v:
                    w("    largest relative change against the store, sample columns:")
                    for col, (x, h) in sorted(worst_v.items()):
                        w(f"      {col:28s} {x:.3g}  ({h})")
            if item == 3:
                for h, r in rows:
                    post = r.get("post") or {}
                    moved = post.get("moved", {})
                    keys = sorted(moved.get("keys", {}))
                    ex = moved.get("excerpt_max_rel", {})
                    top = sorted(ex.items(), key=lambda kv: -kv[1])[:4]
                    w(
                        f"    {h}: {r.get('outcome') or r.get('status')}, golden bitwise "
                        f"{post.get('bitwise')}; keys moved {keys}; largest excerpt changes "
                        + ", ".join(f"{k} {x:.2g}" for k, x in top)
                    )
            # per history
            w(
                f"    {'history':42s} {'outcome':8s} {'wall':>7s} {'x None':>6s} {'steps':>8s} {'x None':>6s} "
                f"{'1st':>4s} {'later':>5s} {'phi':>4s} {'disc':>5s} {'max hw out':>10s} {'max rel vs None (value)':>30s}"
            )
            for h, r in rows:
                base = by.get((h, "None"))
                s = r.get("scan", {})
                wall_ratio = (
                    r["wall"] / base["wall"]
                    if base and base.get("wall") and r.get("wall")
                    else None
                )
                st = r.get("accepted_steps")
                st_ratio = (
                    st / base["accepted_steps"]
                    if st and base and base.get("accepted_steps")
                    else None
                )
                worst = None
                if (
                    base
                    and r.get("outcome") == "success"
                    and base.get("outcome") == "success"
                ):
                    a, b = values_of(base), values_of(r)
                    for k in a:
                        if k in b:
                            x = rel(a[k], b[k])
                            if worst is None or x > worst[0]:
                                worst = (x, k)
                    if r.get("samples_sha256") == base.get("samples_sha256"):
                        worst_text = "samples bitwise"
                    else:
                        worst_text = f"{worst[0]:.2g} ({worst[1]})" if worst else "-"
                else:
                    worst_text = "-"
                w(
                    f"    {h:42s} {str(r.get('outcome') or r.get('status'))[:8]:8s} {g(r.get('wall'), '.1f'):>7s} "
                    f"{g(wall_ratio, '.2f'):>6s} {str(st if st is not None else r.get('radau_accepted_steps_reached')):>8s} "
                    f"{g(st_ratio, '.2f'):>6s} {s.get('flagged_first', '-')!s:>4s} {s.get('flagged_later', '-')!s:>5s} "
                    f"{(s.get('flag_phi_only', 0) + s.get('flag_both', 0)) if s else '-'!s:>4s} "
                    f"{s.get('discarded_steps', '-')!s:>5s} {g(s.get('max_h_omega_outward')):>10s} {worst_text:>30s}"
                )
        w("")

    # the overview, one block per variant over every item
    w("=== overview per variant, over every history of the batch")
    KEY = (
        "Delta_N_eff_1MeV",
        "Delta_N_eff_today",
        "rho_chi_ratio_70keV",
        "parking_N",
        "parking_log_T_Jordan",
        "injections",
        "density_NP_ratio_1MeV",
        "density_NP_ratio_70keV",
        "phi_Einstein_1MeV",
        "first_bounce_N",
    )
    for v in VARIANTS:
        rs = [r for (h, vv), r in by.items() if vv == v]
        if not rs:
            continue
        outs = defaultdict(int)
        bad_kind = later_phi = ratio_only = disc = 0
        for r in rs:
            outs[r.get("outcome") or r.get("status")] += 1
            for row in r.get("rows", []):
                if row.get("discarded"):
                    continue
                if row["phi_condition"] and row["first"]:
                    bad_kind += 1
                elif row["phi_condition"]:
                    later_phi += 1
            s_ = r.get("scan", {})
            ratio_only += s_.get("flag_ratio_only", 0)
            disc += s_.get("discarded_steps", 0)
        gold = [r for r in rs if r["spec"].get("golden")]
        gb = sum(1 for r in gold if (r.get("post") or {}).get("bitwise"))
        st = [r for r in rs if r.get("comparison") is not None]
        sb = sum(1 for r in st if r["comparison"]["bitwise"])
        worst = {}
        for r in st:
            for col, d in r["comparison"]["history_columns"].items():
                if col in KEY and d.get("max_rel", 0) > worst.get(col, 0):
                    worst[col] = d["max_rel"]
        w(f"--- {v}: {len(rs)} tasks; outcomes {dict(outs)}")
        w(
            f"    kept steps flagged by the phi condition: first steps of an instance {bad_kind}, later "
            f"{later_phi}; ratio-only flags made {ratio_only}; discarded by F4 {disc}"
        )
        w(
            f"    goldens bitwise {gb} of {len(gold)}; against the store bitwise {sb} of {len(st)}"
        )
        if worst:
            w(
                "    largest relative change against the store (items 4, 5): "
                + ", ".join(f"{k} {worst[k]:.2g}" for k in KEY if k in worst)
            )
    w("")

    # cost per M
    w(
        "=== cost per M: total accepted steps and wall-clock over the histories both runs completed, "
        "against None"
    )
    Ms = sorted({r["spec"]["M"] for r in recs})
    w(f"    {'variant':10s} " + " ".join(f"{M:>16g}" for M in Ms))
    for v in VARIANTS:
        cells = []
        for M in Ms:
            st_v = st_n = wl_v = wl_n = 0.0
            n = 0
            for (h, vv), r in by.items():
                if vv != v or r["spec"]["M"] != M or r.get("accepted_steps") is None:
                    continue
                base = by.get((h, "None"))
                if not base or base.get("accepted_steps") is None:
                    continue
                st_v += r["accepted_steps"]
                st_n += base["accepted_steps"]
                wl_v += r["wall"]
                wl_n += base["wall"]
                n += 1
            cells.append(
                f"{st_v / st_n:6.2f}/{wl_v / wl_n:5.2f}({n:2d})" if n else f"{'-':>16s}"
            )
        w(f"    {v:10s} " + " ".join(f"{c:>16s}" for c in cells))
    w("    (each cell: steps ratio/wall ratio(histories))")
    w("")

    # the flag's distributions under None
    w("=== the flag under None, over every history of the batch")
    hist = None
    edges = None
    least_unflagged, greatest_flagged = None, None
    n_out = n_sign = n_phi = n_ratio = 0
    hw_hist = None
    for (h, v), r in by.items():
        if v != "None" or "scan" not in r:
            continue
        s = r["scan"]
        edges = s["pi_ratio_edges"]
        hist = (
            s["pi_ratio_hist"]
            if hist is None
            else [a + b for a, b in zip(hist, s["pi_ratio_hist"])]
        )
        hw_hist = (
            s["h_omega_hist"]
            if hw_hist is None
            else [a + b for a, b in zip(hw_hist, s["h_omega_hist"])]
        )
        n_out += s["outward_steps"]
        n_sign += s["sign_change_outward_steps"]
        n_phi += s["flag_phi_only"] + s["flag_both"]
        n_ratio += s["flag_ratio_only"]
        if s["least_pi_ratio_unflagged"] is not None:
            least_unflagged = (
                s["least_pi_ratio_unflagged"]
                if least_unflagged is None
                else min(least_unflagged, s["least_pi_ratio_unflagged"])
            )
        if s["greatest_pi_ratio_flagged"] is not None:
            greatest_flagged = (
                s["greatest_pi_ratio_flagged"]
                if greatest_flagged is None
                else max(greatest_flagged, s["greatest_pi_ratio_flagged"])
            )
    w(
        f"    outward steps {n_out}; with a sign change of pi {n_sign}; flagged by the phi condition {n_phi}, "
        f"by the ratio condition alone {n_ratio}"
    )
    w(
        f"    least pi_end/pi_start of an unflagged step with no sign change {g(least_unflagged, '.6g')}; "
        f"greatest of a flagged one {g(greatest_flagged, '.6g')}"
    )
    labels = (
        ["<0"] + [f"[{a:g},{b:g})" for a, b in zip(edges[:-1], edges[1:])] + [">=1"]
    )
    w(
        "    pi_end/pi_start, steps with no sign change: "
        + ", ".join(f"{l}: {c}" for l, c in zip(labels, hist))
    )
    hw_edges = list(range(-12, 5))
    hw_labels = (
        ["0 or <1e-12"]
        + [f"[1e{a},1e{b})" for a, b in zip(hw_edges[:-1], hw_edges[1:])]
        + [">=1e4"]
    )
    w(
        "    h*omega (stale J), outward steps: "
        + ", ".join(f"{l}: {c}" for l, c in zip(hw_labels, hw_hist) if c)
    )
    phi_rows = []
    for (h, v), r in by.items():
        if v == "None":
            for row in r.get("rows", []):
                if row["phi_condition"]:
                    phi_rows.append((h, row))
    w(f"    every step flagged by the phi condition under None ({len(phi_rows)}):")
    for h, row in phi_rows:
        w(
            f"      {h}: N0={row['N0']:.10g} {row['reason']} first={row['first']} h={row['h']:.3g} "
            f"h*omega={row['h_omega']:.3g} pi ratio={row['pi_ratio']:.3g} sign change={row['sign_change']} "
            f"least phi/phi0={row['phi_min_over_start']:.3g} a_in={row['a_in']:.3g}"
        )
    ratio_rows = [
        (h, row)
        for (h, v), r in by.items()
        if v == "None"
        for row in r.get("rows", [])
        if row["ratio_condition"] and not row["phi_condition"]
    ]
    if ratio_rows:
        a_pos = sum(1 for _, row in ratio_rows if row["a_in"] > 0)
        first = sum(1 for _, row in ratio_rows if row["first"])
        w(
            f"    the ratio-only flags under None: {len(ratio_rows)}; with inward acceleration -pi' > 0 "
            f"(the field decelerating) {a_pos}; first steps of an instance {first}; largest h*omega "
            f"{max(row['h_omega'] for _, row in ratio_rows):.3g}"
        )

    (OUT / "measure-summary.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines[:3]))


if __name__ == "__main__":
    main()
