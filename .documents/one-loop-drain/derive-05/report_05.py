"""
one-loop-drain prompt 05 §1.6's report (verification §5): the run of run_05.py and the tool at c_a = 1, 0 and 10 on its
export root.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/derive-05/report_05.py > .documents/one-loop-drain/derive-05/out/report.txt

Reads var/one-loop-drain/05/ (scheduler.json, run/*.result.json, exports/QCD_Cosmology/quanta_histories*.csv) and the
tool's printed lines, .documents/one-loop-drain/derive-05/out/tool-assoc{1,0,10}.txt, and 02d's sample.jsonl for each
task's group. Prints:
  1. the tasks: run, failed (with the reason), the step 2 check, the samples, the readback and the store's deletion;
  2. the walls: the run's, each step's, the integration's against 02d's per-integration wall, the peak RSS, the loads;
  3. the merge check;
  4. the tool: its summary line at each scale, its wall per history against the integration's, the leftover's
     placement, the K check's largest difference and the bitwise counts, ruling 7's and ruling 8's counts;
  5. per A and per group (P, W, B, L, R, C), at c_a = 1: the median and the maximum of delta_stim, n_bar_max,
     ln_n_g_max, the E maxima and r_chi E's (field and tracker), the minimum producing phase slip (with its minimum too)
     and backreaction_max;
  6. the same at c_a = 0 and 10, as ratios X(c_a)/X(1) per history: their minimum, median and maximum;
  7. delta_stim's range over the sample;
  8. the magnitudes behind the K check's tolerance: the largest |ln K| + 4|N| + |ln rho_R,E| + 4|ln(GeV/M_P)| over every
     sample with K(N) > 0 of every history (ln rho_R,E in M_P^4), and the largest parking_share/(1 - parking_share) of a
     leftover after the last candidate.
"""

import csv
import json
import re
import statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
VAR = ROOT / "var" / "one-loop-drain" / "05"
EXPORT = VAR / "exports" / "QCD_Cosmology"
OUT = ROOT / ".documents" / "one-loop-drain" / "derive-05" / "out"
SAMPLE = ROOT / "var" / "one-loop-drain" / "02d" / "sample.jsonl"
GROUPS = ("P", "W", "B", "L", "R", "C")
SCALES = ((1.0, ""), (0.0, "-assoc0"), (10.0, "-assoc10"))
MEASURES = (
    "delta_stim",
    "n_bar_max",
    "ln_n_g_max",
    "E_field_max",
    "E_tracker_max",
    "r_chi_E_field_max",
    "r_chi_E_tracker_max",
    "phase_slip_min_producing",
    "backreaction_max",
)


def fl(x):
    return None if x in ("", None) else float(x)


def fmt(x):
    if x is None:
        return "-"
    return f"{x:.3g}"


def dist(values, with_min=False):
    v = [x for x in values if x is not None]
    if not v:
        return "n/a"
    s = f"med {fmt(statistics.median(v))} max {fmt(max(v))}"
    if with_min:
        s = f"min {fmt(min(v))} " + s
    return s + f" (n={len(v)})"


def main():
    lines = {
        json.loads(t)["id"]: json.loads(t) for t in SAMPLE.read_text().splitlines()
    }
    summary = json.loads((VAR / "scheduler.json").read_text())
    results = {}
    for p in sorted((VAR / "run").glob("*.result.json")):
        r = json.loads(p.read_text())
        results[r["id"]] = r

    # 1. the tasks
    print("== 1. the tasks ==")
    print(
        f"tasks {summary['tasks']}, workers {summary['workers']}, results {len(results)}, failures {len(summary['failures'])}"
    )
    for tid, why in sorted(summary["failures"].items()):
        print(f"  FAILED {tid}: {why}")
    ok = [r for r in results.values() if "error" not in r]
    c = lambda key: sum(1 for r in ok if r["checks"][key])
    print(
        f"step 2: candidates equal to 02d by repr {c('candidates_equal_02d')} of {len(ok)}; samples equal to 02d's count "
        f"{sum(1 for r in ok if r['checks']['n_samples'] == r['checks']['n_samples_02d'])}; fields.csv rows = samples "
        f"{c('fields_rows_equal')}; bounces.csv rows = rows read = stored = bounce_rows {c('bounces_rows_equal')}; store deleted "
        f"{c('store_deleted')}; stores left {len(list((VAR / 'stores').iterdir())) if (VAR / 'stores').exists() else 0}"
    )
    print(
        f"candidates over the run {sum(r['checks']['n_candidates'] for r in ok)}; samples {sum(r['checks']['n_samples'] for r in ok)}"
    )

    # 2. the walls
    print("\n== 2. the walls ==")
    print(f"the run: {summary['wall']:.0f} s ({summary['wall'] / 60:.1f} min)")
    for step in ("integrate", "store", "export", "readback_delete"):
        w = [r["walls"][step] for r in ok]
        print(
            f"  {step:16s} sum {sum(w):8.1f} s, median {statistics.median(w):7.2f} s, max {max(w):7.1f} s"
        )
    ratio = [r["walls"]["integrate"] / r["integration_wall_02d"] for r in ok]
    print(
        f"integration wall / 02d's per-integration wall: median {statistics.median(ratio):.2f}, min {min(ratio):.2f}, max {max(ratio):.2f}"
    )
    big = max(ok, key=lambda r: r["walls"]["integrate"])
    print(
        f"largest: {big['id']} integrate {big['walls']['integrate']:.0f} s (02d {big['integration_wall_02d']:.0f} s)"
    )
    rss = max(ok, key=lambda r: r["peak_rss"])
    print(
        f"peak RSS: largest {rss['peak_rss'] / 1024**3:.2f} GiB ({rss['id']}); median {statistics.median(r['peak_rss'] for r in ok) / 1024**3:.2f} GiB"
    )
    l1 = [r["load_start"][0] for r in ok] + [r["load_end"][0] for r in ok]
    print(
        f"one-minute loads at the children's starts and ends: {min(l1):.2f}-{max(l1):.2f}"
    )

    # 3. the merge
    print("\n== 3. the merge ==")
    print(json.dumps(summary["merge"]))

    # 4. the tool
    print("\n== 4. the tool ==")
    tool_lines = {}
    for c_a, sfx in SCALES:
        text = (OUT / f"tool-assoc{c_a:g}.txt").read_text().splitlines()
        print(f"c_a = {c_a:g}: {text[-1]}")
        tool_lines[c_a] = text
    pat = re.compile(
        r"^(?P<key>one-loop\S* A=(?P<A>\S+) M_Mp=(?P<M>\S+) n=(?P<n>\S+) beta=(?P<beta>\S+)): generations (?P<g>\d+); "
        r"drains without spectrum (?P<ws>\d+); leftover (?P<pl>.+?); K bitwise (?P<kb>\d+)/(?P<kc>\d+); K check (?P<kq>\S+); "
        r"K\(N\) = 0 with a generation born (?P<kz>\d+); between N_turn and N (?P<bt>\d+); wall (?P<w>\S+) s$"
    )
    by_key = {}
    for t in tool_lines[1.0]:
        m = pat.match(t)
        if m:
            by_key[(float(m["A"]), float(m["M"]), float(m["beta"]))] = m
    task_of = {(l["A"], l["M"], l["beta"]): tid for tid, l in lines.items()}
    walls = []
    placements = {}
    for key, m in by_key.items():
        tid = task_of[key]
        placements[m["pl"]] = placements.get(m["pl"], 0) + 1
        if tid in results and "error" not in results[tid]:
            walls.append(
                (float(m["w"]) / results[tid]["walls"]["integrate"], float(m["w"]), tid)
            )
    walls.sort()
    print(f"histories measured at c_a = 1: {len(by_key)}")
    print(
        f"tool wall per history / its integration's: median {statistics.median(w[0] for w in walls):.3g}, max "
        f"{walls[-1][0]:.3g} ({walls[-1][2]}); largest tool wall {max(w[1] for w in walls):.2f} s "
        f"({max(walls, key=lambda w: w[1])[2]})"
    )
    print(f"the leftover's placement: {placements}")
    kq = max(by_key.items(), key=lambda kv: float(kv[1]["kq"]))
    print(
        f"K check: largest relative difference {float(kq[1]['kq']):.3g} ({task_of[kq[0]]}); K_before + delta_K equal to the next "
        f"K_before bit for bit at {sum(int(m['kb']) for m in by_key.values())} of {sum(int(m['kc']) for m in by_key.values())} pairs"
    )
    kz = [(int(m["kz"]), task_of[k]) for k, m in by_key.items() if int(m["kz"]) > 0]
    bt = [(int(m["bt"]), task_of[k]) for k, m in by_key.items() if int(m["bt"]) > 0]
    print(
        f"ruling 7: samples with K(N) = 0 and a generation born: {sum(x for x, _ in kz)} in {len(kz)} histories"
        + (f" (largest {max(kz)})" if kz else "")
    )
    print(
        f"ruling 7: samples between a generation's N_turn and its N: {sum(x for x, _ in bt)} in {len(bt)} histories"
        + (f" (largest {max(bt)})" if bt else "")
    )
    ws = sorted(
        (task_of[k], int(m["ws"])) for k, m in by_key.items() if int(m["ws"]) > 0
    )
    print(f"ruling 8: histories with drains without a spectrum: {len(ws)}: {ws}")

    # 5. the distributions at c_a = 1
    tables = {}
    for c_a, sfx in SCALES:
        with open(EXPORT / f"quanta_histories{sfx}.csv", newline="") as f:
            for row in csv.DictReader(f):
                key = (float(row["alpha"]), float(row["M_Mp"]), float(row["beta"]))
                tables.setdefault(c_a, {})[task_of[key]] = row
    base = tables[1.0]
    print("\n== 5. at c_a = 1, per A and per group (median, maximum) ==")
    As = sorted({lines[t]["A"] for t in base})
    for label, select in [
        (f"A = {a:g}", lambda t, a=a: lines[t]["A"] == a) for a in As
    ] + [(f"group {g}", lambda t, g=g: lines[t]["group"] == g) for g in GROUPS]:
        ids = [t for t in base if select(t)]
        print(f"-- {label} ({len(ids)} histories)")
        for m in MEASURES:
            vals = [fl(base[t][m]) for t in ids]
            print(
                f"   {m:26s} {dist(vals, with_min=(m == 'phase_slip_min_producing'))}"
            )

    # 6. the ratios
    for c_a in (0.0, 10.0):
        print(
            f"\n== 6. at c_a = {c_a:g}, X({c_a:g})/X(1) per history (min, median, max) =="
        )
        other = tables[c_a]
        for label, select in (
            [(f"A = {a:g}", lambda t, a=a: lines[t]["A"] == a) for a in As]
            + [(f"group {g}", lambda t, g=g: lines[t]["group"] == g) for g in GROUPS]
            + [("all", lambda t: True)]
        ):
            ids = [t for t in base if select(t) and t in other]
            parts = []
            for m in MEASURES:
                r = []
                for t in ids:
                    a, b = fl(other[t][m]), fl(base[t][m])
                    if a is not None and b not in (None, 0.0):
                        r.append(a / b)
                parts.append(
                    f"{m} {fmt(min(r)) if r else '-'}/{fmt(statistics.median(r)) if r else '-'}/{fmt(max(r)) if r else '-'}"
                )
            print(f"-- {label} ({len(ids)}): " + "; ".join(parts))

    # 7. delta_stim's range
    ds = sorted(
        (fl(base[t]["delta_stim"]), t)
        for t in base
        if fl(base[t]["delta_stim"]) is not None
    )
    print(
        f"\n== 7. delta_stim over the sample at c_a = 1: min {ds[0][0]:.3g} ({ds[0][1]}), median "
        f"{statistics.median(x for x, _ in ds):.3g}, max {ds[-1][0]:.3g} ({ds[-1][1]}); histories {len(ds)}"
    )
    for c_a in (0.0, 10.0):
        d = sorted(
            (fl(tables[c_a][t]["delta_stim"]), t)
            for t in tables[c_a]
            if fl(tables[c_a][t]["delta_stim"]) is not None
        )
        print(
            f"   at c_a = {c_a:g}: min {d[0][0]:.3g}, max {d[-1][0]:.3g} ({d[-1][1]})"
        )


def magnitudes():
    from math import log

    from Units import Planck_units
    from tools import quanta_measures as qm

    units = Planck_units()
    log_GeV_Mp = log(units.GeV / units.PlanckMass)
    worst, where = 0.0, None
    worst_ps, where_ps = 0.0, None
    for qpath in sorted(EXPORT.rglob("quanta_samples.csv")):
        d = qpath.parent
        with open(d / "fields.csv", newline="") as f:
            fields = list(csv.DictReader(f))
        with open(qpath, newline="") as f:
            quanta = list(csv.DictReader(f))
        for fr, qr in zip(fields, quanta):
            K = float(qr["K_Mp4"])
            if K == 0.0:
                continue
            N = float(fr["raw_N"])
            m = (
                abs(log(K))
                + 4 * abs(N)
                + abs(float(fr["log_rhorad_Einstein_GeV4"]) + 4 * log_GeV_Mp)
                + 4 * abs(log_GeV_Mp)
            )
            if m > worst:
                worst, where = m, str(d.relative_to(EXPORT))
    with open(EXPORT / "quanta_histories.csv", newline="") as f:
        for row in csv.DictReader(f):
            ps = fl(row["parking_share"])
            if ps and ps / (1 - ps) > worst_ps:
                worst_ps, where_ps = ps / (1 - ps), (
                    row["M_Mp"],
                    row["beta"],
                    row["alpha"],
                )
    eps = 2.0**-52
    print(f"\n== 8. the K check's tolerance ({qm.K_CHECK_TOLERANCE:g}) ==")
    print(
        f"largest |ln K| + 4|N| + |ln rho_R,E| + 4|ln(GeV/M_P)| = {worst:.1f} ({where}); times eps {worst * eps:.2e}"
    )
    print(
        f"largest parking_share/(1 - parking_share) = {worst_ps:.4g} (M_Mp, beta, A = {where_ps}); times eps {worst_ps * eps:.2e}"
    )
    print(f"margin: {qm.K_CHECK_TOLERANCE / ((2 * worst + worst_ps) * eps):.0f}")


if __name__ == "__main__":
    main()
    magnitudes()
