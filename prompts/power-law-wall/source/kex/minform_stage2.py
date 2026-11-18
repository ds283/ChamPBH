"""
power-law-wall prompt 07b2 §1.5 (P95), the runs: the new points of prediction.json by 07b's stage-2 machinery, and
their validation. Run only after prediction.json exists; it is read, never written.

Every point is stage2_kex.run_point_kex(L, ln R, "v3", n): crossover's stage2.solve with the mode grid from the new
k_ex (07b's way of building a point, on a curve or off it), scheduled by stage2_kex.schedule (ruling 3: at most four at
once, one at a time while the one-minute load is above 24, the load printed at each start and end, a point stopped at
30 minutes). Per point, as 07b's stage2_new.txt: f from the modes and from the balance, v_out/v_in, whether the field
came back out, the loop parameter at the turn (eps_turn), |dE/E|, the passes and modes, the run time with the load at
its start and end; and here also the start time, ell and the regime in both forms, the predicted f and the residual
(f_modes - f_pred). A machine-readable copy of each result is the line starting "PT ", which minform.new_points_07b2
reads.

Modes:
  points    every point of prediction.json;
  validate  at the three points prediction.json names (one at the crossing on the laboratory curve, one on the fall of
            L-M1e-16, one on the rise of the n = 2 curve): stage2_kex.task_validate's classical limit (loop off against
            walls.py's test field, tolerance 1e-4), the Born limit taken off the curve at the point's L where
            ell_new = 6 (born-offcurve: on the curve, at ell_new <= 4, k_IR = 10 m_ta is above k_ex and the Born
            reduction does not hold, 07b's D4; tolerance 2e-3) and convergence (crossover's V4 variants, tolerance
            +-6e-3). Energy is checked at every point by `points` (|dE/E|).

    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B minform_stage2.py points \\
        > ../../../../.documents/power-law-wall/kex-07b2/stage2_points.txt
    cd prompts/power-law-wall/source/kex && ../../../../venv/bin/python -B minform_stage2.py validate \\
        > ../../../../.documents/power-law-wall/kex-07b2/stage2_validate.txt
"""

import datetime
import json
import os
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import stage2_kex as s2  # noqa: E402  (07b's; imported, never changed)

ROOT = os.path.abspath(os.path.join(HERE, "../../../.."))
PRED = os.path.join(ROOT, ".documents/power-law-wall/kex-07b2/prediction.json")


def chains_text(L, lnR, n):
    c = s2.chains(L, lnR, n)
    return c, " ".join(
        f"{form}: {c[form][0]} ell {'none' if c[form][1] is None else format(c[form][1], '.4f')} k_ex "
        f"{c[form][2]:.5e};"
        for form in ("lambda", "v3")
    )


def task(job):
    label, L, lnR, n, p = job
    t0 = time.time()
    l0 = s2.load1()
    res = s2.run_point_kex(L, lnR, "v3", n)
    c, ctext = chains_text(L, lnR, n)
    f = res["frac_modes"]
    resid = f - p["f_predicted"]
    secs = time.time() - t0
    l1 = s2.load1()
    start = (
        datetime.datetime.fromtimestamp(t0).astimezone().isoformat(timespec="seconds")
    )
    human = (
        f"{label:40s} n={n} L={L:10.5f} lnR={lnR:9.4f} | frac {f:.5f} balance {res['frac_balance']:.5f} vout/vin "
        f"{res['vout']:.4f} back {str(res['returned']):5s} eps_turn {res['eps_turn']:.1e} |dE/E| {res['drift']:.1e} "
        f"passes {res['passes']} modes {res['nk']} | {ctext} | predicted {p['f_predicted']:.4f} ({p['branch']}), "
        f"residual {resid:+.4f} | start {start}, {secs:.0f} s, load {l0:.2f} -> {l1:.2f}"
    )

    def e(x):
        return "none" if x is None else repr(x)

    machine = (
        f"PT id={p['id']} set={p['set'].replace(' ', '_')} kind={p['kind'].split()[0]} n={n} L={L!r} lnR={lnR!r} "
        f"frac={f!r} balance={res['frac_balance']!r} vout={res['vout']!r} back={res['returned']} "
        f"eps_turn={res['eps_turn']!r} drift={res['drift']!r} passes={res['passes']} modes={res['nk']} "
        f"reg_old={c['lambda'][0]} ell_old={e(c['lambda'][1])} reg_new={c['v3'][0]} ell_new={e(c['v3'][1])} "
        f"f_pred={p['f_predicted']!r} resid={resid!r} start={start} secs={secs:.1f} load0={l0:.2f} load1={l1:.2f}"
    )
    return human + "\n" + machine


def main():
    mode = sys.argv[1]
    doc = json.load(open(PRED))
    t0 = time.time()
    print(
        f"# minform_stage2.py {mode}; start: {os.popen('uptime').read().strip()}",
        flush=True,
    )
    print(
        f"# prediction.json written {doc['written']} (at HEAD {doc['git_head']}); this run started "
        f"{datetime.datetime.now().astimezone().isoformat(timespec='seconds')}",
        flush=True,
    )
    pts = doc["points"]
    if mode == "points":
        jobs = [(p["label"], p["L"], p["lnR"], p["n"], p) for p in pts]
        s2.schedule(task, jobs)
    elif mode == "validate":
        by = {p["label"]: p for p in pts}
        jobs = []
        for lab in doc["validate"]:
            p = by[lab]
            for kind in ("classical", "born-offcurve", "convergence"):
                jobs.append((lab, p["L"], p["lnR"], p["n"], kind))
        s2.schedule(s2.task_validate, jobs)
    else:
        raise SystemExit(f"unknown mode {mode!r}")
    print(
        f"# minform_stage2.py {mode} end ({time.time() - t0:.0f} s): {os.popen('uptime').read().strip()}",
        flush=True,
    )


if __name__ == "__main__":
    main()
