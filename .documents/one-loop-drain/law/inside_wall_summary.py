"""
one-loop-drain prompt 01 §1.7: the summary of inside_wall_3873.py's two runs of 3873 (var/one-loop-drain/01/
oneloop/3873-inside-wall-*.json): the reflections asked for, by branch; W/(pi^2/2) at each; the failing reflection,
its branch and the assessment it was judged on; and the decisions of its infall, step start by step start.

    PYTHONPATH=. ./venv/bin/python .documents/one-loop-drain/law/inside_wall_summary.py > .documents/one-loop-drain/law/out/inside_wall_3873.txt
"""

import json
import statistics
import os
import sys
from pathlib import Path

RUNS = Path(
    os.environ.get(
        "ONE_LOOP_DRAIN_01_RUNS",
        "/Users/ds283/Documents/Code/ChamPBH/var/one-loop-drain/01/oneloop",
    )
)


def main():
    for name in ("3873-inside-wall-one_loop-A1", "3873-inside-wall-fixed-a0.03"):
        d = json.loads((RUNS / f"{name}.json").read_text())
        g = d["guards"]
        ratios = [x["ratio"] for x in g]
        print(
            f"== {name}: law {d['law']}, A/alpha {d['A']}, at the stored normalization {d['normalization']!r}"
        )
        print(
            f"   outcome: {'failure: ' + d['failure_reason'] if d['failure'] else 'success'}; wall {d['wall_s']:.1f} s"
        )
        if not d["failure"]:
            print(f"   parking N {d['parking_N']!r}; reflections {d['reflections']}")
        print(f"   drain record: {d['drain']}")
        print(
            f"   reflections asked for: {d['reflections_asked']} ({d['branches']}); W/(pi^2/2): max {max(ratios):.6g}, "
            f"median {statistics.median(ratios):.6g}; above 0.5 at {sum(r > 0.5 for r in ratios)}, above 1 at "
            f"{sum(r > 1.0 for r in ratios)}; floor-branch max {max((x['ratio'] for x in g if x['branch'] == 'floor'), default=None)}"
        )
        unresolved = [x for x in g if x["branch"] == "unresolved"]
        late = [
            x
            for x in unresolved
            if sum(1 for e in x["infall_decisions"] if e["phi_over_phi_t"] < 3.0) > 1
        ]
        print(
            f"   unresolved reflections: {len(unresolved)}; fired at the first step start below 3 phi_t: "
            f"{len(unresolved) - len(late)}; later (phi_t still above the threshold there): {len(late)}"
        )
        if unresolved:
            print(
                f"   at the unresolved triggers: phi/phi_t in [{min(x['phi_over_phi_t'] for x in unresolved):.4f}, "
                f"{max(x['phi_over_phi_t'] for x in unresolved):.4f}]; ln W - ln E in "
                f"[{min(x['log_W_minus_assessed_log_E'] for x in unresolved):.4f}, "
                f"{max(x['log_W_minus_assessed_log_E'] for x in unresolved):.4f}]; ln R in "
                f"[{min(x['log_R'] for x in unresolved):.2f}, {max(x['log_R'] for x in unresolved):.2f}]; "
                f"phi_t/threshold in [{min(x['phi_t_Mp'] for x in unresolved) / 1e-8:.4g}, "
                f"{max(x['phi_t_Mp'] for x in unresolved) / 1e-8:.4g}]"
            )
        print("   the last six reflections asked for:")
        for x in g[-6:]:
            print(
                f"     N = {x['N']:.10g}: {x['branch']}, phi = {x['phi_Mp']:.6g} M_P, phi/phi_t = {x.get('phi_over_phi_t', float('nan')):.4f}, "
                f"phi_t = {x.get('phi_t_Mp', float('nan')):.6g} M_P, ln R = {x.get('log_R', float('nan')):.3f}, "
                f"ln W - ln E = {x.get('log_W_minus_assessed_log_E', float('nan')):.4f}, W/(pi^2/2) = {x['ratio']:.6g}, "
                f"ln E (step start's assessment: {x['step_start_assessed']}) = {x.get('assessed_log_E', float('nan')):.6f}, "
                f"passed {x['passed']}"
            )
        if d["failure"]:
            x = g[-1]
            print(
                "   the failing infall's decisions, step start by step start (the last 30):"
            )
            for e in x["infall_decisions"][-30:]:
                print(
                    f"     phi/phi_t = {e['phi_over_phi_t']:.6g}, phi_t/threshold = {e['phi_t_over_threshold']:.6g}, "
                    f"ln R = {e['log_R']:.4f}, fired {e['fired']}"
                )
        print()


if __name__ == "__main__":
    sys.exit(main())
