"""
reflection-restart prompt 04 §1.2 (c): where in the history the wall search meets a small margin. Reads the calls that
candidates.py recorded (var/reflection-restart/04/candidates-<tree>-<serial>.npz, one row per call of
classical_turning_point, in call order) for 2197 and 2188 on both trees, and reports:

- the infalls: assess_infall is called at every step start with pi < 0, so the calls of one infall run inwards
  (phi falling) and a new infall starts where phi rises from the previous call (here: by more than 1.5x);
- the least margin of each tree's run, whether its call is the first of an infall, and its neighbours;
- the infall starts whose ln E lies more than 3 below the median of the 21 starts around them ("outliers"), each with
  its neighbours' start, second call and last call, so that an outlier's energy can be set against the cascade's:
  the energy of an infall is carried by its later calls; the first call's is the kinetic energy at the first step
  start after the plateau turn, where pi has just changed sign.

Reads only. Run from the repository root:

    ./venv/bin/python .documents/reflection-restart/close/infall_starts.py
"""

import numpy as np

D = "var/reflection-restart/04"
TREES = (("89a0760", "89a0760 (before 03)"), ("741dcce", "this branch (03's code)"))


def infalls(phi):
    starts = np.nonzero(phi[1:] > phi[:-1] * 1.5)[0] + 1
    starts = np.r_[0, starts]
    ends = np.r_[starts[1:] - 1, len(phi) - 1]
    return starts, ends


def context(phi, lE, m, starts, ends, k, width=3):
    lines = []
    for j in range(max(0, k - width), min(len(starts), k + width + 1)):
        a, b = starts[j], ends[j]
        second = f"{lE[a + 1]:9.3f}" if b > a else "        —"
        lines.append(
            f"     {'*' if j == k else ' '}{j:5d}  phi {phi[a]:.4e}  start ln E {lE[a]:9.3f}  second {second}  "
            f"last {lE[b]:9.4f}  calls {b - a + 1:4d}  start margin {m[a]:.4g}"
        )
    return lines


def main():
    for s in (2197, 2188):
        for tag, name in TREES:
            z = np.load(f"{D}/candidates-{tag}-{s}.npz")
            phi, lE, m, out = z["phi"], z["log_E"], z["margin"], z["outcome"]
            starts, ends = infalls(phi)
            ls = lE[starts]
            print(
                f"== {s}, {name}: {len(phi)} calls, {len(starts)} infalls; outcomes none/point/raise "
                f"{int(np.sum(out == 0))}/{int(np.sum(out == 1))}/{int(np.sum(out == 2))}"
            )
            j = int(np.argmin(m))
            k = int(np.searchsorted(starts, j, side="right") - 1)
            print(
                f"   least margin {m[j]:.6g} quanta at call {j} (phi {phi[j]:.6e}, ln E {lE[j]:.6f}), the "
                f"{'first' if starts[k] == j else 'call ' + str(j - starts[k] + 1)} call of infall {k}:"
            )
            print("\n".join(context(phi, lE, m, starts, ends, k)))
            print(
                f"   least margin over infall starts {m[starts].min():.6g}; over later calls "
                f"{np.delete(m, starts).min() if len(m) > len(starts) else float('nan'):.6g}"
            )
            outliers = [
                q
                for q in range(len(starts))
                if ls[q] < np.median(ls[max(0, q - 10) : q + 11]) - 3.0
            ]
            print(
                f"   outlier starts (ln E more than 3 below the local median of 21 starts): {len(outliers)}"
            )
            for q in outliers:
                med = np.median(ls[max(0, q - 10) : q + 11])
                print(
                    f"   - infall {q}: start ln E {ls[q]:.3f} against local median {med:.3f}"
                )
                print("\n".join(context(phi, lE, m, starts, ends, q)))
            print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
