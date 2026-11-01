"""
The 48 points of the inelastic-reflection measurement (prompt 11; README §8.2 P40, §8.3 U10).

Each point is one run of `tools/history_and_bbn.py`, tuned (the tool's default, as main.py), at
phi* = 5 M_P (the tool's default) on the small network (the tool's default):
- natural mode (alpha = 0): M in {0.5, 1e-7} M_P x beta in {1.2, 2, 3, 5}, 8 points;
- dissipation mode: M in {4e-31, 1e-12} M_P x the same beta x alpha in {0.03, 0.1, 0.3, 0.9},
  32 points;
- the parking epsilon (README §8.2 P42): epsilon in {1e-4, 1e-8} at M = 4e-31 M_P, alpha = 0.1,
  each beta, by `eps_point.py`, 8 points. epsilon = 1e-6 there is the grid's own dissipation point.

`POINTS` is the list, in the order `run_grid.py` starts them. Each is a `Point` with its slug, its
kind ("natural", "dissipation" or "eps"), its parameters and its command, an argv list run from the
repository root with PYTHONPATH=. (`run_grid.py`). Slugs read `nat-M0.5-b2`, `dis-M4e-31-b2-a0.1`
and `eps1e-08-M4e-31-b2-a0.1`, every number formatted `:g`.

Run as a script it prints the points, one per line, and writes nothing:

    PYTHONPATH=. ./venv/bin/python .documents/inelastic-reflection/measure/points.py

It reads and writes no file. The runner that uses it writes only under var/inelastic-reflection-11/.
"""

from collections import namedtuple

PYTHON = "./venv/bin/python"
TOOL = "tools/history_and_bbn.py"
EPS_POINT = ".documents/inelastic-reflection/measure/eps_point.py"

BETAS = (1.2, 2.0, 3.0, 5.0)
NATURAL_M = (0.5, 1.0e-7)
DISSIPATION_M = (4.0e-31, 1.0e-12)
ALPHAS = (0.03, 0.1, 0.3, 0.9)
EPS_M = 4.0e-31
EPS_ALPHA = 0.1
EPSILONS = (1.0e-4, 1.0e-8)

Point = namedtuple(
    "Point", ["slug", "kind", "M", "beta", "alpha", "epsilon", "command"]
)


def _g(x: float) -> str:
    return f"{x:g}"


def _points():
    points = []
    # natural mode first: its points at M = 1e-7 are the slowest (README §8.2 P40)
    for M in NATURAL_M:
        for beta in BETAS:
            points.append(
                Point(
                    f"nat-M{_g(M)}-b{_g(beta)}",
                    "natural",
                    M,
                    beta,
                    0.0,
                    None,
                    ["-u", TOOL, _g(beta), _g(M)],
                )
            )
    for M in DISSIPATION_M:
        for beta in BETAS:
            for alpha in ALPHAS:
                points.append(
                    Point(
                        f"dis-M{_g(M)}-b{_g(beta)}-a{_g(alpha)}",
                        "dissipation",
                        M,
                        beta,
                        alpha,
                        None,
                        ["-u", TOOL, _g(beta), _g(M), "--alpha", _g(alpha)],
                    )
                )
    for eps in EPSILONS:
        for beta in BETAS:
            points.append(
                Point(
                    f"eps{_g(eps)}-M{_g(EPS_M)}-b{_g(beta)}-a{_g(EPS_ALPHA)}",
                    "eps",
                    EPS_M,
                    beta,
                    EPS_ALPHA,
                    eps,
                    [
                        "-u",
                        EPS_POINT,
                        _g(eps),
                        "--",
                        _g(beta),
                        _g(EPS_M),
                        "--alpha",
                        _g(EPS_ALPHA),
                    ],
                )
            )
    return points


POINTS = _points()
BY_SLUG = {p.slug: p for p in POINTS}

assert len(POINTS) == 48 and len(BY_SLUG) == 48


def command_line(point: Point) -> list:
    """The full argv of a point, from the repository root."""
    return [PYTHON, *point.command]


if __name__ == "__main__":
    for p in POINTS:
        print(f"{p.slug:28s} {p.kind:12s} {' '.join(command_line(p))}")
    print(f"{len(POINTS)} points")
