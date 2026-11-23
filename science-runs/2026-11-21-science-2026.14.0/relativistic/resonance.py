"""
The resonance measures of the 2026.14.0 run, by wall and M: band overlap (n_bar, delta_stim) and the phase slip.

    ./venv/bin/python science-runs/2026-11-21-science-2026.14.0/relativistic/resonance.py > science-runs/2026-11-21-science-2026.14.0/relativistic/resonance.txt

Reads only the copies of tools/quanta_measures.py's quanta_histories*.csv in inputs/<wall>/ (c_a = 1, 0 and 10), the
one-loop rows alone (the fixed law's rows carry no generations' measures). The measures are quanta_measures.py's:
- n_bar_max: the largest, over a history's generations j, of n_bar_j = sum over earlier generations g of
  A_g Omega(ln Q_g - ln Q_j), the occupation that earlier quanta leave in the band where generation j is born
  (Q = k_ex a, the comoving peak). It is small when Q moves by more than a band's width between generations;
- delta_stim = sum_j 2 n_bar_j Delta K_j / K_final, the fractional change of the final rho_chi that stimulated
  emission would make at first order with random phases;
- the phase slip |theta(x, b) - theta(a, x)| of the comoving peak's mode between consecutive bounces, taken massless
  (the mass only adds phase, so it understates the slip). Resonance needs slips below about 1 rad over many
  consecutive bounces.

Needs no Ray and no store. Written 2026-11-23 at the user's request ("Add those resonance numbers to the relativistic
NOTES"); prints the tables relativistic/NOTES.md quotes.
"""

from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
WALLS = ("exp", "pl2", "pl1")
FILES = {
    1: "quanta_histories.csv",
    0: "quanta_histories-assoc0.csv",
    10: "quanta_histories-assoc10.csv",
}
# Figure 6's beige "not at the wall" edge, computed there from the data (fig6/NOTES.md): below it the field does not
# reach the wall before delivery
WALL_EDGE_BETA = 1.06875
M_CRIT_BELOW = (
    1e-20  # block C's M (1e-15, 1e-12) lie above this; the laboratory region below
)


def load(wall, scale):
    d = pd.read_csv(HERE / "inputs" / wall / FILES[scale])
    return d[d.drain_model != "fixed"]


def by_m(wall):
    d = load(wall, 1)
    print(f"\n== {wall}, c_a = 1, one-loop law, by M")
    print(
        f"{'M/M_P':>8} {'hist':>5} {'d_stim med':>10} {'d_stim max':>10} {'nbar max':>9} {'#nbar>1':>7} "
        f"{'#nbar>0.1':>9} {'#slip<1':>7} {'min slip':>9}"
    )
    for M, g in sorted(d.groupby("M_Mp"), key=lambda t: -t[0]):
        print(
            f"{M:8.0e} {len(g):5d} {g.delta_stim.median():10.2e} {g.delta_stim.max():10.2e} "
            f"{g.n_bar_max.max():9.3g} {(g.n_bar_max > 1).sum():7d} {(g.n_bar_max > 0.1).sum():9d} "
            f"{(g.phase_slips_below_1 > 0).sum():7d} {g.phase_slip_min.min():9.2e}"
        )


def lab_region(wall):
    print(
        f"\n== {wall}, laboratory region (M < {M_CRIT_BELOW:g} M_P), one-loop law, by c_a"
    )
    for scale in (0, 1, 10):
        d = load(wall, scale)
        d = d[d.M_Mp < M_CRIT_BELOW]
        print(
            f"c_a = {scale:2d}: {len(d)} histories; delta_stim max {d.delta_stim.max():.3g}; n_bar_max max "
            f"{d.n_bar_max.max():.3g}; n_bar > 1 in {(d.n_bar_max > 1).sum()}, > 0.1 in {(d.n_bar_max > 0.1).sum()}; "
            f"slip < 1 rad in {(d.phase_slips_below_1 > 0).sum()}; least slip {d.phase_slip_min.min():.3g} rad"
        )
    d = load(wall, 1)
    d = d[(d.M_Mp < M_CRIT_BELOW) & (d.n_bar_max > 0.1)]
    if len(d):
        above = (d.beta >= WALL_EDGE_BETA).sum()
        print(
            f"n_bar_max > 0.1 at c_a = 1: {len(d)} histories, beta {d.beta.min():g}-{d.beta.max():g}, "
            f"{above} at or above the wall edge beta = {WALL_EDGE_BETA}; their delta_stim at most {d.delta_stim.max():.3g}"
        )
        print(
            d[
                [
                    "M_Mp",
                    "beta",
                    "alpha",
                    "n_bar_max",
                    "delta_stim",
                    "phase_slip_min_producing",
                ]
            ].to_string(index=False)
        )


def surfing(wall):
    d = load(wall, 1)
    d = d[(d.M_Mp < M_CRIT_BELOW) & (d.beta >= WALL_EDGE_BETA)]
    print(
        f"\n== {wall}, laboratory region, beta >= {WALL_EDGE_BETA} (at the wall), c_a = 1: {len(d)} histories; "
        f"delta_stim median {d.delta_stim.median():.3g}, max {d.delta_stim.max():.3g}; n_bar_max max {d.n_bar_max.max():.3g}"
    )


if __name__ == "__main__":
    for w in WALLS:
        by_m(w)
        lab_region(w)
        surfing(w)
