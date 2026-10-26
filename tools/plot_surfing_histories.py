# (c) University of Sussex 2026
# Created by David Seery
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Plot the field and Jordan-frame temperature histories of a few beta, up to each one's first
bounce, from histories integrated here, with no Ray cluster, no datastore and no BBN.

Written 2026-10-14, after the surfing-temperature campaign closed, to show the histories near
beta = 3.0 that are not captured (.documents/surfing-temperature-verification.md §1.8, §3). Run
from the repository root, since the EOS reads its table from the working directory:

    ./venv/bin/python tools/plot_surfing_histories.py --out PREFIX \\
        [--beta-values B [B ...]] [--phi-init-Mp X] [--M M] [--T-stop-GeV T] [--N-min N] \\
        [--arrays]

Each history is tools/surfing_scan.py's `run_history`: the setup of tools/history_and_bbn.py
(Lambda = 1e-3 eV, pi* = 0, T* = 2e4 GeV), integrated by integrate_history (until
jordan-normalization prompt 07, compute_scalar_history._function, which now tunes the history),
with the IntegrationResult kept. The histories are sampled on its dense output from the start to the first
bounce, and the turning points of T_J are `surfing_turning_points`, labelled with their capture
kick by `summarise`, as the scan's CSV is. The default beta are the two captured edges of the
fine scan's uncaptured window (2.998 by QCD, 3.020 by EW) and four histories inside it.

Three panels share the e-fold axis N, which starts at --N-min (default 4, after the initial fall
from T*): T_J in GeV (log), phi in M_P, and 1 + (d ln Omega/d phi) pi on a symlog axis, which is
1 + beta pi/M_P for the exponential coupling. T_J turns where the last crosses 0. A captured
history is dashed, one that is not is solid; the T_J minima and maxima are marked, and so is the
first bounce. The kick peaks inside the T_J range are dotted lines. At most six beta are drawn,
since the colours are a fixed categorical order and are not cycled.

It writes PREFIX.pdf and PREFIX.png, and with --arrays one PREFIX_beta<B>.npz per history (N,
phi_Mp, pi_Mp, T_J_GeV). Exit status: 0 if every history ran and the figure was written, 1 if a
history failed (the others are drawn) or saving failed, 2 if the run was refused.
"""

import argparse
import os
import sys
import time
from collections import namedtuple
from math import exp
from pathlib import Path
from typing import List, Optional, Sequence

import numpy as np
from matplotlib.figure import Figure
from matplotlib.lines import Line2D

# make the repository root importable when run as `python tools/plot_surfing_histories.py`
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from extract_common import _save, find_kicks  # noqa: E402
from tools.history_and_bbn import LAMBDA_EV, PHI_INIT_MP  # noqa: E402
from tools.surfing_scan import (  # noqa: E402
    EOS_TABLE,
    M_MP_DEFAULT,
    T_STOP_GEV_DEFAULT,
    _git,
    make_setup,
    run_history,
    summarise,
)

DEFAULT_BETAS = (2.998, 3.000, 3.004, 3.012, 3.019, 3.020)
N_MIN_DEFAULT = 4.0
N_SAMPLES = 40001

# the first six slots of a validated categorical order, assigned by position, never cycled
COLOURS = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300")

# the symlog panel: linear within +-G_LINTHRESH
G_LINTHRESH = 1e-3

# one history, sampled to its first bounce
#   N, phi_Mp, pi_Mp, T_J_GeV, g:  arrays on the dense output, g = 1 + (d ln Omega/d phi) pi
#   turning_points:                (N, T_J_GeV, direction) per crossing, direction "min" or "max"
#   bounce_N, bounce_T_GeV:        the first bounce
#   capture_kick:                  the capture's kick label, or None if not captured
HistoryTrace = namedtuple(
    "HistoryTrace",
    [
        "beta",
        "N",
        "phi_Mp",
        "pi_Mp",
        "T_J_GeV",
        "g",
        "turning_points",
        "bounce_N",
        "bounce_T_GeV",
        "capture_kick",
    ],
)


def trace_history(
    setup,
    kicks,
    beta: float,
    phi_init_Mp: float,
    M_Mp: float,
    T_stop_GeV: float,
    n_samples: int = N_SAMPLES,
) -> tuple:
    """
    (trace, None) for a history that ran, or (None, failure_reason) for one that failed. The
    trace is sampled at n_samples points of the dense output from its first step to its first
    bounce.
    """
    outcome = run_history(setup, beta, phi_init_Mp, M_Mp, T_stop_GeV)
    if outcome.failure_reason is not None:
        return None, outcome.failure_reason

    units = setup.units
    GeV, Mp = units.GeV, units.PlanckMass
    solution = outcome.result.solution
    N = np.linspace(solution.ts[0], outcome.bounce.N, n_samples)
    Y = solution(N)
    phi_Mp = Y[0] / Mp
    pi_Mp = Y[1] / Mp
    T_J_GeV = np.exp(Y[4]) / GeV
    row = summarise(outcome.crossings, outcome.bounce, kicks, [], units)

    trace = HistoryTrace(
        beta=beta,
        N=N,
        phi_Mp=phi_Mp,
        pi_Mp=pi_Mp,
        T_J_GeV=T_J_GeV,
        g=1.0 + beta * pi_Mp,
        turning_points=[
            (c.N, exp(c.log_T_Jordan) / GeV, c.direction) for c in outcome.crossings
        ],
        bounce_N=outcome.bounce.N,
        bounce_T_GeV=exp(outcome.bounce.log_T_Jordan) / GeV,
        capture_kick=row["capture_kick"] if row["captured"] else None,
    )
    return trace, None


def build_figure(
    traces: Sequence[HistoryTrace],
    kick_peaks: Sequence[tuple],
    title: str,
    N_min: float = N_MIN_DEFAULT,
) -> Figure:
    """
    The three-panel figure. kick_peaks is (label, T_peak in GeV) per kick; a peak inside the
    T_J panel's range is drawn as a dotted line. Needs no cosmology and runs no history.
    """
    if len(traces) > len(COLOURS):
        raise ValueError(f"at most {len(COLOURS)} histories can be drawn")

    fig = Figure(figsize=(8, 11), layout="constrained")
    ax_T, ax_phi, ax_g = fig.subplots(3, 1, sharex=True)

    T_lo, T_hi, g_lo = np.inf, 0.0, 0.0
    handles = []
    for trace, colour in zip(traces, COLOURS):
        style = "--" if trace.capture_kick is not None else "-"
        if trace.capture_kick is not None:
            label = f"β = {trace.beta:.3f}, captured ({trace.capture_kick})"
        else:
            label = f"β = {trace.beta:.3f}, not captured"
        shown = trace.N >= N_min

        ax_T.plot(trace.N, trace.T_J_GeV, style, color=colour, lw=1.6, gid="history-T")
        ax_phi.plot(
            trace.N, trace.phi_Mp, style, color=colour, lw=1.6, gid="history-phi"
        )
        ax_g.plot(trace.N, trace.g, style, color=colour, lw=1.6, gid="history-g")
        handles.append(Line2D([], [], ls=style, color=colour, lw=1.6, label=label))

        for ax, y in (
            (ax_T, trace.bounce_T_GeV),
            (ax_phi, trace.phi_Mp[-1]),
            (ax_g, trace.g[-1]),
        ):
            ax.plot(
                [trace.bounce_N],
                [y],
                "o",
                ms=5,
                mfc="white",
                mec=colour,
                mew=1.4,
                gid="bounce",
            )
        for N_c, T_c, direction in trace.turning_points:
            ax_T.plot(
                [N_c],
                [T_c],
                "v" if direction == "min" else "^",
                ms=6,
                color=colour,
                gid="turning-point",
            )

        if np.any(shown):
            T_lo = min(T_lo, float(np.min(trace.T_J_GeV[shown])))
            T_hi = max(T_hi, float(np.max(trace.T_J_GeV[shown])))
            g_lo = min(g_lo, float(np.min(trace.g[shown])))

    N_max = max((t.bounce_N for t in traces), default=N_min + 1.0)
    ax_T.set_xlim(N_min, N_max + 0.02 * (N_max - N_min))

    if np.isfinite(T_lo) and T_hi > 0:
        ax_T.set_yscale("log")
        ax_T.set_ylim(T_lo / 1.4, T_hi * 1.4)
        for label, T_peak in kick_peaks:
            if T_lo / 1.4 < T_peak < T_hi * 1.4:
                ax_T.axhline(T_peak, color="0.6", lw=0.8, ls=":", gid="kick-peak")
                ax_T.text(
                    N_min + 0.2,
                    T_peak * 1.06,
                    f"{label} kick peak {T_peak:.3g} GeV",
                    color="0.35",
                    fontsize=8,
                )
    ax_T.set_ylabel("T_J (GeV)")

    ax_phi.set_ylabel("φ / M_P")

    ax_g.set_yscale("symlog", linthresh=G_LINTHRESH, linscale=0.5)
    ax_g.set_ylim(min(-3 * G_LINTHRESH, 1.5 * g_lo), 1.5)
    ax_g.axhline(0.0, color="0.3", lw=0.8)
    ax_g.text(
        N_min + 0.2,
        -2.2 * G_LINTHRESH,
        "below 0: faster than the kinematic velocity (captured)",
        color="0.35",
        fontsize=8,
    )
    ax_g.set_ylabel("1 + β π / M_P")
    ax_g.set_xlabel("N (e-folds from T*)")

    for ax in (ax_T, ax_phi, ax_g):
        ax.grid(color="0.92", lw=0.6)

    fig.suptitle(title, fontsize=10)
    fig.legend(
        handles=handles,
        loc="outside lower center",
        ncol=3,
        fontsize=8,
        title="▾ ▴ T_J minimum / maximum (β π/M_P = −1)    ○ first bounce",
        title_fontsize=8,
    )
    return fig


def _title(phi_init_Mp: float, M_Mp: float) -> str:
    head = _git("rev-parse", "--short=7", "HEAD")
    status = _git("status", "--porcelain", "--untracked-files=no")
    if head is None:
        provenance = "HEAD unknown"
    else:
        provenance = f"git HEAD {head.strip()}"
        if status:
            provenance += " + tracked changes"
    return (
        f"Histories to the first bounce (φ* = {phi_init_Mp:g} M_P, M = {M_Mp:g} M_P, "
        f"Λ = {LAMBDA_EV * 1e3:g} meV); {provenance}"
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plot the field and Jordan-frame temperature histories of a few beta."
    )
    parser.add_argument(
        "--beta-values",
        type=float,
        nargs="+",
        default=list(DEFAULT_BETAS),
        help="the beta to draw, at most six (default: six near 3.0)",
    )
    parser.add_argument(
        "--phi-init-Mp",
        type=float,
        default=PHI_INIT_MP,
        help="initial value of the scalar field in units of M_P (default 5, as main.py)",
    )
    parser.add_argument(
        "--M",
        type=float,
        default=M_MP_DEFAULT,
        help="the potential's M, in units of M_P (default 1e-3)",
    )
    parser.add_argument(
        "--T-stop-GeV",
        type=float,
        default=T_STOP_GEV_DEFAULT,
        help="stopping temperature in the Jordan frame, in GeV (default 1e-3)",
    )
    parser.add_argument(
        "--N-min",
        type=float,
        default=N_MIN_DEFAULT,
        help="the first e-fold drawn (default 4, after the initial fall from T*)",
    )
    parser.add_argument(
        "--arrays",
        action="store_true",
        help="also write PREFIX_beta<B>.npz per history",
    )
    parser.add_argument(
        "--out", required=True, help="path prefix; writes PREFIX.pdf and PREFIX.png"
    )
    args = parser.parse_args(argv)

    if not Path(os.getcwd(), EOS_TABLE).is_file():
        print(
            f"!! plot_surfing_histories: run from the repository root; the EOS reads "
            f"{EOS_TABLE} from the working directory",
            file=sys.stderr,
        )
        return 2
    if len(args.beta_values) > len(COLOURS):
        print(
            f"!! plot_surfing_histories: at most {len(COLOURS)} beta can be drawn, "
            f"{len(args.beta_values)} given",
            file=sys.stderr,
        )
        return 2

    start = time.time()
    setup = make_setup()
    units = setup.units
    cosmology = setup.cosmology
    kicks = find_kicks(
        cosmology,
        cosmology._eos._T_min * units.GeV,
        cosmology._eos._T_max * units.GeV,
    )
    kick_peaks = [(k["label"], k["T_peak"] / units.GeV) for k in kicks]

    Path(args.out).parent.mkdir(exist_ok=True, parents=True)
    traces: List[HistoryTrace] = []
    failed = 0
    for beta in args.beta_values:
        t0 = time.time()
        trace, reason = trace_history(
            setup, kicks, beta, args.phi_init_Mp, args.M, args.T_stop_GeV
        )
        if trace is None:
            failed += 1
            print(f"!! beta {beta:g}: history failed ({reason}); not drawn")
            continue
        traces.append(trace)
        kick = trace.capture_kick or "not captured"
        print(
            f"   beta {beta:g}: {kick}, {len(trace.turning_points)} turning points, first "
            f"bounce at N = {trace.bounce_N:.4f}, T_J = {trace.bounce_T_GeV:.4g} GeV "
            f"({time.time() - t0:.1f} s)",
            flush=True,
        )
        if args.arrays:
            np.savez(
                f"{args.out}_beta{beta:.3f}.npz",
                N=trace.N,
                phi_Mp=trace.phi_Mp,
                pi_Mp=trace.pi_Mp,
                T_J_GeV=trace.T_J_GeV,
            )

    if not traces:
        print("!! plot_surfing_histories: no history ran", file=sys.stderr)
        return 1

    fig = build_figure(
        traces, kick_peaks, _title(args.phi_init_Mp, args.M), N_min=args.N_min
    )
    if not _save(fig, Path(args.out + ".pdf")):
        return 1
    print(f"wrote {args.out}.pdf and {args.out}.png in {time.time() - start:.0f} s")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
