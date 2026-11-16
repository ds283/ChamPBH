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
The pieces shared by the two figure tools, tools/plot_quanta_grid.py and tools/plot_quanta_history.py (one-loop-drain
prompt 07 §1.1; README §8.15 P58, P61; orchestrator note 07 rulings 1-3, 7, 9). Nothing here opens a store or starts Ray;
the tools read 04's exports and 05's outputs (`tools/quanta_measures.py`) and draw them.

- **The regimes' colours** (`REGIMES`, `REGIME_COLOURS`): one fixed categorical order, one-loop, gap, classical,
  unassessed. A candidate row whose regime is none of the four is refused (`FigureRefused`).
- **Finding the files.** The root is one potential's directory `<output>/<label>/<model>/` (power-law-wall prompt 06;
  README §6.10 R12, P50). Its histories.csv must have the `model` column (P51): `read_histories` refuses a file
  without it as unreadable, naming the column. A history's csv directory is
  `quanta_measures.history_dir`; its plots directory is the same path with the `csv` component replaced by `plots`
  (`plots_dir`), as the plot scripts write it; a row's directory is `extract_common.alpha_path`. `to_output` maps a
  path below the root to the same path below `--output`. No path is formatted inline.
- **The model check** (`check_model_rows`, `read_checked`): every row of every quanta file names the expected
  `spectrum_model`, the one of its row's wall (`ComputeTargets.quanta_spectrum.SPECTRUM_MODELS` of
  `quanta_measures.wall_of` of the histories.csv row, from its `model` and `n`; power-law-wall prompt 06, P53), and the
  `assoc_scale` asked for, compared as `float(cell) == scale` (ruling 3). A per-history file expects its history's
  model; quanta_histories.csv expects, row by row, the model of the histories.csv row of the same key
  (`expected_models`). A file that fails is refused with the reason.
- **The rows to draw** (`classify`, `walk`): a histories.csv row is counted in the first of these that it meets, and only
  there (ruling 1): `fixed` (`drain_model` is `fixed`, marker or not); `marker` (`failure_reasons` is exactly
  `MARKER_REASON` and `normalization` is blank); `failed` (any other row with a blank `normalization`); `drawn`.
- **Saving** (`save_figure`): the PDF and the PNG at 150 dpi, creating the directories it writes into.
"""

import csv
import math
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import numpy as np

from ComputeTargets import quanta_spectrum as qs
from CosmologyConcepts.drain_model import DRAIN_MODEL_FIXED
from tools import quanta_measures as qm

# the plot script's marker of a grid point with no history (plot_by_beta.py)
MARKER_REASON = "no ScalarHistory row in the store"

# a fixed categorical order, the same in both tools (an Okabe-Ito subset: distinguishable by the colour-blind, and none
# of them the grey that marks a blank or zero cell)
REGIMES = ("one_loop", "gap", "classical", "unassessed")
REGIME_COLOURS = {
    "one_loop": "#0072B2",
    "gap": "#E69F00",
    "classical": "#CC79A7",
    "unassessed": "#009E73",
}
REGIME_LABELS = {
    "one_loop": "one-loop",
    "gap": "gap",
    "classical": "classical",
    "unassessed": "unassessed",
}

PNG_DPI = 150


class FigureRefused(Exception):
    """A figure the tool will not draw, with the reason (the history or the A is refused, the others are drawn)."""


class InputDefect(FigureRefused):
    """A value no tool input can hold (a negative on a log panel, a non-finite cell, ln n_g out of range): a defect of
    04's or 05's output, not of the figure (orchestrator note 07, rulings 4 and 5; prompt §7).
    """


# ---------------------------------------------------------------------------------------------------------------------
# The rows to draw (ruling 1)
# ---------------------------------------------------------------------------------------------------------------------

FIXED, MARKER, FAILED, DRAWN = "fixed", "marker", "failed", "drawn"


def classify(row: dict) -> str:
    """The first of fixed, marker, failed, drawn that the histories.csv row meets (ruling 1)."""
    if row["drain_model"] == DRAIN_MODEL_FIXED:
        return FIXED
    if row.get("normalization", "") == "":
        if row.get("failure_reasons", "") == MARKER_REASON:
            return MARKER
        return FAILED
    return DRAWN


def walk(rows: Sequence[dict]) -> Dict[str, List[dict]]:
    """The rows of histories.csv by class, each list in file order."""
    out: Dict[str, List[dict]] = {FIXED: [], MARKER: [], FAILED: [], DRAWN: []}
    for r in rows:
        out[classify(r)].append(r)
    return out


def read_histories(root: Path) -> List[dict]:
    """histories.csv of the root; raises OSError, csv.Error or UnicodeDecodeError, which the tools map to exit 2, and
    csv.Error for a file without the `model` column (power-law-wall prompt 06, P51), naming it.
    """
    columns, rows = qm.read_csv(Path(root) / "histories.csv")
    if qm.MODEL_COLUMN not in columns:
        raise csv.Error(
            f"it has no {qm.MODEL_COLUMN!r} column, which names the potential whose spectrum each row takes "
            f"(power-law-wall prompt 06)"
        )
    return rows


def spectrum_model_of(row: dict) -> str:
    """The spectrum model of a histories.csv row's wall (quanta_measures.wall_of); FigureRefused with the reason
    if the row has none (power-law-wall prompt 06, P53)."""
    try:
        return qs.spectrum_model(qm.wall_of(row))
    except qm.Refused as e:
        raise FigureRefused(str(e))


def expected_models(histories: Sequence[dict]) -> Callable[[dict], str]:
    """
    The expected spectrum model of each row of a quanta_histories file, by its key (`key_of`): the model of the
    histories.csv row of the same key (power-law-wall prompt 06, P53). A row whose key no histories.csv row has, or
    whose histories.csv row has no wall, is FigureRefused.
    """
    by_key = {key_of(h): h for h in histories}

    def expected(row: dict) -> str:
        h = by_key.get(key_of(row))
        if h is None:
            raise FigureRefused(f"the key {key_of(row)} is no row of histories.csv")
        return spectrum_model_of(h)

    return expected


def models_text(rows: Iterable[dict]) -> str:
    """The spectrum models of the walls of the rows of histories.csv, distinct and in the order met, for a printed
    line (quanta_measures.root_models_text: every row, whatever its law; 'no spectrum' if none has a wall).
    """
    return qm.root_models_text(list(rows))


def key_of(row: dict) -> Tuple[str, ...]:
    """The five key cells as strings, as 05 copies them verbatim into its outputs."""
    return tuple(row[c] for c in qm.KEY_COLUMNS)


def is_close(value: float, cell: str) -> bool:
    """A value typed on the command line against a histories.csv cell (ruling 2): `isclose` at rel_tol 1e-9."""
    try:
        return math.isclose(value, float(cell), rel_tol=1e-9)
    except ValueError:
        return False


# ---------------------------------------------------------------------------------------------------------------------
# The model check (ruling 3)
# ---------------------------------------------------------------------------------------------------------------------


def check_model_rows(
    rows: Sequence[dict],
    scale: float,
    what: str,
    expected: Union[str, Callable[[dict], str]],
) -> None:
    """
    Every row names its expected spectrum model and `scale`, or FigureRefused says which row and why. `expected` is the
    model of every row (a per-history file's, its history's wall's), or a function giving each row's
    (`expected_models`, for quanta_histories); no wall is assumed (power-law-wall prompt 06, P53).
    """
    for k, r in enumerate(rows):
        model = r.get("spectrum_model")
        want = expected if isinstance(expected, str) else expected(r)
        if model != want:
            raise FigureRefused(
                f"{what}: row {k + 2} names spectrum_model {model!r}, not {want!r}"
            )
        cell = r.get("assoc_scale")
        try:
            ok = float(cell) == scale
        except (TypeError, ValueError):
            ok = False
        if not ok:
            raise FigureRefused(
                f"{what}: row {k + 2} names assoc_scale {cell!r}, not {scale:g}"
            )


def read_checked(
    path: Path, scale: float, expected: Union[str, Callable[[dict], str]]
) -> List[dict]:
    """The rows of a quanta file, after the model check (`check_model_rows`, against `expected`); FigureRefused if it
    cannot be read or fails the check."""
    try:
        _, rows = qm.read_csv(path)
    except (OSError, csv.Error, UnicodeDecodeError) as e:
        raise FigureRefused(f"cannot read {path}: {e}")
    check_model_rows(rows, scale, path.name, expected)
    return rows


# ---------------------------------------------------------------------------------------------------------------------
# Where things go (ruling 7, 9)
# ---------------------------------------------------------------------------------------------------------------------


def to_output(root: Path, path: Path, output: Optional[Path]) -> Path:
    """`path`, below the root, mapped to the same path below `output` (the path itself if there is no output)."""
    if output is None:
        return Path(path)
    return Path(output) / Path(path).relative_to(Path(root))


def plots_dir(root: Path, csv_directory: Path) -> Path:
    """A history's csv directory with its `csv` component replaced by `plots` (as the plot scripts write it)."""
    rel = Path(csv_directory).relative_to(Path(root))
    parts = list(rel.parts)
    i = parts.index("csv")
    return Path(root) / Path(*parts[:i], "plots", *parts[i + 1 :])


def save_figure(fig, stem: Path) -> Tuple[Path, Path]:
    """`stem`.pdf and `stem`.png (150 dpi), creating the directory; returns the two paths."""
    stem = Path(stem)
    stem.parent.mkdir(parents=True, exist_ok=True)
    pdf = stem.with_name(stem.name + ".pdf")
    png = stem.with_name(stem.name + ".png")
    fig.savefig(pdf)
    fig.savefig(png, dpi=PNG_DPI)
    return pdf, png


# ---------------------------------------------------------------------------------------------------------------------
# Reading cells
# ---------------------------------------------------------------------------------------------------------------------


def number(cell: Optional[str]) -> float:
    """A cell as a float, nan for a blank."""
    if cell is None or cell == "":
        return math.nan
    return float(cell)


def column(rows: Iterable[dict], name: str) -> np.ndarray:
    """A column of rows as a float array, nan where blank; FigureRefused if the column is missing."""
    try:
        return np.array([number(r[name]) for r in rows], dtype=float)
    except KeyError:
        raise FigureRefused(f"a column {name!r} is missing")
