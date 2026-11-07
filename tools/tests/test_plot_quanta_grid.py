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
tools/plot_quanta_grid.py (one-loop-drain prompt 07 §1.2, §1.5; orchestrator note 07 rulings 2-4, 7, 9), on the constructed
exports of tests/test_quanta_figures.py (`ExportMixin`), drawn with the Figure of each panel read back (images, patches and
lines by gid, the norms, the colour bars):

- the ten panels in order, with their columns, scales and norm types and limits (linear 0-1, log10, linear, diverging
  symmetric about 0 and not TwoSlopeNorm, log10 from quanta_histories);
- the cell count of each panel against the constructed histories of that A; a blank hatched, a zero solid grey, a failed
  history crossed in every panel, a drawn history with no quanta_histories row crossed in panels (h)-(j) only; each counted;
- marker and fixed rows skipped and counted; two A give two figures; an A not in histories.csv exits 2;
- the degenerate colour ranges (none, one value) of ruling 4; a negative value on a log10 panel refuses the figure; two rows
  in one cell refuse the A's figure;
- `--assoc-scale` reads and writes the suffixed names; a file naming another model refuses every figure; `--output` writes
  below the other root and nothing below the input; the PDF and the PNG are both written; an unreadable root exits 2.

No Ray, no store, no PRyMordial solve.
"""

import contextlib
import io
import unittest
from pathlib import Path

import numpy as np
from matplotlib.colors import LogNorm, Normalize, TwoSlopeNorm

from tools import plot_quanta_grid as pg
from tools import quanta_figures as qf
from tools.tests.test_quanta_figures import ExportMixin, capturing

LETTERS = "abcdefghij"
LOG_PANELS = "bchij"


def panel(fig, letter):
    (ax,) = [a for a in fig.axes if a.get_gid() == f"panel-{letter}"]
    return ax


def by_gid(ax, gid):
    return [a for a in list(ax.patches) + list(ax.lines) if a.get_gid() == gid]


def run_tool(root, **kw):
    out = io.StringIO()
    p, figures = capturing(pg)
    with p:
        rc = pg.run(root, out=out, **kw)
    return rc, out.getvalue(), figures


class TestThePanels(ExportMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.build()
        rc, self.text, self.figures = run_tool(self.root, A_values=[1.0])
        self.assertEqual(rc, 0, self.text)
        ((self.fig, self.stem),) = self.figures

    def test_ten_panels_in_order_with_their_titles(self):
        axes = [
            a
            for a in self.fig.axes
            if (a.get_gid() or "").startswith("panel-") and len(a.get_gid()) == 7
        ]
        self.assertEqual([a.get_gid()[-1] for a in axes], list(LETTERS))
        expected = [
            "drained_share_one_loop",
            "drained_share_gap + drained_share_classical + drained_share_unassessed",
            "parking_share",
            "ell_min",
            "all_worst_birth_margin",
            "all_worst_reflection_margin",
            "all_worst_tracker_margin",
            "delta_stim",
            "E_field_max",
            "backreaction_max",
        ]
        for ax, letter, name in zip(axes, LETTERS, expected):
            self.assertTrue(
                ax.get_title().startswith(f"({letter}) {name}"), ax.get_title()
            )
            self.assertEqual(len(ax.images), 1)
            # a colour bar for each, with its own axes
            self.assertEqual(
                len(
                    [
                        a
                        for a in self.fig.axes
                        if a.get_gid() == f"panel-{letter}-colourbar"
                    ]
                ),
                1,
            )

    def test_the_scales(self):
        for letter in LETTERS:
            norm = panel(self.fig, letter).images[0].norm
            if letter in LOG_PANELS:
                self.assertIsInstance(norm, LogNorm, letter)
            elif letter == "a":
                self.assertIs(type(norm), Normalize)
                self.assertEqual((norm.vmin, norm.vmax), (0.0, 1.0))
            elif letter == "d":
                self.assertIs(type(norm), Normalize)
            else:
                self.assertIs(type(norm), Normalize)
                self.assertNotIsInstance(norm, TwoSlopeNorm)
                self.assertEqual(norm.vmin, -norm.vmax)
                self.assertGreater(norm.vmax, 0.0)

    def test_a_log_range_is_the_finite_positive_values(self):
        for letter, name in (
            ("h", "delta_stim"),
            ("i", "E_field_max"),
            ("j", "backreaction_max"),
        ):
            norm = panel(self.fig, letter).images[0].norm
            vals = [
                float(r[name])
                for r in self.read(self.root / "quanta_histories.csv")
                if r["alpha"] == "1.0" and r[name] != ""
            ]
            self.assertGreater(max(vals), min(vals), name)
            self.assertAlmostEqual(norm.vmin / min(vals), 1.0)
            self.assertAlmostEqual(norm.vmax / max(vals), 1.0)
            self.assertNotIn(letter, self.fig.quanta_degenerate)

    def test_the_diverging_range_is_the_largest_magnitude(self):
        # beta - 2.5 for beta = 2 and 3 at M = 1e-20 (the third history's margins are 0, the fourth is not measured but drawn)
        norm = panel(self.fig, "e").images[0].norm
        self.assertAlmostEqual(norm.vmax, 0.5)
        norm = panel(self.fig, "f").images[0].norm
        self.assertAlmostEqual(norm.vmax, 2.0)

    def test_axes_are_sorted_categories(self):
        ax = panel(self.fig, "a")
        # beta 2, 3, 4 (the failed one) across; M 1e-20, 2e-20 up
        self.assertEqual([t.get_text() for t in ax.get_xticklabels()], ["2", "3", "4"])
        self.assertEqual(
            [t.get_text() for t in ax.get_yticklabels()], ["1e-20", "2e-20"]
        )
        self.assertEqual(ax.images[0].origin, "lower")
        self.assertEqual(ax.images[0].get_array().shape, (2, 3))

    def test_cell_counts_equal_the_constructed_histories_of_the_A(self):
        # A = 1: drawn 4 (three measured, one with no quanta row), failed 1: five cells in every panel
        for letter in LETTERS:
            n = self.fig.quanta_counts[letter]
            self.assertEqual(sum(n.values()), 5, (letter, n))
            ax = panel(self.fig, letter)
            im = ax.images[0].get_array()
            drawn_cells = (
                int(im.count())
                + len(by_gid(ax, "blank"))
                + len(by_gid(ax, "zero"))
                + sum(
                    len(by_gid(ax, g)[0].get_xdata())
                    for g in ("cross-failed", "cross-noquanta")
                )
            )
            self.assertEqual(drawn_cells, 5, letter)
        self.assertEqual(self.fig.quanta_totals, dict(drawn=4, failed=1, noquanta=1))

    def test_a_failed_history_is_crossed_in_every_panel(self):
        for letter in LETTERS:
            (cross,) = by_gid(panel(self.fig, letter), "cross-failed")
            # beta = 4 is the third category, M = 1e-20 the first
            self.assertEqual(
                (list(cross.get_xdata()), list(cross.get_ydata())), ([2], [0]), letter
            )
            self.assertEqual(cross.get_marker(), "x")

    def test_no_quanta_row_is_crossed_in_h_to_j_only(self):
        for letter in LETTERS:
            (cross,) = by_gid(panel(self.fig, letter), "cross-noquanta")
            if letter in "hij":
                # beta = 3, M = 2e-20: the second category, the second
                self.assertEqual(
                    (list(cross.get_xdata()), list(cross.get_ydata())),
                    ([1], [1]),
                    letter,
                )
            else:
                self.assertEqual(len(cross.get_xdata()), 0, letter)

    def test_a_blank_is_hatched_grey_and_counted(self):
        # ell_min is blank for beta = 3, M = 1e-20 (panel d only), and the history with no quanta row has ell_min
        ax = panel(self.fig, "d")
        blanks = by_gid(ax, "blank")
        self.assertEqual(len(blanks), 1)
        self.assertTrue(blanks[0].get_hatch())
        self.assertEqual(blanks[0].get_xy(), (0.5, -0.5))
        self.assertEqual(self.fig.quanta_counts["d"]["blank"], 1)
        # no other panel has a blank, but h-j do not see the no-quanta history as one
        for letter in "abcefghij":
            self.assertEqual(self.fig.quanta_counts[letter]["blank"], 0, letter)

    def test_a_zero_on_a_log_panel_is_solid_grey_and_counted(self):
        # panel c: parking_share is 0 in every constructed A = 1 history (4 drawn)
        ax = panel(self.fig, "c")
        zeros = by_gid(ax, "zero")
        self.assertEqual(len(zeros), 4)
        self.assertFalse(zeros[0].get_hatch())
        self.assertEqual(self.fig.quanta_counts["c"]["zero"], 4)
        # panel b: the sum of the three shares is 0 for beta = 3, M = 1e-20 only
        self.assertEqual(self.fig.quanta_counts["b"]["zero"], 1)
        # a zero is a value, not a special cell, on the linear panels
        self.assertEqual(self.fig.quanta_counts["a"]["zero"], 0)
        self.assertEqual(self.fig.quanta_counts["e"]["zero"], 0)
        # the zero cells are not in the image
        self.assertEqual(ax.images[0].get_array().count(), 0)

    def test_the_footer_and_the_printed_line_give_the_counts(self):
        texts = " ".join(t.get_text() for t in self.fig.texts)
        for needle in (
            "drawn 4",
            "failed 1",
            "no quanta_histories row 1",
            "A = 1",
            "U4-U17-v1",
            "c_a = 1",
            "one-loop-measured-v1",
        ):
            self.assertIn(needle, texts)
        self.assertIn("drawn 4, failed 1, no quanta row 1", self.text)
        self.assertIn(
            "fixed 2, marker 1, failed 1, drawn 6", self.text
        )  # ruling 1's order, the whole file

    def test_the_output_names(self):
        self.assertEqual(self.stem.name, "quanta_grid")
        self.assertEqual(
            self.stem.relative_to(self.root).parts[:3],
            ("one-loop-measured-v1", "A=1", "plots"),
        )


class TestTheRun(ExportMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.build()

    def test_two_A_give_two_figures_and_skips_are_counted(self):
        rc, text, figures = run_tool(self.root)
        self.assertEqual(rc, 0, text)
        self.assertEqual(len(figures), 2)
        self.assertEqual(
            sorted(s.parent.parent.name for _, s in figures), ["A=1", "A=2"]
        )
        self.assertIn("figures written 2", text)
        self.assertIn("skipped as fixed-law 2, marker rows 1", text)
        counts = {s.parent.parent.name: f.quanta_totals for f, s in figures}
        self.assertEqual(counts["A=2"], dict(drawn=2, failed=0, noquanta=0))

    def test_an_A_not_in_histories_exits_2(self):
        out = io.StringIO()
        with contextlib.redirect_stderr(out):
            rc, text, figures = run_tool(self.root, A_values=[7.0])
        self.assertEqual(rc, 2)
        self.assertEqual(figures, [])
        # a fixed-law A is no one-loop A
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(run_tool(self.root, A_values=[0.1])[0], 2)

    def test_an_A_matches_by_isclose(self):
        rc, text, figures = run_tool(self.root, A_values=[2.0000000001])
        self.assertEqual((rc, len(figures)), (0, 1))

    def test_both_files_are_written_and_nothing_else(self):
        rc = pg.run(self.root, A_values=[2.0], out=io.StringIO())
        self.assertEqual(rc, 0)
        plots = self.root / "one-loop-measured-v1/A=2/plots"
        self.assertEqual(
            sorted(p.name for p in plots.iterdir()),
            ["quanta_grid.pdf", "quanta_grid.png"],
        )

    def test_assoc_scale_reads_and_writes_the_suffixed_names(self):
        for c, tag in ((0.0, "-assoc0"), (10.0, "-assoc10")):
            rc = pg.run(self.root, A_values=[2.0], assoc_scale=c, out=io.StringIO())
            self.assertEqual(rc, 0)
            plots = self.root / "one-loop-measured-v1/A=2/plots"
            self.assertTrue((plots / f"quanta_grid{tag}.png").is_file())
            self.assertTrue((plots / f"quanta_grid{tag}.pdf").is_file())
        self.assertFalse((plots / "quanta_grid.png").exists())

    def test_the_wrong_scale_refuses_every_figure(self):
        # the c_a = 0 file renamed as the c_a = 1 one
        (self.root / "quanta_histories.csv").write_bytes(
            (self.root / "quanta_histories-assoc0.csv").read_bytes()
        )
        rc, text, figures = run_tool(self.root)
        self.assertEqual(rc, 1)
        self.assertEqual(figures, [])
        self.assertIn("assoc_scale", text)

    def test_a_missing_quanta_histories_file_writes_no_figure(self):
        (self.root / "quanta_histories.csv").unlink()
        rc, text, figures = run_tool(self.root)
        self.assertEqual((rc, figures), (1, []))
        self.assertIn("no figure is written", text)

    def test_output_writes_below_the_other_root_and_nothing_below_the_input(self):
        before = sorted(str(p) for p in self.root.rglob("*"))
        other = self._dir / "elsewhere" / "QCD"
        rc = pg.run(self.root, output=other, out=io.StringIO())
        self.assertEqual(rc, 0)
        self.assertEqual(sorted(str(p) for p in self.root.rglob("*")), before)
        written = sorted(
            str(p.relative_to(other)) for p in other.rglob("*") if p.is_file()
        )
        self.assertEqual(
            written,
            [
                "one-loop-measured-v1/A=1/plots/quanta_grid.pdf",
                "one-loop-measured-v1/A=1/plots/quanta_grid.png",
                "one-loop-measured-v1/A=2/plots/quanta_grid.pdf",
                "one-loop-measured-v1/A=2/plots/quanta_grid.png",
            ],
        )

    def test_main_maps_argparse_and_an_unreadable_root_to_2(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(pg.main([str(self._dir / "absent")]), 2)
            self.assertEqual(pg.main([]), 2)
            self.assertEqual(pg.main([str(self.root), "--assoc-scale", "-1"]), 2)
            # a directory with no histories.csv
            empty = self._dir / "empty"
            empty.mkdir()
            self.assertEqual(pg.main([str(empty)]), 2)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(pg.main(["--help"]), 0)

    def test_main_draws(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(pg.main([str(self.root), "--A-values", "2"]), 0)
        self.assertTrue(
            (self.root / "one-loop-measured-v1/A=2/plots/quanta_grid.png").is_file()
        )


class TestSpecialRanges(ExportMixin, unittest.TestCase):
    """Ruling 4's degenerate ranges, on cells built directly."""

    @staticmethod
    def cells(values, failed=False):
        out = []
        for k, v in enumerate(values):
            row = dict(
                beta=str(1 + k),
                M_Mp="1e-20",
                drain_model="one-loop-measured-v1",
                alpha="1.0",
                n="1",
            )
            for c in (
                "drained_share_one_loop",
                "drained_share_gap",
                "drained_share_classical",
                "drained_share_unassessed",
                "parking_share",
                "ell_min",
                "all_worst_birth_margin",
                "all_worst_reflection_margin",
                "all_worst_tracker_margin",
            ):
                row[c] = "" if v is None else repr(v)
            q = dict(
                delta_stim=repr(1.0),
                E_field_max="",
                backreaction_max=repr(v if v is not None else 0.0),
            )
            out.append(pg.Cell(row, q, failed))
        return out

    def fig(self, values):
        return pg.build_figure(
            self.cells(values), Path("/r"), "one-loop-measured-v1", 1.0, 1.0
        )

    def test_no_positive_value_has_no_ticks_and_says_so(self):
        fig = self.fig([0.0, 0.0])
        self.assertEqual(fig.quanta_degenerate["c"], "none")
        self.assertIn("(no positive value)", panel(fig, "c").get_title())
        (cb,) = [a for a in fig.axes if a.get_gid() == "panel-c-colourbar"]
        self.assertEqual(len(cb.get_yticks()), 0)
        # the zero cells are still drawn
        self.assertEqual(len(by_gid(panel(fig, "c"), "zero")), 2)
        # E_field_max is blank everywhere: (no positive value), and its blanks drawn
        self.assertEqual(fig.quanta_degenerate["i"], "none")
        self.assertEqual(len(by_gid(panel(fig, "i"), "blank")), 2)

    def test_one_value_is_widened(self):
        fig = self.fig([1e-3, 1e-3])
        norm = panel(fig, "c").images[0].norm
        self.assertEqual(fig.quanta_degenerate["c"], "one")
        self.assertAlmostEqual(norm.vmin, 1e-3 / 10**0.5)
        self.assertAlmostEqual(norm.vmax, 1e-3 * 10**0.5)
        norm = panel(fig, "d").images[0].norm
        self.assertEqual((norm.vmin, norm.vmax), (1e-3 - 0.5, 1e-3 + 0.5))
        # a diverging panel of zeros has m = 1
        fig = self.fig([0.0])
        norm = panel(fig, "e").images[0].norm
        self.assertEqual((norm.vmin, norm.vmax), (-1.0, 1.0))
        self.assertEqual(fig.quanta_degenerate["e"], "one")

    def test_a_negative_value_on_a_log_panel_refuses_the_figure(self):
        with self.assertRaises(qf.InputDefect):
            self.fig([-1e-3])

    def test_two_rows_in_one_cell_refuse_the_A(self):
        cells = self.cells([1e-3, 2e-3])
        cells[1].row["beta"] = cells[0].row["beta"]
        with self.assertRaises(qf.FigureRefused) as cm:
            pg.build_figure(cells, Path("/r"), "one-loop-measured-v1", 1.0, 1.0)
        self.assertIn("two rows in one cell", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
