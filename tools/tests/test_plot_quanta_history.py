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
tools/plot_quanta_history.py (one-loop-drain prompt 07 §1.3, §1.5; orchestrator note 07 rulings 3, 5, 9), on the constructed
exports of tests/test_quanta_figures.py (`ExportMixin`), the Figure read back:

- the six panels share N (one shared x-axis group over the six and panel 3's twin) and have the stated scales;
- panel 1 holds one point per constructed generation, coloured by its regime, the m a lines of fields.csv and a tick for each
  plateau and wall event; panel 2's drawn and undrawn candidates by cause; panel 3's curves and the zeros not drawn; panel 5's
  points and zeros; panel 6's phase slips and the 1 rad line; the legend of the regimes once; the footer;
- a history refused, and nothing written for it while the others are written (exit 1): quanta_bounces.csv out of step with
  bounces.csv, quanta_samples.csv not fields.csv's rows, a generation count that is not quanta_histories.csv's, one quanta file
  only, a regime of none of the four, a negative value on a log10 panel, an ln_n_g out of range; a history with neither
  quanta file skipped and counted (exit 0);
- the selectors (a product), a selector matching nothing (exit 2); `--assoc-scale`'s file names, a file of another scale
  refusing every history; `--output` below the other root and nothing below the input; both files written; an unreadable root.

No Ray, no store, no PRyMordial solve.
"""

import contextlib
import io
import unittest

import numpy as np
from matplotlib.colors import to_rgba

from tools import plot_quanta_history as ph
from tools import quanta_figures as qf
from tools.tests.test_quanta_figures import (
    GENERATION_REGIMES,
    ExportMixin,
    capturing,
)


def run_tool(root, **kw):
    out = io.StringIO()
    p, figures = capturing(ph)
    with p:
        rc = ph.run(root, out=out, **kw)
    return rc, out.getvalue(), figures


def panel(fig, k):
    (ax,) = [a for a in fig.axes if a.get_gid() == f"panel-{k}"]
    return ax


def named(ax, gid):
    return [a for a in list(ax.lines) + list(ax.collections) if a.get_gid() == gid]


def finite_points(collection):
    off = np.asarray(collection.get_offsets())
    return int(np.count_nonzero(np.isfinite(off[:, 1])))


class TestTheFigure(ExportMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.build()
        rc, self.text, figs = run_tool(
            self.root, A_values=[1.0], beta_values=[2.0], M_values=[1e-20]
        )
        self.assertEqual(rc, 0, self.text)
        ((self.fig, self.stem),) = figs

    def test_six_panels_share_N(self):
        axes = [panel(self.fig, k) for k in range(1, 7)]
        twin = [a for a in self.fig.axes if a.get_gid() == "panel-3-twin"][0]
        group = self.fig.axes[0].get_shared_x_axes()
        for a in axes[1:] + [twin]:
            self.assertTrue(group.joined(axes[0], a))
        self.assertIn("N", axes[-1].get_xlabel())

    def test_the_scales_and_labels(self):
        self.assertEqual(panel(self.fig, 1).get_yscale(), "linear")
        for k in range(2, 7):
            self.assertEqual(panel(self.fig, k).get_yscale(), "log", k)
        twin = [a for a in self.fig.axes if a.get_gid() == "panel-3-twin"][0]
        self.assertEqual(twin.get_yscale(), "log")
        for k in range(1, 7):
            label = panel(self.fig, k).get_ylabel()
            self.assertTrue(
                any(u in label for u in ("[M_P]", "dimensionless", "[rad]", "[-]")),
                label,
            )

    def test_panel_1_one_point_per_generation_coloured_by_regime(self):
        ax = panel(self.fig, 1)
        total = 0
        for regime in qf.REGIMES:
            (sc,) = named(ax, f"generation-{regime}")
            n = len(sc.get_offsets())
            self.assertEqual(n, GENERATION_REGIMES.count(regime), regime)
            total += n
            if n:
                self.assertEqual(
                    tuple(sc.get_facecolor()[0]), to_rgba(qf.REGIME_COLOURS[regime])
                )
            self.assertTrue(sc.get_rasterized())
        self.assertEqual(total, 5)
        self.assertEqual(self.fig.quanta_counts["generations"], 5)

    def test_panel_1_lines_are_log_m_plus_N_and_events_are_ticks(self):
        ax = panel(self.fig, 1)
        fields = self.read(self.dirs[(1.0, 2.0, 1e-20)] / "fields.csv")
        N = np.array([float(r["raw_N"]) for r in fields])
        (line,) = named(ax, "m-field")
        np.testing.assert_allclose(
            line.get_ydata(), np.array([float(r["log_m_eff_Mp"]) for r in fields]) + N
        )
        (line,) = named(ax, "m-tracker")
        np.testing.assert_allclose(
            line.get_ydata(),
            np.array([float(r["log_m_tracker_Mp"]) for r in fields]) + N,
        )
        (plateau,) = named(ax, "event-plateau")
        (wall,) = named(ax, "event-wall")
        self.assertEqual(len(plateau.get_segments()), 1)
        self.assertEqual(plateau.get_segments()[0][0][0], 1.5)
        self.assertEqual(wall.get_segments()[0][0][0], 3.6)

    def test_panel_2_counts_by_cause(self):
        c = self.fig.quanta_counts
        # seven candidates; the classical one drains nothing (alpha_eff 0 and dK = 0)
        self.assertEqual((c["alpha_drawn"], c["undrawn_alpha_zero_or_blank"]), (6, 1))
        self.assertEqual(
            (c["fraction_drawn"], c["undrawn_dK_zero"], c["undrawn_K_before_blank"]),
            (6, 1, 0),
        )
        ax = panel(self.fig, 2)
        a = sum(len(named(ax, f"alpha-{r}")[0].get_offsets()) for r in qf.REGIMES)
        f = sum(len(named(ax, f"fraction-{r}")[0].get_offsets()) for r in qf.REGIMES)
        self.assertEqual((a, f), (6, 6))
        # the unassessed drain is drawn in the unassessed colour, filled and open
        (sc,) = named(ax, "alpha-unassessed")
        self.assertEqual(len(sc.get_offsets()), 1)
        self.assertEqual(
            tuple(sc.get_facecolor()[0]), to_rgba(qf.REGIME_COLOURS["unassessed"])
        )
        (op,) = named(ax, "fraction-unassessed")
        self.assertEqual(len(op.get_facecolor()), 0)  # open markers
        self.assertEqual(
            tuple(op.get_edgecolor()[0]), to_rgba(qf.REGIME_COLOURS["unassessed"])
        )

    def test_panel_2_the_fraction_is_dK_over_K_after(self):
        ax = panel(self.fig, 2)
        rows = self.read(self.dirs[(1.0, 2.0, 1e-20)] / "bounces.csv")
        want = sorted(
            float(r["delta_K_Mp4"])
            / (float(r["K_before_Mp4"]) + float(r["delta_K_Mp4"]))
            for r in rows
            if r["source"] == "candidate" and float(r["delta_K_Mp4"]) > 0.0
        )
        got = sorted(
            float(y)
            for r in qf.REGIMES
            for _, y in named(ax, f"fraction-{r}")[0].get_offsets()
        )
        np.testing.assert_allclose(got, want, rtol=1e-12)

    def test_panel_3_curves_and_zeros(self):
        ax = panel(self.fig, 3)
        (K,) = named(ax, "K")
        self.assertAlmostEqual(float(K.get_ydata()[-1]), 1.0)
        self.assertEqual(self.fig.quanta_counts["K_final_zero"], 0)
        # the parking fraction is 0 at every sample of this history (no leftover): not drawn, counted
        z = self.fig.quanta_counts["zeros"]
        self.assertEqual(z["parking_fraction"], 31)
        (pf,) = named(ax, "parking")
        self.assertTrue(np.all(np.isnan(pf.get_ydata())))
        twin = [a for a in self.fig.axes if a.get_gid() == "panel-3-twin"][0]
        self.assertEqual(len(named(twin, "r_chi")), 1)

    def test_panel_5_points_and_zeros(self):
        ax = panel(self.fig, 5)
        self.assertEqual(finite_points(named(ax, "n_g")[0]), 5)
        z = self.fig.quanta_counts["zeros"]
        # the first generation has no earlier one (n_bar = 0), and the first candidate no earlier quanta (ratio 0)
        self.assertEqual(z["n_bar"], 1)
        self.assertEqual(z["backreaction_ratio"], 1)
        self.assertEqual(finite_points(named(ax, "n_bar")[0]), 4)
        qb = self.read(self.dirs[(1.0, 2.0, 1e-20)] / "quanta_bounces.csv")
        want = sorted(np.exp(float(r["ln_n_g"])) for r in qb if r["ln_n_g"] != "")
        got = sorted(
            float(y) for _, y in named(ax, "n_g")[0].get_offsets() if np.isfinite(y)
        )
        np.testing.assert_allclose(got, want, rtol=1e-12)

    def test_panel_6_phase_slips_and_the_1_rad_line(self):
        ax = panel(self.fig, 6)
        (ref,) = named(ax, "one-rad")
        self.assertEqual(list(ref.get_ydata()), [1.0, 1.0])
        self.assertIn("1 rad", ref.get_label())
        # six assessed candidates (the unassessed one has no log_k_ex); k_ex/H at each
        self.assertEqual(finite_points(named(ax, "k_ex_over_H")[0]), 6)
        self.assertGreaterEqual(finite_points(named(ax, "phase_slip")[0]), 1)
        # the line is inside the axis range
        lo, hi = ax.get_ylim()
        self.assertTrue(lo <= 1.0 <= hi)

    def test_the_legend_names_the_regimes_once(self):
        legends = [leg for leg in self.fig.legends]
        self.assertEqual(len(legends), 1)
        self.assertEqual(
            [t.get_text() for t in legends[0].get_texts()],
            [qf.REGIME_LABELS[r] for r in qf.REGIMES],
        )
        for ax in self.fig.axes:
            leg = ax.get_legend()
            if leg is not None:
                for t in leg.get_texts():
                    self.assertNotIn(
                        t.get_text(), [qf.REGIME_LABELS[r] for r in qf.REGIMES]
                    )

    def test_the_footer(self):
        text = " ".join(t.get_text() for t in self.fig.texts)
        for needle in (
            "one-loop-measured-v1 A=1.0 M_Mp=1e-20 n=1 beta=2.0",
            "U4-U17-v1",
            "c_a = 1",
            "generations 5, bounce rows 9",
            "delta_stim",
            "parking share",
            "zeros not drawn",
        ):
            self.assertIn(needle, text)

    def test_the_printed_line_and_the_output_name(self):
        self.assertIn("generations 5; bounce rows 9", self.text)
        self.assertEqual(self.stem.name, "quanta_history")
        d = qf.plots_dir(self.root, self.dirs[(1.0, 2.0, 1e-20)])
        self.assertEqual(
            self.stem.relative_to(self.root).parts,
            (*d.relative_to(self.root).parts, "quanta_history"),
        )
        self.assertEqual(
            self.stem.relative_to(self.root).parts[:4],
            ("one-loop-measured-v1", "A=1", "plots", "beta=2"),
        )

    def test_a_parking_leftover_draws_the_parking_curve(self):
        rc, text, figs = run_tool(self.root, A_values=[2.0], beta_values=[2.0])
        ((fig, _),) = figs
        ax = panel(fig, 3)
        (pf,) = named(ax, "parking")
        self.assertTrue(np.any(np.isfinite(pf.get_ydata())))
        self.assertLess(fig.quanta_counts["zeros"]["parking_fraction"], 31)


class TestTheRun(ExportMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.build()

    def test_no_selector_draws_every_measured_drawn_history(self):
        rc, text, figs = run_tool(self.root)
        # six drawn rows; the one with no quanta files is skipped and counted, not refused
        self.assertEqual(rc, 0, text)
        self.assertEqual(len(figs), 5)
        self.assertIn("figures written 5, refused 0, no quanta files 1", text)
        self.assertIn(
            "no quanta files one-loop-measured-v1 A=1.0 M_Mp=2e-20 n=1 beta=3.0", text
        )
        self.assertIn("skipped as fixed-law 2, marker rows 1, failed histories 1", text)

    def test_selectors_match_a_product(self):
        rc, text, figs = run_tool(self.root, A_values=[2.0])
        self.assertEqual(len(figs), 2)
        rc, text, figs = run_tool(self.root, beta_values=[2.0])
        self.assertEqual(len(figs), 3)  # (1, 2, 1e-20), (1, 2, 2e-20), (2, 2, 1e-20)
        rc, text, figs = run_tool(
            self.root, A_values=[1.0, 2.0], beta_values=[2.0], M_values=[1e-20]
        )
        self.assertEqual(len(figs), 2)
        rc, text, figs = run_tool(
            self.root, A_values=[1.0], beta_values=[3.0], M_values=[1e-20]
        )
        self.assertEqual(len(figs), 1)
        # a typed value need not be the cell's repr
        rc, text, figs = run_tool(
            self.root, A_values=[1.0000000001], beta_values=[2.0000000001]
        )
        self.assertEqual(len(figs), 2)

    def test_a_selector_that_matches_nothing_exits_2(self):
        for kw in (
            dict(A_values=[7.0]),
            dict(beta_values=[9.0]),
            dict(M_values=[3e-20]),
            dict(A_values=[0.1]),
        ):
            with contextlib.redirect_stderr(io.StringIO()) as err:
                rc, text, figs = run_tool(self.root, **kw)
            self.assertEqual(rc, 2, kw)
            self.assertEqual(figs, [])
            self.assertIn("matches no", err.getvalue())

    def refuses(self, key, path_name, edit, reason):
        d = self.dirs[key]
        self.rewrite(d / path_name, edit)
        out = io.StringIO()
        rc = ph.run(self.root, out=out)
        text = out.getvalue()
        self.assertEqual(rc, 1, text)
        self.assertIn("REFUSED", text)
        self.assertIn(reason, text)
        # nothing is written for the refused history; the others are
        plots = qf.plots_dir(self.root, d)
        self.assertFalse(plots.exists())
        self.assertIn("figures written 4, refused 1", text)
        self.assertTrue(
            qf.plots_dir(self.root, self.dirs[(2.0, 2.0, 1e-20)])
            .joinpath("quanta_history.png")
            .is_file()
        )

    def test_quanta_bounces_out_of_step_refuses_that_history(self):
        def edit(rows):
            rows[3]["ordinal"] = "99"

        self.refuses(
            (1.0, 2.0, 1e-20),
            "quanta_bounces.csv",
            edit,
            "quanta_bounces row 5 is ordinal 99",
        )

    def test_quanta_samples_not_the_fields_rows_refuses(self):
        def edit(rows):
            rows[4]["raw_N"] = "123.5"

        self.refuses((1.0, 3.0, 1e-20), "quanta_samples.csv", edit, "raw_N")

    def test_a_short_quanta_samples_refuses(self):
        def edit(rows):
            del rows[-1]

        self.refuses(
            (1.0, 3.0, 1e-20), "quanta_samples.csv", edit, "quanta_samples has 30 rows"
        )

    def test_generations_not_those_of_quanta_histories_refuses(self):
        def edit(rows):
            for r in rows:
                if r["alpha"] == "1.0" and r["beta"] == "2.0" and r["M_Mp"] == "1e-20":
                    r["generations"] = "6"

        self.rewrite(self.root / "quanta_histories.csv", edit)
        out = io.StringIO()
        self.assertEqual(ph.run(self.root, out=out), 1)
        self.assertIn("states '6'", out.getvalue())
        self.assertFalse(qf.plots_dir(self.root, self.dirs[(1.0, 2.0, 1e-20)]).exists())

    def test_one_quanta_file_only_refuses(self):
        (self.dirs[(1.0, 2.0, 1e-20)] / "quanta_samples.csv").unlink()
        out = io.StringIO()
        self.assertEqual(ph.run(self.root, out=out), 1)
        self.assertIn("only one of", out.getvalue())
        self.assertFalse(qf.plots_dir(self.root, self.dirs[(1.0, 2.0, 1e-20)]).exists())

    def test_a_regime_of_none_of_the_four_refuses(self):
        def edit(rows):
            rows[2]["regime"] = "bogus"

        self.refuses((1.0, 2.0, 1e-20), "bounces.csv", edit, "regime 'bogus'")

    def test_a_negative_value_on_a_log_panel_refuses(self):
        def edit(rows):
            rows[5]["E_field"] = "-1.0"

        self.refuses((1.0, 2.0, 1e-20), "quanta_samples.csv", edit, "negative value")

    def test_ln_n_g_out_of_range_refuses(self):
        def edit(rows):
            rows[0]["ln_n_g"] = "800.0"

        self.refuses((1.0, 2.0, 1e-20), "quanta_bounces.csv", edit, "ln_n_g runs from")

    def test_a_file_of_another_model_refuses_every_history(self):
        def edit(rows):
            for r in rows:
                r["spectrum_model"] = "other-v9"

        self.rewrite(self.root / "quanta_histories.csv", edit)
        rc, text, figs = run_tool(self.root)
        self.assertEqual((rc, figs), (1, []))
        self.assertEqual(
            text.count("REFUSED"), 6
        )  # every selected history, the one with no quanta files included
        self.assertIn("other-v9", text)

    def test_assoc_scale_reads_and_writes_the_suffixed_names(self):
        for c, tag in ((0.0, "-assoc0"), (10.0, "-assoc10")):
            rc = ph.run(
                self.root,
                A_values=[2.0],
                beta_values=[2.0],
                assoc_scale=c,
                out=io.StringIO(),
            )
            self.assertEqual(rc, 0)
            d = qf.plots_dir(self.root, self.dirs[(2.0, 2.0, 1e-20)])
            self.assertTrue((d / f"quanta_history{tag}.png").is_file())
            self.assertTrue((d / f"quanta_history{tag}.pdf").is_file())
        self.assertFalse((d / "quanta_history.png").exists())

    def test_the_wrong_scale_file_refuses(self):
        (self.root / "quanta_histories-assoc10.csv").write_bytes(
            (self.root / "quanta_histories.csv").read_bytes()
        )
        out = io.StringIO()
        self.assertEqual(ph.run(self.root, assoc_scale=10.0, out=out), 1)
        self.assertIn("assoc_scale", out.getvalue())

    def test_both_files_are_written_and_nothing_else(self):
        rc = ph.run(self.root, A_values=[2.0], beta_values=[2.0], out=io.StringIO())
        self.assertEqual(rc, 0)
        d = qf.plots_dir(self.root, self.dirs[(2.0, 2.0, 1e-20)])
        self.assertEqual(
            sorted(p.name for p in d.iterdir()),
            ["quanta_history.pdf", "quanta_history.png"],
        )

    def test_output_writes_below_the_other_root_and_nothing_below_the_input(self):
        before = sorted(str(p) for p in self.root.rglob("*"))
        other = self._dir / "elsewhere" / "QCD"
        rc = ph.run(self.root, A_values=[2.0], output=other, out=io.StringIO())
        self.assertEqual(rc, 0)
        self.assertEqual(sorted(str(p) for p in self.root.rglob("*")), before)
        written = sorted(
            str(p.relative_to(other)) for p in other.rglob("*") if p.is_file()
        )
        want = sorted(
            str(
                qf.plots_dir(self.root, self.dirs[(2.0, b, 1e-20)]).relative_to(
                    self.root
                )
                / f"quanta_history.{ext}"
            )
            for b in (2.0, 2.5)
            for ext in ("pdf", "png")
        )
        self.assertEqual(written, want)
        self.assertEqual(len(written), 4)

    def test_main_maps_argparse_and_an_unreadable_root_to_2(self):
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(ph.main([str(self._dir / "absent")]), 2)
            self.assertEqual(ph.main([]), 2)
            self.assertEqual(ph.main([str(self.root), "--beta-values", "x"]), 2)
            empty = self._dir / "empty"
            empty.mkdir()
            self.assertEqual(ph.main([str(empty)]), 2)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(ph.main(["--help"]), 0)
            self.assertEqual(
                ph.main([str(self.root), "--A-values", "2", "--beta-values", "2.5"]), 0
            )
        self.assertTrue(
            qf.plots_dir(self.root, self.dirs[(2.0, 2.5, 1e-20)])
            .joinpath("quanta_history.png")
            .is_file()
        )


if __name__ == "__main__":
    unittest.main()
