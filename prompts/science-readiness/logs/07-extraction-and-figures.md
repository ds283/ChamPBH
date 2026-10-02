# Log 07 — Extraction and the science figures

**Prompt:** prompts/science-readiness/07-extraction-and-figures.md
**Commit:** the commit that adds this file ("Add the extraction and the four science figures to plot_by_beta"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-10-02
**Result:** COMPLETE WITH DEVIATIONS

Worked on top of `489ab26` (the re-planned prompt, after prompt 06b). This is the second
implementation of prompt 07: the first (`a2deb00`) was reverted in `8fcb295` (board, Decisions,
2026-10-02). I took the design the user ruled from it by checking out its four code files
(`extract_common.py`, `plot_by_beta.py`, `config/argument_parser.py`, `test_extraction.py`; none of
the three non-test files had changed since) and changed them as the re-plan requires: no
`value_at_T_Jordan`, `_do_not_populate` kept, the fixed-`T` values read from
`ScalarModel.fixed_T_values`. `VERSION_LABEL` (`"2026.6.0"`) and `PRYM_VERSION`
(`"bf24c3d+ri02+sr01"`) are unchanged; no schema column changed; no compute target, factory, or
`.documents/` file other than the index was touched.

## What shipped

- `extract_common.py` (new public symbols; all take plain floats, sequences or stored objects
  through attribute names, never a datastore handle):
  - `relative_shift(value, baseline) -> float`: `(value - baseline)/baseline`; NaN for a missing,
    non-finite or zero-baseline input.
  - `running_band(x, y, half_width) -> (median, p16, p84)`: three arrays aligned with `x`; the
    window `[x_i - h, x_i + h]` has inclusive edges; non-finite `y` ignored; an empty window is NaN.
  - `kick_threshold_curve(cosmology, T_grid) -> (T, beta_th)`: `1/sqrt(3 Σ)`, `Σ = 1 − 3 cosmology.w(T)`;
    points with `Σ ≤ 0` omitted. **Three** pure functions, not four: `value_at_T_Jordan` is
    withdrawn (U6) and is not in the tree.
  - `adiabatic_Q_caption(M_over_Mp) -> Optional[str]`: the max |Q| caveat for `M ≤ 1e-3 M_P`.
  - `build_history_record(*, beta, M_Mp, Lambda_eV, phi_init_Mp, scalar, bbn, baseline, units,
    failure_reasons=())`: one plain record per history, from a successful `ScalarModel` /
    `BBNData` (or `None`). It reads only parent-row properties: `bbn`'s four abundances,
    `scalar.first_bounce`, `scalar.extra_metadata` (reflection count) and
    **`scalar.fixed_T_values`**. It never touches `values`, so both objects may be built with
    `_do_not_populate`. φ is divided by `units.PlanckMass`; a `None` field gives NaN, in the pair
    of the temperature not reached only.
  - `plot_abundance_shifts` (figure 1), `plot_convergence_in_M` (figure 2), `plot_T_deliver`
    (figure 3), `plot_fixed_T` (figure 4, built by the private `_fixed_T_figure`),
    `write_histories_csv`; `CSV_COLUMNS`; `FIXED_T_MEV` (the tags `1MeV`, `70keV`). The figures
    use `matplotlib.figure.Figure`, so no pyplot state or backend is involved; each writes `.pdf`
    and `.png` beside each other, and returns `False` (writing nothing) when it has nothing to plot.
- `histories.csv` columns (`CSV_COLUMNS`): `beta, M_Mp, Lambda_eV, phi_init_Mp, Yp_BBN, D_over_H,
  He3_over_H, Li7_over_H, delta_Yp, delta_D_over_H, T_deliver_GeV, phi_1MeV_Mp, phi_70keV_Mp,
  rho_ratio_1MeV, rho_ratio_70keV, reflections, failure_reasons`. Shifts are fractions;
  `D_over_H` is the stored ×1e5 quantity; missing values are empty cells.
- `config/argument_parser.py`: `--band-half-width` (float, default 0.025), one option.
- `plot_by_beta.py`:
  - `build_beta_plot` takes `scalar_failures` and `bbn_failures` (beta -> reason), builds the
    records, writes figures 1, 3 and 4 into the existing per-(M, Λ) `plots/…/` directory
    (`abundance_shifts`, `T_deliver`, `fixed_T`, each `.pdf` and `.png`), and **returns the
    records** (it returned `None`). The early return for "no valid BBN data" returns them too.
  - `report_dropped_scalar_models` and `report_dropped_bbn_models` still print as before and now
    also return `{beta: reason}`.
  - **`_do_not_populate: True` is kept on every lookup.** `grep -c _do_not_populate plot_by_beta.py`
    is 4 on `HEAD~1` and 4 after; the diff of `plot_by_beta.py` has no `_do_not_populate` line.
  - `run_pipeline`'s work queue keeps its results (`store_results=True`); after the per-potential
    loop it writes `<output>/<model_label>/histories.csv` and
    `<output>/<model_label>/plots/M_convergence.pdf` (figure 2) from the gathered records.
  - Under `--no-baseline` figures 1 and 2 are skipped with a printed message. Figures 3 and 4 do
    not use the baseline and are still drawn.
  - The max |Q| plot gets `adiabatic_Q_caption`'s text as a small `fig.text` for `M ≤ 1e-3 M_P`:
    "maximum |Q| may be set by aliased late samples; see
    [post-adiabatic-Q-reads-aliased-late-samples]". Nothing was removed.
- `ComputeTargets/tests/test_extraction.py` (new, 12 tests).
- `prompts/science-readiness/logs/07-figures/`: the four figures as PNGs, from **synthetic**
  records (`fig1_abundance_shifts`, `fig2_M_convergence`, `fig3_T_deliver`, `fig4_fixed_T`); each
  title says so. The 1 MeV ratio is negated in these, as on the roster histories (log 06b).

## Deviations from the prompt

### 1. The figure builders and the record builder are in `extract_common.py` (STRUCTURALLY REQUIRED)

The prompt says "the figure builders, factored to take lists of plain records".
`plot_by_beta.py` parses `sys.argv` and calls `ray.init` at import, so a test cannot import from
it. The builders therefore live in `extract_common.py`, and `plot_by_beta.py` calls them; it is
tested by `ast` only. This is the design ruled in the re-plan (README §2 (k) amendment).

### 2. `build_beta_plot` returns its records; figure 2 and the CSV are written by `run_pipeline` (STRUCTURALLY REQUIRED)

Figure 2 spans every `M` the run read, but `build_beta_plot` is one Ray task per potential and
returned nothing. The fix is local to `plot_by_beta.py`: return the plain records and set
`RayWorkPool(store_results=True)`. Ruled in the re-plan (log 07's earlier deviation 2 accepted);
§5's second stop condition does not apply.

### 3. The CSV and figure 2 are per cosmology model, not at the single output root (IMPLEMENTATION CHOICE)

The prompt says `histories.csv` "at the output root". The script's layout is
`<output>/<model_label>/…` and `model_list` can hold several cosmology models; one file at
`<output>/histories.csv` would be overwritten by each. I wrote `<output>/<model_label>/histories.csv`
and `<output>/<model_label>/plots/M_convergence.pdf`. The alternative, one file with a `model`
column, needs a column the README §2 (k) list does not have.

### 4. CSV column names carry units (IMPLEMENTATION CHOICE)

README §2 (k) lists the quantities without names. I used `M_Mp` (M in M_P), `Lambda_eV`,
`phi_init_Mp`, `T_deliver_GeV`, `phi_1MeV_Mp`, `phi_70keV_Mp`. The existing output directories
name M and Λ in eV; I chose M_P for M because the campaign quotes `M` in M_P. The record carries
one more key, `first_bounce_reflected`, that figure 3 reads; the CSV does not write it.

### 5. Figure 2 has two panels, not one (IMPLEMENTATION CHOICE)

"The running medians of figure 1 for every M on one panel": figure 1 has two quantities (D/H,
Yp). One panel for both would mix scales, so figure 2 has the two panels of figure 1, one line per
(M, Λ) in each.

### 6. Figures 3 and 4 are not skipped under `--no-baseline` (IMPLEMENTATION CHOICE)

The prompt skips "a figure that needs" the baseline. Only figures 1 and 2 do (the shifts); 3 and 4
read the first bounce and the stored fixed-`T` values.

### 7. Figure 4's axes are signed, and no point is dropped (IMPLEMENTATION CHOICE)

The first implementation plotted figure 4 on log axes after discarding non-positive values. The
ratio ρ_NP/ρ_R,J at 1 MeV is **negative** on the three roster histories (log 06b, driver output:
−4.81e-2 at β = 2, M = 0.5; −7.81e-2 at β = 2, M = 10⁻³; −1.04e-2 at β = 1.6, M = 10⁻⁵), so that
code would have drawn nothing at 1 MeV. An axis whose data are all positive is logarithmic; one
with a non-positive value is symmetric-log, with a linear region below 1 per cent of the largest
value. Alternative: a linear axis, which hides the φ range (1e-4 to 1e-2 M_P). Test
`test_e_figure_4_keeps_negative_ratios` checks that every finite negative point reaches the axes.

### 8. Test (c) withdrawn; one test added (the re-plan)

Test (c) of the prompt, `value_at_T_Jordan`, is withdrawn with the function (its two tests are not
in the file). The "T outside the range" case is prompt 06b's test (b). In their place:
`test_e_records_carry_the_fixed_T_values` reads the values from stand-in objects whose `values`
raises, so a record builder that loaded samples would fail; and
`test_e_figure_4_keeps_negative_ratios` (deviation 7).

## Verification performed

- `black --check` on `extract_common.py`, `plot_by_beta.py`, `config/argument_parser.py` and
  `ComputeTargets/tests/test_extraction.py`: clean.
- `ComputeTargets.tests.test_extraction`: 12 tests, OK, under a second, no Ray, no datastore, no
  PRyMordial solve. (a) `relative_shift`; (b) the centre of 101 points to 1e-12 (median 0.5, p16
  0.16, p84 0.84), the empty window, the inclusive edges (a window `0.25` wide reaches the points
  at its edges; `0.2499` does not); (d) `Σ = 1/12` gives `β_th = 2` to 1e-14, and `Σ < 0` and
  `Σ = 0` points are omitted; (e) the records carry the fixed-`T` values (φ, ratio at 1 MeV and
  70 keV), a pair is NaN where the temperature is not reached, and a failure reason; all four
  figures (pdf and png, over 1 kB) and `histories.csv` are built from synthetic records into a
  temporary directory, the CSV header equals `CSV_COLUMNS`; figure 4 keeps negative ratios; a
  figure with nothing to plot returns `False` and writes nothing; (f) the parser default is 0.025
  and `--band-half-width 0.1` is read, `plot_by_beta.py` (by `ast`) calls every new function,
  reads `args.band_half_width`, has exactly four `"_do_not_populate": True` literals and no
  `value_at_T_Jordan`; the max |Q| caveat appears for `M ≤ 1e-3` and not above.
- **On `HEAD~1` (`489ab26`) the functions do not exist** (the stand-in the prompt allows), run
  before the commit when `HEAD` was `489ab26`:
  - `git show HEAD:plot_by_beta.py | grep -c "histories.csv\|plot_T_deliver\|abundance_shifts\|T_deliver\|fixed_T\|M_convergence\|band_half_width"` prints 0;
  - `git show HEAD:extract_common.py | grep -c "def relative_shift\|def running_band\|def kick_threshold_curve"` prints 0;
  - `git show HEAD:config/argument_parser.py | grep -c band-half-width` prints 0;
  - `git show HEAD:plot_by_beta.py | grep -c _do_not_populate` prints 4, and `grep -c` on the
    working tree prints 4 (README §6.8's witness for the lookups).
- A smoke check of `kick_threshold_curve` on the real EOS, not a test: `QCD_Cosmology(0,
  Planck_units(), Planck2018())`, a 400-point grid of 0.05–50 GeV (the grid `plot_by_beta.py`
  uses), all 400 points have `Σ > 0`; `β_th` runs from 1.030 to 8.385 (2.001 at 0.05 GeV, 1.272 at
  0.141 GeV, 8.385 at 9.0 GeV). This answers the first run-needed note of the earlier log 07: the
  cosmology's `w` accepts that grid.
- Suites (`unittest discover`, from the repository root), before and after:

  | package | before (`489ab26`) | after |
  |---|---|---|
  | CosmologyModels | 18 | 18 |
  | ComputeTargets | 91 | 103 |
  | Datastore | 31 | 31 |

- **Not run, and needing a run the user must make:** `plot_by_beta.py` against a datastore (there
  is none; README §0.5). The record builder, the figure functions and the CSV are tested on
  stand-in objects with the stored classes' attribute names. The wiring in `plot_by_beta.py`
  (`store_results=True`, the failure dicts, the properties read off objects built with
  `_do_not_populate`) is checked by `ast` and by reading. `ScalarModel.fixed_T_values` and
  `first_bounce` working under `_do_not_populate` is log 06b's round-trip test, not repeated here.
  I looked at figures 1, 3 (earlier run) and 4 (this run).

## Observations not acted on

- `build_beta_plot`'s early-return message for no valid BBN data lacks its `f` prefix
  (`'{model_label}'` prints literally). It pre-dates this prompt and is cosmetic (housekeeping, not
  an issue).
- Each stored sample also carries `Sigma`; the kick-threshold overlay uses the cosmology's `w`.
  The two should agree; not compared (no store).

## State handed to the next prompt

- **The command line that produces the figures from a datastore** (repository root, with a store
  made by this campaign's code, `VERSION_LABEL = "2026.6.0"`):

  ```bash
  ./venv/bin/python plot_by_beta.py --database <store.db> --output <dir> \
      [--band-half-width 0.025] [--phi-init-Mp 5.0] [--T-stop-GeV <GeV>]
  ```

  `--phi-init-Mp` and `--T-stop-GeV` must match the run, because they are part of the
  `ScalarModel` lookup key. Add `--no-baseline` to skip the Standard-Model solve; figures 1 and 2
  are then skipped. Per cosmology model `<label>` it writes, under `<dir>/<label>/`:
  - `plots/M=<M>eV_Lambda=<L>eV/abundance_shifts.{pdf,png}` (figure 1), `T_deliver.{pdf,png}`
    (figure 3), `fixed_T.{pdf,png}` (figure 4), beside the existing `timings`, `BBN`, `max_Q`;
  - `plots/M_convergence.{pdf,png}` (figure 2), across every potential read;
  - `histories.csv`, one row per history, sorted by (M, Λ, β).
- **Names for prompt 08's documents:** `extract_common.CSV_COLUMNS`; the figure functions
  `plot_abundance_shifts`, `plot_convergence_in_M`, `plot_T_deliver`, `plot_fixed_T`;
  `write_histories_csv`; `build_history_record`; the three pure functions `relative_shift`,
  `running_band`, `kick_threshold_curve`. Figure 4 plots the **stored** values of
  `ScalarModel.fixed_T_values` at 1 MeV and 70 keV, labelled φ and ρ_NP/ρ_R,J (README §2 (k)
  amendment, §2 (n)).
- **A reading note for the science run:** the max |Q| plot's caveat appears for `M ≤ 1e-3 M_P`.
  The ratio at 1 MeV is negative on the roster histories (log 06b), so figure 4's ratio panel is
  symmetric-log.
- **Suite counts after this prompt:** CosmologyModels 18, ComputeTargets 103, Datastore 31.
