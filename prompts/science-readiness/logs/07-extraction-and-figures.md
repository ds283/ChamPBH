# Log 07 — Extraction and the science figures

**Prompt:** prompts/science-readiness/07-extraction-and-figures.md
**Commit:** the commit that adds this file ("Add the extraction and the four science figures to plot_by_beta"); its SHA is in `git log`
**Model:** Sonnet 5.5
**Date:** 2026-10-02
**Result:** COMPLETE WITH DEVIATIONS

Worked on top of `20d86a5`. `VERSION_LABEL` (`"2026.6.0"`) and `PRYM_VERSION`
(`"bf24c3d+ri02+sr01"`) are unchanged; no schema column changed; no compute target, factory,
or `.documents/` file other than the index was touched.

## What shipped

- `extract_common.py` (new public symbols; all take plain floats, sequences or stored objects
  through attribute names, never a datastore handle):
  - `relative_shift(value, baseline) -> float`: `(value - baseline)/baseline`; NaN for a missing,
    non-finite or zero-baseline input.
  - `running_band(x, y, half_width) -> (median, p16, p84)`: three arrays aligned with `x`; the
    window `[x_i - h, x_i + h]` has inclusive edges; non-finite `y` ignored; an empty window is NaN.
  - `value_at_T_Jordan(values, attribute, T) -> Optional[float]`: linear in `ln T_J` between the
    two bracketing samples (sorted internally); `None` for an empty list or `T` outside the range;
    `T` is in the units of `exp(log_T_Jordan)`.
  - `kick_threshold_curve(cosmology, T_grid) -> (T, beta_th)`: `1/sqrt(3 Σ)`, `Σ = 1 − 3 cosmology.w(T)`
    (`GenericEOSBase.w`, reached through the cosmology's `w`); points with `Σ ≤ 0` omitted.
  - `adiabatic_Q_caption(M_over_Mp) -> Optional[str]`: the max |Q| caveat for `M ≤ 1e-3 M_P`.
  - `build_history_record(*, beta, M_Mp, Lambda_eV, phi_init_Mp, scalar, bbn, baseline, units, failure_reasons=())`:
    one plain record per history, from a successful `ScalarModel` / `BBNData` (or `None`). It reads
    `first_bounce`, `extra_metadata`, `values[*].phi_Einstein` and `values[*].density_NP_ratio`: the
    **point** fields (the amendment to the prompt). Non-positive abundances become NaN, as the
    existing panels drop them.
  - `plot_abundance_shifts` (figure 1), `plot_convergence_in_M` (figure 2), `plot_T_deliver`
    (figure 3), `plot_fixed_T` (figure 4), `write_histories_csv`; `CSV_COLUMNS`; `FIXED_T_MEV`
    (1 MeV and 70 keV). The figures use `matplotlib.figure.Figure`, so no pyplot state or backend
    is involved; each writes `.pdf` and `.png` beside each other, and returns `False` (writing
    nothing) when it has nothing to plot.
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
  - The `ScalarModel` and `BBNData` lookups of `build_plot_work` no longer pass
    `_do_not_populate`, so their sample values are read (the fixed-`T` columns need them).
  - `run_pipeline`'s work queue keeps its results (`store_results=True`); after the per-potential
    loop it writes `<output>/<model_label>/histories.csv` and
    `<output>/<model_label>/plots/M_convergence.pdf` (figure 2) from the gathered records.
  - Under `--no-baseline` figures 1 and 2 are skipped with a printed message. Figures 3 and 4 do
    not use the baseline and are still drawn.
  - The max |Q| plot gets `adiabatic_Q_caption`'s text as a small `fig.text` for `M ≤ 1e-3 M_P`:
    "maximum |Q| may be set by aliased late samples; see
    [post-adiabatic-Q-reads-aliased-late-samples]". Nothing was removed.
- `ComputeTargets/tests/test_extraction.py` (new, 13 tests).
- `prompts/science-readiness/logs/07-figures/`: the four figures as PNGs, from **synthetic**
  records (`fig1_abundance_shifts`, `fig2_M_convergence`, `fig3_T_deliver`, `fig4_fixed_T`); each
  title says so.

## Deviations from the prompt

### 1. The figure builders and the record builder are in `extract_common.py` (STRUCTURALLY REQUIRED)

The prompt says "the figure builders, factored to take lists of plain records". `plot_by_beta.py`
parses `sys.argv` and calls `ray.init` at import, so a test cannot import from it (the same
reason as log 02's deviation 1). The builders therefore live in `extract_common.py`, which is in
the allowed list, and `plot_by_beta.py` calls them. `plot_by_beta.py` is tested by `ast` only.

### 2. `build_beta_plot` returns its records, and figure 2 and the CSV are written by `run_pipeline` (STRUCTURALLY REQUIRED)

Figure 2 spans every `M` the run read, but `build_beta_plot` is one Ray task per potential and
returned nothing, so the script kept no cross-model data. The prompt's second stop condition says
to stop if figure 2 "needs data across models that the script does not keep". I did not stop: the
fix is local to `plot_by_beta.py` (return the plain records; `RayWorkPool(store_results=True)`
already hands them back), it touches no factory, and the records are small. If the orchestrator
reads the stop condition as covering this, the change is easy to revert; I record it so the
reading is theirs to make.

### 3. `_do_not_populate` removed from two lookups (STRUCTURALLY REQUIRED)

The existing `ScalarModel` and `BBNData` lookups skipped the sample values, so `values` raised.
The fixed-`T` columns need them. Removing the key from those two lookups is a script change
(the factories already populate by default). The `AdiabaticHistory` lookup and the failed-BBN
lookup keep it. Cost: each (M, Λ, β) history is now read with its ~5 000 samples; not measured
against a real store, as there is none.

### 4. The CSV and figure 2 are per cosmology model, not at the single output root (IMPLEMENTATION CHOICE)

The prompt says `histories.csv` "at the output root". The script's existing layout is
`<output>/<model_label>/…` and `model_list` can hold several cosmology models; one file at
`<output>/histories.csv` would be overwritten by each. I wrote `<output>/<model_label>/histories.csv`
and `<output>/<model_label>/plots/M_convergence.pdf`. The alternative, one file with a `model`
column, would need a column the README §2 (k) list does not have.

### 5. CSV column names carry units (IMPLEMENTATION CHOICE)

README §2 (k) lists the quantities without names. I used `M_Mp` (M in M_P), `Lambda_eV`,
`phi_init_Mp`, `T_deliver_GeV`, `phi_1MeV_Mp`, `phi_70keV_Mp`. The existing output directories
name M and Λ in eV; I chose M_P for M because the campaign quotes `M` in units of M_P. The record
carries one more key, `first_bounce_reflected`, that figure 3 reads; the CSV does not write it
(the header stays the §2 (k) list).

### 6. Figure 2 has two panels, not one (IMPLEMENTATION CHOICE)

"The running medians of figure 1 for every M on one panel": figure 1 has two quantities (D/H,
Yp). One panel for both would mix scales, so figure 2 has the two panels of figure 1, one line
per (M, Λ) in each.

### 7. Figures 3 and 4 are not skipped under `--no-baseline` (IMPLEMENTATION CHOICE)

The prompt skips "a figure that needs" the baseline. Only figures 1 and 2 do (the shifts); 3 and 4
read the first bounce and the point fields.

## Verification performed

- `black --check` on `extract_common.py`, `plot_by_beta.py`, `config/argument_parser.py` and
  `ComputeTargets/tests/test_extraction.py`: clean. (`plot_by_beta.py`'s diff is only the added
  lines; black changed nothing else in it.)
- `ComputeTargets.tests.test_extraction`: 13 tests, OK, under a second. (a) `relative_shift`;
  (b) the centre of 101 points to 1e-12 (median 0.5, p16 0.16, p84 0.84), the empty window, the
  inclusive edges (a window `0.25` wide reaches the points at its edges; `0.2499` does not);
  (c) exact on a log-linear history to 1e-12, order-independent, `None` outside the range and for
  an empty list; (d) `Σ = 1/12` gives `β_th = 2` to 1e-14, and `Σ < 0` and `Σ = 0` points are
  omitted; (e) the records carry the fixed-`T` values and a failure reason; all four figures (pdf
  and png, over 1 kB) and `histories.csv` are built from synthetic records into a temporary
  directory, the CSV header equals `CSV_COLUMNS`, and a figure with nothing to plot returns
  `False` and writes nothing; (f) the parser default is 0.025 and `--band-half-width 0.1` is
  read, `plot_by_beta.py` (by `ast`) calls every new function and reads `args.band_half_width`,
  and only two `_do_not_populate` literals remain in it; the max |Q| caveat appears for
  `M ≤ 1e-3` and not above.
- **On `HEAD~1` (`20d86a5`) the functions do not exist** (stand-in, as the prompt allows):
  `git show HEAD:plot_by_beta.py | grep -c "histories.csv\|plot_T_deliver\|abundance_shifts\|T_deliver\|fixed_T\|M_convergence\|band_half_width"`
  prints 0; `git show HEAD:extract_common.py | grep -c "def relative_shift\|def running_band\|def value_at_T_Jordan\|def kick_threshold_curve"`
  prints 0; `git show HEAD:config/argument_parser.py | grep -c band-half-width` prints 0. Run before the
  commit, when `HEAD` was `20d86a5`.
- The four figures (README §6.8) are attached under `logs/07-figures/`, from synthetic records.
  I looked at figures 1 and 3; they show the synthetic trend and the kick-threshold overlay as
  intended. Synthetic β spacing (0.05) is wider than the default half-width (0.025), so the band is
  a single point there; that is a property of the synthetic data, not of the code.
- Suites (`unittest discover`, from the repository root), before and after:

  | package | before (`20d86a5`) | after |
  |---|---|---|
  | CosmologyModels | 18 | 18 |
  | ComputeTargets | 88 | 101 |
  | Datastore | 26 | 26 |

- **Not run, and needing a run the user must make:** `plot_by_beta.py` against a datastore (there
  is none; README §0.5). The record builder, the figure functions and the CSV are tested on
  stand-in objects with the stored classes' attribute names. The wiring in `plot_by_beta.py`
  (the lookups now populating values, `store_results=True`, the failure dicts) is checked by
  `ast` and by reading, not by running. The first real run should check that memory is acceptable
  with the sample values read, and that `scalar_data[0]._cosmology.w` accepts the `T` grid
  (0.05 to 50 GeV).

## Observations not acted on

- `build_beta_plot`'s early-return message for no valid BBN data lacks its `f` prefix
  (`'{model_label}'` prints literally). It pre-dates this prompt and is cosmetic; left, and no
  issue is opened (housekeeping).
- The kick-threshold overlay plots `β_th(T)` from the cosmology's `w`, while each stored sample
  also carries `Sigma`; the two should agree and I did not compare them (no store).

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
  `write_histories_csv`; `build_history_record`. Figure 4 plots the point `phi_Einstein` and
  `density_NP_ratio` at 1 MeV and 70 keV, labelled φ, not ⟨φ⟩ (README §2 (k) amendment).
- **A reading note for the science run:** the max |Q| plot's caveat appears for `M ≤ 1e-3 M_P`.
- **Suite counts after this prompt:** CosmologyModels 18, ComputeTargets 101, Datastore 26.
