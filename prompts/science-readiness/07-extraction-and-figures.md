# Prompt 07 — Extraction and the science figures

**Campaign:** [`README.md`](README.md) · **Board item:** **G** · **Board:**
`IMPLEMENTATION_STATE.md`. Update your row and G.
**Closes:** `[00-no-extraction-for-the-science-figures]` on this board.
**Recommended model:** **Sonnet.** Pure functions with tests, and plotting code around them.

> **Amended 2026-10-02 (the user's ruling on prompt 05; README §0.2 amendment and §2 (k)
> figure 4).** Prompt 05 added no fields or columns. Figure 4 and the CSV's fixed-`T` values
> use the point fields:
> - `ScalarModelValue.phi_Einstein`, through `value_at_T_Jordan`;
> - `BBNDataValue.density_NP_ratio`, which is the point ratio.
>
> README §0.3's "prompt 05's averaged φ" does not exist. Log 05 has no column names to hand on;
> take them from log 03 and the existing value classes.

> **Suspended 2026-10-02 (the user's ruling on `a2deb00`; board Decisions).** A first
> implementation was reverted in `8fcb295`. The four fixed-`T` values (φ and ρ_NP/ρ_R,J at
> 1 MeV and 70 keV) are to be stored on the `ScalarModel` row, like the first bounce, and not
> interpolated from the samples at plot time. `_do_not_populate` stays on every lookup in
> `plot_by_beta.py`. The first amendment above is superseded on this point. Do not dispatch this
> prompt until it is re-planned.

> **Re-planned 2026-10-02 (README §0.2, the U6 amendment; §2 (k) and (n)).** This supersedes both
> notes above. Dispatch only after prompt 06b has landed.
> - **Figure 4 and the CSV's four fixed-`T` values** read `ScalarModel.fixed_T_values`
>   (log 06b). No sample is loaded for them.
> - **`value_at_T_Jordan` is withdrawn.** There are three pure functions, not four. Test (c) is
>   withdrawn. The "`T` outside the range" case is covered by prompt 06b's test (b).
> - **`_do_not_populate` stays on every lookup in `plot_by_beta.py`.** Removing it from any
>   lookup is §5's first stop condition. Stop and ask; do not record it as a deviation.
> - **The ruled design, from the first implementation:**
>   - The figure and record builders live in `extract_common.py`, because `plot_by_beta.py`
>     parses `argv` and starts Ray at import.
>   - `build_beta_plot` returns its plain records, and `run_pipeline` gathers them across
>     potentials (`store_results=True`) to draw figure 2 and write `histories.csv`. §5's second
>     stop condition does not apply to this.
> - **Read first** also includes `logs/06b-fixed-T-values.md`, "State handed to the next prompt",
>   for the property and the four column names.

**Read first:**

1. [`README.md`](README.md) §0.2 (U5), §0.3 (the last point), §2 (k), §5, §6.8.
2. `source/campaign_reevaluation_2026-10-01.md` §3, "Phase D". It is data: README §2 (k) is what
   to build.
3. Logs 03 and 05, "State handed to the next prompt": the column and property names.
4. `extract_common.py`, all of it. `plot_by_beta.py`, all of it: `build_beta_plot`, how the
   `ScalarModel`, `BBNData` and `AdiabaticHistory` lookups are made per (M, Λ) model, the SM
   baseline, and the drop report. `config/argument_parser.py`.
5. `CosmologyModels/GenericEOS/`: how `w(T)` is evaluated, giving `Σ = 1 − 3w`.

---

## 1. The changes

- **`extract_common.py`**: the four pure functions of README §2 (k). Each takes plain floats or
  sequences, or stored value objects through attribute names, and never a datastore handle.
  `value_at_T_Jordan` interpolates linearly in `ln T_J` between the two bracketing samples. It
  returns `None` outside the range or for an empty list.
- **`plot_by_beta.py`**:
  - **Figures 1, 3 and 4** of README §2 (k), per model, in the existing output layout beside the
    current figures. The SM baseline is the one the script already computes; a figure that needs
    it is skipped, with a message, under `--no-baseline`.
  - **Figure 2** after the per-model loop, across every `M` the run read.
  - **The CSV**, `histories.csv`, at the output root.
  - **`--band-half-width`** (float, default 0.025) in the shared parser.
- **The adiabatic panels:** for `M ≲ 10⁻³`, append to the existing max |Q| caption: "may be set
  by aliased late samples; see [post-adiabatic-Q-reads-aliased-late-samples]". Nothing is
  removed.

## 2. Tests — `ComputeTargets/tests/test_extraction.py` (or the package where `extract_common`'s tests live)

- **(a)** `relative_shift`.
- **(b)** `running_band`:
  - on 101 points with a known distribution, the median and percentiles at the centre to
    `1e-12`;
  - an empty window gives NaN;
  - the window edges are inclusive.
- **(c)** `value_at_T_Jordan`:
  - exact on a log-linear synthetic history;
  - `None` outside the range.
- **(d)** `kick_threshold_curve`: on a stand-in EOS with `Σ = 1/(3·4)` at one `T`, `β_th = 2`
  there. Where `Σ ≤ 0`, the point is omitted.
- **(e) The figures.** The figure builders, factored to take lists of plain records, build all
  four figures and the CSV from synthetic records into a temporary directory with no datastore.
  The CSV header is README §2 (k)'s list.
- **On `HEAD~1`:** the functions do not exist. The stand-in is that `plot_by_beta.py` on
  `HEAD~1` has no figure or CSV of these names: grep, recorded.

## 3. What this prompt does not do

No new compute target, and no change to `ScalarModel`, `BBNData` or `AdiabaticHistory`. No change
to the existing figures beyond the caption. It does not run `plot_by_beta.py` against a datastore:
there is none (README §0.5).

## 4. Acceptance

README §6.8, every row. The four figures from (e) are attached to the log as PNGs under
`logs/07-figures/`, built from synthetic records and labelled as such. All three suites pass and
rise. `black --check` clean. The board and the index: G done, its issue closed.

## 5. Stop conditions — stop and ask the user

- `plot_by_beta.py`'s lookups cannot return the new columns without a change to a factory.
- Figure 2 needs data across models that the script does not keep.

## 6. The log and the board

`logs/07-extraction-and-figures.md`, in the README §5.1 template. In "State handed to the next
prompt", give the command line that produces the figures from a datastore, for the handover.
