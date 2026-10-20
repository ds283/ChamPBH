# Prompt 08 — the driver and the tools

**Campaign:** [`README.md`](README.md) · **Board items:** **D1**–**D7** ·
**Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)
**Written:** 2026-10-20, after the user's rulings (README §6: U4, "tune within the tool. These should
be standalone tools, not depending on a datastore"; P11, P12, P17, P18). Its orchestrator note,
written after 07's review, governs where it differs from this file.
**Gate:**
- 07 landed and reviewed, the gate "Store" holding;
- on branch `jordan-normalization`, no tracked change in `git status`;
- no ChamPBH Ray process is running. You start none.

**Closes:** `[05-surfing-scan-runs-at-a-fixed-lambda]` (`surfing-temperature` board). **Opens:** only
what the work finds.
**Recommended model:** **Opus**. The user-facing surface of the change: the options, the warnings,
and the standalone tools, one of which produces a figure for the paper.

**Read first:**

1. [`README.md`](README.md): §2.2 (08's charter, and what 07 already did), §5, §6.1 U4, §6.2 P11,
   P12, P17, P18.
2. 06's and 07's logs: the tuning lines (T6), what 07 removed from `main.py` and the parser, and
   what it left for this prompt.
3. `main.py` and `config/argument_parser.py` as 07 left them; `exponential.yaml`;
   `pipeline_selection.warn_super_planckian`; `RunDrivers/registered_run.py` (`scope`,
   `StageTracker`) and its tests; `plot_inputs.py`'s option matching.
4. `tools/history_and_bbn.py`, `tools/surfing_scan.py` (`_INPUT_MATCH`, `COLUMNS`, the sidecar,
   `T_STOP_GEV_DEFAULT`), `tools/plot_surfing_histories.py`, `tools/plot_surfing_temperature.py`,
   and their tests in `tools/tests/`.
5. The `surfing-temperature` campaign's handover (`.documents/review-remediation-verification.md`
   §4.14) and its verification document §1.1, §1.7, §1.8: the commands, the costs, what the
   normalization could move.
6. `extract_common.py`'s `CSV_COLUMNS` and the `histories.csv` writer.
7. The issue's entry on the `surfing-temperature` board.

---

## 1. What is wanted

The options say what a tuned run is, the warnings use what is known before a run, and every tool
that runs its own histories runs tuned ones, standalone, with no store (U4).

### 1.1 The items

- **D1 — `main.py`'s options.** `--potential-type` goes (one model type). New:
  `--tuning-tolerance` (default 10⁻⁵, a key of the history), `--tuning-max-integrations`
  (default 30), and `--n-values` (default `1`; R14), each with help that says what it means.
  `exponential.yaml` gains nothing it does not need; its `log10-one-plus-z-high` follows 03's
  default or is removed (say which, and why). `plot_inputs.py`'s option matching covers the new
  keys, so the plot scripts still stop (exit 2) on a mismatch.
- **D2 — the super-Planckian warning** (P11): A(φ*) is evaluated with the seed's φ_ref
  (`model.seed(cosmology)`), before Ray, and the warning's text says so ("at the seed's φ_ref; the
  tuned value differs by …"). It remains a warning: nothing is skipped. Its docstring's arithmetic
  (`ln(M_P/T*)` and the thresholds in β) is corrected for φ_ref.
- **D3 — the wrapper's scope** gains n and the tolerance; its tests follow. Check once more that
  the tuning lines (06, T6) beat and are not taken for a stage; a test with a stand-in program that
  prints them.
- **D4 — `tools/history_and_bbn.py`.** By default a tuned history (`tune_history`), printing its
  tuning lines and, at the end, the T7 outputs; `--fixed-normalization NORM PHI_REF_MP` runs
  `integrate_history` instead (P18), for diagnostics, with NORM in the model's own meaning (ln V₀
  for the exponential model, as a history stores it). Its `--T-stop-GeV` remains only with
  `--fixed-normalization` (a tuned history ends at T_CMB). It opens no store.
- **D5 — the surfing tools** (U4). `tools/surfing_scan.py` tunes each β to T_CMB, standalone, and
  analyses the final integration's dense output as before:
  - its analysis window's lower edge stays 1 MeV by default, but is now a window on the tuned
    history, not where the integration stops: rename the option so that it says so, and keep the
    old name refused with a message;
  - its CSV drops `Lambda_eV` and `T_stop_GeV`, and gains `normalization`, `V_ref_eV4`,
    `phi_ref_Mp`, `residual_H`, `residual_phi`, `tuning_iterations`; `_INPUT_MATCH` and the sidecar
    follow (the tolerance added). A CSV of the old format is refused on resume, with a message;
  - `tools/plot_surfing_histories.py` follows; `tools/plot_surfing_temperature.py` reads the new
    columns and its title no longer prints Λ.
  This closes `[05-surfing-scan-runs-at-a-fixed-lambda]`. The committed CSVs and figures of
  `.documents/surfing-temperature/out/` are records and are not regenerated here.
- **D6 — `histories.csv`** (`extract_common.py`): Λ is replaced by `V_ref`, `normalization` and
  `phi_ref`; `phi_0`, `tracking`, `residual_H`, `residual_phi` and `tuning_iterations` are columns;
  rows sort by (M, n, β).
- **D7 — the tests.** For each of D1–D6: the removed options refused (exit 2) with a message naming
  the replacement; the warning's text; the wrapper's scope; `history_and_bbn.py` in both modes on
  a cheap point (its cost in the docstring); the surfing scan's CSV round trip and resume refusal;
  `histories.csv`'s columns from a temporary store.
- **The prose this commit makes false:** `CLAUDE.md`'s plot-scripts bullet and test counts; the
  tools' docstrings and usage lines.

---

## 2. Verification

1. **Before any edit:** the five suites at the counts 07 handed on; both golden histories pass.
2. **A short surfing scan.** Three β of S1's grid (1.50, 3.00, 4.00), with the new tool, into your
   scratch space: the run's cost per β; the roots against the committed
   `surfing_scan_phi5_M1e-3.csv`, which should agree to 05's units shift (about 2.8 × 10⁻⁴
   relative, since the roots are in GeV); the capture flag, delivery and the tuned values, as a
   measurement. From the cost, the predicted time of the full S1 and S5 scans.
3. **After:** the five suites; both golden histories; `black --check`; the `--help` of every tool
   changed.
4. **Deliberate breakages**, each failing a named test: an old option accepted silently; the
   warning evaluated at φ_ref = 0; the scan resuming into an old-format CSV.

## 3. Acceptance

1. D1–D7 as §1.1 says.
2. §2's checks in the log, with item 2's table and the predicted scan times.
3. **One commit**, with the log `logs/08-the-driver-and-the-tools.md`. The board: row 08, D1–D7,
   the gate "Driver", the issue closed on `surfing-temperature`'s board with its index row deleted
   and the count corrected, `### 5.9 After prompt 08 (<date>)`.

## 4. Stop conditions: stop and ask

- 08 needs a factory or a table changed (README §4: a lettered prompt, 07b).
- The short scan's roots differ from the committed ones by more than the units shift.
- A tool cannot tune without a store.

## 5. What this prompt does not do

- No factory, table or fingerprint changes; no store built but the tests' temporary ones; no full
  surfing scan (the user's, from 10's handover).
- **Files it changes:** `main.py`, `config/argument_parser.py`, `exponential.yaml`, `plot_inputs.py`,
  `pipeline_selection.py`, `RunDrivers/registered_run.py`, `extract_common.py`, the four tools of
  "Read first" item 4, their tests and the others of D7, `CLAUDE.md`, the boards and the index, the
  log.

**One commit**, in `CLAUDE.md`'s form, staged by explicit path only.
