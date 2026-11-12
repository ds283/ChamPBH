> Copied 2026-11-12 by the orchestrator from the 01 agent's handoff in the session scratchpad, so that 01 can be
> resumed from a fresh context. Paths under `S` are scratch probes and may no longer exist; the measurements
> are recorded here and in README §8.1.

# HANDOFF — electroweak-eos prompt 01 (install the table), ON HOLD

Written 2026-11-12 by the implementing agent of 01 at its stop (ruling 3). Branch `electroweak-eos`, main
checkout `/Users/ds283/Documents/Code/ChamPBH`, HEAD `468266d` at dispatch. **Nothing is committed, staged or
stashed.** Read the prompt (`prompts/electroweak-eos/01-install-the-table.md`), the campaign README, the board,
and the orchestrator note copied at
`/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-3mZNLI/d6424de6-59fe-49c8-99ac-4427afe4a6c1/scratchpad/ee01-orchestrator-s0.md`
(its rulings 1–6 bind).

`S` below = `/private/tmp/claude-35086/-Users-ds283--claude-spares-spare-3mZNLI/d6424de6-59fe-49c8-99ac-4427afe4a6c1/scratchpad/ee01-agent`.

## 0. Status

01 is **on hold** until a separate classifier prompt lands. That prompt deals with the spurious early wall/plateau
pair (§3), and the orchestrator will plan it. After it lands, 01 resumes. Re-pins and goldens are then made on a
tree that does not have the pair. Do not commit 01 before that.

## 1. Files changed or created (all uncommitted)

| path | state |
|---|---|
| `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv` | new; byte copy of `git show ec6bca8:CosmologyModels/GenericEOS/Xav_EOS_data.csv`; SHA-256 `28edd85477f3b508ccf977f05faf79b3dfd76358b8be9ee6f358a45b9eda29a8` (equal to the git blob's) |
| `tools/build_w_table.py` | new, black-clean; composes the table by U2 (b), asserts the four §1.2 checks, writes `repr(float(x))`, `--check` exits 0/1 and writes nothing |
| `tools/tests/test_build_w_table.py` | new, black-clean; 15 tests (regeneration and `--check`, composition, each assertion fired, the class on the table: Σ = 0 at/above 386.7085 GeV, EW peak 0.0377570 at 54.019 GeV ± 2e-6 / 0.2 %); OK in 0.07 s |
| `CosmologyModels/GenericEOS/Xav_EOS_data.csv` | **the new table installed** by `./venv/bin/python tools/build_w_table.py`; 170 rows (14 old, 126 notebook, 29 points, 1 notebook top), 1e-5 to 386.7085411060535 GeV; SHA-256 `690090c86e285f3ee30d03244a2c242dd2b5b3c3f405e4f94229b0e481d0119d`; `--check` exit 0 |
| `CosmologyModels/GenericEOS/Xav_EOS_spline.py` | module docstring only, rewritten for the new table (P5); code untouched; black-clean |
| `config/version.py` | `VERSION_LABEL = "2026.11.0"` with a dated history entry (2026-11-12, electroweak-eos prompt 01) |
| `ComputeTargets/tests/test_reachability_roots.py` | re-pinned d1 and d2 (§4), with comments naming the campaign; the T_lo/T_hi comment updated; black left it unchanged; 15 tests OK |
| `ObjectFactories/tests/test_dissipation_store.py` | test_10 re-pinned to "2026.11.0", its comment and docstring item 10 updated (§4); black left it unchanged; OK |
| `.documents/electroweak-eos/table/sigma_change.py` | new, black-clean; the §1.8 measurement through the class (CSV read redirected per table) |
| `.documents/electroweak-eos/table/out/sigma_change.txt`, `.json` | its output on the installed table |
| `.documents/electroweak-eos/table/compare_goldens.py` | new, black-clean, **not yet run** (the ee01 goldens do not exist yet); compares rr03 → ee01 records and checks form (keys, solver_label, sample_fields, n_samples, excerpt indices and ln(1+z)) |

The user's files from ruling 1 were never touched: ` M CLAUDE.md`, ` M science-runs/2026-11-08-science-2026.10.0/README.md`,
` M science-runs/README.md`, `?? science-runs/2026-11-08-science-2026.10.0/relativistic/`.

Scratch drafts: `S/xav_docstring.txt` (applied), `S/version_entry.txt` (applied, then reworded in place),
`S/new_table_preview.csv` (identical to the installed CSV, `cmp` equal), `S/installed_backup.csv` (copy used for the breakage test; identical).

## 2. The old-table suites (before any change to the CSV), serial, from 14:12 UTC

Runner: `S/run_suites.sh OUTDIR`; outputs in `S/suites-old/` (`summary.txt` and one file per suite).

| suite | tests | result | time | load (1-min) at start |
|---|---|---|---|---|
| CosmologyModels | 26 | OK | 73.4 s | 32.22 |
| ObjectFactories | 163 | OK | 130.8 s | 26.40 |
| RunDrivers | 76 | OK | 25.1 s | 15.11 |
| ComputeTargets test_[a-f] | 219 | OK | 151.6 s | 12.54 |
| ComputeTargets test_[g-z] | 446 | OK | 536.4 s | 13.06 |
| tools | 292 | FAILED (12) | 282.6 s | 14.53 |

The tools run counted 292 because `test_build_w_table.py` was already in the tree. All 277 pre-existing tests passed.
The 12 failures (9 tests and their subtests) are all in the new module, failing on the old table as expected. This
is also part of the breakage record. uptime when the suites finished: 14:35, load averages 10.52 / 14.19 / 17.71.

On the new table so far: CosmologyModels 26 OK (68.1 s, load 11.20); test_build_w_table 15 OK;
test_reachability_roots 15 OK; test_fingerprint_format_guard + test_store_fingerprint_golden 9 OK (unchanged);
test_dissipation_store TestTheVersion OK. **The full suites have not been re-run on the new table.**

## 3. Measurements reported at the stop

### The table and the build
- Build output: 170 rows (old 14, notebook_low 126, points 29, notebook_top 1). The notebook rows left out sit off the points' spline by |ΔΣ| = 3.73e-6 (158.5 GeV), 5.63e-12 (251.2 GeV) and 9.47e-12 (316.2 GeV).
- Table facts:
  - The bottom row has w = 0.3333333333333326; the top row has w = 1/3 exactly.
  - The largest w − 1/3 is 3.3e-11, at 11.2 keV.
  - Rows with |Σ| < 1e-9: 10 keV to 15.8 keV, and 386.7 GeV.
  - Spacing: 0.05 dex up to 100 GeV, then 10 GeV to 380, then 386.7085.
  - Row maxima of Σ: 0.100718 at 0.1585 MeV, 0.313767 at 0.1778 GeV, 0.037716 at 56.23 GeV.
- Spline peaks, from minimize_scalar on the class's spline. New: 0.100732 at 0.16045 MeV, 0.314532 at 0.181893 GeV, 0.037757 at 54.0188 GeV. Old: the same e± and QCD values, and EW 0.037436 at 53.2623 GeV.
- pandas' default `read_csv` parser does not parse the floats correctly rounded (`float_precision="round_trip"` does).
  - Against Python's float(), T differs by up to 8.6e-13 relative on the new CSV and 5.0e-12 on the old.
  - Rewriting the 14 old rows below T0 as `repr` text moves the class's nodes there by up to 5.02e-12 relative (1 of 14 bitwise equal).
  - Recorded, not acted on: a candidate for "Observations not acted on".
- Breakage:
  - One byte changed in the installed CSV (row 100: `0.30576` → `0.30577`): 4 tests of test_build_w_table failed and `--check` exited 1.
  - Restored: SHA as above, `--check` exit 0.
  - On the old table, 9 of the new tests failed (§2).

### Σ's change (`.documents/electroweak-eos/table/out/sigma_change.txt`)
- Largest |ΔΣ| by decade, from 1e-5 GeV up:

  | decade (GeV) | largest \|ΔΣ\| |
  |---|---|
  | 1e-5–1e-4 | 1.175e-13 |
  | 1e-4–1e-3 | 1.543e-14 |
  | 1e-3–1e-2 | 2.2e-16 |
  | 1e-2–1e-1 | 4.4e-16 |
  | 1e-1–1 | 4.2e-15 |
  | 1–10 | 3.046e-10 |
  | 10–100 | 2.803e-3 (at 99.79 GeV) |
  | 100–1000 | 4.058e-3 (at 336.6 GeV) |
  | 1e3–1e4 | 6.031e-4 |
  | 1e4–3e4 | 6.290e-6 |

- Where the rows are the same values:
  - 1e-5 GeV to T0: 2.309e-14.
  - T0 to 22 GeV: 2.064e-5, at 21.98 GeV, inside the spline interval [19.95, 22.39] GeV, whose upper row is one the notebook changed.
- EW peak on the 1000/decade grid: old 0.037436 at 53.221 GeV; new 0.037757 at 53.962 GeV.
- ∫Σ d ln T (old / new / addendum's merged / addendum's pipeline / LM):

  | T (GeV) | old | new | merged | pipeline | LM |
  |---|---|---|---|---|---|
  | 30–100 | 0.040662 | 0.041830 | 0.041830 | 0.040600 | 0.040787 |
  | 100–300 | 0.016101 | 0.016524 | 0.016524 | 0.016119 | 0.016496 |
  | 300–25000 | 0.003090 | 0.000164 | 0.000164 | 0.003101 | −0.000594 |

### The early turning point (ruling 3)
Probe scripts are in `S/probe/`:
- `early_tp.py`: one history on a chosen table, with `--trace`, `--atol` and `--alpha`.
- `show.py`: reads that output.
- `scan.py`, `scan2.py`: β × M scans; `scan2_new.json` is the result.
- `inj.py`: test_injection's histories.
- `release.py`: test_release's histories.
- `bounces.py`: P2197's bounce rows.
- `run_failing.py`: runs the 22 tests with the CSV redirected and records every integration. Results are in `failing_new2.json` and `.log`; `failing_ids.txt` lists the tests.

To evaluate on the old table, each script points the class's read at `prompts/electroweak-eos/data/pipeline_w_table_ec6bca8.csv`.

**Golden β = 1.5, M = 1e-3, atol = rtol = 1e-8:**
- New table: a pair of turning points.
  - "wall" at N = 3.941510, φ − φ* = −5.26e-12 M_P, T_J = 388.49 GeV (ln T_J = −36.37434).
  - "plateau" at N = 3.947334, φ − φ* = −1.89e-12, T_J = 386.24 GeV.
- Old table: none before N = 18.64.
- **The mechanism.**
  - On the new table π follows the f_m-only attractor: −3.9e-14 at N = 0.25, −3.3e-12 at N = 3.75. Σ = 0 above 386.7085 GeV, and f_m ≈ 1.8e-12.
  - On the old table π = −6.3e-3 at N = 4.
  - The step [3.937266, 3.949192] has π = −3.97e-12 and −1.82e-9 at its nodes, but its dense-output interpolant swings to +6.74e-10 inside the step.
  - The kick −β(Σ + f_m) < 0 throughout, so the exact π cannot cross zero there.
  - |π| is below atol = 1e-8.
- **Dependence on atol** (run to 100 GeV): the pair is present at 1e-9 and 1e-10 and absent at 1e-12 and 1e-14.
- **Nonadiabaticity at α = 0.05:** wall A = 3.11e6 (m_eff/H = 3.46e-6), plateau 4.40e8 (5.68e-4). On test_injection's β = 2, M = 0.5, α = 0.1 seed history: 2.33e6 and 2.03e8.
- **The verdict:** the interpolant read below the solver's absolute tolerance. Σ switching on at T_max sets it off. It is not a turning of the field.

**Which histories have it:**
- `scan2` (20 histories, golden normalization, α = 0, to 100 GeV): present at β = 1.2, 1.5, 2.5, 3, 4, 5.5 and 10, at both M = 4e-31 and 1e-3, so 14 of 20. Absent at β = 0.5, 2 and 20.
  - Every occurrence is wall then plateau, ΔN = 0.0007 to 0.0063, T_J = 385 to 389 GeV, |φ − φ*| ≤ 2.1e-9 M_P.
  - In no history was there a step node with π > 0 before N = 4.2.
- An earlier 32-history scan also covered M = 1e-12 and 0.5, on both tables. On the old table no history has such a pair.
- The pair depends on the step sequence. With the seed realization, β = 2 at M = 0.5 has it, and at M = 1e-16 does not; β = 10 at M = 0.5 does not.
- **The tests' histories:** the pair is in 31 of 33 integrations, 13 of 15 distinct setups.
  - The wrapper saw 31 integrations, 29 with the pair. test_release's 2 histories bypass the wrapper; a direct probe found the pair in both.
  - Absent: test_injection's β = 10, M = 0.5 (the history of test_11) and test_wall_rounding's β = 2, M = 1e-16.

**What it changes:**
- **first_bounce does not move.** first_bounce reads the signs at step nodes. This corrects note 01, which said it does.
  - Golden β = 1.5: N = 18.64397 old, 18.64396 new; wall-side, φ = 9.36e-6 M_P, T_J = 0.3614 GeV.
  - P2197 (β = 1.185, M = 4e-31, one-loop A = 1): reflected at N = 17.6297 on both tables.
  - So `T_deliver` and surfing_scan's bounce are untouched.
- **`turning_points` changes.** test_injection test_1 fails because walls[0] (N = 3.94) is no longer first_bounce (N = 18.64).
- **At α > 0 under the fixed law:** the spurious wall arms a drain, which is dropped at the plateau (`dropped_drains` +1). The wall gets no injection; injection indices are unchanged.
- **Under the one-loop law:** two event rows at the head of the bounce rows, source "wall" (undrained) and "plateau", with delta_K None. Ordinals shift by 2.
  - P2197: rows 9 → 11.
  - test_drain_store's extremes test copies rows[0] as its template, which is why it fails with the `None * float` error.
  - The first real candidate's A_k goes from 387.6 to 1.59e5. w_gate stays about 1, and alpha_eff goes from 0.0822760 to 0.0822767. Not separated: how much of this is the pair and how much is Σ switching on.
- **Readers of the pair downstream:**
  - margin_inputs, which takes plateaus and undrained walls as margin events;
  - bounce_record and bounce_rows, which make ScalarHistoryBounce and the `bounce_rows` column;
  - extract_common's bounces.csv;
  - tools/plot_quanta_history.py, which draws event markers.
  - Not tools/quanta_measures.py, which reads candidate rows only.
- test_release β = 1.2 parks at candidate 8, not 6. Not separated: whether the pair or the table causes it.

## 4. Re-pins made

| test | constant | old | new | reason |
|---|---|---|---|---|
| `ComputeTargets/tests/test_reachability_roots.py` d1 | EW Σ_peak (tolerance 5e-7 kept) | 0.037436 | 0.037757 | table: the new rows raise the EW peak (measured 0.03775699 via find_kicks) |
| same, d1 | EW T_peak (relative 1e-3 kept) | 53.2623 GeV | 54.0188 GeV | table: the peak moves hotter (measured 54.018802). Note 01 named only Σ; this pin also failed |
| same, d2 | EW threshold centre (tolerance 5e-3 kept) | 3.01 | 2.9992 | table: sqrt((2+Σ)/(6Σ)) at the new peak = 2.999176 (3.0118 before) |
| `ObjectFactories/tests/test_dissipation_store.py` test_10 | VERSION_LABEL | "2026.10.0" | "2026.11.0" | the label bump of §1.5. A failure outside §0's table, re-pinned on the user's ruling |

Note 01's ruling 5 applied to d1 and d2: the tolerances are kept, not widened.

## 5. Rulings so far

The orchestrator's rulings 1–6 from note 01 (quoted in its file):
1. **The user's uncommitted work is never touched;** CLAUDE.md's counts are not updated, and they are recorded in the log and on the board. STRUCTURALLY REQUIRED. It holds: CLAUDE.md is still modified.
2. **The 22 expected failures.** IMPLEMENTATION CHOICE.
3. **The early turning point is a stop.** STRUCTURALLY REQUIRED. Done; see below.
4. **The golden record names.** As the prompt says.
5. **Tolerances are kept.** IMPLEMENTATION CHOICE.
6. **No file but §4's list,** less CLAUDE.md, plus the modules whose pins move and the golden modules. STRUCTURALLY REQUIRED.

The user's rulings at the stop, in their words: "Handle it in a separate classifier prompt; re-pin test_10 to 2026.11.0".
- **On ruling 3:** the early pair is handled in a separate classifier prompt, which the orchestrator plans. The working proposal: an interior sign change of π counts only when it is resolved above atol, or when π at the step's nodes changes sign, with interior sampling kept. That prompt is to land before 01 commits.
  - 01 is on hold.
  - Do not re-pin: test_injection's four tests, test_drain_store's extremes test, test_release, test_wall_rounding, test_drain_law, test_adiabatic_dark_radiation, the goldens, or anything else whose change may come from the pair.
  - Do not commit.
  - Class: STRUCTURALLY REQUIRED (the finding).
- **On test_10:** re-pin it to "2026.11.0". It is a failure outside §0's table, re-pinned on the user's ruling. STRUCTURALLY REQUIRED (the label bump of §1.5). Done.

## 6. Deviations so far, with classes

- **Note 01 §0 said the early wall turning point "becomes first_bounce".** Measured, it does not; it becomes the first wall-side TurningPoint. A finding, recorded in the log; no change was made.
- **test_build_w_table.py was in the tree during the old-table tools run** (292 tests, not 277; 9 new tests failing on the old table). UNINTENDED DRIFT. It is harmless: the 277 pre-existing tests all passed, and it serves as breakage evidence.
- **d1's T_peak pin was re-pinned as well as Σ_peak,** because it fails on the table, and note 01 named only Σ. STRUCTURALLY REQUIRED, since the test cannot pass otherwise. The tolerance is kept.
- **The centre of d2 is 2.9992:** four decimals, the measured value rounded, as 3.01 was the paper's. IMPLEMENTATION CHOICE.
- **test_dissipation_store test_10** is a failure outside §0's table, re-pinned on the user's ruling. STRUCTURALLY REQUIRED.
- **sigma_change.py reports the parse-level node differences as well.** An addition the prompt did not ask for. IMPLEMENTATION CHOICE.
- **The build script parses with Python's float()** (csv module), not pandas. IMPLEMENTATION CHOICE; pandas' default parser is not correctly rounded.
- **test_build_w_table's EW-peak pins come from minimize_scalar,** checked on a 3000-per-decade grid. The prompt says "near 54.0 GeV with Σ ≈ 0.03776". IMPLEMENTATION CHOICE.
- **Not edited: test_kicking_function.py's stale comments and docstrings** (25118.86 GeV / 25.1 TeV top row; "0.037436 at 53.2214 GeV"). This follows ruling 6, since no pin there moves. Its top-row pins still pass and still test w = 1/3 at and above T_max, which is read from the class. The same goes for eos_reference.py's "Xav's table ends at 25 TeV". Candidates for "Observations not acted on".

## 7. What remains, in order (after the classifier prompt lands)

1. **Gate on resume.**
   - `git log`: the classifier prompt's commit is an ancestor; the working tree still holds §1's files. Its CSV SHA is `690090c8…`, and `./venv/bin/python tools/build_w_table.py --check` exits 0.
   - `git status`: the user's files are unchanged.
   - `runregistrykit list`: nothing running.
   - Re-check whether CLAUDE.md is still modified (ruling 1).
2. **Re-measure the pair on the new tree.** Run `S/probe/scan2.py` and `S/probe/run_failing.py` on the new tree (`--table` defaults: the installed CSV, or pass a path). Confirm the pair is gone; if not, stop.
3. **Run the five suites on the new table:** serial, ComputeTargets in two halves, the [g-z] half and tools in the background. Use `S/run_suites.sh S/suites-new`.
   - The failures should be note 01's 22, minus reachability d1/d2 (done), minus any the classifier prompt changes, plus none new.
   - test_dissipation_store test_10 is re-pinned and should pass.
   - For each failure, show it is the table's (§1.6) and re-pin it, keeping the tolerance. A failure outside the list is a stop.
4. **Goldens (§1.7).** In each of `golden_history.py`, `golden_tuned_history.py` and `golden_reflecting_history.py`:
   - point the path at `data/golden_history_ee01*.json`;
   - add the docstring sentence;
   - regenerate with `PYTHONPATH=. ./venv/bin/python -m ComputeTargets.tests.<module>`, beside the rr03 files, never over them.

   Then run `PYTHONPATH=. ./venv/bin/python .documents/electroweak-eos/table/compare_goldens.py > .documents/electroweak-eos/table/out/compare_goldens.txt`. If it reports "FORM: CHANGED", that is a stop.

   Breakage: with the old CSV temporarily restored, the golden tests fail against ee01. Restore the new CSV and check its SHA.
5. **Re-pin the rest** (test_injection, test_drain_store, test_release, test_wall_rounding, test_drain_law, test_adiabatic_dark_radiation, tools tests), recording old, new and reason for each. State whether β = 3 is now captured at the electroweak scale (test_plot_surfing_histories, test_surfing_scan; the reachability prediction is 2.9992).
6. **Run the five suites again.** No count may fall. tools is expected at 277 + 15 = 292.
7. **`CodeComparisonTools/XavEOS_test.py`:** a dated docstring note (P5).
8. **Write `.documents/electroweak-eos-verification.md` §1:** inputs and hashes, the build and `--check`, the composition, Σ's change, the re-pins, the goldens, the labels.
9. **Write the log `prompts/electroweak-eos/logs/01-install-the-table.md`** (template: dk-and-rpk-migration README §5.1).
   - Record every deviation with its class, and the rulings.
   - Fill in "Observations not acted on": the pandas parse, the stale comments in test_kicking_function and eos_reference, and A_k on P2197.
   - Fill in "State handed to the next prompt": the new commit, ec6bca8 as old, the golden's tuned values on both, and that CLAUDE.md's counts are to be updated once the user commits their edits.
10. **Update the board:** row 01, its items, the Table gate, and §3/§4. Update `.documents/OPEN_ISSUES.md` if any issue is opened.
11. **black** on the changed Python files.
12. **Stage by explicit path only,** the files of ruling 6. Show `git diff --cached --name-status`. Make one commit, its message per CLAUDE.md ending `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Write "this commit" where the SHA would go.
