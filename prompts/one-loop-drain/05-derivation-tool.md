# Prompt 05 — the derivation tool

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-06, on branch `one-loop-drain` in the main checkout, after 04 landed and was reviewed (`64a6274`,
`48d98fd`). It is written from:
- README §8.3 (the measures), §8.4 (row 05), §8.5 (the Derive gate) and §8.13 (the review before this prompt, U16 and
  P41–P49);
- README §1.1 and §6.1–§6.2: the measures as first defined, U4 (the spectrum), and P4–P5 (its constants and kernels);
- 04's log §7 (the export format, as corrected in the review);
- the scoping probes `source/probe/all_generations.py` and `source/probe/drain/phase_slip.py`;
- the tree's code at the commit that adds this prompt.

**Not to be dispatched until the user has answered U16** (README §8.13). The prompt is written on the proposal,
U4 as ruled. If the user rules otherwise, the orchestrator note says what changes.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Where it runs.** In the main checkout `/Users/ds283/Documents/Code/ChamPBH`, on branch `one-loop-drain`, from the
repository root, with `PYTHONPATH=.` and `./venv/bin/python`. Outputs go under `var/one-loop-drain/05/`.

**Gate:**
- on branch `one-loop-drain`, at or after the commit that adds this prompt, with no tracked change in `git status`;
- `./venv/bin/python -m runregistrykit list`: no run `running`, stale or unknown;
- the suites stand at the counts of `CLAUDE.md` (26, 651 = 219 + 432, 163, 173, 72), all passing;
- `var/one-loop-drain/02d/sample.jsonl` (240 lines) and its `tasks/` exist.

If any of these fails, stop and ask.

**Closes:** nothing. **Narrows:** `[00-later-bounces-act-on-quanta-already-present]`, with δ_stim measured on 02d's
sample (P49). **Opens:** only what the work finds.

**Recommended model:** **Opus**, as README §8.4 plans. The care is in the kernels' accuracy, in rebuilding K(N)
exactly from the exports, and in a detached run of 230 integrations.

**Read first:**
1. [`README.md`](README.md) §8.13 whole; §8.3; §1.1 (measures 2 and 4, for the definitions); §6.1's U4 and §6.2's P4
   and P5.
2. [`logs/04-drivers-and-exports.md`](logs/04-drivers-and-exports.md) §7, the export format, whole.
3. [`logs/03-the-store.md`](logs/03-the-store.md) §1.1 (which columns each bounce row fills) and §7.
4. `ComputeTargets/ScalarHistory.py`:
   - the comments above `PeakEstimate`, `MarginCandidate` and `Generation`, which define `log_k_ex`, `log_m` and
     `log_q`;
   - the comment above `BounceRow` and the `DrainCandidate` record, which define `N`, `N_turn`, `log_E` and `log_H`;
   - `drain_history_values`, for `parking_share`.
5. `ComputeTargets/dark_radiation.py`: `DarkRadiationLedger` (`add`, `K`, `cumulative_K`), and how the parking leftover
   enters it.
6. `extract_common.py`: `alpha_path`, `build_fields_frame`, `write_bounces_csv`, `write_histories_csv`,
   `build_history_record`.
7. `source/probe/all_generations.py` (`excess`) and `source/probe/drain/phase_slip.py`.
8. `.documents/one-loop-drain/store-03/cost_03.py` and `launch_03.sh`: how 03 realized 02d's accepted integrations,
   stored them on temporary stores, and launched them detached.
9. `.documents/one-loop-drain/export-04/build_store.py` and `export_tree.py`: how 04 built a store and exported it.
10. The sources of the spectrum, read only, in `/Users/ds283/Documents/Git paper repositories/Chameleon PBH (David's
    stuff)/`:
    - `BriefingNotes/production_energy_ratio.tex`, "Results" and "Energy with the computed spectrum";
    - `KadanoffBaym/kb_partB_report.tex`, "The soft occupation".
    Nothing is written there.

---

## 0. Where this prompt sits

04 exports, for each one-loop history, every bounce row and every sample mass. This prompt adds the spectrum model and
every measure that depends on it, outside the integrator, so that a better spectrum needs a re-run of the tool, not a
rebuild (the user's ruling 4 of README §6.3). It then runs the tool on 02d's sample.

It changes no value of the integration, no schema, no export and no label. `FINGERPRINT_FORMAT` stays 7, and
`VERSION_LABEL` stays 2026.10.0. 07's figure tools read only this prompt's output format, so §5's handover states it
exactly.

## 1. What is wanted

Notation, as README §1.1:
- all momenta are comoving, q = p a, with a = e^N (Einstein frame);
- a generation g has birth N_g, scale Q_g = e^{log_q}, and energy ΔK_g (`delta_K_Mp4`);
- x = q/Q_g, and s(x) is the shape;
- I = ∫₀^∞ x³ s(x) dx, and J = ∫₀^∞ x² s(x) dx / I;
- the occupation is n_g(q) = A_g s(q/Q_g), with A_g = 2π² ΔK_g / (Q_g⁴ I), so that (1/2π²) ∫ q³ n_g dq = ΔK_g.

Every quantity is in the unit its export column names (M_P, M_P⁴, GeV). Where the tool must convert between them, it
uses the `Units` package's constants, never a literal.

### 1.1 The spectrum module (P41; U4 as ruled)

**`ComputeTargets/quanta_spectrum.py`**, pure: plain floats and numpy arrays in, the same out. No I/O, nothing of
`ScalarHistory` or the store, no Ray.
- **The shape,** s(x) = s_dir(x) + c_a s_assoc(x), with c_a the associated piece's scale (default 1; P46):
  - s_dir(x) = x² for x ≤ x_p, and x_p² e^{−κ (x − x_p)} above, with x_p = 0.62;
  - s_assoc(x) = 49 x_*³ / x, with x_* = 0.0058, not saturated at the mass.
  Check that the two pieces as written give s(x_*) = 50 s_dir(x_*) at c_a = 1, and say so in the log.
- **κ is fitted** to `production_energy_ratio.tex`'s statements: the spectrum falls exponentially above the peak, to
  about 10⁻²⁰ at 5 ω_t = 10 k_ex from the peak values of its Table ("One reflection off V₀e^{M/φ}"). State the fit,
  its inputs and its result in the module and the log. If `BriefingNotes/scripts/` reproduces the spectrum, a fit to
  its output is allowed instead; say which. A fit that cannot be made from these sources is a stop.
- **The constants** (x_p, κ, x_*, the 49) sit together at the top of the module, each with its source quoted: the
  note, its section, and the number taken.
- **A version string,** `SPECTRUM_MODEL` (for example `"U4-v1"`), names the shape and its constants. It changes
  whenever either does, and every output names it (§1.5).
- **The moments:** I and J, at each c_a, computed once.

### 1.2 The kernels (P5, as revised in README §8.6)

As functions in the module, each at a given c_a:
- **G(y)** = ∫ x³ s(x) [√(1 + y²/x²) − 1] dx / I, the matter-like excess of a generation whose mass-to-scale ratio
  is y = m a / Q_g;
- **Ω(u)** = ∫ x³ s(x) s(x e^{−u}) dx / I, the occupation overlap of a generation whose scale is e^u times the bounce's,
  per unit A_g. Then a generation g contributes A_g Ω(ln(Q_g/Q_j)) to bounce j's overlap n̄_j.
- **Tabulated once,** at import or on first use, on a grid fine enough that interpolation error is below 10⁻⁷
  relative.
- **Closed-form asymptotes beyond the grid:**
  - for G, y²/2 · ∫x s/I as y → 0, and y · ∫x² s/I as y → ∞;
  - for Ω, its forms as u → ±∞, derived from the shape's pieces.
  The derivation goes in a comment and in the log. Where c_a = 0 changes an asymptote, both forms are given.
- **Direct-quadrature references** of every kernel and moment, slow and used only by the tests and the gate.

The F kernel and S_j are cut (README §8.3), and so are ε_j and δ_resc.

### 1.3 The tool's inputs (P42, P43)

**`tools/quanta_measures.py EXPORT_ROOT [--assoc-scale C]`.** EXPORT_ROOT is a plot run's `<output>/<label>/`,
holding histories.csv and the per-row directories (04's log §7). No store is opened, and no Ray started.
- **Finding a history.** Each histories.csv row with `drain_model` other than `fixed` and a successful history (a
  non-blank `normalization`; `failure_reasons` may still name a BBN failure) names one directory, which 04's `alpha_path` and the path formatting of `plot_ScalarHistory` give. The tool builds
  that path with those helpers, not with a copy of them.
  - Two rows that give one directory are refused, naming both.
  - A row whose directory has no fields.csv or no bounces.csv is counted and reported, not refused (a plot run may
    have skipped it).
  - A fixed-law row is skipped and counted.
- **The generations.** Candidate rows (`source` = `candidate`) with `delta_K_Mp4` > 0 and a `log_q_Mp`, in `ordinal`
  order. Each is born at its `N_turn`, with Q_g = exp(`log_q_Mp`) M_P and ΔK_g = `delta_K_Mp4` M_P⁴.
- **K(N), rebuilt from the rows.** It is right-continuous, and changes only at candidate rows with ΔK > 0, at their
  `N` (the drain N, where the ledger enters them).
  - Below the first such row, K is that row's `K_before_Mp4` (the ledger's K_initial).
  - After each such row j, K is `K_before_Mp4` + `delta_K_Mp4`, which equals the ledger's cumulative K bitwise
    (03, ruling 6).
  - **The parking leftover** enters at the cut candidate's own N (`dark_radiation.py`).
    - Where a later candidate follows the cut, that candidate's `K_before_Mp4` exceeds the previous cumulative by the
      leftover, so the jump is placed at the cut candidate.
    - Where none follows, the leftover is the last entry. K_final is the last cumulative divided by
      (1 − `parking_share`).
    - Say which held in each history of §1.6, and how often.
  - **The check.** K(N) at every sample against r_χ ρ_R,E e^{4N}, from fields.csv's `rho_chi_over_rho_rad`,
    `log_rhorad_Einstein_GeV4` (converted to M_P⁴ by `Units`) and `raw_N`. The tolerance is a number with its reason.
    A history that fails it is reported and its measures are not written. More than one such history in §1.6 is a
    stop.
- **The masses.** `log_m_eff_Mp` and `log_m_tracker_Mp` per sample. A blank is NULL, and that sample's E for that mass
  is NULL.

### 1.4 The measures, exactly (P44)

Each is computed in float64, from the inputs of §1.3 alone.
- **n_g, at birth, per generation:** ln n_g = ln A_g + ln s(x_p), the occupation at the direct peak.
- **n̄_j, per producing bounce** (a generation j):
  - n̄_j = Σ_{g: N_g < N_j} A_g Ω(ln(Q_g/Q_j)), over the generations born strictly before it;
  - if a sum is truncated, the truncation is stated with its bound on the omitted part (below 10⁻¹² of the sum), and
    the gate checks the untruncated sum on its constructed cases;
  - the cost is O(G²): state it for the largest history of §1.6 (13 343 candidates in 02d).
- **δ_stim, per history** = Σ_j 2 n̄_j ΔK_j / K_final: the fractional change of the final ρ_χ that stimulated emission
  would make, at first order, with random phases. Rescattering is bounded by it (about 2n, README §8.3). The bound is
  stated in the log and the outputs' documentation, not computed separately.
- **E(N), per sample, for each of the two masses:**
  - E(N) = Σ_{g: N_g ≤ N} ΔK_g G(m(N) e^N / Q_g) / K(N), with m from `log_m_eff_Mp` or `log_m_tracker_Mp`.
  - The parking leftover has no spectrum (`[00-the-parking-leftover-has-no-spectrum]`). It is in K(N) and not in the
    sum, and its share K_parking/K(N) is reported beside E.
  - **Per history:**
    - the maximum of E with the field's mass, and its N;
    - E today, at the last sample;
    - the maximum of r_χ E, with r_χ from fields.csv;
    - each of the above with the tracker's mass too.
- **The phase slip, per assessed candidate** (rows with `log_k_ex_Mp`), as `phase_slip.py` defines it, with that
  script's names:
  - Q = exp(`log_k_ex_Mp` + N) for the middle row x;
  - θ(p, q) = Q e^{−(N_p + N_q)/2} (N_q − N_p) / H_p, with H_p = exp(`log_H_Mp`) of the earlier row p
    (`log_H` is ln H_E before the candidate, the probe's `H_before`);
  - slip_j = |θ(j, j+1) − θ(j−1, j)| for each interior row of the sequence of assessed candidates, in `ordinal`
    order, and k_ex/H at each.
  - The rows' `N` is the probe's `N`.
  - **Per history:** the minimum slip over producing bounces (ΔK > 0) and over all; the number of slips below 1 rad;
    the minimum k_ex/H.
- **The backreaction ratio, per candidate row** with `log_m_Mp` and `log_E_Mp4`:
  - n_χ a³ = Σ_{g: N_g < N_k} ΔK_g J / Q_g, the comoving number density of the generations born before it;
  - the ratio is n_χ e^{−3 N_k} m_k / E_k, with m_k = exp(`log_m_Mp`), E_k = exp(`log_E_Mp4`) and N_k the row's
    `N_turn`;
  - **per history,** its maximum and the N there.

### 1.5 The outputs (P45)

- **Per one-loop history,** beside its bounces.csv:
  - **`quanta_bounces.csv`**, one row per bounce row, by `ordinal`: `ordinal`, `source`, `N`, `N_turn`,
    `ln_n_g` (generations), `n_bar` (producing bounces), `phase_slip` and `k_ex_over_H` (assessed candidates),
    `backreaction_ratio` (candidates with a mass and an energy). Blank where a measure does not apply.
  - **`quanta_samples.csv`**, one row per fields.csv row, in its order: `raw_N`, `log_one_plus_z`, `K_Mp4`,
    `parking_fraction`, `E_field`, `E_tracker`, `r_chi_E_field`, `r_chi_E_tracker`.
- **Per export root,** **`quanta_histories.csv`** at `<label>/`, one row per one-loop history measured. Its columns:
  - the key columns of histories.csv (`drain_model`, `alpha`, `M_Mp`, `n`, `beta`);
  - `generations`;
  - `delta_stim`, `n_bar_max`, and the `ordinal` of the maximum;
  - `ln_n_g_max`, `ln_n_g_median`;
  - the E summaries of §1.4;
  - the phase-slip summaries;
  - `backreaction_max` and its N;
  - `parking_share`;
  - `K_check_max_rel`, the largest relative difference of §1.3's check.
  It is sorted as histories.csv is.
- **Every file names the model:** a leading comment line, or two columns on every row, giving `SPECTRUM_MODEL` and
  the associated piece's scale. Choose which, and say so. The file names above are those at scale 1. At another scale
  they carry a suffix (for example `quanta_bounces-assoc0.csv`), and 07 reads only the default's.
- **Numbers** are written as 04 writes them (`repr`, blank for NULL). Integers are written as integers.
- **The tool prints** one line per history and a summary: measured, skipped as fixed-law, missing files, refused,
  failed the K check, and the wall time.

### 1.6 The run on 02d's sample (P47)

- **Which.** Every task of `var/one-loop-drain/02d/sample.jsonl` whose outcome is `success`: 230. C-3873 and the
  three failures are not among them.
- **Each task, in a child process:**
  1. **Realize and integrate,** as `cost_03.py` step 1 does. The model is realized from the line's `tuning`
     (`normalization`, `phi_ref`), and integrated once under the one-loop law at the line's A, with `tune_history`'s
     T_stop, z grid and tolerances. Here the masses are computed, as `tune_history` computes them on the payload it
     returns.
  2. **Check** every `DrainCandidate` against the last line of `var/one-loop-drain/02d/tasks/<id>.partial.jsonl` by
     `repr`. A difference is a stop.
  3. **Store** the payload, with the line's tuning keys, on a temporary store under
     `var/one-loop-drain/05/stores/<id>/`. Use the stand-in cluster, as `cost_03.py` step 3 does: never Ray, and never
     registered.
  4. **Export** it with 04's writers, in the plot scripts' layout, under `var/one-loop-drain/05/exports/<label>/`: the
     history's record into a per-task part of histories.csv, merged into one file after the last task; fields.csv
     and bounces.csv in the history's directory. BBN and the adiabatic history are not computed, and a fixed-law
     record is not written.
  5. **Delete** the temporary store once the export is read back.
  6. **Record** the wall of each step, the 1-, 5- and 15-minute loads at the start and the end, and an end marker.
- **Launch.** Detached (`nohup`), the pids recorded, logs and end markers under `var/one-loop-drain/05/`. Make one
  check that the run started, then wait in the background for the end markers; never poll. Run at most six at once,
  the largest tasks first.
- **Then** run the tool on the merged export root, at c_a = 1, 0 and 10.
- **Report** (verification §5):
  - per A and per group (P, W, B, L, R, C): the distributions (median, maximum) of δ_stim, n̄_max, ln n_g max, the E
    maxima and r_χ E's, the minimum producing slip, and the backreaction maximum;
  - the same at c_a = 0 and 10, as ratios to c_a = 1;
  - the tool's wall per history against its integration's;
  - which way the leftover was placed (§1.3), and the K check's largest difference.

### 1.7 Tests

No Ray and no persistent store. Every test of a stored history goes through `_DrainStoreCase`, and states its cost in
its docstring. P-2197 at A = 1 by `one_loop_payload` takes seconds.
- **`ComputeTargets/tests/test_quanta_spectrum.py`** (instant to seconds):
  - the shape's pieces, s(x_*) = 50 s_dir(x_*), continuity at x_p, the fitted κ against its inputs;
  - I and J against direct quadrature;
  - G and Ω at grid points, between them, and beyond the grid, against direct quadrature, to 10⁻⁶ relative, at
    c_a = 1, 0 and 10;
  - their asymptotes;
  - `SPECTRUM_MODEL` is set.
- **`tools/tests/test_quanta_measures.py`:**
  - **On constructed exports** (a histories.csv row and its directory written by 04's writers from constructed
    rows):
    - each measure against a direct computation by quadrature over the full spectrum (no kernel), to 10⁻⁶
      relative, with a few generations at known scales;
    - K(N) in each of §1.3's cases, the leftover after the last candidate and before a later one included;
    - the refusal of two rows giving one directory; a fixed-law row skipped; a missing bounces.csv counted;
    - every output column and its blanks.
  - **One stored history,** P-2197 at A = 1, written on a temporary store, exported by 04's writers, and measured:
    - K(N) at every sample equals the payload ledger's K(N), bitwise where §1.3 says bitwise and to its tolerance
      after a leftover no later row brackets;
    - the generations equal the payload's `generations` (count, N_turn, log_q, ΔK);
    - one value of each measure against the direct computation.
  - **The probe** (the gate's second part, §2.2), if it runs in seconds.
- **Existing tests.** Nothing existing should change. If one must, name it in the log. **No suite's count may
  fall.**

## 2. The Derive gate (README §8.5, P48)

### 2.1 Against direct quadrature

§1.7's tests pass. In addition, a script in `derive-05/` evaluates every kernel and moment on a dense set of points
spanning the grid and beyond it, against direct quadrature, at c_a = 1, 0 and 10. It reports the largest relative
difference of each, which must be below 10⁻⁶. A larger one is a stop.

### 2.2 Against the scoping probe, with the spectrum collapsed to its peak

With s(x) = δ(x − 1), G(y) = √(1 + y²) − 1, which is the probe's `excess`.
- **The case.** P-2197 at A = 1, integrated by `one_loop_payload`, stored, exported by 04's writers, and read by the
  tool with the shape replaced by the peak. The tool must take a shape as an argument, or the module must allow one,
  for this check; say how.
- **The probe's function,** `excess` from `source/probe/all_generations.py`, is run unedited: extracted by `ast` and
  executed with the names it reads bound to the payload's values. Its generations are the payload's `generations`
  (N_turn, log_q, share × K_final), its masses the payload's `log_m_eff` (and `log_m_tracker`), and its K(N) the
  payload's ledger. Say how in the log.
- **Compare** E at every sample, for both masses: the tool's against the probe's. The tolerance is a number with its
  reason (the sums run in different orders). A difference beyond it is a stop.
- **K(N):** the tool's rebuilt K at every sample against the payload ledger's K(N), as §1.7.

### 2.3 On the sample

§1.6's run ends with every task's end marker, every check of step 2 equal, and the tool run at the three scales. Any
task that fails is reported with its reason. More than three failures is a stop.

## 3. What is not wanted

- **No value of the integration moves,** and no schema, factory, key, fingerprint, label or export format changes. If
  the tool needs an export column that 04 does not write, stop and ask.
- **No measure enters the integrator or the store.** The integration code is not edited.
- **No threshold** refuses or warns on a measure, and no measured value opens an issue (P49).
- **No Ray, no cluster, no registered run, and no run of `main.py`, `plot_by_beta.py` or `plot_ScalarHistory.py` as a
  program.** §1.6's children call 04's functions directly.
- **No figure** (07's). No shape other than U4's in the module, unless the user's answer to U16 asks for one.
- **Nothing is written** under the briefing notes' repository, or under `var/one-loop-drain/0[1-4]*/`.
- No merge, no push. `PRyM/`, `thirdparty/` and the packages are not touched.

## 4. Verification

- **The suites,** before the change and after, per `CLAUDE.md`: ComputeTargets in two halves, and `tools/tests`
  alone. Record the counts, the times and `uptime`.
- **Where things go.**
  - §2's and §1.6's scripts go in `.documents/one-loop-drain/derive-05/`, with their text outputs in
    `derive-05/out/`.
  - The stores, the exports and the run's logs go under `var/one-loop-drain/05/`.
  - Scratch goes in your subdirectory of the session scratchpad, never `/tmp`.
- **Reproducibility.** Every number of the verification section comes from a script in `derive-05/` or a test, or from
  a file under `var/one-loop-drain/05/` that the section names.
- **Nothing is left running.** Before the commit, no pid recorded under `var/one-loop-drain/05/` answers `kill -0`,
  and every end marker exists.

## 5. The commit, the log and the board

One commit (invariant 1), with:
- **the code:** `ComputeTargets/quanta_spectrum.py`, `tools/quanta_measures.py`, and their tests;
- **the scripts:** `.documents/one-loop-drain/derive-05/` and its `out/`;
- **the verification document:** `.documents/one-loop-drain-verification.md`, a new §5 holding §2's gate item by item
  and §1.6's report. It is additive (invariant 6), and §1–§4 stay as they are;
- **dated notes:**
  - in `.documents/architecture-summary.md`, the two modules and the outputs;
  - in `.documents/numerical-strategies.md`, the spectrum model, its constants and sources, the kernels, their grid
    and asymptotes, and the accuracy measured;
- **`CLAUDE.md`:** the test counts;
- **the log** `logs/05-derivation-tool.md` (template: `dk-and-rpk-migration` README §5.1):
  - every deviation from this prompt, classified;
  - "Observations not acted on";
  - a "State handed to the next prompt" that gives 07 the output format exactly. That means, for each of
    `quanta_bounces.csv`, `quanta_samples.csv` and `quanta_histories.csv`: where it is written, which histories have
    it, every column with its unit and meaning, how NULL is written, how a file names the model, and how it joins 04's
    exports. It also tells 06 how to run the tool on a rehearsal's exports;
- **the board:** row 05, its items, the Derive gate, and **Last updated**; `[00-later-bounces-act-on-quanta-already-present]`
  narrowed in §3, with δ_stim's range on the sample; `.documents/OPEN_ISSUES.md`'s row for it, in the same commit.

Format the Python files you change with `black`. Stage by explicit path, and show `git diff --cached --name-status`
before committing. Nothing under `var/` is committed. The commit message follows `CLAUDE.md` invariant 5. Write "this
commit" where the SHA goes; the orchestrator substitutes it.
