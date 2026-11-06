# Orchestrator — prompt 05, the derivation tool

Read [`../README.md`](../README.md) first: §5, and §8 whole, in particular §8.3 (the measures), §8.5 (the Derive row)
and §8.13 (U16, U17, P41–P49). Then the prompt. This note binds 05 only, and governs where it differs from the prompt
(prompt, "Written"). Note 01's conventions bind as well: a tolerance is a number with its reason, timing is taken
under load, and the suites run serially.

**The prompt:** [`05-derivation-tool.md`](../05-derivation-tool.md) · **Gate:** "Derive" · **Closes:** nothing ·
**Narrows:** `[00-later-bounces-act-on-quanta-already-present]` · **Opens:** only what the work finds · **Model:**
Opus, as the prompt recommends · **Network:** none. Nothing is pushed.

**When:** at once, at the user's request of 2026-11-06 ("Please write the orchestration note and dispatch it, and
afterwards carry out a review of the implementation agent's work"), in a fresh session.

**Gate**, checked at `4820df5` on 2026-11-06 at 11:45 local time, before this note was committed:
- `one-loop-drain` is checked out in the main checkout at `4820df5`, which records U16 on top of `8378b4f`, the commit
  that adds prompt 05. `git status --porcelain` is empty.
- `./venv/bin/python -m runregistrykit list`: 23 runs, all finished; none running, stale or unknown.
- `var/one-loop-drain/02d/sample.jsonl` has 240 lines: 230 `success`, 7 `timeout`, 3 `failure`. `tasks/` holds 977
  files, a `.partial.jsonl` for each task among them.
- The suites were not re-run for this note. 04's review left them at 26, 651 (219 + 432), 163, 173 and 72, all
  passing, and only `4aa332b` (imports of `plot_ScalarHistory.py`) has touched code since. The agent measures them
  again first (prompt §4).
- Load averages 10.56 / 9.66 / 13.22 at 11:45. The machine has 10 cores and 16 GB of memory, and it is shared.

## 0. What the orchestrator found

For this note the orchestrator read: README §1.1, §6.1 (U4), §6.2 (P4, P5), §8.3–§8.5 and §8.13; 04's log §7;
`BounceRow`, `BounceRecord`, `ledger_K_before`, `DrainCandidate`, `DrainRecord`, `PeakEstimate`, `MarginCandidate`,
`Generation`, `DrainHistoryValues` and `drain_history_values` in `ComputeTargets/ScalarHistory.py`;
`DarkRadiationLedger` whole; `extract_common.py`'s `alpha_path`, `CSV_COLUMNS`, `build_history_record`,
`write_histories_csv`, `build_fields_frame`, `bounces_csv_rows` and `write_bounces_csv`; the per-history paths and
`write_csv_file` in `plot_ScalarHistory.py`; `source/probe/all_generations.py` and `source/probe/drain/phase_slip.py`
whole, and `drain_probe.py`'s candidate dump; `store-03/cost_03.py` and `launch_03.sh` whole;
`export-04/build_store.py` and the head of `export_tree.py`; `production_energy_ratio.tex` §"Results", §"Energy with
the computed spectrum" and §"Provenance"; `kb_partB_report.tex` §"The soft occupation"; and every line of 02d's
`sample.jsonl` (outcomes, drain values, candidate counts, walls, peak RSS).

Here is what it found where the prompt is silent, loose or wrong.

- **The associated piece as written has no upper end.** s_assoc = 49 x_*³/x for every x makes x³ s_assoc = 49 x_*³ x²,
  so I = ∫x³ s dx diverges, and with it Ω and the large-y form of G. Nothing in the sources gives an end. The KB report
  measures n ∝ 1/q on its resolved probes only (q ≤ 0.05 ω_t, x ≤ 0.1). It calls the piece the soft band's occupation,
  below the hard/soft split near 0.3 ω_t ≈ x_p. The orchestrator put three ends to the user, with their shares of I at
  c_a = 1 (fall as the direct piece above x_p: 2.9 × 10⁻⁵; hard cut at x_p: 8 × 10⁻⁶; hard cut at x = 0.1:
  3 × 10⁻⁸). The user chose the first (ruling 1).
- **κ.** `BriefingNotes/scripts/` does not hold the spectrum's script. `production_energy_ratio.tex` §"Provenance" names
  `beta_k_reflection.py`, which is not in the repository, so the fit is to the Table and the statement "to about
  10⁻²⁰ at k = 5 ω_t". The Table's five rows give five κ. With s(10)/s(x_p) = 10⁻²⁰/|β|²_max and x_p = 0.62, the
  orchestrator's arithmetic gives κ = 4.133, 4.018, 3.966, 3.924, 3.882 at L = 40, 70, 90, 110, 134 (ruling 2).
- **The prompt's asymptotes are too short.** As y → ∞, G = y ∫x² s/I − 1 + O(1/y), and the −1 is a relative 1/(yJ):
  dropping it misses 10⁻⁷ until y ~ 10⁷. As y → 0 with c_a > 0, the region x ≲ y of the 1/x piece gives a non-analytic
  O(y³) term, a relative O(y · 49 x_*³ c_a / ∫x s) ≈ 3 × 10⁻⁵ c_a y beside the y² term, not O(y⁴). Ω has terms of the
  same kind (ruling 3).
- **The probe's `excess` cancels.** It writes √(1 + x²) − 1, which loses every digit for x below about 10⁻⁸ until its
  own switch at ln x < −20. Each of its terms carries an absolute error up to about ε ΔK_g (ε = 2⁻⁵²). The tool must
  not inherit this, and §2.2's tolerance must allow for it (ruling 5).
- **K(N) = 0 with a generation born.** A generation is born at its N_turn, and its energy enters K at its drain N, a
  step later. A sample between the first generation's N_turn and its N has a nonzero numerator and K(N) = K_initial,
  which is 0 here. The probe divides by zero there (ruling 7).
- **A drain with no spectrum besides the leftover.** A candidate with ΔK > 0 and no `log_q` (unassessed, or with its
  margins not computed) is in K and not in E's sum. The prompt names only the parking leftover. In 02d,
  `K_share_unassessed` is 0 in all 230 lines (ruling 8).
- **"Bitwise" depends on the units.** K_before_Mp4 + delta_K_Mp4 equals the next candidate's K_before_Mp4 bit for bit
  only where the conversion to M_P⁴ is exact. It is exact in `Planck_units` and not in units where M_P ≠ 1. Note 04's
  ruling 1 is the precedent: the user said there that the units are a choice (ruling 6).
- **No helper gives the history's directory.** `plot_ScalarHistory.py` builds `beta=<β:.5g>/M=<M_eV:.5g>eV_n=<n>`
  inline, twice, and the script parses its arguments on import. "With those helpers, not with a copy of them" (prompt
  §1.3) cannot be met today (ruling 9).
- **The run's memory.** In 02d the largest task (L-M5e-20-A0.1, 13 343 candidates) peaked at 1.48 GiB. The next five
  peaked between 0.79 and 1.19 GiB. The per-integration walls (`tuning_wall` / `tuning_iterations`) sum to 7 422 s,
  with the largest at 1 079 s (ruling 11).
- **The parking leftover is common.** 221 of the 230 histories park, and the largest `K_share_parking` is 0.99999. So
  both of §1.3's placements will occur, and in some histories E is the spectrum of 10⁻⁵ of K.

**Rulings.** Each is recorded in the log, with its class.
1. **U17: the associated piece's upper end** (the user, 2026-11-06).
   - The question was: "U4's associated piece 49x*³/x has no upper end, so I = ∫x³s dx diverges. Where should it
     stop?" The user selected "1/x to x_p, then e^{-κ} (Recommended)", of three options. The other two were a hard
     cut at x_p and a hard cut at x = 0.1.
   - So s_assoc(x) = 49 x_*³/x for x ≤ x_p, and 49 x_*³/x · e^{−κ(x − x_p)} above, with the direct piece's κ. s is
     continuous at x_p, and s(x_*) = 50 s_dir(x_*) holds unchanged, since x_* < x_p.
   - The module quotes this ruling beside the constants. `SPECTRUM_MODEL` names a shape with this end.

   *STRUCTURALLY REQUIRED* (§0, first point); ruled by the user.
2. **κ.**
   - The fit is κ_L = ln(|β|²_max,L / 10⁻²⁰) / (10 − x_p), at each of the Table's five rows. The module takes the
     median row, L = 90.
   - The module and the log give the five values, as the fit's spread. The agent recomputes them; it does not copy §0's.
   - The test checks the module's κ against this formula at L = 90, from the Table's 1.43 × 10⁻⁴.

   *IMPLEMENTATION CHOICE*, ruled here (§0, second point).
3. **The kernels' accuracy, beyond the grid as within it.**
   - Each asymptote carries the terms it needs for 10⁻⁷ relative at the grid's end where it takes over, at c_a = 0, 1
     and 10. That includes G's −1 and its O(1/y) term as y → ∞, and the O(y³) term as y → 0 where c_a > 0.
   - The grid's ends are chosen where the terms left out are below 10⁻⁷. The log gives each term's derivation and the
     bound on what is left out.
   - The tests compare each kernel with direct quadrature at points inside the grid, between nodes, at each end, and
     a decade beyond each end, at the three c_a.
   - The direct quadratures split at x_p and, for Ω, at x_p e^u. Their own error is stated, below 10⁻⁹ relative.

   *STRUCTURALLY REQUIRED* (§0, third point).
4. **The shape as an object, and the peak.**
   - The module exposes the spectrum as one object or set of functions per c_a, named by the agent. It carries the
     shape, I, J, G, Ω, the version string and the scale, and the tool takes it as an argument internally.
   - A peak-collapsed spectrum, s = δ(x − 1), is defined beside it for §2.2 only. It has I = J = 1, and
     G(y) = y²/(√(1 + y²) + 1), the cancellation-free form. It has no Ω. Nothing on the command line selects it
     (U16).
   - The tool's E path calls G only through the spectrum object, so that §2.2 tests the code 07 will read.

   *IMPLEMENTATION CHOICE*, ruled here.
5. **§2.2, exactly.**
   - **The probe's names.** `excess` is extracted by `ast` from the file as committed and run unedited, with these
     names bound:
     - `Ns`, the samples' `raw_N` from the payload, sorted, as the probe sorts them;
     - `gN`, `gq` and `gdK`: the payload's `generations` `N_turn` and `log_q`, and `share` × `ledger.K(N_final)`;
     - `ledger`, the payload's `"dark_radiation"`;
     - `np` and `isfinite`.
     `lm` is the payload's `log_m_eff` (then `log_m_tracker`) in the samples' order, with None bound as NaN. The
     probe's values are in the cosmology's units and the tool's in the `_Mp` cells. E is dimensionless, so no unit
     is singled out.
   - **The tolerance.** |E_tool − E_probe| ≤ a ε Σ_born ΔK_g/K(N) + b |E_probe|. The first term is the probe's
     cancellation (§0, fourth point). The second is the order of the sums, with b a small multiple of ε times the
     number of generations born. The agent sets a and b, gives their reasons, and reports the largest of each part.
     A difference beyond the tolerance is a stop.
   - **Where the probe gives no number.** At a sample whose mass is None the probe writes 0.0 and the tool a blank.
     Where K(N) = 0 the probe gives 0.0 or a non-finite number and the tool a blank (ruling 7). Each pair is counted
     as matched only in its own case, and the counts are reported.

   *As the prompt says; made exact* (§0, fourth point).
6. **K(N), in any units** (after note 04, ruling 1).
   - **Order.** In `ordinal` order, the candidate rows must have strictly increasing `candidate_index` and
     non-decreasing `N`. A history whose rows do not is refused and reported.
   - **Bitwise becomes a tolerance.** Wherever prompt §1.3 or §1.7 says "bitwise", the check is to a tolerance of a
     few ulps relative, with its reason. That covers K_before + ΔK against the next K_before, and the tool's K against
     the payload ledger's K converted by `to_column`. The count of bitwise-equal values is reported as a measurement
     (equal at every value, in `Planck_units`), and is never asserted.
   - **The leftover.** A leftover is a jump above that tolerance. Its size must equal `parking_share` × K_final to
     the tolerance, and a history where it does not is refused. Where no row brackets the leftover, it is placed at
     the last candidate row's `N`. The log says how often each placement held.
   - **The check against fields.csv.**
     - The tolerance is derived from the magnitudes of ln K, 4N and ln ρ_R,E, times ε, with a margin, and stated as
       one number.
     - Where K(N) = 0, `rho_chi_over_rho_rad` must be exactly 0.
     - ln(GeV/M_P) comes from a `UnitsLike` of the `Units` package, never a literal. A test shows that the check's
       result does not depend on which `UnitsLike` gives it, beyond the tolerance.

   *STRUCTURALLY REQUIRED* (§0, seventh point).
7. **Where K(N) = 0.**
   - At such a sample, `E_field`, `E_tracker`, the two `r_chi_E_*` and `parking_fraction` are blank, whether or not a
     generation is born.
   - Where K(N) > 0 and no generation is born, E is 0.0.
   - `parking_fraction` is 0.0 before the leftover enters.
   - The tool counts, per history, the samples where K(N) = 0 with a generation born, and the samples that lie
     between some generation's N_turn and its N. It reports both in the log as measurements.

   *IMPLEMENTATION CHOICE*, ruled here (§0, fifth point).
8. **Drains with no spectrum.**
   - A candidate row with ΔK > 0 and no `log_q` is in K, and is not a generation.
   - The tool counts such rows per history and prints the count on the history's line. The log names every history
     of §1.6 that has one.
   - No column is added for them.

   *IMPLEMENTATION CHOICE*, ruled here (§0, sixth point).
9. **The history's directory.**
   - One helper in `extract_common.py` (the agent names it) gives `beta=<β:.5g>/M=<M_eV:.5g>eV_n=<n>` from β, M in eV
     and n. `plot_ScalarHistory.py`'s `plot_path` and `csv_path` call it, and their strings are unchanged character
     for character.
   - A test checks the helper against the old f-string on literals. An `ast` test checks that both lines of
     `plot_ScalarHistory.py` call it.
   - The tool gets M in eV as `M_Mp` × M_P/eV, with M_P/eV from the `Units` package. It parses `n` as an integer, and
     refuses a row whose `n` is not one.

   *STRUCTURALLY REQUIRED* (§0, eighth point). The change to `plot_ScalarHistory.py` moves no output.
10. **The outputs.**
    - **The model.** Every row of every file begins with two columns, `spectrum_model` and `assoc_scale`, so that a
      plain CSV reader needs no comment handling. This chooses between the prompt's two options.
    - **At another scale,** the suffix is `-assoc<c:g>` (`quanta_bounces-assoc0.csv`, `quanta_bounces-assoc10.csv`).
      At c = 1, given or by default, there is none. The tool overwrites its own files.
    - **Exit status:** 2 on a usage error or an unreadable export root. Otherwise 1 if any row was refused or any
      history failed the K check, after writing everything else; else 0.
    - **No truncation of n̄,** unless the cost requires it. Then prompt §1.4's bound applies.

    *IMPLEMENTATION CHOICE*, ruled here.
11. **§1.6's run.**
    - **Steps 1 and 3** are `cost_03.py`'s, imported or copied, with the copy recorded. The integration runs with the
      masses computed (`compute_sample_masses` at its default).
    - **The export** reads the history back from the temporary store, with its samples and bounce rows, and builds
      every file from the object read back, never from the payload.
      - The histories.csv record has `bbn=None` and `baseline=None`.
      - fields.csv uses unavailable stand-ins for BBN and Q, as 04 did (note 04, ruling 6).
      - `<label>` is the plot scripts' label for the cosmology.
    - **The merge.** Each child writes its record as JSON (NaN allowed) beside a one-row histories.csv part. After the
      last task, `write_histories_csv` runs over every record, and a check shows that the merged file's data rows are
      the parts' rows, byte for byte.
    - **Workers:** min(6, ⌊8 GiB / 1.48 GiB⌋) = 5 at once, the largest first by 02d's per-integration wall. Each child
      records its own peak RSS.
    - **A per-child wall limit** of 4 × its 02d per-integration wall + 900 s. Past it, the scheduler stops that
      child, and the task counts as a failure with its reason (prompt §2.3).
    - **The scheduler** is a script in `derive-05/`, launched once with `nohup`, and its pid recorded. The agent
      waits in one background shell command for the scheduler's own end marker, and does not poll from its turn.

    *IMPLEMENTATION CHOICE*, ruled here (§0, ninth point).
12. **Files.**
    - The commit holds prompt §5's files and `extract_common.py` and `plot_ScalarHistory.py` for ruling 9.
    - The helper's tests go in `tools/tests/` (the agent names the module).
    - Where a forced change touches them, the existing test modules are included too, each named in the log.
    - Nothing under `var/` is committed. Nothing under `.documents/one-loop-drain/` but `derive-05/` changes. The
      README is not edited (U17 is recorded there in this note's commit).

    *STRUCTURALLY REQUIRED*; followed.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before
  committing. Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The Gate, and the five suites, before anything else. They run serially: ComputeTargets in its two halves, with
     the `test_[g-z]` half and `tools/tests` in the background, waiting for each notification.
  2. The module, its kernels and their tests (§1.1, §1.2), and §2.1's script. A kernel beyond 10⁻⁶ stops here.
  3. The helper (ruling 9), and the tool on constructed exports, with its tests (§1.3–§1.5).
  4. The stored P-2197 test and §2.2.
  5. §1.6's run, detached, and the tool at the three scales on its export root.
  6. `black`; the suites again; the documents; the records; the commit.
- **Scratch.** Outputs go under `var/one-loop-drain/05/`. Probes and suite outputs go in the agent's own subdirectory
  of the session scratchpad. Nothing goes in `/tmp`. Nothing is written under `var/one-loop-drain/0[1-4]*/` or in the
  briefing notes' repository.
- **No Ray.** Nothing starts a cluster, and no `ray` command is run. No store is opened but the tests' and the run's
  temporary ones.
- **Stop conditions are real:**
  - the prompt's:
    - a kernel or moment beyond 10⁻⁶ in §2.1, or an asymptote that cannot reach 10⁻⁷;
    - κ unfittable;
    - §2.2 beyond its tolerance;
    - a candidate differing by `repr` in §1.6 step 2;
    - more than one history failing the K check in §1.6;
    - more than three failed tasks;
    - an export column that 04 does not write;
  - a suite count that falls.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `4820df5`, before this note was committed:
1. **The Gate above.**
2. **The reading of §0,** and the question of ruling 1 put to the user and answered.
3. **U17 recorded** in README §8.13, in this note's commit.

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout, in the background. Give it:
- the prompt, the README, the board and this note;
- `HEAD`, and the Gate as measured.

The agent reads this note and no other file of `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with ruling 12's files. Its log is `logs/05-derivation-tool.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Nothing it starts outlives its turn.** §1.6's scheduler and children end before the commit, and no pid of theirs
  answers `kill -0`.
- **Stop and ask** on any of the conventions' stop conditions.
- **It replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - the constants with their sources, κ's five values and the one taken, I and J at the three scales;
  - the kernels' grids, their asymptotes with the terms kept, and §2.1's largest differences;
  - §2.2: the tolerance with its reasons, the largest difference of each part, and the matched-blank counts;
  - the three outputs' columns, as built;
  - §1.6: tasks run and failed, the leftover's placements, the K check's largest difference, the walls, and the
    report of prompt §1.6 (the distributions per A and group, and the ratios at c_a = 0 and 10);
  - every existing test changed, and why;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only ruling 12's files. Its parent is this note's commit, or a later commit
     that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, and nothing is pushed.
   - No line under `ComputeTargets/` changes but `quanta_spectrum.py` and its test, and none under `ObjectFactories/`.
     `FINGERPRINT_FORMAT` is 7 and `VERSION_LABEL` 2026.10.0.
   - `plot_ScalarHistory.py`'s diff is ruling 9's two lines and the import.
   - No pid recorded under `var/one-loop-drain/05/` answers `kill -0`, and nothing under `var/one-loop-drain/0[1-4]*/`
     is newer than the dispatch.
2. **The suites, re-run by me**, serially as the conventions say: the counts the log states, all passing, none fallen.
3. **The spectrum, independently.** In a scratch script of mine, with my own quadrature:
   - the shape at a few x, with U17's tail;
   - κ from the Table;
   - I and J at the three scales;
   - G and Ω at random points inside, between nodes and beyond the grid, against the module, at the three scales.
4. **The measures, independently.** On one constructed export of my own (three generations at known scales, a
   leftover after the last candidate, a mass blank), and on one history of §1.6's export (P-2197 at A = 1):
   - K(N), n_g, n̄_j, δ_stim and E by my own direct quadrature over the full spectrum;
   - the phase slip and k_ex/H by `phase_slip.py`'s formula;
   - the backreaction ratio;
   - all compared with the tool's files.
5. **§2.2, independently.** Run `excess` by my own `ast` extraction on P-2197's payload. Compare it with the tool's
   peak-collapsed E, under the agent's tolerance.
6. **The run.**
   - Every task has its end marker and result, the step 2 check equal, and the temporary store deleted.
   - The merged histories.csv equals the parts.
   - Each A's directory count matches the successes.
   - Re-run the tool at c_a = 1 on the export root, and check that its outputs are byte-identical to the agent's.
   - Spot-check the report's medians from the files.
7. **The records.**
   - The log is in §5.1's form, with every deviation classed and §0's rulings among them. Its §7 gives 07 the format
     and 06 the command, as prompt §5 asks.
   - The verification document's §1–§4 are unchanged, and §5 follows prompt §2 and §1.6.
   - The board: row 05, its items, the Derive gate, `[00-later-bounces-act-on-quanta-already-present]` narrowed with
     δ_stim's range, and **Last updated**. `.documents/OPEN_ISSUES.md`'s row matches.
   - `CLAUDE.md`'s counts are those measured.
   - `architecture-summary.md` and `numerical-strategies.md` have their dated notes, with U17's end in the latter.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 05 landed, the Derive gate held, and this note added to the list of orchestrator notes, marked
  "used for 05";
- an *Orchestrator review of prompt 05* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3
and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the Derive gate: the kernels, §2.2, and the run;
- δ_stim, E and the backreaction on the sample, in a line each;
- anything opened;
- that 06 (the rehearsal) is written next.
