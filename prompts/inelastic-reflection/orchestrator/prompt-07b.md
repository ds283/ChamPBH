# Orchestrator — prompt 07b, the wall search at the rounding of ln V

Read [`../README.md`](../README.md) first: §5 (the rules), §8.1 (R9, R18, R19), and 07b's charter in
§8.4. Then the prompt, the board's entry for `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]`,
log 04 §1 and §8, log 05 §5.5, log 06 §5.4, and
[`source/planning-probes-2026-10-30/`](../source/planning-probes-2026-10-30/README.md) §1. This note binds
07b only. The conventions of notes 02–07 bind here as well, and are restated below: a tolerance is a
number with its reason, timing is taken under load, and the suites run serially.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`07b-the-wall-search-at-the-rounding-of-ln-V.md`](../07b-the-wall-search-at-the-rounding-of-ln-V.md)
· **Gate:** "Band" · **Closes:** `[05-the-wall-search-refuses-a-wall-below-the-rounding-of-ln-V]` ·
**Opens:** only what the work finds · **Model:** Opus, as the prompt recommends · **Network:** none.
Nothing is pushed.

**Gate**, checked at `3252a70` on 2026-10-30 at 08:17 GMT, before this note was committed:
- `inelastic-reflection` is checked out in the main checkout, at `3252a70`, the commit that adds prompt 07b.
  That commit came after 07 landed (`5397f8f`) and was reviewed (`0b8f420`).
  - `git status --porcelain --untracked-files=all` is empty, and `git worktree list` shows the main
    checkout alone.
  - No code has changed since 07's review: `git diff --stat 0b8f420 3252a70` lists files under
    `prompts/` and `.documents/OPEN_ISSUES.md` only.
  - So the prompt's line numbers hold for this tree. Checked: `AbstractModel.py` :93–:105, :130, :137,
    :167, :522, :665; `ScalarHistory.py` :902, :1038; `test_wall_model.py` :600, :687. The probes of
    `0b8f420` hold too.
  - `HEAD` is the commit that adds this note, which touches only `orchestrator/`.
- `pgrep -lf 'gcs_server|raylet|ray::'` finds nothing. No Ray is started by this prompt.
- `./venv/bin/python -m runregistrykit list`: 11 runs, all finished, none `running` or stale.
- SciPy is 1.17.0.
- The suites were not re-run for this note. The agent measures the baselines (prompt §3.1):
  26 / 427 (96 + 331) / 103 / 115 / 64.
- The load averages were 5.2 / 9.4 / 9.0. Other workloads share the machine.

## 0. What makes this prompt unusual

**The fix is one comparison in a helper. The hard part is the derivation of its width: the failure the
probes saw does not set it.** For this note the orchestrator read the helper, `_evaluate`,
`_search_log_field`, `classical_turning_point`, `one_loop_turnaround`, `assess_infall`,
`natural_floor_warning` (`ScalarHistory.py:2051`) and the step loop's assessment (`:2440-2456`). It
re-ran four of `band_shipped.out`'s inputs through the shipped `classical_turning_point`. It also ran
`fixprobe.py` at K = 1 on five band histories. Here is what it found where the prompt is silent,
loose or wrong.

- **The shipped search fails at its first doubling probe, not in a halving.** Take the failing inputs at
  β = 2, M = 10⁻¹⁶, 10⁻¹³, 10⁻²⁵ and β = 1.2, M = 10⁻¹⁶.
  - The field itself sits at u₀ = M/φ = 0.50–0.55 quanta, with a quantum q = ulp(276.6) = 5.684×10⁻¹⁴.
    So ln V − ln V_floor rounds to one quantum.
  - The first probe, one e-fold in, is at e·u₀ = 1.36–1.50 quanta. That also rounds to one quantum.
  - The two residuals are therefore equal, and the search raises "a probe moved away from the target".
    The printed probe pairs are equal to 17 digits.
  - The window is u₀ ∈ [0.5, 0.552) q, about 0.099 wide in ln φ. A history in the band fails when a
    step start with π < 0 lands in it.
  - The root lies at M/φ_t = ln(1 + E/V₀) ≈ 71–132, with E/V₀ = 10³¹–10⁵⁷. That is about 35 e-folds in
    ln φ from the band. So every halving on these inputs happens far from the band.
- **One quantum is enough for every failure the probes saw.** `fixprobe.py` at K = 1 gives the same
  result as K = 64 on all five histories run:
  - β = 2 at M = 10⁻¹³, 10⁻¹⁶ and 10⁻²⁵;
  - β = 1.2 at 10⁻²⁰;
  - β = 3 at 10⁻¹⁶.

  All five reach T_CMB. The park, `N_final`, the step count and the range of ln 𝓡 equal `band_fixed.out`.

  This has three consequences:
  - K is set by comparisons that the band's histories never make. These are the halvings of a search
    whose root lies near the width.
  - The prompt's B2 (K = 1) will pass items 2 and 3 as written. Only item 1 catches it, unless a test
    makes the comparison that binds K. Ruling 4.
  - A derivation that covers only the observed failure gives K ≈ 1. Ruling 1 says what it must cover.
- **The halvings at an outer end of −∞ compare against the inner end at a shrinking separation.** While
  `r_out` is −∞ the loop halves up to `WALL_SEARCH_HALVINGS + WALL_SEARCH_MAX_DOUBLINGS` times. At each
  halving the check `r_out < r_mid < r_in` compares two finite values whenever `r_mid` is finite.
  - If `r_mid` ≥ 0, the comparison is `r_mid < r_in`, between two probes past the root. Their
    separation in ln φ halves each time.
  - So K alone cannot guarantee these comparisons. The guarantee depends on how far above the width the
    root lies.
  - Below the width the search ends with "the bracket's outer end is still −∞". A root inside the
    width is the case §0.3 of the prompt calls unresolvable: "E itself is within the rounding of V".
  - With K, that region grows from half a quantum to about K quanta. The log must say so, and §2 must
    measure how far every real root lies from it. Rulings 1 and 5.
- **The band's upper edge is predictable, and its lower edge may be ragged.**
  - The upper edge comes from where the history starts: at φ* = 5 M_P and π* = 0 the window is first
    met where M/(5 M_P) = 0.552 q, which is M ≈ 1.6×10⁻¹³ M_P. That is consistent with 10⁻¹³ failing
    and 3×10⁻¹³ running.
  - Below the band the window lies at φ ≈ M/(0.5 q), and the infall crosses it in few steps. Whether a
    step start lands in a window 0.099 wide in ln φ may then not be monotone in M. A bisection can
    report an edge that is not one. Ruling 7.
- **Item 3's reference is printed to eight significant figures.** Log 06 §5.4 gives Σ + leftover =
  0.012616907 at M = 10⁻³⁰, β = 2.
  - Half a unit of the last printed digit is 5×10⁻¹⁰ absolute, or 4.0×10⁻⁸ relative. That is larger
    than the 2×10⁻⁸ M-dependence the prompt allows.
  - The 2×10⁻⁸ is itself printed to one digit (log 06 §5.4, after the table).
  - Ruling 6.
- **Two docstrings say what −∞ means.**
  - `_evaluate`'s (`:142`) says −∞ "is ln(V − V_floor) where V rounds to V_floor".
  - The helper's own docstring says "-inf where V rounds to V_floor or below".
  - After the change, both are incomplete. The prompt freezes `_evaluate` (§1.2), which means its code.
  - Ruling 3.
- **The rule also reaches histories outside the band, and they should still be bitwise.**
  - At M = 10⁻¹², u at φ* = 5 M_P is 3.5 quanta. So the first infall's step starts near φ* call the
    helper with a difference that a width of K ≥ 4 makes −∞.
  - The search then goes on to the same bracket and the same root. That is why `bitwise.out` holds at
    K = 64.
  - The bitwise comparison is therefore the real test outside the band. It is not vacuous, and a count
    of the reclassified calls shows that. Ruling 5.
- **Natural mode calls the search once, at its first floor reflection** (`natural_floor_warning`).
  There the field is at the wall, not in the band. So §2.5's warning text is expected unchanged. A
  "could not be assessed" warning at the dispatch commit would mean otherwise. The log says which.
- **Warnings are not in the prompt's bitwise list (§2.4).** They are lines of `IntegrationResult`, and
  P23's text depends on the search. Ruling 8.

**Rulings.** Each binds the agent and is recorded in the log with its class.

1. **K's derivation covers every comparison the search makes between two finite values.**
   - **The rounding.** For the exponential:
     - ln V = normalization + (M/φ)ⁿ is rounded once;
     - ln V_floor is the normalization exactly;
     - the difference is exact by Sterbenz;
     - so the computed difference is an integer number j of quanta q, within q/2 of the true one.

     State the sufficient condition for two computed differences to be strictly ordered: their true
     values differ by at least one quantum. State also what a generic model adds: c half-quanta of
     rounding on ln V, and ln V_floor not exact.
   - **The comparisons, each with its smallest separation in ln φ and the slope of
     ln(V − V_floor) in ln φ** (n for the exponential, at every u):
     1. consecutive doubling probes: at least 1 e-fold;
     2. the first `WALL_SEARCH_HALVINGS` halvings with the outer end finite: the midpoint against
        both ends, at least 2^−`WALL_SEARCH_HALVINGS` e-folds;
     3. the halvings at an outer end of −∞: the midpoint against the inner end (§0, third point).
   - **The inequality on K** comes from (1) and (2). Say which comparison binds, and give the K it
     gives. Say why the probe's 64 does or does not satisfy it, and why K = 1 fixes the failures
     observed (§0, first two points).
   - For (3), give the condition on the root, as ln E − ln V_floor against ln(K q), under which every
     comparison is strict. Name the failure the search raises below it. This is the unresolvable
     region, and the log says how K enlarges it.

   *STRUCTURALLY REQUIRED* (prompt §1.1 "derived, not chosen"; §0 here).
2. **K's value.** The constant is the integer that ruling 1's binding inequality gives for the
   exponential, at n = 1 and the shipped `WALL_SEARCH_HALVINGS`.
   - Its comment states the derivation in one or two lines. It also states the assumptions under which
     it covers a generic model: the rounding of ln V in quanta, and the smallest slope of
     ln(V − V_floor) in ln φ.
   - Any factor between the inequality and the constant is named in the comment and the log, with its
     reason. No unnamed margin.
   - If the agent's K is not 64, every measurement of §2 is made at the agent's K, and the probes'
     K = 64 results are references only.

   *IMPLEMENTATION CHOICE*, ruled here.
3. **The docstrings.** The helper's docstring states the new meaning of −∞: "V rounds to V_floor, or
   lies within K quanta of it". `_evaluate`'s phrase at `:142` may be changed to the same words, and
   nothing else in it.
   - No line of code changes in `_evaluate`, `_search_log_field`, `classical_turning_point` or
     `one_loop_turnaround`.
   - Their docstrings change only where they describe −∞, and the log lists each change.
   - The review checks that `git diff` on those four functions touches docstring lines only.

   *IMPLEMENTATION CHOICE*, ruled here.
4. **A test of the comparison that binds K.** Item 2 gains one synthetic case on the exponential. Choose
   the field φ and E so that the search makes ruling 1's binding comparison with both values finite
   and near the width. For example, a field in the band and a root a few times K quanta in, so that the
   first doubling brackets and the halvings compare probes 2^−4 e-folds apart near K quanta.
   - At K it returns φ_t equal to the closed form, to item 2's tolerance.
   - Under B2 (K = 1) it raises "not monotone". The diff and the failure are recorded with B2.
   - If no input makes the binding comparison fail under K = 1, the log says why, gives the case it
     tried, and B2 is then caught by item 1 alone.

   *IMPLEMENTATION CHOICE*, ruled here.
5. **What the rule touches**, measured by a spy in the agent's scratch script, never committed. On every
   history of §2.1 at the final tree and of §2.4, give:
   - the number of helper calls;
   - the number the rule reclassifies: shipped value finite, now −∞;
   - the smallest margin of a root above the width, over every `classical_turning_point` call that
     returns a turning point. The margin is ln E − ln V_floor − ln(K · ulp(|ln V_floor|)), in e-folds.

   **If a margin falls below ruling 1's condition (3) on any history, stop and ask.** In §2.4 a history
   with reclassified calls that is bitwise is the expected case; the log says which have them.

   *IMPLEMENTATION CHOICE*, ruled here.
6. **Item 3's tolerance.** |Σ + leftover − 0.012616907| ≤ 0.012616907 × 2.5×10⁻⁸ + 5×10⁻¹⁰.
   - The first term is log 06's M-dependence at β = 2, taken as 2.5×10⁻⁸, the upper end of its
     one-digit print.
   - The second is half a unit of the reference's last printed digit.
   - The log also gives the difference against a full-precision M = 10⁻³⁰ value from §2.4's run. If
     the test fails this tolerance, give both values and stop (prompt §5).

   *IMPLEMENTATION CHOICE*, ruled here.
7. **§2.2, the edges.** These are measured at the dispatch commit, at quarter decades, every one
   reported. No bisection.
   - The upper edge: β = 2 at M = 10^−13.00, 10^−12.75, 10^−12.50, against the prediction
     M ≈ 0.552 q × 5 M_P (§0).
   - The lower edge: β = 2 at every quarter decade from 10^−25.00 to 10^−28.00.

   For each history that fails, give N, φ and u₀ = M/φ in quanta at the failure. For each that runs, say
   whether a step start with π < 0 fell in the window [0.5, 0.552) q, read from a spy on the helper.
   If the failures are not contiguous, the log says so, and the edge is reported as the range of
   failing quarter decades. *IMPLEMENTATION CHOICE*, ruled here.
8. **§2.4 and §2.5, bitwise.** Both are compared against the dispatch commit, in one temporary
   worktree of this note's commit, removed afterwards. On every history, compare by `repr`:
   - every record, the candidates' assessments included;
   - the samples, the final state, `N_final`, `nfev`, `accepted_steps` and `ts`;
   - the parking and release records;
   - **the warnings.**

   §2.1's final-tree histories use the same harness. *IMPLEMENTATION CHOICE*, ruled here.
9. **Item 2's inputs** are literals copied from `band_shipped.out` to 17 digits: the normalization, φ
   and ln E.
   - At least the five the prompt names; all twelve are preferred.
   - The model is `ExponentialModel(M · M_P, n = 1, β)` with a float β. The method is called on the
     model with the printed normalization.
   - The tolerance on ln φ_t is the sum of named terms: brentq's xtol and rtol·|ln φ_t|; the
     residual's rounding divided by its slope at the root; and the closed form's own rounding. Each
     term gets its number in the docstring.

   *IMPLEMENTATION CHOICE*, ruled here.
10. **No file but the prompt's §6 list changes.** In particular these are byte-identical:
    - `ComputeTargets/` (all but the new test module);
    - `CosmologyConcepts/Models/ExponentialModel.py`;
    - the golden modules and JSON;
    - every existing test module;
    - the `integrator-remediation` board.

    In `AbstractModel.py` the diff is the constant with its comment, the helper, and ruling 3's
    docstring lines.
11. **Timing and the suites.**
    - The helper's cost is the minimum over at least 10⁵ calls, shipped against final, in the band and
      far from it, with `uptime` beside it.
    - §2.1's wall times are the minimum of three rounds for at least three histories, with the
      dispatch and the final tree run back to back. The rest are single rounds.
    - Run `CLAUDE.md`'s commands serially, with ComputeTargets in its two halves. Send the output to a
      file in your own scratch directory, grep the verdict from it, and record `uptime` beside each.
    - A run that may pass 600 s goes in the background. Wait for its notification without polling.
    - §2's histories run at most four at a time, in the background. One that has not ended after 15
      minutes is stopped and recorded as such.
    - A failure is re-run alone before it is called real.

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status`
  before committing. Count, then paste. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its review.
- **Order.**
  1. The five suites before any edit.
  2. Ruling 1's derivation, written down before the constant is set.
  3. The helper, the constant and ruling 3's docstrings.
  4. The tests of prompt §1.4, with ruling 4's case.
  5. The breakages B1–B4. The measurement of §2.
  6. The suites and `black --check`.
  7. The records: the log, `.documents/`, the board, and `.documents/OPEN_ISSUES.md`.
- **Breakage diffs** are checked with `git apply --check` and `-R --check`, and replayed in `bash`, not
  zsh. None is committed.
- **Scratch.** Use your own subdirectory of the session scratchpad, never `/tmp`. No scratch file goes
  under the repository. Pickles go there through `PROBE_OUT` if `fixprobe.py` is used. §2's worktree
  of the dispatch commit is removed afterwards, so that `git worktree list` shows only the main
  checkout.
- **Stores and Ray.** No store is opened. No Ray is started. Nothing is written to `var/`. No golden
  JSON is regenerated.
- **Stop conditions are real.** They are:
  - the prompt's §5;
  - ruling 5's;
  - ruling 1's, if (1) and (2) give no finite K;
  - an existing test failing for a reason this note does not name.

  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

Done at `3252a70`, before this note was committed:
1. **The Gate above**, and the reading of §0.
2. **The failing searches re-run** on four of `band_shipped.out`'s inputs. The probe pairs and u₀ are
   in §0's first point.
3. **`fixprobe.py` at K = 1** on five band histories, all reaching T_CMB as at K = 64 (§0, second
   point).
4. **The suites' baselines** are the agent's (Gate).

## 2. Dispatch

One fresh-context subagent, on **Opus**, in the main checkout. Give it:
- the prompt, the README, the board, logs 04 and 05 and the probes' README;
- `HEAD` and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly the prompt's §6 files. Its log is
  `logs/07b-the-wall-search-at-the-rounding-of-ln-V.md`, in `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **No Ray, no store.**
- **Stop and ask** on any of the prompt's §5 conditions or §0's.
- **It replies with:**
  - the log's path and the message as committed;
  - `git show --stat` of its commit;
  - the suites before and after, with counts, times and `uptime`;
  - K, its derivation (ruling 1) and its comment;
  - the diff of `AbstractModel.py`;
  - the new tests by name, each with what it asserts and its tolerance;
  - the tables of §2, with ruling 5's counts and margins;
  - each breakage, with the tests it failed and how;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — nine checks

1. **Scope.**
   - `git show --stat <agent>` lists exactly the prompt's §6 files.
   - Its parent is this note's commit, or a later commit that is not the agent's.
   - `git status --porcelain --untracked-files=all` is empty, `git worktree list` shows the main
     checkout alone, and nothing is pushed.
   - `git diff <dispatch> <agent> -- CosmologyConcepts/Models/AbstractModel.py` touches only the
     constant, the helper and ruling 3's docstring lines.
2. **α = 0 and the frozen tests.** The tests of prompt §1.4 item 4 pass at `<agent>` unedited, the bump
   test in particular. `git diff` on every existing test module is empty.
3. **The helper** against prompt §1.1, line by line:
   - ≤, not <;
   - ulp of the larger magnitude;
   - −∞ where ln V ≤ ln V_floor;
   - the shipped expression, unchanged, elsewhere.
4. **The derivation.** Re-derive K from ruling 1 independently. Check:
   - the binding comparison;
   - the constant against the inequality;
   - the condition on the root for (3) and its failure;
   - the claims about 64 and 1.
5. **An independent probe** of my own (session scratchpad, not committed):
   - `classical_turning_point` on all twelve `band_shipped.out` inputs at `<agent>`, against the
     closed form;
   - a sweep of a root down towards the width, to find where the search starts to fail, against the
     log's condition (3);
   - β = 2 at M = 10⁻¹⁶ and M = 10⁻¹⁷ to T_CMB, and β = 2 at M = 10⁻¹² and β = 5 at M = 0.5 against the
     dispatch commit, bitwise including warnings, in a temporary worktree that I remove.
6. **The breakages.** Replay each diff with `git apply` in `bash`. Each must fail the tests the log
   names, and is then reverted. This covers B1–B4 and ruling 4's case under B2.
7. **The numerics of the tests.** Check item 2's tolerance terms, item 3's tolerance (ruling 6), and
   item 1's construction with `nextafter` at j = 0…K+1.
8. **The measurement and the records.**
   - **§2:** the tables are complete. §2.1 has all thirty histories at both trees. §2.2 is per
     quarter decade. §2.3 gives the gap table for 08. §2.4 is bitwise, with warnings. §2.5 and §2.6
     are given. Ruling 5's counts and margins are reported.
   - **The log:** it is in §5.1's form, with every deviation classed and §0's rulings among them.
   - **`.documents/`:** the notes are dated and additive only.
   - **The issue:** the board's row 07b, the gate Band, §5.9, the issue moved to §4 with its "Closed"
     line, and its row deleted from `.documents/OPEN_ISSUES.md` §1.19, with the count and the date
     corrected.
9. **The suites** at `<agent>`, run serially:
   - the counts the log gives, each at least the baselines;
   - ComputeTargets up by the new tests only;
   - `black --check` on the changed `.py` files.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, in the board, the log and `.documents/OPEN_ISSUES.md` wherever it appears;
- the board's row 07b landed, the gate Band held, and this note added to the list of orchestrator
  notes, marked "used for 07b";
- under `### 5.9`, an *Orchestrator review of prompt 07b* paragraph holding what each check found.

Residue the review finds is fixed in that commit or a follow-up of its own. Anything that is not
residue is opened on the board's §3 and in `.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate Band;
- the counts;
- K and why;
- the band before and after, and its edges;
- what does not change, and the reclassified calls;
- the gap table for 08;
- the costs;
- anything opened or closed;
- that 08 is written next, from 07b's "State handed to the next prompt" (README §2.1).
