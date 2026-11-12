# Prompt 01a — turning points resolved above the solver's tolerance

**Campaign:** [`README.md`](README.md) · **Board:** [`IMPLEMENTATION_STATE.md`](IMPLEMENTATION_STATE.md)

**Written:** 2026-11-12, after prompt 01's stop and the user's two rulings. It is written from:
- README §8 whole: what 01 measured (§8.1), the prompt and its gate (§8.2), and U8–U11 as ruled. The
  user's words: "Handle it in a separate classifier prompt; re-pin test_10 to 2026.11.0", then "U8 to U11
  are fine as proposed; write 01a";
- the board's §3 entry `[01-turning-points-read-below-the-solver-tolerance]`, which this prompt closes.

If an orchestrator note is written for this prompt at dispatch, it governs where it differs from this file.

**Gate:**
- you are in a **worktree** on branch `electroweak-eos-01a`, cut from the commit that adds this prompt
  (U10). It is not the main checkout, which holds prompt 01's uncommitted work and the user's;
- `git status` in the worktree is clean;
- `./venv/bin/python -m runregistrykit list`, run in the main checkout: no run `running`, stale or unknown;
- the worktree's `CosmologyModels/GenericEOS/Xav_EOS_data.csv` is byte-identical to `git show ec6bca8:` of
  the same path: **the old table**. 01's new table is not installed here.

If any of these fails, stop and ask.

**Closes:** `[01-turning-points-read-below-the-solver-tolerance]`. **Opens:** only what the work finds.

**Recommended model:** **Opus**. The change is a few lines in the step loop, but it sits in the classifier
that every turning point, drain and bounce row passes through, and it must change nothing on any history
the old table gives.

**Read first:**
1. [`README.md`](README.md): §8 whole, §5, and §1's out-of-scope list.
2. `ComputeTargets/ScalarHistory.py`:
   - `TURNING_POINT_INTERIOR_SAMPLES` and its comment (:105–:108);
   - the `IntegrationResult` namedtuple and its comment block (about :646–:720);
   - `integrate_scalar_history`'s docstring, the paragraph "Turning points and the drain" (about :1934);
   - `observe_turning_points` (about :2480–:2620) and `handle_turning_point` after it: how the samples are
     folded into the half-oscillation window (`fold`, `close_half_oscillation`) and how a turning point
     arms or drops a drain;
   - where Radau is built (`rtol=params.rtol, atol=params.atol`, about :2186 and :2856), to see which
     tolerance applies to π on a step, and whether the state the solver integrates is the state the
     interpolant returns (`y[1]` is π);
   - `first_bounce` (about :3391) and `surfing_turning_points` (about :3481).
3. `ComputeTargets/tests/test_injection.py`, the module docstring and `TestTwoTurningPointsOnOneStep` (11):
   the interior pair that led the user to rule interior sampling (inelastic-reflection 02, its note's
   ruling 15). That pair is real and must be kept.
4. `.documents/numerical-strategies.md` §6.2, "Turning points, at every α".
5. The handoff of prompt 01 (the orchestrator gives you its path, or the parts you need): how the pair
   was measured, and the probes that measured it.

---

## 0. Where this prompt sits

Prompt 01 installed the new w table, and is held uncommitted at its stop. On the new table Σ = 0 above
386.7 GeV, so a history that starts at rest keeps |π| at 10⁻¹⁴ to 10⁻⁹ M_P until Σ switches on. On the
step that straddles 386.7 GeV, π at both nodes is negative, and Radau's interpolant swings above zero
between them, by less than the solver's absolute tolerance. `observe_turning_points` reads that as a wall
and a plateau. The field never turns: the kick there is negative and friction cannot reverse π (README §8.1).

This prompt makes the classifier ignore such a pair. It runs on the **old** table, where no such pair is
known, so its effect there must be nil. It checks its effect on the new table by probe only. 01 then
resumes on top of it, and makes its re-pins and golden records once, without the pair.

## 1. What is wanted

### 1.1 The rule (U8)

On an accepted step, π is read at the two nodes and at the interior samples, as now. Let `changes` be the
sign changes between consecutive samples, as the code finds them now. Then:

- **If the samples show exactly one change and π at the two nodes is of opposite signs,** it is a turning
  point, located end to end as now. Nothing about this case changes, whatever its size.
- **Otherwise**, walk `changes` in order from the step's start. For a change *i* that has a successor
  *i + 1*, the **excursion** between them is the largest |π| over the samples strictly between the two
  crossings: the samples after change *i*'s crossing and before change *i + 1*'s, all of the opposite sign
  to π before change *i*. The **tolerance** is

  τ = atol + rtol · max(|π_low|, |π_high|),

  with π_low and π_high the nodes' π and atol, rtol the values the step's Radau instance was built with.
  If the excursion is not greater than τ, **both changes are dropped** and the walk continues at *i + 2*.
  Otherwise change *i* is kept and the walk continues at *i + 1*. A change with no successor is kept.
- Each kept change is refined on its own sub-interval, exactly as the code does now for two or more
  changes. The end-to-end bracket of the first case is used only when the samples show exactly one
  change, as now, even if dropping a pair leaves one.

The rule is stated in the solver's tolerance on the state it integrates. Show in the log that π enters
Radau's error norm as `y[1]` with the scalar atol and rtol, so that τ is that component's tolerance. If it
does not (a scaled state, a vector atol), stop and ask.

### 1.2 A dropped pair (U9)

A dropped pair is **as if it were not there**. It gives no `TurningPoint`. It neither arms nor drops a
drain. It is no boundary of the half-oscillation window, so its samples are folded as if the step held no
turning point. It gives no bounce row.

It is counted in a new field of `IntegrationResult`, `unresolved_turning_point_pairs`: the number of pairs
dropped over the history, counted at every α, α = 0 included. The field is appended last, with default `0`
for stand-ins, as `restart_retakes` was, and the loop always passes it. It is **not** a payload key, not a
column, and not part of any record or fingerprint. Its comment in the block above the namedtuple says so,
and cites this prompt.

### 1.3 Where the rule lives

Factor the decision into a module-level function of `ComputeTargets/ScalarHistory.py`, so that it can be
tested on constructed inputs. It takes the samples' N and π and the tolerance τ, and returns the kept
changes in order and the number of pairs dropped. `observe_turning_points` calls it, and is otherwise
unchanged. When no pair is dropped, every kept change, its sub-interval and its root are exactly what the
code gives now. That is what makes the old table's histories bitwise unchanged.

Update `integrate_scalar_history`'s docstring paragraph "Turning points and the drain", and the comment on
`TURNING_POINT_INTERIOR_SAMPLES`, to state the rule and cite this prompt and README §8. Keep the
history of the interior sampling (ruling 15) where it is.

### 1.4 Tests

A new module, `ComputeTargets/tests/test_resolved_turning_points.py`. Its docstring states the rule, the
source of each case, and the runtime of each test.
- **On constructed samples** (no integration), through the module-level function:
  - an interior pair with an excursion below τ is dropped, and counted;
  - the same pair with an excursion above τ is kept;
  - an excursion exactly equal to τ is dropped (the rule is "greater than");
  - a pair whose excursion lies between atol and τ, with nodes large enough that only the rtol term drops
    it, is dropped;
  - a single change across the nodes is kept whatever its size, even below τ;
  - three changes on one step, with the first pair unresolved, keep only the third. With the first pair
    resolved, they keep all three;
  - a sample exactly 0.0 is treated as the code treats it now.
- **On integrations:**
  - test (11)'s pair (β = 10, M = 0.5 M_P, α = 0.1, parking cut off) is kept, with its excursion and τ
    stated in the docstring as measured;
  - a history that starts at rest with Σ zero above some temperature, built **without** installing a
    table, gives no turning point there and counts its dropped pair. Construct it with a stand-in EOS
    or a patched Σ inside the test, so that the module runs on the old table. Say how in the docstring.
    It must take no more than a few seconds.

Every existing test passes unchanged. If one needs a change, stop and ask.

## 2. What is not wanted

- **No other change to how a turning point is found or classified.** No change to the number or placement
  of the interior samples, to brentq's xtol, to `first_bounce`, to `surfing_turning_points`, to the drain,
  the gate, the drain law or the parking and release.
- **`surfing_turning_points`** samples g the same way. Check whether it can record a pair below its
  solver's tolerance in the same circumstances, and record what you find in the log's "Observations not
  acted on". Do not change it.
- **No change to the table**, to `Xav_EOS_spline`, to `config/version.py` (U11: no label bump), to
  `CLAUDE.md`, or to any file prompt 01 has changed or created. The orchestrator gives you that list.
  `CLAUDE.md` is left alone because it holds the user's uncommitted edits in the main checkout, and a
  change to it in this worktree would stop the fast-forward (U10).
- **No golden record** is regenerated, and none may move.
- **No store** but temporary ones in tests. **No Ray.** No figure.
- A pair split across two steps (one change at the end of one step, one at the start of the next) is not
  this rule's. If you see one, record it; do not handle it.

## 3. Verification

### 3.1 Before you change any code

- The five suites in the worktree, per `CLAUDE.md` (ComputeTargets in two halves, tools alone, the long
  ones in the background), with counts, times and `uptime`.
- **The baseline scans**, on the old table and the unchanged code: the turning points (N, kind, φ_E,
  ln T_J, nonadiabaticity, m_eff/H) and `first_bounce` of each history of 01's 20-history scan and its
  32-history scan, written to JSON with floats in hex. Use 01's probes, copied into your own scratch
  directory, or your own.

### 3.2 After

- **Old table, bitwise (the Resolved gate, first part).**
  - The five suites at their counts plus this prompt's module, every existing test passing unchanged.
  - The three golden tests pass against the committed `rr03` records.
  - Both scans again: every turning point and `first_bounce` identical to the baseline to the last bit,
    and `unresolved_turning_point_pairs` 0 in every history.
  - If any history on the old table drops a pair, stop and ask.
- **New table, by probe (the Resolved gate, second part).** 01's new CSV is given to you as a file in the
  session scratchpad. Read it through a redirect of the class's CSV path inside the probe, or by swapping
  the worktree's CSV for the duration of a probe and restoring it. If you swap, show the SHA-256 equal to
  `ec6bca8`'s after restoring. Never commit it. On the new table:
  - the pair is dropped in every history of 01's scans where 01 saw it, and in the 22 tests' histories
    (01's `run_failing.py`, or your own);
  - at α = 0, each history is bitwise identical with and without this prompt's change, except for
    `turning_points` and the new count. Compare the solution's nodes and the final state;
  - `first_bounce` is unchanged;
  - list, for 01, which of note 01's 22 tests pass on the new table with this prompt's change, and which
    still fail, each with its first failing assertion.
- **The breakage record.** Each new test is shown to fail when what it guards is broken: with the
  comparison made `>=`, with τ's rtol term removed, and with the old code (no pair dropped).
- `black` on the Python files you changed or added.

The measurements go in `.documents/electroweak-eos/turning-points/`: the probe scripts, `out/` as text
only, and a short `measurements.md` stating what each shows. This folder is new. 01's folder,
`.documents/electroweak-eos/table/`, and the verification document, which 01 creates, are not touched.

## 4. The commit, the log and the board

One commit (invariant 1), on `electroweak-eos-01a` in the worktree, with:
- `ComputeTargets/ScalarHistory.py`;
- `ComputeTargets/tests/test_resolved_turning_points.py`;
- `.documents/electroweak-eos/turning-points/`;
- `.documents/numerical-strategies.md`: a dated note in §6.2 under "Turning points, at every α", stating the
  rule and citing this prompt;
- the log `logs/01a-resolved-turning-points.md` (template: `dk-and-rpk-migration` README §5.1), with every
  deviation classified, "Observations not acted on", and "State handed to the next prompt". The last says
  what 01 needs: the list of §3.2, the new suite counts, and that `CLAUDE.md`'s counts wait for 01;
- the board: row 01a, the Resolved gate, the issue moved from §3 to §4, with `.documents/OPEN_ISSUES.md` in
  the same commit (its row deleted, the count and date corrected).

Stage by explicit path, and show `git diff --cached --name-status` before committing. The commit message
follows `CLAUDE.md` invariant 5. Write "this commit" where the SHA goes; the orchestrator substitutes it.
Do not push, and do not merge or fast-forward anything: the orchestrator brings the commit into
`electroweak-eos` (U10).
