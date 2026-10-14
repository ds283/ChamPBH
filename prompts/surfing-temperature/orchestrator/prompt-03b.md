# Orchestrator — prompt 03b, the first bounce of uncaptured histories

Read [`../README.md`](../README.md) first: §2.2 (03b's charter), §4 (the figure), §5 (rule 13) and
§6.1 (U6, U7). Then the board's §5.4 and §5.4a. The conventions are note 03's
([`prompt-03.md`](prompt-03.md), "Conventions"), restated here where 03b needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- run the agent's plot script into the scratchpad;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`03b-plot-uncaptured-deliveries.md`](../03b-plot-uncaptured-deliveries.md)
**Board item:** F4 · **Gate:** none of its own · **Closes:** nothing · **Opens:** only what the
work finds · **Model:** Sonnet, as the prompt recommends · **No network.**

**Gate:**
- 03 has landed (`1f40b53`, reviewed `9ab3e65`); U6 is recorded (`c4c38bc`) and 03b written
  (`2076fcd`). `HEAD` is the commit that adds this note, or a later one that touches no code.
  Since `a8823a9`, only 01's, 02's and 03's commits have touched anything outside `prompts/`.
- One prompt at a time in this checkout. 04 is not dispatched until 03b is reviewed.

## 0. What makes this prompt unusual

**It changes a script that has already been reviewed, under 03's tests.** README §5 rule 13 holds:
03's 15 tests are not edited, and the new tests are a new class. So the new series must be added in
a way none of 03's tests can see, except where U7 means them to.

**The inputs**, copied into the agent's scratch directory, `agent03b/inputs/`:
- `scan79.csv` and its sidecar: the orchestrator's probe of note 03, β = 1.10–5.00 by 0.05, run at
  `61e0c42`, 79 rows, all `status=ok`. Uncaptured at β = **1.10** (first bounce 0.1434 GeV) and
  **3.00** (1.680 GeV).
- `edges.csv` and its sidecar: the orchestrator's probe of board §5.4a, 14 β near 3.0, run at
  `9ab3e65` (`git_tracked_changes: false`). Ten captured, four not:

  | β | captured | first bounce (GeV) |
  |---|---|---|
  | 2.991–2.997 (4 β) | QCD | 1.802 → 1.644 |
  | 2.999 | no | 1.641 |
  | 3.012 | no | 39.19 |
  | 3.013 | no | 41.04 |
  | 3.015 | no | 44.14 |
  | 3.020–3.045 (6 β) | EW | 49.72 → 61.18 |

  3.012 and 3.013 are 0.001 apart, the spacing of 04's fine grid (S5), so this CSV shows whether
  adjacent new markers stay distinguishable there.

Neither is 04's scan; neither is committed.

**The figure as it is, on `edges.csv`** (the orchestrator's probe at `2076fcd`, scratch, 5 s; the
fine root grid is 21 β, 2.95–3.05):
- It draws correctly: the QCD + μ/π merged band, EW from 3.015, the EW threshold label at 3.012
  (the three thresholds outside the range are not drawn), the four uncaptured edge marks.
- The upper axis spans about 10⁻⁴ to 10³ GeV, because of the e⁺e⁻ band. The new points (1.64 to
  44 GeV) lie between the QCD and EW bands and will be clear of both.
- **Pre-existing, and not 03b's:** the e⁺e⁻ band's label ("e⁺e⁻ / reachable, not surfed") sits at
  the bottom centre of the upper panel and overlaps the uncaptured edge mark at β = 2.999. It is
  out of 03b's scope (§1.2: "everything else the figure draws is unchanged"). **Ruled here:** the
  agent does not move it; it records it in "Observations not acted on", says whether it is worse
  in its figure, and the orchestrator takes it to the user at the review.

**The code, at `2076fcd`** (`tools/plot_surfing_temperature.py`, 591 lines):
- `parse_rows` gives each row a `kind`: `psp.CAPTURED`, `psp.UNCAPTURED` ("not captured") or
  `psp.FAILURE`. Blanks are `None`.
- `build_figure(rows, roots, sidecar)` has two axes, `ax` (upper) and `rx` (lower):
  - the four series loop, `SERIES` at `:100-114`, drawn at `:424-450`, over `captured` rows only,
    each with `ax.plot` and gid `marker-<key>` (upper) and `ratio-<key>` (lower);
  - the edge marks at `:452-474`, gid `edge-uncaptured-upper/-lower` and `edge-failure-...`, in
    the axis transform at y = 0, colour "0.25" for uncaptured;
  - the upper y-limits at `:476-488`, from the root curves and the captured rows' four columns
    only: `ax.set_ylim(min(ys) / 3.0, max(ys) * 25.0)`. **The new points must be added to `ys`.**
  - the legend at `:508-535`, one `fig.legend` from `handles`.
- The delivery series is a black filled disc, size 6.5, zorder 3 + 5. The new marker must differ
  from it in fill, in colour and in greyscale (prompt §1.1).

**03's tests that the change could trip** (`tools/tests/test_plot_surfing_temperature.py`):
- **`TestBuildFigure.test_e` asserts `len(self.upper.collections) == 5`.** Drawing the new series
  with `ax.scatter` adds a collection and fails it. **Draw it with `ax.plot`**, as a `Line2D`, like
  the existing series.
- `one(ax, gid)` asserts exactly one artist per gid, so the new artist needs a gid of its own (the
  prompt's `marker-delivery-uncaptured` is fine).
- `test_a` asserts β = 1.2 (the synthetic uncaptured row, `T_deliver_GeV` 0.14) is *not* in
  `marker-delivery`. That stays true and is wanted.
- `test_c` asserts the legend has "not captured" and "failure", and has neither when only captured
  rows are drawn. `assertIn` on the list of legend texts compares whole strings, so a new entry
  "first bounce, not captured" does not affect it.
- The synthetic CSV already has an uncaptured row with a delivery (β = 1.2), so 03's `setUp` figure
  will contain the new marker. The new class may reuse `SYNTHETIC_ROWS`, `make_row`, `write_csv`,
  `STUB_ROOTS`, `one` and `legend_texts` from the module; it may not change them.

**The conventions of note 03 hold.** In short:
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes it in its follow-up.
- **Suites.** `CLAUDE.md`'s commands, ComputeTargets in its two halves, each in the foreground,
  serially. Each run's output goes to a file in the agent's scratch directory, the verdict is grepped
  from it, and the load (`uptime`) is recorded beside it. A failure is re-run alone before it is
  called real.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
- **Scratch.** The agent's own subdirectory of the session scratchpad, never `/tmp`. Python's
  `tempfile` in tests is fine. No scratch file goes under `tools/` or `.documents/`. The figures
  of §2 step 4 are written there and are not committed.
- **The venv.** Nothing installed or uninstalled.
- **Stores and Ray.** None opened, none started. 03b runs no history and no PRyMordial.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit.

**What moves on purpose:**
- `tools/plot_surfing_temperature.py`: the new series, its `ys`, its legend handle, the module
  docstring.
- `tools/tests/test_plot_surfing_temperature.py`: one new class at the end, and the module
  docstring's list if the agent extends it (a docstring is not a test; say so in the log).
- The log `logs/03b-plot-uncaptured-deliveries.md`, and the board.
- Nothing else. No issue is expected; the index is untouched unless one opens.

**The facts, checked by the orchestrator on 2026-10-14 at `2076fcd`.** Pass them on.
- **The tree.** Branch `surfing-temperature`, `HEAD` `2076fcd` before this note; nothing staged.
  The untracked set is unchanged: 17 `pilot-2026.6.0*.db`, `pilot-main.log`, `pilot-plot.log`,
  `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`,
  `prompts/jordan-normalization/`.
- **The cost.** `main` on `edges.csv` took 5 s at load 7. On `scan79.csv` it takes one to two and
  a half minutes (781 β of roots; log 03).
- **The suites at `2076fcd`** are given in the dispatch (§1 step 2).

**What the review exists to establish.**
- **(E1) F4.** One new `Line2D` in the upper panel only, gid of its own, at (β, `T_deliver_GeV`)
  for every uncaptured row with a delivery, open grey circle, legend entry only when non-empty, in
  the y-limits.
- **(E2) Nothing else in the figure moves**, and 03's 15 tests pass unedited.
- **(E3) The new tests** as the prompt's §2 step 2 lists them.
- **(E4) Nothing else in the tree moves.**
- **(E5) Legibility,** judged by the orchestrator on both CSVs' PNGs.
- **(E6) The pins.** Each of the three breakages fails a named new test.
- **(E7) The records.** The log, the board, the counts.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline a8823a9..HEAD -- . ':!prompts'` lists only `2c98370`, `f166b31` and
   `1f40b53`.
2. **Baselines.** The four roots at `HEAD`, serially, with the load. Give the agent the numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompts 04–05;
- its scratch directory, with the inputs of §0 in it.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/03b-plot-uncaptured-deliveries.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may change are the prompt's §5 list, and nothing else.** 03's tests are not
  edited (rule 13); the new tests are one new class.
- **Draw the new series with `ax.plot`**, not `scatter` (§0: `test_e` counts collections).
- **§2 step 4 plots both CSVs**, and the agent looks at both PNGs and a greyscale copy of each.
- **The e⁺e⁻ label collision on `edges.csv` is not its to fix** (§0); it records it.
- **It starts no Ray, opens no store, runs no history or PRyMordial, and installs nothing.**
- **Stop and ask** on any of the prompt's §4 conditions.
- **It replies with:**
  - the log's path and the message as committed;
  - the diff of `build_figure`, in full;
  - the marker chosen (shape, size, face, edge colour, z-order) and why it reads against the
    delivery disc in colour and greyscale;
  - what it saw in each PNG, and anything hard to read;
  - the new tests, with counts and wall time;
  - the four suites;
  - the three breakages and the tests each fails;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — nine checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` touches only the script, the test module, the
   board and the log. `git status` shows §0's untracked set and nothing new. No figure is in the
   tree.
2. **E4 and rule 13.** `git diff <agent>~1 <agent> -- extract_common.py tools/surfing_scan.py
   ComputeTargets/ config/` is empty. The test module's diff only adds: no line of 03's classes or
   helpers is changed or removed (`git diff ... | grep '^-' ` shows only the docstring, if that).
3. **E1 and E2, by reading.** The diff of `build_figure` in full:
   - uncaptured rows with a non-`None` `T_deliver_GeV`, and only those;
   - drawn on `ax` only, by `ax.plot`, with its own gid;
   - open face, grey edge, distinct from the delivery disc;
   - added to `ys` before `set_ylim`;
   - a legend handle only when the series is non-empty;
   - the four series, the edge marks, the bands and the ratio panel untouched.
4. **E5, a probe.** Run the committed script from the root on `agent03b/inputs/scan79.csv` and
   `edges.csv`, into the scratchpad. Look at both PNGs and greyscale copies. Check that:
   - on `scan79.csv`, open circles at 1.10 (0.143 GeV) and 3.00 (1.68 GeV), and the rest of the
     figure as in the review of 03;
   - on `edges.csv`, four open circles, the rise from 1.64 to 44 GeV visible, 3.012 and 3.013 told
     apart;
   - the open circles read against the delivery discs at 2.997 and 3.020 on either side;
   - the e⁺e⁻ label collision (§0) no worse.
   Independently of the agent's tests, read the new artist's data off the upper panel and compare
   it with the CSV's uncaptured rows.
5. **E2, by test.** 03's 15 tests pass at the agent's commit, run alone.
6. **E6.** In `bash`, replay each breakage diff with `git apply`, run the named test, see it fail for
   the stated reason, revert with `git apply -R`, and check `git status` is clean. Then read the
   new tests: does a test pin each item of the prompt's §2 step 2?
7. **E7, the records.**
   - **The board:** row 03b ✅ with "this commit" and the log; F4 ✅;
     `### 5.4b After prompt 03b (<date>)`.
   - **Issues:** §3 and the index untouched, or, if one opened, the index row, count and date in
     the same commit.
   - **The log:** in §5.1's form, every deviation classified, with §2's checks and their output,
     what each PNG showed, and the e⁺e⁻ collision under "Observations not acted on".
8. **The suites** at the agent's commit, serially: 18, 61 + 115, 69, 64 + new. `black --check` on
   both touched files.
9. **The interface for 04.** The log's "State handed to the next prompt": the command line, the
   outputs, the exit codes, unchanged from 03's. Anything 04 needs and does not have goes to 04's
   note.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 03b* paragraph in §5.4b. Fix
  small residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - this note in the notes table, marked "used for 03b";
  - the status line: 4 of 6 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the marker, and the PNG of `edges.csv`;
  - the tests added and the breakages;
  - the four suites' counts;
  - the e⁺e⁻ label collision, which 04's fine figure will show more of (some twenty uncaptured edge
    marks across 2.998–3.019), for the user to rule on before 04;
  - anything 04's note must carry.
- **Then 04.** Its note is written after this review.
