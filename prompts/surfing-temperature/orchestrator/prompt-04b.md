# Orchestrator — prompt 04b, the e⁺e⁻ band label

Read [`../README.md`](../README.md) first: §5 (rule 13) and §6.1 (U8). Then the board's §5.5. The
conventions are note 03's ([`prompt-03.md`](prompt-03.md), "Conventions"), restated here where 04b
needs them.

**You do not write code.** You may:
- run the `tools` suite;
- run the committed plot script, or a scratch copy of it, into the session scratchpad;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`04b-tidy-band-label.md`](../04b-tidy-band-label.md)
**Board item:** F5 · **Gate:** none of its own · **Closes:** nothing · **Opens:** nothing expected ·
**Model:** Sonnet, as the prompt recommends · **No network.**

**Gate:**
- 04 has landed (`90d44f8`, reviewed `25cdd2e`), and 04b is written (`1afb2fb`). `HEAD` is the
  commit that adds this note, or a later one that touches no code. Since `a8823a9`, only `2c98370`,
  `f166b31`, `1f40b53`, `ee4a2df` and `90d44f8` have touched anything outside `prompts/`.
- One prompt at a time in this checkout. 05 is not dispatched until 04b is reviewed.

## 0. What makes this prompt unusual

**It is a tidy-up, and the user costed it as one** (U8): no new test, no deliberate breakage, only
the `tools` suite. The review is cut to match. Do not let it grow.

**The geometry, measured by the orchestrator at `1afb2fb`.** A scratch copy of the plot script with
the divisor at `tools/plot_surfing_temperature.py:520` changed, run on both committed CSVs, and the
figure drawn at the PNG's DPI. The gap is the e⁺e⁻ label's bottom (its window extent) less the
edge marks' top (the bottom spine plus 7 pt: `"|"`, `markersize` 14, centred on the spine).

| Divisor | Fine figure: lower y-limit | gap | Full figure: lower y-limit | gap |
|---|---|---|---|---|
| 3 (now) | 2.65e-5 | −9.5 pt | 2.11e-5 | −7.8 pt |
| 4 | 1.99e-5 | −4.3 pt | 1.59e-5 | −3.0 pt |
| 5 | 1.59e-5 | −0.4 pt | 1.27e-5 | +0.6 pt |
| 6 | 1.32e-5 | +2.7 pt | 1.06e-5 | +3.5 pt |
| 8 | 9.93e-6 | +7.5 pt | 7.93e-6 | +8.0 pt |
| 10 | 7.94e-6 | +11.1 pt | 6.34e-6 | +11.4 pt |

- In the fine figure nine edge marks lie under the label's horizontal span (2.999–3.007, which
  agrees with log 04's eight plus the one "just clear"). In the full figure none does: its two
  uncaptured β (1.10 and 3.00) are outside the label's span. At divisor 3 the full figure's label
  bottom already sits 0.8 pt *below* the spine.
- **So 5 is not enough, 6 clears by a hair, and 8 or 10 clear comfortably.** At 10 a 10⁻⁵ tick
  label appears in both figures; at 8 the axis reaches just below 10⁻⁵. Either is acceptable.
  **Ruled here:** the agent picks a divisor of at least 8 and says in the log why; it need not
  reproduce this table, but its log states the gap it measured, in points or pixels, on the fine
  figure.
- **The rest of the figure does not move.** The probe's PNGs at divisors 6 and 10 differ from the
  committed fine PNG only inside the box x 31–823, y 49–521 (of 827 × 900): the upper panel and its
  tick labels. The lower panel, the legend and the title are pixel-identical. The agent's redrawn
  PNGs should show the same, for both figures; this is how §1.2 of the prompt is checked.
- **No test pins the divisor.** `TestUncapturedDeliveries.test_f` checks only that the new points
  lie inside the y-limits, which a larger divisor keeps true. Nothing reads the label's position.

**The redraw.**
- The commands are those of the verification document §1.1, "the two figures", writing into
  `.documents/surfing-temperature/out/` over the committed files. The full figure takes about one
  minute (781 β of roots, 60 s at load 10 in the probe), the fine one a few seconds.
- The title is built from the sidecar, not the tree, so it still reads "git HEAD e7ed715". The
  sidecars and CSVs are read, not written.
- The PDFs carry a creation date, so they always differ byte-wise. Compare PNGs.
- **Order:** change the script, run `tools/tests` and `black`, then redraw. A figure drawn before
  the script is final is redrawn.

**The verification document.** It has one section, §1, ending at §1.9 "The suites". The new
section is `## 2. The figures redrawn (prompt 04b, 2026-10-14)`, appended at the end. §1.6's
account of the overlap stays as written (invariant 6); §2 says that §1.6 and log 04 describe the
figures as committed in `90d44f8`.

**What moves on purpose:**
- `tools/plot_surfing_temperature.py`: the divisor and a short comment saying why. If the agent
  touches the module docstring, the log says so; it does not need to.
- the four figures under `.documents/surfing-temperature/out/` (two PDF, two PNG);
- `.documents/surfing-temperature-verification.md`: a new §2 only;
- the log `logs/04b-tidy-band-label.md`, and the board (row 04b, F5, `### 5.5a`).
- Nothing else: not the CSVs, sidecars, tests, `extract_common.py`, `tools/surfing_scan.py` or the
  index.

**The conventions of note 03 hold.** In short:
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. A log states only what the diff, the tests and the figures show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes it in its follow-up.
- **The suite.** `tools/tests` only, in the foreground, its output to a file in the agent's scratch
  directory, the verdict grepped from it, the load (`uptime`) beside it. A failure is re-run alone
  before it is called real, and then it is a stop.
- **Scratch.** The agent's own subdirectory of the session scratchpad, `agent04b/`, never `/tmp`.
  No scratch file goes under `tools/` or `.documents/`.
- **The venv.** Nothing installed or uninstalled.
- **Stores and Ray.** None opened, none started. No history, no PRyMordial.
- **Stop conditions are real** (prompt §4). A stop goes to the user through the orchestrator, and
  the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit.

**The facts, checked by the orchestrator on 2026-10-14 at `1afb2fb`.** Pass them on.
- **The tree.** Branch `surfing-temperature`, `HEAD` `1afb2fb` before this note; nothing staged.
  The untracked set is unchanged: 17 `pilot-2026.6.0*.db`, `pilot-main.log`, `pilot-plot.log`,
  `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`,
  `prompts/jordan-normalization/`.
- **The line.** `tools/plot_surfing_temperature.py:520` is
  `ax.set_ylim(min(ys) / 3.0, max(ys) * 25.0)  # headroom for the threshold labels`; the file is
  623 lines and `black`-clean.
- **The `tools` suite at `1afb2fb`:** 70 tests, OK, 13 s, at load 9.4.

**What the review exists to establish.**
- **(E1) F5.** The e⁺e⁻ label clears every edge mark in the fine figure, and is readable in both.
- **(E2) Nothing else moves:** in the script, only the lower y-limit (or the label's position, if
  the agent chose otherwise and said why); in each figure, only the upper panel; in the tree, only
  the prompt's §5 list.
- **(E3) The figures are the committed CSVs',** redrawn by the committed script.
- **(E4) The records.** The log, the verification §2, the board.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline a8823a9..HEAD -- . ':!prompts'` lists only the five commits of the
   gate.
2. **The baseline.** `tools/tests` at `HEAD`, with the load. Give the agent the number.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Sonnet**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baseline;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompt 05;
- its scratch directory, `agent04b/`.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/04b-tidy-band-label.md`, short, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may change are the prompt's §5 list, and nothing else.**
- **A divisor of at least 8** (§0), unless it finds a simpler placement that hides nothing; then it
  says why in the log.
- **The order of §0:** script, suite and `black`, then the redraw.
- **It compares each redrawn PNG with the committed one** (before it overwrites them, keep a copy
  in scratch) and reports the bounding box of the difference.
- **It adds no test and no breakage, and runs no suite but `tools`.**
- **It starts no Ray, opens no store, runs no history or PRyMordial, and installs nothing.**
- **Stop and ask** on any of the prompt's §4 conditions.
- **It replies with:**
  - the log's path and the message as committed;
  - the diff of the script, in full;
  - the divisor chosen and the gap it measured;
  - what it saw in each PNG, and the difference boxes;
  - the `tools` suite, with the load, and `black`;
  - `git diff --stat` of the commit;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — six checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` lists the script, the four figures, the
   verification document, the log and the board, and nothing else. `git status` shows §0's untracked
   set and nothing new. The CSVs and sidecars are not in the diff.
2. **E2, the script.** `git diff <agent>~1 <agent> -- tools/` changes only the y-limit line and a
   comment (and the docstring, if the log says so). `black --check` is clean.
3. **E3 and E2, the figures.** Run the committed script on each committed CSV into the scratchpad:
   - the PNGs equal the committed ones, pixel for pixel;
   - against the PNGs of `90d44f8` (`git show 90d44f8:<path>`), the difference lies inside the
     upper panel, and the lower panel, legend and title are pixel-identical;
   - the titles still read "git HEAD e7ed715".
4. **E1, by measurement and by eye.** With §0's probe (`clearance.py` in the orchestrator's
   scratchpad, `probe04b/`), or by colour on the committed fine PNG, the label's bottom is above the
   marks' tops. Look at both PNGs: the label reads, no mark is hidden, nothing else looks different.
5. **The suite** at the agent's commit: `tools/tests`, 70, OK.
6. **E4, the records.**
   - **The board:** row 04b ✅ with "this commit" and the log; F5 ✅; `### 5.5a After prompt 04b
     (<date>)`.
   - **The verification document:** a new §2, a few lines, saying what moved, that the data are
     unchanged, and that §1.6 describes the figures of `90d44f8`; §1 untouched
     (`git diff <agent>~1 <agent> -- .documents/surfing-temperature-verification.md` only adds
     lines after §1.9).
   - **Issues:** §3 and the index untouched.
   - **The log:** in §5.1's form, short, every deviation classified.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 04b* paragraph in §5.5a. Fix
  small residue in a follow-up commit of its own:
  - "this commit" → the SHA (board, log, and the verification document if it names it);
  - this note in the notes table, marked "used for 04b";
  - the status line: 6 of 7 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the redrawn fine PNG;
  - the divisor and the gap;
  - the suite;
  - anything 05's note must carry.
- **Then 05.** Its note is written after this review.
