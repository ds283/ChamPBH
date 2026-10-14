# Orchestrator — prompt 03, the plot

Read [`../README.md`](../README.md) first: §0.1, §0.3 (with the U5 addendum), §2.2 (03's charter),
§4 (the CSV and the figure), §5, §6.1 (U1–U3, U5) and §6.2 (P4, P8). Then the board's §5.3, whose
"For the next notes" this note carries. The conventions are note 02's
([`prompt-02.md`](prompt-02.md), "Conventions"), restated here where 03 needs them.

**You do not write code.** You may:
- run the suites and the tests the log names;
- replay the log's deliberate-breakage diffs with `git apply`, and revert them;
- run a probe from the session scratchpad, never committing one;
- run the agent's plot script into the scratchpad;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`03-plot-surfing-temperature.md`](../03-plot-surfing-temperature.md)
**Board items:** F1–F3 · **Gate:** none of its own (the figure is gate "Scan"'s, in 04) ·
**Closes:** nothing · **Opens:** only what the work finds · **Model:** Sonnet, as the prompt
recommends · **No network.**

**Gate:**
- 01 has landed (`2c98370`, reviewed `b5d1c94`) and 02 has landed (`f166b31`, reviewed `61e0c42`).
  `HEAD` is the commit that adds this note, or a later one that touches no code. Since `a8823a9`,
  only 01's and 02's commits have touched anything outside `prompts/`.
- One prompt at a time in this checkout. 04 is not dispatched until 03 is reviewed.

## 0. What makes this prompt unusual

**04 may not change the plot script** (04 §S2: "a change to the plot script is out of scope
here"). So the figure must be legible on the full 79-point scan *now*, not only on T5's five rows.
The orchestrator has therefore run 02's tool over the whole grid into the scratchpad, as a probe
(below). It is not 04's scan, it is not committed, and 04 runs its own. The agent plots it as well
as T5's CSV (§2 step 3), and the legibility stop of the prompt's §4 is judged on it.

**The inputs**, copied into the agent's scratch directory, `agent03/inputs/`:
- `surfing_t5.csv` and its sidecar: 02's five rows (β = 1.15, 1.5, 2.5, 3.5, 5.0), from 02's
  session. Its sidecar has `git_head` `f6440d1…` and `git_tracked_changes: true`, because T5 ran
  before 02 committed.
- `scan79.csv` and its sidecar: the orchestrator's probe, β = 1.10–5.00 by 0.05, run at `61e0c42`
  (`git_tracked_changes: false`), 79 rows, all `status=ok`, in 308 s at load 4–8.

**The CSV, as 02 committed it** (log 02, "State handed to the next prompt"):
- The header, verbatim:
  `beta,phi_init_Mp,M_Mp,Lambda_eV,T_stop_GeV,status,failure_reason,captured,capture_kick,T_capture_GeV,N_capture,T_max_GeV,N_max,T_second_dip_GeV,N_second_dip,T_deliver_GeV,N_deliver,deliver_reflected,n_crossings,reflections,accepted_steps,wall_s,T_hot_GeV,T_cold_GeV`
- **A blank** is an empty field. **A boolean** (`captured`, `deliver_reflected`) is `true` or
  `false`, lower case. **A float** is `repr`. `tools.surfing_scan.COLUMNS` is the header as a list,
  and the synthetic CSV of F3 should be written from it, so that the two cannot drift.
- **A failure row** has the inputs, `status=failure`, `failure_reason` and `wall_s`, and *every
  other field blank, `captured` included*. So a row is classified by `status` first, and only then
  by `captured`. A blank `captured` must not be read as false, nor crash the parse.
- **An uncaptured row** (`captured=false`, `n_crossings=0`) keeps `T_deliver_GeV`, `N_deliver` and
  `deliver_reflected`, and has `capture_kick`, `T_hot_GeV` and `T_cold_GeV` blank. On the grid these
  are β = **1.10** (below the QCD threshold; delivery 0.1434 GeV) and β = **3.00** (just below the
  EW threshold; the field reaches the wall at 1.680 GeV, before the QCD kick).
  - **Ruled here:** the prompt stands. These deliveries are not surfing temperatures and are not
    drawn as measured markers; the row is the "not captured" edge mark of F2, nothing more.
- **The sidecar** is `{"runs": [...]}`, one entry per invocation, each with `start_time`,
  `git_head`, `git_tracked_changes`, `version_label`, `options`, `Lambda_eV`, `beta_grid`,
  `resumed`, `skipped`, `python`, `scipy`. A resumed CSV can carry more than one `git_head`.
  - **Ruled here:** the title gives the short SHA of every distinct `git_head` in `runs`, in order,
    and marks any run with `git_tracked_changes: true` (for example "f6440d1 + tracked changes").
    A missing sidecar is the agent's choice (refuse with exit 2, or "HEAD unknown" in the title),
    and the log says which.

**The probe's scan, as the figure will see it** (scratch; a measurement, not a specification):
- **Captures:** QCD at β = 1.15–2.95 (37 rows), EW at 3.05–5.00 (40 rows); uncaptured at 1.10 and
  3.00; no `mu-pi` or `ee` capture; no failure; no reflection (`deliver_reflected` false
  throughout).
- **Second dip** at 43 rows, β = 2.60–5.00 with gaps; `n_crossings` is 0, 2, 4, 6, 8 or 10.
- **The measured temperatures** span 0.1434 to 187.85 GeV.
- **The ratios to `T_hot_GeV`** (lower panel), over the captured rows: capture 0.778–0.906; largest
  0.965–1.067; second dip 0.985–1.000; delivery 0.965–1.019. So the lower panel spans about
  0.75–1.10, and its four series crowd near 1.
- **Coincident markers.** At 12 β, `T_max_GeV` equals `T_deliver_GeV` to 1e-3 relative: β =
  1.15–1.65 and 3.05 (at 1.15–1.50 to six digits). This is the wall-type maximum of board §5.3
  (the last T_J maximum is the wall turning the field round), U1 applied as written. **Both markers
  must stay visible where they coincide**; how (draw order, open against filled, size) is the
  agent's choice, and the log says which and shows it holds on `scan79.csv`.

**The roots on the fine grid** (the orchestrator's probe at `61e0c42`, `find_kicks` with the
table's ends, then `reachability_roots` at each β of `beta_grid(1.10, 5.00, 0.005)`, 781 values):
- **The thresholds** √((2 + Σ_peak)/(6 Σ_peak)): QCD 1.10745, `ee` 1.86434, `mu-pi` 2.01779,
  EW 3.01178. `ee` and `mu-pi` are 0.15 apart, and QCD's is 0.0074 inside the left edge of a
  1.10–5.00 axis, so the four labels must be placed so that they do not collide or fall off.
- **First β of the fine grid with roots:** QCD 1.110, `ee` 1.865, `mu-pi` 2.020, EW 3.015.
- **The merge.** `merged` is set on QCD (= `"mu-pi"`) and on `mu-pi` (= `"QCD"`) from β = 2.155 to
  5.00 (570 β), and nowhere else. `multiple_crossings` is never true. At the merge, in GeV:

  | β | QCD | `mu-pi` |
  |---|---|---|
  | 2.150 | 0.91189, 0.076712 | 0.069206, 0.032592 |
  | 2.155 | 0.91793, 0.032428 (merged) | the same (merged) |

  - **The QCD cold root jumps** from 76.7 to 32.4 MeV between two adjacent fine β, and the separate
    `mu-pi` band ends. **Ruled here:** the lines break where `merged` changes; nothing joins the
    two sides with a near-vertical segment.
  - **A merged interval is one band, filled once.** Both records carry the same roots, so drawing
    each record's band would double the fill. Its hot edge is in QCD's colour and its cold edge in
    `mu-pi`'s (F2).
- **Other values,** hot then cold: β = 1.11 QCD 0.18833, 0.17596; β = 1.865 `ee` 1.6483e-4,
  1.5595e-4; β = 2.02 `mu-pi` 0.044998, 0.041471; β = 3.015 EW 55.826, 50.653; β = 5.00 EW 175.99,
  18.431, QCD (merged) 4.2609, 0.015989, `ee` 8.8436e-4, 6.3413e-5. The root curves span
  6.34e-5 to 176 GeV, so with the markers the upper axis covers about 6.5 decades. The `ee` band
  lies below T_stop (1 MeV); that is expected (P3) and needs no comment on the figure.
- **"Reachable, not surfed." Ruled here,** generalising F2's rule for U5: the label goes on *every*
  kick's band that no row captures, not only e⁺e⁻. On the scan that is `ee` and the separate
  `mu-pi` band (2.02–2.15). Above the merge the `mu-pi` band is QCD's, which is surfed, and carries
  no such label.
- **The cost.** 781 calls took **191 s at load 8** (0.245 s per β), against board §5.2's 0.077 s
  per β at a lighter load. So `main` takes one to three minutes on the full range. That is
  acceptable, and the log records the time measured. Building the cosmology and `find_kicks` take
  under a second together.

**The fine grid, ruled here.**
- **"Rounded outwards to 0.05"** is done with a tolerance, so that a β already on the 0.05 grid is
  not moved a cell. In floating point 1.15/0.05 = 22.999999999999996, so a bare `floor` gives 1.10.
  Use `floor(β_min/0.05 + 1e-9)` and `ceil(β_max/0.05 − 1e-9)`, or an equivalent, and say which.
- **The grid is built by index**, β_k = β_lo + k·0.005 rounded to 10 places, as
  `tools.surfing_scan.beta_grid` builds it. Calling `beta_grid` itself is fine. Importing
  `tools.surfing_scan` costs about 2 s and starts no Ray.
- For the T5 CSV the range is 1.15–5.00 (771 β); for `scan79.csv` it is 1.10–5.00 (781 β).
- **F3.4's test need not build the full range.** It may use its own small CSV over a narrow range,
  for example β = 1.15 and 1.20, which gives 11 calls. It checks the count and the two end β, and
  its docstring gives its time.

**Paths.** `_save(fig, path)` (`extract_common.py:901`) writes `path` and
`path.with_suffix(".png")`. Build the PDF path as `Path(out + ".pdf")` by concatenation, not with
`with_suffix` on the prefix, which would cut a prefix such as `run-2026.6.0`. 04 will pass
`--out .documents/surfing-temperature/out/surfing_temperature_phi5_M1e-3`.

**What moves on purpose:**
- New: `tools/plot_surfing_temperature.py` and `tools/tests/test_plot_surfing_temperature.py`.
- The log `logs/03-plot-surfing-temperature.md`, and the board.
- Nothing else. `extract_common.py`, `tools/surfing_scan.py` and every production module are
  unchanged. No issue is expected; the index (`.documents/OPEN_ISSUES.md`) is untouched unless one
  opens.

**The facts, checked by the orchestrator on 2026-10-14 at `61e0c42`.** Pass them on.
- **The tree.** Branch `surfing-temperature`, `HEAD` `61e0c42` before this note; nothing staged.
  The untracked set is unchanged: 17 `pilot-2026.6.0*.db`, `pilot-main.log`, `pilot-plot.log`,
  `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`,
  `prompts/jordan-normalization/`.
- **Line numbers have drifted since the prompt was written.** In `extract_common.py` (1122 lines)
  at `61e0c42`:
  - `kick_threshold_curve` is at `:555`, `find_kicks` at `:649`, `reachability_roots` at `:712`;
  - `_save` is at `:901-910` and `plot_T_deliver` at `:995-1046`.
- **The signatures.** `find_kicks(cosmology, T_lo, T_hi, n_grid=…)` and
  `reachability_roots(cosmology, beta, kicks, n_grid=…)`: **`kicks` is a required argument**, so
  `find_kicks` is called once per run and passed in. The table's ends are reachable only through
  the private `cosmology._eos._T_min` and `_T_max`, in GeV:
  `find_kicks(cosmology, cosmology._eos._T_min * units.GeV, cosmology._eos._T_max * units.GeV)`.
  The kick's key is `label`; the record's is `kick`. Temperatures in both are in the cosmology's
  units, so divide by `units.GeV`. A kick with no interval has `T_hot = T_cold = None`.
- **The cosmology** is built as `tools/surfing_scan.py`'s `make_setup` builds it:
  `QCD_Cosmology(0, Planck_units(), Planck2018())`. It prints one `@@` line on construction.
- **The figure idiom** (`plot_T_deliver`): `matplotlib.figure.Figure` without `pyplot`,
  `fig.subplots(...)`, `fig.suptitle(title, fontsize="small")`, saved through `_save`. Matplotlib
  is 3.10.8. `extract_common` already imports `Patch` from `matplotlib.patches`.
- **The root check.** 02's tool refuses a run whose working directory lacks
  `CosmologyModels/GenericEOS/Xav_EOS_data.csv` (its `EOS_TABLE`), with exit 2 and nothing written.
  The plot script does the same.
- **The existing `tools/tests`** import their tool as `import tools.<module> as …`
  (`test_surfing_scan.py:41`).
- **The suites at `61e0c42`** are given in the dispatch (§1 step 2).

**What the review exists to establish.**
- **(E1) F1.** The options, the root check, the outputs, the sidecar read, the fine grid (rounded
  outwards with a tolerance, built by index, Δβ = 0.005), `find_kicks` once per run, and the
  figure built by a function that needs no cosmology.
- **(E2) F2.** The figure as the prompt and this note say: the bands, the merged band filled once
  and broken at the merge, "reachable, not surfed" where no row captures, the four thresholds, the
  four marker series drawn only for captured rows with blanks as gaps, the edge marks for
  uncaptured and failure rows, the ratio panel against `T_hot_GeV`, the title.
- **(E3) F3.** The tests as the prompt lists them, with the failure row's blank `captured`.
- **(E4) Nothing else moves.**
- **(E5) Legibility,** judged by the orchestrator on both CSVs' PNGs, the coincident markers
  included.
- **(E6) The pins.** Each of the three breakages fails a named new test.
- **(E7) The records.** The log, the board, the counts.

**Conventions.** All of these bind the agent, and the brief says so.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. A log states only what the diff and the tests show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes the SHA in its
  follow-up.
- **Suites.** `CLAUDE.md`'s commands, ComputeTargets in its two halves, each in the foreground,
  serially. Each run's output goes to a file in the agent's scratch directory, the verdict is grepped
  from it, and the load (`uptime`) is recorded beside it. A failure is re-run alone before it is
  called real.
- **Breakage diffs** are recorded so that `git apply` replays them from a clean tree at the agent's
  commit. Check each with `git apply --check` and `-R --check`, and replay in `bash`, not zsh.
- **Scratch.** The agent's own subdirectory of the session scratchpad, never `/tmp`. Python's
  `tempfile` in tests is fine. No scratch file goes under `tools/` or `.documents/`. The figures
  of §2 step 3 are written there and are not committed.
- **The venv.** Nothing installed or uninstalled.
- **Stores and Ray.** None opened, none started. 03 runs no history and no PRyMordial.
- **Stop conditions are real.** A stop goes to the user through the orchestrator, and the agent
  resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline a8823a9..HEAD -- . ':!prompts'` lists only `2c98370` and `f166b31`.
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
- **One commit, at the end.** Its log is `logs/03-plot-surfing-temperature.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **The files it may add or change are the prompt's §5 list, and nothing else.**
- **§2 step 3 plots both CSVs**, T5's and `scan79.csv`, and the agent looks at both PNGs. The
  legibility stop is judged on `scan79.csv`, because 04 may not change the script.
- **The rulings of §0** govern where they differ from the prompt: the failure row's blank
  `captured`; the uncaptured deliveries not drawn; the title's SHAs; the merged band filled once
  and broken at the merge; "reachable, not surfed" on every uncaptured kick; the outward rounding
  with a tolerance; F3.4 on a narrow range.
- **It starts no Ray, opens no store, runs no history or PRyMordial, and installs nothing.**
- **Stop and ask** on any of the prompt's §4 conditions. An existing test that fails is current
  behaviour moving, never a reason to edit it.
- **It replies with:**
  - the log's path and the message as committed;
  - the script's options, exit codes and the function that builds the figure, with its signature;
  - the colours and markers chosen, and how the coincident markers stay visible;
  - the time of `main` on each CSV;
  - what it saw in each PNG, and anything hard to read;
  - the new tests by class, with counts and wall time;
  - the four suites;
  - the three breakages and the tests each fails;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — ten checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` touches only the new script, the new test module,
   the board and the log. `git status` shows §0's untracked set and nothing new. No figure, CSV or
   sidecar is in the tree.
2. **E4.** `git diff <agent>~1 <agent> -- extract_common.py tools/surfing_scan.py ComputeTargets/
   config/` is empty.
3. **E1, by reading.** The script in full:
   - the options, the root check and the exit codes;
   - the CSV read: by header name, the failure row classified by `status` first, blanks as `None`
     or NaN and never as 0, booleans as written;
   - the sidecar read, and the title's SHAs and tracked-change marks;
   - the outward rounding with its tolerance, the grid by index, `find_kicks` called once;
   - the figure function's inputs (rows, roots, sidecar) and no cosmology inside it;
   - `<out>.pdf` and `<out>.png` written through `_save`, and nothing else written.
4. **E2, by reading.** The figure function in full:
   - each kick's hot (solid) and cold (dashed) line, in its colour, with the band filled;
   - the merged interval filled once, the lines broken where `merged` changes;
   - "reachable, not surfed" decided from the rows' `capture_kick`, for every kick;
   - the four threshold lines from `Sigma_peak`, labelled;
   - the four series' markers, only for captured rows, with blanks skipped;
   - the edge marks and legend entries for uncaptured and failure rows;
   - the ratio panel's divisor `T_hot_GeV`, and the line at 1;
   - the shared β axis, the panel heights, the axis labels and the title.
5. **E5, a probe.** Run the committed script from the root on `agent03/inputs/surfing_t5.csv` and
   on `scan79.csv`, into the scratchpad, timing each. Look at both PNGs (and the PDF of the 79-point
   one). Check that:
   - all 77 captured rows have their markers, and 1.10 and 3.00 their edge marks;
   - the coincident max and delivery markers are both visible at 1.15–1.65 and 3.05;
   - the QCD/`mu-pi` merge is one band with no joining segment at β = 2.155;
   - the `ee` and separate `mu-pi` bands carry "reachable, not surfed";
   - the four threshold labels are legible and do not collide;
   - the figure reads at A4 width, and in greyscale (convert the PNG).
   Independently of the agent's tests, read the ratio markers' data off the lower panel's artists
   and compare them with the CSV's quotients.
6. **E1, the behaviour by probe.**
   - A run outside the root: refused, nothing written.
   - A run on a CSV with a failure row and a blank `captured` (made in the scratchpad from
     `surfing_t5.csv`): no crash; the failure appears in the legend.
   - A count of `reachability_roots` calls on T5's CSV (771) and on `scan79.csv` (781), with a
     wrapper.
7. **E6.** In `bash`, replay each breakage diff with `git apply`, run the named test, see it fail for
   the stated reason, revert with `git apply -R`, and check `git status` is clean. Then read the
   tests: does a test pin each of these?
   - no marker at a blank;
   - the ratio's divisor;
   - the uncaptured and failure rows in the legend;
   - the failure row's blank `captured`;
   - the fine grid's count and ends.
8. **E7, the records.**
   - **The board:** row 03 ✅ with "this commit" and the log; F1–F3 ✅;
     `### 5.4 After prompt 03 (<date>)`.
   - **Issues:** §3 and the index untouched, or, if one opened, the index row, count and date in
     the same commit.
   - **The log:** in §5.1's form, every deviation classified, with §2's checks and their output,
     and what each PNG showed.
9. **The suites** at the agent's commit, serially: 18, 61 + 115, 69, 49 + new. `black --check` on
   both new Python files.
10. **The interface for 04.** Read the log's "State handed to the next prompt": the command line,
    the outputs, the time, the exit codes. Check them against 04's S2 and S4 (read here by the
    orchestrator; the agent did not). Anything 04 needs and does not have goes to 04's note.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 03* paragraph in §5.4. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (board and log);
  - this note in the notes table, marked "used for 03";
  - the status line: 3 of 5 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - the figure's design, and the PNG of the 79-point probe;
  - the tests added and the breakages;
  - the four suites' counts;
  - anything 04's note must carry. At the least: the scan's real cost (308 s for the probe at
    load 4–8; β = 5.0 took 25 s, against 7.9 s in T5), and the plot's cost on the full range.
- **Then 04.** Its note is written after this review.
