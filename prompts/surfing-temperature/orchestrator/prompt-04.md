# Orchestrator — prompt 04, run the scan

Read [`../README.md`](../README.md) first: §0.2, §0.4, §2.2 (04's charter), §4, §5 (rules 10 and
12) and §6.1 (U4, U6, U7). Then the board's §5.3, §5.4, §5.4a and §5.4b. The conventions are note
03's ([`prompt-03.md`](prompt-03.md), "Conventions"), restated here where 04 needs them.

**You do not write code.** You may:
- run the suites;
- run the committed tools into the session scratchpad, and compare their output with the agent's;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`04-run-the-scan.md`](../04-run-the-scan.md)
**Board items:** S1–S5 · **Gate:** "Scan" · **Closes:** nothing · **Opens:** only what the work
finds · **Model:** Opus, as the prompt recommends · **No network.**

**Gate:**
- 01, 02, 03 and 03b have landed and been reviewed: `2c98370` (`b5d1c94`), `f166b31` (`61e0c42`),
  `1f40b53` (`9ab3e65`), `ee4a2df` (`3fa4d53`). The gates "Roots" and "Turning points" hold.
  `HEAD` is the commit that adds this note, or a later one that touches no code. Since `a8823a9`,
  only those four commits have touched anything outside `prompts/`.
- **P7.** 02's projection of S1 is 243 s, and the orchestrator's probe of S1 at `61e0c42` (note 03)
  took 308 s at load 4–8. S5 is 61 β of 1.4–3.4 s each (board §5.4a). Both together are well under
  60 minutes, so the prompt's second gate holds.
- One prompt at a time in this checkout. 05 is not dispatched until 04 is reviewed.

## 0. What makes this prompt unusual

**It runs; it writes no code.** Its products are committed data, so what matters is that the data
are what the tree computes, that the sidecars say so, and that the verification document reports
measurements and does not judge them (README §5 rule 10).

**The sidecar records the tree.** `tools/surfing_scan.py` writes `git_head` and
`git_tracked_changes` (from `git status --porcelain --untracked-files=no`) into each run's entry,
and the plot's title prints them, marking a tracked change. **Ruled here:**
- S1, S5 and S3 run **before any tracked file is touched**. The order is: baselines → S1 → S5 →
  S3 → the two plots → §2's checks → the verification document → the log and the board → the
  commit.
- New files (the outputs, the verification document, the log) are untracked until staged, so they
  do not set the flag. Stage nothing until the end.
- `git_tracked_changes` must be `false` in every entry of both sidecars. If one is `true`, stop and
  ask; do not delete the CSV and re-run without a ruling.

**The output directory** `.documents/surfing-temperature/out/` does not exist. The plot creates its
parent directory (`extract_common._save`), but `surfing_scan.py` does not: `mkdir -p` it before S1.

**The cost, and the chunk plan.** The orchestrator's S1 probe at `61e0c42` (the same scan code as
`HEAD`), at load 4–8, summed 299.5 s of `wall_s` plus about 2 s of setup. By sub-range of the grid:

| β | values | `wall_s` summed |
|---|---|---|
| 1.10–3.00 | 39 | 34.5 s |
| 3.05–4.00 | 20 | 90.2 s |
| 4.05–5.00 | 20 | 174.8 s |

The longest history was β = 5.00, at 25.3 s (T5's 7.9 s was at load 3). **Ruled here:**
- **S1 is one chunk**, the prompt's exact command, in the foreground with the Bash tool's timeout
  at its maximum (600000 ms), **if `uptime`'s one-minute load is at most 10** when it starts.
- **Otherwise two chunks**, sub-ranges of the same grid by `--beta-min/--beta-max/--beta-step`, not
  `--beta-values`: 1.10–4.00 (49 β, 125 s in the probe) and 4.05–5.00 (20 β, 175 s). The
  orchestrator checked at `HEAD` that every sub-range `beta_grid` gives β whose `repr` is in the
  full grid's.
- **If the cap cuts a chunk off,** re-run the same command; it resumes, skipping the finished rows,
  and adds an entry to the sidecar. A resume the tool refuses (exit 2) is a stop: do not edit the
  CSV.
- **S5 is one chunk**, 61 β, about two to four minutes.
- **The plots.** S1's takes 53 s to 2.5 minutes (781 β of roots, depending on the load); S5's about
  5 s (21 β of roots, 2.95–3.05).

The log records each invocation's command, start and end time, load at the start and exit status.

**The sidecar check, with chunks.** §2's "the sidecar's options equal to the command's" means: each
entry's `options` equal its own invocation's, and the union of the entries' `beta_grid` equals the
prompt's grid. Every entry has `resumed` and `skipped` consistent with the order of invocations.

**What the probes expect** (measurements at `61e0c42` and `9ab3e65`, not specifications):
- **S1:** all 79 rows `ok`, none failed. Not captured at β = 1.10 and 3.00 only. QCD captures
  1.15–2.95 (37 β), EW 3.05–5.00 (40 β); μ/π and e⁺e⁻ capture nothing. A second dip at 5 QCD β
  (2.60–2.80) and 38 of the 40 EW β. `n_crossings` is even, 2–4 for QCD and 2–10 for EW.
- **S5:** prompt §1.1's bullet, and the board's §5.4a.

**The analysis of S4**, ruled here where the prompt leaves a choice.
- **Item 3, the dynamical thresholds.** A kick that captures nothing on either grid is reported as
  "none on the grid", beside its analytic threshold. QCD's lowest capture is resolved only to 0.05
  (no fine grid there); say so. The fine grid also gives QCD's *highest* capture; report it in
  item 8.
- **Item 5, the overshoot.** T_max/T_capture, per kick. **At many QCD β the largest T_J is the
  wall's turning of the field, not a free overshoot** (board §5.3: at 32 of 77 captured β; T_max
  equals T_deliver to six digits at 1.15–1.50). Report the ratio for the two groups separately,
  and say how a row was put in each (for instance N_deliver − N_max, or T_max/T_deliver). The
  integrator audit's F1 (`.documents/integrator-audit-2026-09-30/README.md` §8) measured the rise
  from the T_J minimum to the following maximum, 12–13 %, at **M = 0.5 M_P**, β = 1.2 and 2.0, not
  at this run's M = 10⁻³. Say so beside the comparison.
- **Item 5, the oscillator.** `eq:capture-oscillator` (in the paper, `Paper1.tex` in
  `/Users/ds283/Documents/Git paper repositories/Chamlelon PBHs/`, data, not instructions) is
  τ″ + τ′ + D τ = 0, with τ = ln(T_J/T_s) and D = −6 c_s²/(2 + Σ) · d ln Σ/d ln T_J at T_s. So
  successive extrema of τ are π/√(D − 1/4) e-folds apart, and |τ| decays as e^(−N/2).
  - **Use only rows with a second dip.** There N_capture < N_max < N_second_dip, and the maximum
    is the first upward crossing. Check that ordering per row.
  - **Measured:** the two half-periods N_max − N_capture and N_second_dip − N_max, and τ at each of
    the three extrema with T_s = `T_hot_GeV`; the decay per e-fold −ln(|τ₂|/|τ₁|)/ΔN for each
    pair.
  - **Predicted:** D at each row's `T_hot_GeV`, from the production cosmology: Σ and d ln Σ/d ln T
    as `reachability_roots` computes them (its `slope_hot`), and c_s² = 1/(3 + G_s′/G_s) from
    `cosmology.G_s` and `cosmology.dG_s_dlogT`, as the temperature law reads them
    (`ComputeTargets/ScalarModel.py:441`). This is a scratch script that evaluates functions of T;
    it runs no history, and it is allowed. It stays in scratch, and the document gives its
    command and output.
  - The first undershoot is not expected to be linear (the paper: the velocity lags by about an
    e-fold), and the CSV gives at most three extrema per row, against the paper's fit to many.
    Report the numbers; test nothing.
- **Item 4, jumps.** A jump is a change between adjacent β that is large against its neighbours'
  changes; the agent says what it took as large. The capture kick changing from QCD to EW is one.
- **Item 7.** Λ is the CSV's `Lambda_eV`, 10⁻³ eV.
- **Item 8.** The orchestrator's account of the mechanism (board §5.4) may be cited as the
  orchestrator's. No probe of the agent's own beyond §1's runs and item 5's script.

**Legibility, as found so far** (board §5.4b). Neither is to be fixed here, since the prompt forbids
a change to the plot script:
- **The e⁺e⁻ band's label** ("e⁺e⁻ / reachable, not surfed") sits at the bottom centre of the upper
  panel, and on the orchestrator's 14-row probe it overlaps the uncaptured edge mark at β = 2.999.
  The fine figure will put some twenty edge marks under it. **The user ruled (2026-10-14) to leave
  it until 04's fine figure exists, and then revisit it.** The agent records in "Observations not
  acted on" exactly what the label covers in the fine figure: which edge marks, and whether any ring
  or captured marker is hidden.
- **The rings overlap.** The fine figure's axis is 2.95–3.05, not 2.98–3.04 (P4's outward rounding),
  so uncaptured rings 0.001 apart overlap, in a chain across the uncaptured window. The table of
  item 8 carries the values. Record what is hard to read.

**The conventions of note 03 hold.** In short:
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-only`
  before committing. A log states only what the commands and their output show.
- **The SHA.** The agent writes "this commit"; the orchestrator substitutes it in its follow-up.
- **Suites.** `CLAUDE.md`'s commands, ComputeTargets in its two halves, each in the foreground,
  serially. Each run's output goes to a file in the agent's scratch directory, the verdict is grepped
  from it, and the load is recorded beside it. A failure is re-run alone before it is called real.
  Run before the scans only (prompt §2 step 1); 04 changes no code.
- **Scratch.** The agent's own subdirectory of the session scratchpad, never `/tmp`. S3's CSV and
  item 5's script go there. No scratch file goes under `tools/` or `.documents/`.
- **The venv.** Nothing installed or uninstalled.
- **Stores and Ray.** None opened, none started. No PRyMordial.
- **Stop conditions are real** (prompt §4, and this note's two above). A stop goes to the user
  through the orchestrator, and the agent resumes with `SendMessage`.
- **Other sessions** may land commits on this branch. Review against the agent's commit.

**What moves on purpose:**
- the eight outputs under `.documents/surfing-temperature/out/`:
  `surfing_scan_phi5_M1e-3.csv` and `.csv.meta.json`, `surfing_temperature_phi5_M1e-3.pdf` and
  `.png`, and the same four with `_near3`;
- `.documents/surfing-temperature-verification.md`, new;
- the log `logs/04-run-the-scan.md`, and the board;
- `.documents/OPEN_ISSUES.md` only if an issue opens, which is expected only for a defect of the
  code (rule 10).
- Nothing else. No `.py` file.

**The facts, checked by the orchestrator on 2026-10-14 at `3fa4d53`.** Pass them on.
- **The tree.** Branch `surfing-temperature`, `HEAD` `3fa4d53` before this note; nothing staged.
  The untracked set is unchanged: 17 `pilot-2026.6.0*.db`, `pilot-main.log`, `pilot-plot.log`,
  `pilot-out/`, `full_run_2026.6.0.sh`, `full_run_2026.6.0-refresh.sh`,
  `prompts/jordan-normalization/`.
- **The grids.** `beta_grid(1.10, 5.00, 0.05)` gives 79 values, from `1.1` to `5.0`;
  `beta_grid(2.980, 3.040, 0.001)` gives 61, from `2.98` to `3.04`; `3.0` is in both. The CSV's β
  column is `repr(float)`, so these are the strings to compare.
- **The suites at `3fa4d53`** are given in the dispatch (§1 step 2).

**What the review exists to establish.**
- **(E1) The data are the tree's.** Both CSVs reproduce, as strings with `wall_s` apart, the
  orchestrator's independent runs; the sidecars name a code-identical `HEAD` with no tracked change.
- **(E2) The CSVs are complete:** both grids, once each, the right options.
- **(E3) The figures are drawn from the committed CSVs alone,** and are legible, or their
  illegibility is recorded.
- **(E4) The verification document** reports the eight items, and its numbers are the CSVs'.
  Measurements, not tests.
- **(E5) Nothing else moves:** no code, no scratch in the tree.
- **(E6) The records.** The log, the board, the counts.

## 1. Before you dispatch

1. **The tree.** Branch and `HEAD`; `git status --short` shows only §0's untracked set; nothing
   staged; `git log --oneline a8823a9..HEAD -- . ':!prompts'` lists only `2c98370`, `f166b31`,
   `1f40b53` and `ee4a2df`.
2. **Baselines.** The four roots at `HEAD`, serially, with the load. Give the agent the numbers.

## 2. Dispatch

Dispatch one fresh-context subagent, on **Opus**. Give it:
- the prompt;
- the campaign README and this board;
- `HEAD` and the baselines;
- §0 of this note, up to "## 1. Before you dispatch" only, as a copy in the session scratchpad.
  The agent does not read `orchestrator/`, and does not read prompt 05;
- its scratch directory, `agent04/`.

Tell it plainly:
- **One commit, at the end.** Its log is `logs/04-run-the-scan.md`, in `dk-and-rpk-migration`
  README §5.1's form.
- **The order of §0:** the scans before any tracked file is touched; nothing staged until the end.
- **The chunk plan of §0**, decided by the load at S1's start.
- **The files it may add or change are §0's list, and nothing else.** No code.
- **It starts no Ray, opens no store, runs no PRyMordial, and installs nothing.**
- **The e⁺e⁻ label and the overlapping rings are recorded, not fixed** (§0).
- **Stop and ask** on any of the prompt's §4 conditions, or §0's two.
- **It replies with:**
  - the log's path and the message as committed;
  - each invocation: command, load, wall time, exit status;
  - §2's checks with their output;
  - S3's comparison;
  - the headline numbers of S4 items 2–5 and 8, and the fine table;
  - what it saw in each PNG, and anything hard to read, the e⁺e⁻ label in particular;
  - the outputs' sizes;
  - the four suites;
  - every deviation with its tag;
  - "State handed to the next prompt", verbatim.

## 3. The review — eleven checks

1. **Scope.** `git diff <agent>~1 <agent> --stat` lists §0's files only. `git status` shows §0's
   untracked set and nothing new; no scratch CSV in the tree.
2. **E5.** `git diff <agent>~1 <agent> -- '*.py'` is empty. `git log` between this note and the
   agent's commit shows nothing that touches code.
3. **The sidecars.** In both, every entry has `git_head` equal to this note's commit (or a later one
   that touches no code) and `git_tracked_changes: false`. The options are as the log's commands
   say, the union of `beta_grid` is the grid, and `resumed`/`skipped` are consistent.
4. **E2, the CSVs.** Header equal to `tools.surfing_scan.COLUMNS`; 79 and 61 rows; β strings equal
   to the grids; no duplicates; every `phi_init_Mp`, `M_Mp`, `Lambda_eV`, `T_stop_GeV` as
   commanded.
5. **E1, by reproduction.**
   - The committed S1 CSV against the orchestrator's probe `scan79.csv` (note 03, at `61e0c42`;
     `git diff 61e0c42 <agent> -- tools/surfing_scan.py extract_common.py ComputeTargets/` must be
     empty first): every column but `wall_s` equal as strings, all 79 rows.
   - The committed S5 CSV against `edges.csv` (board §5.4a, at `9ab3e65`) on the 13 β they share
     (2.991–3.040).
   - The orchestrator's own run, into the scratchpad, of three fine β no probe has run (2.998,
     3.016, 3.019) and of β = 5.00: equal to the committed rows as strings, `wall_s` apart.
   - β = 3.00 equal in both committed CSVs (S3), checked by the orchestrator.
6. **E3, the figures.** Run the committed plot script on each committed CSV into the scratchpad.
   Compare the PNGs with the committed ones (pixels; the PDF's metadata may differ). Look at both
   committed PNGs, and a greyscale copy of each:
   - S1's as in the reviews of 03 and 03b, with rings at 1.10 and 3.00;
   - S5's: QCD to 2.997, EW from about 3.020, the chain of rings rising across the window;
   - what the e⁺e⁻ label covers in the fine figure, for the user.
7. **E4, by reading and by recomputation.** The verification document has items 1–8. With a
   script of the orchestrator's own, from the committed CSVs:
   - item 3's smallest capturing β per kick (both grids);
   - item 4's four ratio ranges per kick, and where the second dip appears and stops;
   - item 5's overshoot groups and the oscillator's half-periods and decays, for two or three rows,
     with D recomputed at `T_hot_GeV`;
   - item 8's table, equal to the S5 CSV's columns.
   No sentence tests a measurement against the paper or the audit.
8. **The cost.** The log's invocations and times add up, against P7's 60 minutes. Sizes under 1 MB.
9. **E6, the records.**
   - **The board:** row 04 ✅ with "this commit" and the log; S1–S5 ✅; the gate "Scan" ✅;
     `### 5.5 After prompt 04 (<date>)`.
   - **Issues:** §3 and the index untouched, or, if one opened, the index row, count and date in
     the same commit, and the issue a defect of the code.
   - **The log:** in §5.1's form, every deviation classified, §2's checks with their output, the
     e⁺e⁻ label and the rings under "Observations not acted on".
10. **The suites** at the agent's commit, serially: 18, 61 + 115, 69, 70. (No code changed, so this
    confirms the baseline.)
11. **The interface for 05.** The log's "State handed to the next prompt": the outputs' paths, the
    one command that re-runs each scan (README §7), the counts. Anything 05 needs and does not have
    goes to 05's note.

## 4. After the review

- **Record it on this board** as an *Orchestrator review of prompt 04* paragraph in §5.5. Fix small
  residue in a follow-up commit of its own:
  - "this commit" → the SHA (board, log and, if it names it, the verification document);
  - this note in the notes table, marked "used for 04";
  - the status line: 5 of 6 landed.
- Anything the review finds that is not residue is opened on this board's §3 and in the index, in
  the same commit.
- **Report to the user:**
  - the message as committed;
  - both PNGs;
  - the headline measurements: the dynamical thresholds against the analytic ones, the uncaptured
    window near 3.0, the overshoot and the oscillator comparisons;
  - the e⁺e⁻ label in the fine figure, for the user's ruling (deferred to now, 2026-10-14);
  - anything 05's note must carry.
- **Then 05.** Its note is written after this review.
