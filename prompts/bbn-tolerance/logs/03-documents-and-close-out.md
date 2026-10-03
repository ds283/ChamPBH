# Log 03 — Documents and close-out

**Prompt:** prompts/bbn-tolerance/03-documents-and-close-out.md
**Commit:** the commit that adds this file ("Close the bbn-tolerance campaign with a handover"); its SHA is in `git log`
**Model:** Claude Sonnet 5.5
**Date:** 2026-10-03
**Result:** COMPLETE WITH DEVIATIONS (one structurally required, two implementation choices; no
stop condition met). The three documents carry the additions the prompt asks for, all additive. The
roster, re-measured on the final tree with the small network, `prod` and no override, equals log
02's figures in every outcome field: 17 of 17, and all 11 histories that fail on the full network
complete. The suites are unchanged at 18, 114 and 31. No code changed, `main.py` was not run and
no store was written.

Everything was run on `086bae5` (the branch head at dispatch; nothing landed on the branch while
the prompt ran) plus this prompt's uncommitted documents. The tool's CSV `commit` column therefore
reads `086bae5` on the first 8 rows and `086bae5+dirty` on the last 9: the documents were edited
while the roster ran. No file the tool reads was changed.

## What shipped

No code. **`VERSION_LABEL`** stays `"2026.6.0"`, **`PRYM_VERSION`** stays
`"bf24c3d+ri02+sr01+bt02"`.

- **`.documents/numerical-strategies.md`: new §7.7**, dated 2026-10-03, between §7.6.5 and §8 (239
  lines added, none removed). Subsections:
  - 7.7.1 the network: production runs the small network, why (log 01 item 6, the Li8(p,d)Li7
    reverse rate), and how the full network is still selected (the one name in `main.py`, no flag,
    the user's reason);
  - 7.7.2 the low-T tolerances (upstream passes none, so 1e-3 applied; now small 1e-6 with `atol`
    1e-11, full 1e-5 with `atol` 1e-15);
  - 7.7.3 the scans: a table from log 01 (full network, five settings) and one from log 01c (small
    network, P11's four criteria at each);
  - 7.7.4 the residuals at the production setting, as measurements (P9): the D/H spread over the
    variants, Yp's spread, the convergence error against 1e-8, the a(T) bias measured on the full
    network only, and the default tolerance's own error;
  - 7.7.5 the offset between the networks (P12) and the new baseline, with the re-pinned constants;
  - 7.7.6 the cost against the old production;
  - 7.7.7 the two `PRyM/` hunks, with line numbers, for an upgrade (§7.6.4 is extended, not edited);
  - 7.7.8 other things recorded: the warning and the refresh route, the two open PRyMordial issues,
    and the Σ_eff threshold curve (deviation 2).
  Its opening lists, by statement, what it supersedes: §7.5's note that production runs the full
  network, §7.6.4's `PRYM_VERSION`, §7.6.5's baseline and §7.6.2's table, §7.6.3's "about 10 s".
- **`.documents/numerical-methods-for-paper.md`: new §4.2**, dated, after §4.1 (67 lines added, none
  removed). It states the network and tolerance, the cost, the precision a single history can claim
  (D/H reproducible to a median of 4×10⁻⁵ and at most 1.5×10⁻⁴; Yp to 1.5×10⁻⁵ and 4.3×10⁻⁵;
  convergence of the low-T stage 5.5×10⁻⁵ and 2×10⁻⁷), the network offset as a systematic of
  PRyMordial's, and that ⁷Li/H from the small network is not reliable and not used. It says that
  wherever §4 or §4.1 says "full network", or gives the old figures, it describes the earlier trees.
- **`.documents/review-remediation-verification.md`: new §4.11**, after §4.10 and before §5 (180
  lines added, none removed). It carries README §7's six points, a list of what it supersedes, the
  refresh route as commands, what the warning prints, a verification table, and reproduction
  commands.
- **`.documents/paper-corrections-numerical-section.md`: not edited.** No sentence of `Paper1.tex`
  states or implies which PRyMordial network ran or its precision (Verification, "paper-corrections").
- **`.documents/OPEN_ISSUES.md`:** the header's description of this board is corrected (6 of 6
  landed, closed 2026-10-03, prompt 03's part), and one sentence is added under §1.10. The count,
  23, is unchanged: prompt 03 opens and closes nothing.
- **`prompts/bbn-tolerance/IMPLEMENTATION_STATE.md`:** the status line reads COMPLETE with the final
  counts; a Decisions entry for this prompt; the prompt row and item D; §3's introduction and an
  **Owner** line under each of the two entries that stay open.
- **`prompts/INDEX.md`:** this campaign's row is **complete**, and the header reads 0 live, 6
  closed.
- **`prompts/bbn-tolerance/logs/03-probes/`:** `run_jobs.py`, `compare.py`, `refresh_commands.py`,
  `remeasure.csv` (17 rows), `compare.txt`, `warning_output.txt`.

`run_jobs.py` also wrote each invocation's stdout to `out/`; that directory was deleted, because the
CSV holds every value it printed.

## Deviations from the prompt

### 1. The refresh route's `--drop` goes on one call, and all eight blocks must run — STRUCTURALLY REQUIRED

- **What the prompt assumed.** "Run `main.py` on the copy with the science run's arguments plus
  `--drop bbn-data`": one `main.py` call carrying the drop.
- **What was there.** `full_run_2026.6.0.sh` makes eight `run` calls (C1, C4, L, three C5 blocks, C3,
  C2; its own header says seven), each a separate `main.py` invocation. `Datastore._drop_actions`
  runs at every start-up and empties `BBNData_tags`, `BBNDataValue` and `BBNData` on every shard. So
  `--drop bbn-data` on every call would leave only the last block's BBN rows; on the first call alone
  it empties the BBN rows of every history, so every block must then be re-run to refill them, and
  an interrupted refresh must be resumed without `--drop`.
- **What was done.** §4.11 point 2 gives the route as three steps (copy the 17 files to a new
  directory with their names; make a copy of the script whose first `main.py` call alone carries
  `--drop bbn-data`; run it with `STORE_DIR` set), and states the two cautions in bold. It says the
  commands have not been run.
- **Why the script is not changed or committed.** `full_run_2026.6.0.sh` is the user's untracked
  file; the prompt forbids touching a store or committing it. A recipe that edits a copy keeps the
  original intact, and §4.11 says exactly what to change.

### 2. §7.7.8 holds more than the prompt lists — IMPLEMENTATION CHOICE

The prompt's §7.7 list has the network, tolerances, scans, residuals, offset, cost and hunks. I added
a closing subsection, 7.7.8, with three things: the warning and the refresh route (a pointer to
§4.11), the two open PRyMordial issues, and the Σ_eff threshold curve of prompt 01b. The alternative
was to leave them to §4.11 alone. I chose to add them because §7 is where a reader of the numerics
looks for what `PRyM/` and the BBN route are now, and §7.7.8 is short, additive and points to §4.11
for detail. The 01b item is not about PRyMordial's numerics; it is there because the campaign
changed it and README §7 point 5 requires the handover to say so.

### 3. §4.11 lists its supersessions more widely than §4.10 did — IMPLEMENTATION CHOICE

README §7 and the prompt ask for six points. I added a "This supersedes" list before them, as §4.10
has, and filled it by grepping §4.2–§4.10 for "full network" and the baseline values: §4.6 point 2,
§4.10 point 5's roster and the SM baseline in three places, and §4.10 point 6's issue and its 7e-4
figure. The alternative was to state nothing, leaving a reader of §4.6 to believe production runs the
full network. Nothing above is edited.

## Verification performed

**Suites.** Run from the repository root with `PYTHONPATH=. ./venv/bin/python -m unittest discover
-s <pkg>/tests -t .`, grepping `Ran`, `OK` and `FAILED`.

| package | before (`086bae5`) | after (`086bae5` + this prompt's documents and probes) |
|---|---|---|
| CosmologyModels | 18, OK (144.9 s, loaded) | 18, OK (65.5 s) |
| ComputeTargets | 114, OK (387 s, loaded) | 114, OK (246 s) |
| Datastore | 31, OK (2.3 s) | 31, OK (2.5 s) |

The counts are unchanged, as README §6.3 requires: no code changed. The first "before" run was
discarded because my `tail` cut off the `Ran N tests` line; the run above is the second. It was made
while the documents were being edited, which no test reads. `black --check` is clean on the three
probe scripts.

**The store stayed read-only.** `/usr/bin/stat -f "%N %m %z"` on all 17 files of
`~/ChamPBH-stores/science-2026.6.0*` was identical before the first solve and after the last (a
`diff` of the two listings was empty). No `-wal` or `-shm` file appeared. The roster ran through
`tools/bbn_from_store.py` and its `mode=ro` connection; no suite, probe or document opens the store.

### README §6.3 row by row

**1. The 17-input roster, small network, `prod`, no override, on the final tree: 17 of 17
identical to log 02.**

- **How it was run.** `03-probes/run_jobs.py --jobs 8`: 17 invocations of `tools/bbn_from_store.py
  … --variant prod --small-network` (the SM baseline, and the 11 failures and 5 controls of README
  §6.0), 8 at a time, in 127 s, all with exit code 0. No `--lowT-rtol` was passed, so the tool's
  override changed nothing; `compare.py` asserts the CSV's `lowT_rtol` and `lowT_atol` are empty and
  the network is `small`.
- **How it was compared.** `compare.py` (`compare.txt`) compares each row with
  `02-probes/acceptance.csv`'s `prod` and SM rows (log 02's figures, which equal log 01c's T1 rows at
  1e-6): status, failure stage, `t reached`, `t target`, Yp, D/H, ³He/H, ⁷Li/H and the failure
  reason, as the CSV strings were written (`repr`, 17 significant digits).
- **The result.** 17 identical, 0 differing, 0 missing, 0 non-ok outcomes. **All 11 histories that
  fail on the full network complete.** Examples: SM, Yp 0.24688021169088586, D/H
  2.4582878928660548, ³He/H 1.0419326951489363, ⁷Li/H 5.48637300688257; control β = 1.6, M = 10⁻³,
  Yp 0.24688971029506177, D/H 2.4609141011473543.
- **Wall times** are not acceptance rows here (load average 15–70 from background processes and the
  suites running alongside). The solves took 39–54 s each, against 11–18 s in log 02 at lower load.
- **Conclusion.** Nothing has drifted since `5a72871`. The §3 stop is not met.

**2. `.documents/` is additive.** `git diff --numstat -- .documents` on the working tree:

| file | added | deleted |
|---|---|---|
| `numerical-strategies.md` | 239 | 0 |
| `numerical-methods-for-paper.md` | 67 | 0 |
| `review-remediation-verification.md` | 180 | 0 |
| `OPEN_ISSUES.md` | 5 | 2 |

`OPEN_ISSUES.md` shows 2 lines deleted, all in the header paragraph's description of this board
(the one the prompt allows) and none in a row; two lines are added under §1.10's introduction. No
document needed a sentence rewritten, so the third §3 stop is not met.

**3. Suites unchanged from prompt 02:** see the table above.

### The documents

- **The warning, quoted in §4.11.** `03-probes/refresh_commands.py` calls
  `warn_foreign_bbn_provenance` on stub objects (663 successful rows made by
  `bf24c3d+ri02+sr01` on the full network and 21 failure rows) and prints, as §4.11 quotes
  (`warning_output.txt`), one header line, one line per foreign pair, one line for the failure rows
  and the refresh-route line. A refreshed stand-in prints nothing. The counts are the φ\* = 5
  histories the brief counted; the store's real counts will differ and no store was opened.
- **Every number in the three documents** is from a log: the figures and tables of §7.7 and §4.2 are
  copied from logs 01, 01c and 02 with the log and item named, and the re-measure figures are this
  prompt's own. Where a figure was measured on one network only (the a(T) bias, the Yp floor from
  tightening four stages, the thermodynamic stage's role in β = 2, M = 10⁻⁵), the text says so.
- **paper-corrections.** The paper path is the one in `paper-corrections-numerical-section.md`'s
  header, `/Users/ds283/Documents/Git paper repositories/Chamlelon PBHs/Paper1.tex`, last modified
  2026-10-01. A grep for `PRyMordial` finds lines 155 (the macro), 3130, 3675, 3689 and 3774; for
  "small network", "reaction network", "full network", "nuclear network", `rtol` and "relative
  tolerance" it finds only line 2964, the integrator's tolerance. Line 3774 is an authors' note
  that the offset of the points from Cooke and the PDG "is a property of the nuclear rates used by
  PRyMordial". None names the network, a PRyMordial tolerance or its precision, so no row was added,
  as the prompt allows.

## Observations not acted on

1. **`full_run_2026.6.0.sh`'s header says "seven main.py invocations"; it makes eight** (C5 is three
   blocks). The file is the user's and untracked, and was not touched. §4.11 says "eight" and notes
   the script's own count.
2. **`tools/bbn_baseline.py`'s module docstring** still lists `--small-network` as an example, now the
   default (log 02, Observations 2). Cosmetic, outside this prompt's files.
3. **`paper-corrections-numerical-section.md` §5.4's bullet on PRyMordial's sensitivity** ("about
   10⁻⁴ relative") and `numerical-methods-for-paper.md` §4's "up to 7×10⁻⁴" are superseded by §4.2's
   reproducibility figures. §4.2 says so, as the rule on additive documents requires; the old
   sentences are left in place.
4. **The a(T) stage's bias** (+4.5×10⁻⁴ in D/H on the full network; log 01 item 9) was not measured on
   the small network. It is recorded in §7.7.4 and §4.2 as a measurement (P9), not as an issue.
5. **The machine was heavily loaded** (1-minute average 15–70) by background processes, and the
   roster ran alongside the first suite run. The suites took 145 s and 387 s then, and 65.5 s and 246 s
   in the second run, against 68.8 s and 85.0 s in `science-readiness` log 09 (103 tests then). No
   result depends on it.
6. **The branch `bbn-tolerance` is not merged into `main`** (`git merge-base --is-ancestor
   bbn-tolerance main` fails). `INDEX.md` says so. Merging is the orchestrator's and the user's.

No issue is opened by this prompt, and none is closed.

## State handed to the next prompt

There is no next prompt: the campaign is complete. What the user has, and what remains theirs:

- **The tree.** `PRYM_VERSION = "bf24c3d+ri02+sr01+bt02"`, `VERSION_LABEL = "2026.6.0"`. Production runs
  the small network (`BBN_SMALL_NETWORK = True`, `main.py:801`; `plot_by_beta.py:76`), with low-T
  `rtol` 1e-6 on it and 1e-5 on the full network. The 11 low-T failures of the 2026.6.0 run
  complete on the patched tree, in all three variants. Suites: **18, 114, 31**.
- **The one thing to do: refresh BBN on the science store**, which still holds the old rows, on the
  full network at the old tolerance. The route and its two cautions (the drop on the first call only,
  all eight blocks) are in `.documents/review-remediation-verification.md` §4.11, point 2. It has not
  been run; it is the user's, on another machine, as `CLAUDE.md` says of production runs.
- **What a store that was not refreshed prints:** the four `!! warning` lines quoted in §4.11 point 2.
- **Residuals** (P9), small network at 1e-6: D/H spread median 4.4×10⁻⁵, at most 1.51×10⁻⁴; Yp spread
  median 1.5×10⁻⁵, at most 4.3×10⁻⁵; convergence against 1e-8 of D/H 5.5×10⁻⁵ and Yp 2.1×10⁻⁷. The
  network offset is D/H −2.4 to −3.6×10⁻⁴ and Yp within ±3.2×10⁻⁵. `numerical-strategies.md` §7.7.
- **Open, owned by this board, not assigned:** `[01-prymordial-li8-p-d-li7-rate-rings-near-1-kev]` and
  `[01-prymordial-dYB8dtLT-unpacks-Y-in-the-superseded-order]`. `OPEN_ISSUES.md` §1.10; 23 open in
  the whole index.
- **Figure 3** (`T_deliver`) is redrawn by re-running `plot_by_beta.py`; the refresh's closing calls do
  it.
- **Not merged:** the branch `bbn-tolerance` has not been merged into `main`.

**Reproduction commands.** Run from the root.

```bash
./venv/bin/python prompts/bbn-tolerance/logs/03-probes/run_jobs.py --jobs 8     # remeasure.csv
./venv/bin/python prompts/bbn-tolerance/logs/03-probes/compare.py               # compare.txt
PYTHONPATH=. ./venv/bin/python prompts/bbn-tolerance/logs/03-probes/refresh_commands.py   # warning_output.txt
```
