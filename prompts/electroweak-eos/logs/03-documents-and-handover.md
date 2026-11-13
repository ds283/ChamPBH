# Log 03 — documents and handover

**Subject:** The campaign's documents, the issues and indexes, the handover, and the close
**Commit:** this commit, on branch `electroweak-eos`, on top of `5b512f1` (the user's second commit; `e1a23dc` is the first)
**Date:** 2026-11-13, by Claude Opus 5.5 (one agent)
**Result:** **COMPLETE, no stop.** The gate Close holds (§3).
- **The documents.** `architecture-summary.md` §8.5 and §14, `numerical-strategies.md` (two notes),
  `surfing-temperature-verification.md` §4, the audit's `report.tex` paragraph and a rebuilt `report.pdf`, the run record's note,
  `CLAUDE.md`'s counts and times, and `electroweak-eos-verification.md` §3.
- **The handover** is `.documents/review-remediation-verification.md` §4.21.
- **The inventory.** On a scratch copy of `electroweak-eos-02-old`, the tip's `main.py --inventory` exits 0 and reads it
  ("problems: none", label 2026.10.0, 48 histories); the copy's 18 files are byte-identical before and after.
- **Issues.** One opened, `[03-plot-quanta-grid-beta-axis-is-categorical]` (P26, U12 (a)). None closed. A dated correction on
  `[02-a-worktree-build-reads-the-data-files-of-the-tree-that-started-the-cluster]`. The index has 32 open.
- **Tests:** none added or changed; the counts are 02's: 26, 684 (219 + 465), 163, 292, 76.

## 1. What shipped

- **`.documents/architecture-summary.md`:** a dated note under §8.5's "Currently `QCD_Cosmology` …" (w a separate table; the
  new table by range; the build and `--check`; the labels; the working-directory pointer), and one dated line in §14's note of
  2026-11-07, on the workers' working directory.
- **`.documents/numerical-strategies.md`:** two dated notes. One follows "Σ_T's source" ("0 beyond the table" now begins at
  386.7085 GeV, and the 3.4e-8 figure is the old table's). The other follows §6's bullet on the stored ρ_R,J against
  (π²/30) g_ρ T_J⁴ (0.210 % and 0.078 % are the old table's; the new ones are 0.328 % and 0.195 %).
- **`.documents/surfing-temperature-verification.md`:** a new §4, "The threshold on the 2026.11.0 table (pointer, 2026-11-13)".
- **`.documents/peakewsurf-kicking-function/report.tex` and `report.pdf`:** one dated paragraph at the end of the addendum
  section, before the appendix; the PDF built (§3.3).
- **`.documents/review-remediation-verification.md` §4.21:** the handover, in §4.20's form, with its "This supersedes" list
  and eight points.
- **`.documents/electroweak-eos-verification.md` §3**, and **`.documents/electroweak-eos/close-03/`** (new): `inventory.sh`,
  `suites.sh`, `check_numbers.py`, and `out/` (the inventory's stdout, stderr and exit line, the three hash lists, the suites'
  log, and the ten numbers' output).
- **`science-runs/2026-11-08-science-2026.10.0/README.md`:** a dated note at the end (U12 (a)).
- **`CLAUDE.md`:** the counts, the times and one clause (U12 (a)); no rule (§2, ruling 2).
- **The board, `prompts/INDEX.md` and `.documents/OPEN_ISSUES.md`:** the campaign closed; the new issue; the correction to the
  `[02-…]` issue; the index row's hook for it changed.
- **This log.**
- **Not committed:** nothing of mine under `var/`. The scratch copy of the store, the LaTeX build, and the suites' full
  outputs are in the session scratchpad.

## 2. Deviations from the prompt

### 2.1 The orchestrator's rulings (note §0)

1. **U12 (a), "(a) for U12, please"** (the user, 2026-11-13). The user committed `e1a23dc` and `5b512f1` before dispatch. The Gate
   found a clean tree. D3, D4 and P26 are done on top of their commit; the (U12 b) items and stop do not apply.
   *STRUCTURALLY REQUIRED.*
2. **The `CLAUDE.md` parenthesis** is replaced by one sentence (date, campaign, the tests it added). The user's commit had not
   changed that paragraph, so the numbers and times are the only other change, with the one clause on the table. I did
   this as ruled. *IMPLEMENTATION CHOICE*, ruled by the note.
3. **The audit's PDF** was rebuilt in a scratch copy and only `report.pdf` copied back. The build succeeded (16 pages, as the
   committed PDF); no failure to name. *IMPLEMENTATION CHOICE*, ruled by the note.
4. **The inventory ran on a copy,** never on a store. *STRUCTURALLY REQUIRED.*
5. **Nothing was measured but the suites and the inventory.** `check_numbers.py` reads the records again; it measures nothing.
   *As the prompt says.*
6. **No file but the prompt's §4 list.** *STRUCTURALLY REQUIRED.*

### 2.2 Others

- **D1.** §4.21's "This supersedes" list has six statements. Two (§4.14 point 3's window, and §4.18 point 6's account of
  `[00-xav-sigma-is-non-monotone-at-25-35-keV]`) are outside the three the prompt names (the w table, 3.020, the `2026.10.0`
  histories), but each names the old table's window or a regeneration of the table as pending. *IMPLEMENTATION CHOICE.*
- **D2.** `numerical-strategies.md` gets a second note, on the bath drift, beyond the prompt's "known candidates". The prompt
  says to search and not rely on its list; that bullet states values read on the old table as present fact. §4.4's account of the
  spline and the statement that Σ is 0.0 exactly at T_CMB (the table's low end is unchanged) are not changed. *IMPLEMENTATION CHOICE.*
- **D3.** `architecture-summary.md` §8.5's note adds a sentence saying g_ρ and g_s are unchanged and not a consistent pair with w
  (naming `[05-kicking-table-and-saikawa-shirai-gs-disagree-through-qcd-and-ew]`). The prompt lists "g_ρ and g_s are unchanged
  (U4)"; the second half is the issue's own statement. *IMPLEMENTATION CHOICE.*
- **D4.** The index row's hook for the `[02-…]` issue is changed, as the prompt says to do if it repeats the claim. It did
  ("not by `one-loop-drain` 06's"). The board's entry is not edited; its correction is dated. *As the prompt says.*
- **D5.** On the board I moved the status paragraph (the four lines from "**Last updated:** … LIVE" to "cut from `main` at
  `ec6bca8`") under "History of the status line", and left the paragraph that follows it as the intro. The board's paragraph on
  orchestrator notes still says `prompt-03.md` was "not dispatched"; that is the orchestrator's to mark (it did so for the notes
  before), and I did not read the `orchestrator/` directory. *IMPLEMENTATION CHOICE.*
- **D6.** The inventory result is a finding against an expectation in §4.20 point 1: that the tip does not read a format-6 store.
  I expected, from the prompt's wording, that it might refuse a 2026.10.0 store as well. It does not: schema and format are the
  same. §4.21 point 1 says so and says what was not run (a build into that store).
  *Not a deviation, recorded as a finding.*
- **D7.** The run record's note gives "four or five lines" of what 02 measured: three bullets, the middle one on the 13 histories.
  *IMPLEMENTATION CHOICE.*
- **D8.** `CLAUDE.md`'s time for `ComputeTargets/tests` is 807 s, the sum of the two halves' reported times (175.6 s and
  631.3 s). I did not run the whole suite as one command (a Bash call is capped, and the file says to run it in halves).
  *IMPLEMENTATION CHOICE.*
- **D9.** `close-03/` also holds `suites.sh`, `check_numbers.py` and `inventory.sh`, the commands that made `out/`. The prompt asks
  for "§2's commands and outputs". *IMPLEMENTATION CHOICE.*

No *UNINTENDED DRIFT*. No code, test, record above a dated addition, or store was changed.

## 3. Verification performed

**The Gate,** repeated before starting (18:19–18:20 GMT): `HEAD` `5b512f1`, `be81f8b` an ancestor; `git status --porcelain` empty;
`git worktree list` one line; `runregistrykit list` "54 run(s) … 54 finished"; `tools/ray_cluster.sh status` "no processes",
ports 6380, 6381, 8267 and 52367 free; `tools/build_w_table.py --check` exit 0; the two stores each with 18 files (`.db`, 16 shards,
manifest); the verification document with §1 and §2 and no §3; `df -h .` 92 % used, 80 GB free; load 18.40 / 15.03 / 20.98.

**The suites** (verification §3.1): 26, 163, 76, 219, 465, 292, all OK, equal to 02's. Seconds and the one-minute loads at the
start: 78.1 (19.40), 158.2 (9.89), 27.2 (4.60), 175.6 (4.41), 631.3 (4.24), 333.3 (6.57). `uptime` at the end: 18:44, load
averages 4.43 / 5.34 / 8.23.

**§2 item 2, every line addressed to 03:**

| line | where | done? |
|---|---|---|
| The documents: §8.5, §14, `numerical-strategies.md`, `surfing-temperature-verification.md` §4, the audit | prompt §1.1; README §6.2 P10, P21 | done (§1) |
| The handover §4.21, in §4.20's form, with a "This supersedes" list | prompt §1.2; README §6.2 P22 | done |
| The standard analyses a rebuild repeats | README §6.2 P12; prompt §1.2 point 3 | done, §4.21 point 3: Figure 6 and Relativistic quanta, the two rows the file holds |
| The run record's dated note | README §4, P23; prompt §1.3 | done |
| `CLAUDE.md`'s counts, by description | logs 01a §6, 01 §6, 02 §6; README P24 | done: 26, 684, 163, 292, 76; the times; the clause on the table |
| The user's open point (P26) | README §10.3 | done: `[03-plot-quanta-grid-beta-axis-is-categorical]`, the board and index |
| The `[02-…]` correction (P25) | README §10.1, P25 | done: dated on the board, the index hook changed |
| The index: §1.25 and the header, count and date | P25, prompt §1.5 | done: 32 open |
| The board and `prompts/INDEX.md` closed | P29 | done |
| Whether the stores and exports are kept | log 02 §6; README §10.1, P28 | kept; §4.21 point 5 lists them with `store_id`s and sizes; retiring is the user's |
| Whether the surfing tests read S1-new | logs 01 §6, 02 §6; P27 | no: they stay on `table/out/surfing_reference_rows.csv`; §4.21 point 7 says where each record is |
| The item-(11) classes' data file must stay | log 01 §6 | stated in §4.21 point 6 |
| The merge into `main` | log 02 §6; P29 | not done, the user's; §4.21 point 8 states the ancestor test (exit 0), a fast-forward |
| The shard warning, the SIGKILL at `stop`, `release_T_GeV` blank, the reference-row and surfing-plot observations | log 02 §5; prompt §1.2 point 6 | stated in §4.21 point 6, each with its log |
| log 01 §5's other observations (pandas' parser; golden-file names in docstrings; comments in `test_release`; `test_injection`'s docstring; the second test of test_11; the β = 4 pair; the 1 MeV knife edge; P2197's A_k; the reflecting golden's warnings) | log 01 §5 | left in log 01 §5: none bears on a rebuild, and none is one of the prompt's listed items. §4.21 point 6 carries the stale comments and `XavEOS_test.py`'s path, which a reader of the table meets |
| log 02 §5's bath drift, P8 and parking observations | log 02 §5 | stated in §4.21 point 2 |
| (U12 b) items | prompt | do not apply |

**The ten numbers** (verification §3.3; `close-03/out/check_numbers.txt`):

| # | number | value read | source |
|---|---|---|---|
| 1 | F-new: QCD to, EW from, window | 2.975; 3.003; 2.976–3.002 (27 β) | `.documents/electroweak-eos/threshold/out/f_new.csv` |
| 2 | F-old: QCD to, EW from, window | 2.998; 3.020; 2.999–3.019 (21 β) | `threshold/out/f_old.csv` |
| 3 | histories with T_deliver across 10 GeV | 13 | `effect/out/effect.csv` |
| 4 | a-edge β = 3.0, T_deliver old → new | 1.68001 → 48.4902 GeV | `effect/out/effect.csv` |
| 5 | a-edge largest relative change of Y_p | 1.1 × 10⁻³ | `effect/out/effect.csv` |
| 6 | parking T at β = 2.99 and 2.995 | 0.701 → 42.9 MeV; 1.05 → 55.4 MeV | `effect/out/effect.csv` |
| 7 | bath drift at 1 MeV, 100 Δ, old and new | −0.2106; −0.3276 | `bath/out/bath_tables.csv` |
| 8 | the installed table | 170 rows; `690090c8…0119d` | `CosmologyModels/GenericEOS/Xav_EOS_data.csv` |
| 9 | the stores' `store_id` and file count | `821adc85…`, 18; `5fd9d146…`, 18 | `var/datastores/electroweak-eos-02-{old,new}.manifest.json` |
| 10 | rows of the index | 32; its header says 32 open | `.documents/OPEN_ISSUES.md` |

Where §4.21 or the notes quote a number of verification §2 that `check_numbers.py` does not re-read, I took it from §2 and its
table, and compared each of them against log 02 §6's. They agree wherever both give a figure (Y_p, D/H and ΔN_eff by run).
Log 02 §6 rounds to two figures where §2.4.5 gives three. I wrote §2.4.5's.

**The inventory** (verification §3.2): exit 0 in 29 s; 75,231 records in 16 classes; "problems: none"; label 2026.10.0; 48
validated histories; the file of hashes `b53f892b…2d34` before and after, equal to the store's own.

**The audit's PDF:** `latexmk -pdf` in a scratch copy of the folder, exit 0, 16 pages, no warnings in `report.log`; the new
paragraph is in the text; only `report.pdf` was copied back (SHA-256 `3c545633…be0c`, the committed one `364c81ea…196f4`, also 16
pages).

**Scope.** `git diff --cached --name-status` was shown before the commit (below). No Python file changes.
`tools/build_w_table.py --check` exits 0 at the end; the installed CSV is `690090c8…0119d`.

## 4. The deliberate-breakage record

Nothing is built or tested here that can break, so no breakage was constructed. `check_numbers.py` and `inventory.sh` are
scripts that read; `inventory.sh` compares the hashes with `cmp` and prints "COPY CHANGED" if they differ, which was not
exercised.

## 5. Observations not acted on

- **The board still says `prompt-03.md` was "not dispatched"** (its paragraph on the orchestrator's notes). Left for the
  orchestrator (D5).
- **A proposal for `CLAUDE.md`, not made:** none is needed. The counts are now stated, and the clause on the table is a
  description.
- **The tip reads a 2026.10.0 store and a build into it was not tried** (D6). `config/version.py` says such a store "is not
  extended at 2026.11.0"; the lookups are keyed on the label. If the user wants that confirmed before a rebuild, it is a probe
  on a copy.
- **`numerical-strategies.md` §4.4** says the spline subclass "freezes the argument at a floor temperature
  `_EOS_T_LO = 2e-3 GeV`"; `Xav_EOS_spline`'s own docstring says "the inherited 2 MeV freeze of w is not used". That predates
  the campaign and is a statement about the g* class's `w`; not changed.
- **The suites' load at the start of the first (19.40) was high,** from the machine's other users; its time (78.1 s) is 11 s above
  02's. Not significant. Recorded.

## 6. State handed on

To the user. Nothing is merged or pushed.

- **The merge into `main`** (a fast-forward; `git merge-base --is-ancestor main electroweak-eos` exits 0; `main` is `ec6bca8`,
  one commit ahead of `github/main` at `edf72b3`, not pushed; the branch has no upstream). Pushing is the user's.
- **The rebuild** is the user's, under `2026.11.0`, and repeats Figure 6 and the relativistic-quanta analysis
  (§4.21 point 3). The cost of the run's long blocks (those at M_crit near β ≈ 1.1–1.2) was not sampled by 02, and no projection is
  made. A worktree build needs a cluster started from its own tree (§4.21 point 4).
- **The stores** `var/datastores/electroweak-eos-02-{old,new}` and `var/electroweak-eos/02/` are kept; retiring them is the user's
  (§4.21 point 5). `git clean -x` and `-X` would delete them.
- **The issues:** three on this board, none assigned, each with no next step proposed (§4.21 point 6). The index has 32 open.
- **The commit** is one. The orchestrator substitutes its SHA for "this commit" in this log, the board, the index row and
  §4.21.

The commit message and `git diff --cached --name-status` are in the report this prompt sent to the orchestrator.
