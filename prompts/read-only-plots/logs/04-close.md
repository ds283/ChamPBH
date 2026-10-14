# Log 04 — close

**Subject:** Close the `read-only-plots` campaign: the documents, the handover and the records
**Commit:** this commit, on branch `read-only-plots`, on top of `a95658b`
**Date:** 2026-10-14, by Claude Sonnet 5.5
**Result:** **COMPLETE.** Z1–Z4 done; the campaign is closed, 4 of 4 prompts landed. The four roots
pass at 18 / 152 (61 + 91) / 63 / 34 before and after. `git diff --numstat` on `.documents/` shows
deletions only in `architecture-summary.md` (3, descriptive lines) and `OPEN_ISSUES.md` (2, the
header); the 11 earlier dated notes of `architecture-summary.md` are byte-identical and one is added.
No code, test or script changed; no store was opened, no Ray started, nothing installed, no process
of anyone else touched. `full_run_2026.6.0.sh` and `full_run_2026.6.0-refresh.sh` were read only.
No issue opened; none open.

## What shipped

- **Z1** `.documents/architecture-summary.md` (28 +, 3 −): the tree gains `plot_inputs.py`; a dated
  note (2026-10-14) after the `science-readiness` note says what `plot_inputs.py` holds and why, that
  both plot scripts open read-only, what each kind of miss does (U4, U3) and that they refuse
  `--drop`; §4.2's paragraph on the drivers' pools says the plot scripts' pools are read-only; §5.1's
  `--prune-unvalidated` and `--drop` lines say they are `main.py`'s (the orchestrator's amendment 2).
  §5.1's `--ray-address` line is as 01 left it, and correct.
- **Z2** `CLAUDE.md`, "Repository mechanics" only (5 +, 3 −): one bullet on the plot scripts, and the
  ComputeTargets counts 114 → 152, 53 → 91 and the date of the parenthesis → 2026-10-14 (the
  orchestrator's amendment 1). "Ray on this machine" was checked and is right; it is untouched.
- **Z3** `.documents/review-remediation-verification.md` §4.13 (83 +, 0 −), after §4.12: what the
  campaign did; what it supersedes in §4.12 by statement (point 3's `run()` and plot-loop rows,
  point 4's last sentence, point 7's two items); the two run scripts line by line; the rule that the
  plot loop must be given the build's options, which options, and the φ*-and-C5 dependence; the
  science rebuild held until `jordan-normalization`.
- **Z4** the board (COMPLETE, row 04, Z1–Z4 ✅, §5.5); `prompts/INDEX.md` (header, the row);
  `.documents/OPEN_ISSUES.md` (header).

## Deviations from the prompt

1. **IMPLEMENTATION CHOICE** (orchestrator's amendment 1, overrulable). `CLAUDE.md` also changes the
   three stale ComputeTargets figures and their date, measured by this prompt's own run (152, 91,
   2026-10-14). The prompt says "nothing else changes".
2. **IMPLEMENTATION CHOICE** (orchestrator's amendment 2). §5.1's `--prune-unvalidated` and `--drop`
   lines gain a parenthesis each; nothing else in §5.1 changed (`--shards N (default 20)` stays).
3. **IMPLEMENTATION CHOICE.** Z2's sentence is its own bullet, after "Long-running jobs".
4. **IMPLEMENTATION CHOICE.** The board's list of orchestrator notes gains a line for
   `orchestrator/prompt-04.md` (named in the board by path only; the file was not read).
5. **IMPLEMENTATION CHOICE.** §4.13 says in a clause that `78769a0` and `de57ec0` did the merge and
   the rename, which §4.12 point 7 listed as left to the user; and its table's φ*-and-C5 statement
   is in point 3 rather than in the table.
6. **IMPLEMENTATION CHOICE.** `OPEN_ISSUES.md`'s header gains one clause for this campaign in the
   same sentence as the `dk-and-rpk-migration` clause (it deleted the closing full stop of that
   sentence; this is the "2" deletions of the numstat). §1.12 is unchanged (empty, saying so).

No `STRUCTURALLY REQUIRED` and no `UNINTENDED DRIFT` deviation.

## Verification performed

**The tree at the start:** `HEAD` `a95658b`, no tracked change; 23 untracked paths, all the user's
(17 `pilot-2026.6.0*.db`, `pilot-main.log`, `pilot-plot.log`, `pilot-out/`, the two run scripts,
`prompts/jordan-normalization/`), the same 23 at the end.

**1. The suites** (serial, foreground; output in the session scratchpad; 1-minute load at the start of
each):

| | Before | After |
|---|---|---|
| CosmologyModels | 18 OK, 68.7 s (6.3) | 18 OK, 67.5 s (5.4) |
| ComputeTargets `test_[a-f]*` | 61 OK, 95.5 s (5.2) | 61 OK, 95.5 s (6.2) |
| ComputeTargets `test_[g-z]*` | 91 OK, 106.9 s (4.7) | 91 OK, 100.0 s (5.4) |
| ObjectFactories | 63 OK, 5.0 s (6.5) | 63 OK, 4.5 s (5.4) |
| tools | 34 OK, 7.5 s (7.0) | 34 OK, 6.7 s (5.2) |

No count moved. `ComputeTargets` is 61 + 91 = 152.

**2. The documents are additive.** `git diff --numstat HEAD` on the tracked files:
`.documents/architecture-summary.md` 28 / 3; `.documents/review-remediation-verification.md` 83 / 0;
`.documents/OPEN_ISSUES.md` 4 / 2; `CLAUDE.md` 5 / 3; `prompts/INDEX.md` 2 / 2;
`prompts/read-only-plots/IMPLEMENTATION_STATE.md` 45 / 7. The deletions in `.documents/`:
- `architecture-summary.md` (3): the line of §4.2 that now also carries the plot scripts' sentence
  (re-wrapped), and the two §5.1 lines `--prune-unvalidated` and `--drop`, which gain parentheses;
- `OPEN_ISSUES.md` (2): the header's date line and the line ending the `dk-and-rpk-migration`
  clause, which now continues into this campaign's.

**The dated notes.** Every `> **Note, …**` block of `architecture-summary.md` (a block is the line
beginning `> **Note,` and the `>` lines that follow) extracted at `HEAD` and from the working tree and
compared with `diff`: 11 at `HEAD`, 12 after, `diff` shows only the added block (the 22 added lines
after the `science-readiness` note's), and no line of the 11 removed or changed.

**3. Every citation exists.** Paths: `plot_inputs.py`, `plot_by_beta.py`, `plot_ScalarModel.py`,
`main.py`, `config/argument_parser.py`, `exponential.yaml`, `tools/bbn_from_store.py`,
`tools/compare_stores.py`, `.documents/read-only-plots-verification.md`,
`prompts/read-only-plots/IMPLEMENTATION_STATE.md`, `prompts/read-only-plots/README.md`,
`.documents/dk-and-rpk-migration/rehearsal/ray_cluster.sh`,
`~/ScienceData/ChamPBH-rehearsals/read-only-plots/` (exists). Functions and names (by `grep` in the
tree): `PlotOptionNotStored`, `parameters`, `stored_grid`, `potentials`, `couplings`, `models` and
`T_stop` in `plot_inputs.py`; `ray_address_problem` (`config/argument_parser.py:331`),
`DEFAULT_T_INIT_GEV` (`:14`), `DEFAULT_PHI_INIT_MP` (`:15`, 5.0). Options: `--abs-tol`, `--rel-tol`,
`--T-init-GeV` (`:123`), `--phi-init-Mp` (`:129`), `--T-stop-GeV` (`:145`), `--drop`,
`--prune-unvalidated`, `--ray-address`, `--inventory`, `--shards` (default 20). The scripts:
`ShardedPool(…, read_only=True)` at `plot_by_beta.py:1046` and `plot_ScalarModel.py:1617`, with no
`shards=` and no `drop_tables=` and `prune_unvalidated=False`; `except plot_inputs.PlotOptionNotStored`
at `:1078` and `:1649`; the `--drop` refusal at `plot_by_beta.py:110-117` and
`plot_ScalarModel.py:91-98`, before `ray_address_problem` and `ray.init` (`:121-125`, `:102-106`);
the message prefix `!! plot: the store holds no` (`plot_inputs.py:78`). Commits: `04486f4`,
`9dc23db`, `e5ef6cb`, `78769a0`, `de57ec0`, `bacca64`, `17fad8b`, `a95658b` (`git cat-file`).
`dk-and-rpk-migration` log 09's Observation 3 (`AdiabaticHistory.build` does not filter on
`validated`). The run scripts' lines, in the next section.

**§4.13's table against the two scripts** (read; `diff` of the two shows only `STORE_DIR`, `:10`, and
the refresh's `DROP`, `:20`, `:25`, `:26`; both have `set -euo pipefail` at `:8`):

| `full_run_2026.6.0.sh` / refresh | Text | At this tree |
|---|---|---|
| `:23-24` / `:24-26` | `run()`'s `main.py` call, no `--ray-address` | refuses (exit 2); the script ends at C1 |
| `:28` / `:30` | `run C1-main-M1e-3 …` | held up by the call above |
| `:28`–`:46` / `:30`–`:48` | the seven `run` blocks | unchanged |
| — / `:20`, `:25`, `:26` | `DROP="--drop bbn-data"`, `$DROP`, `DROP=""` | unchanged; `--drop` goes to `main.py` only |
| `:38-40` / `:40-42` | the C5 loop, φ* = 1, 2, 5 | stores the φ* the plot loop draws |
| `:48-49` / `:50-51` | `main.py … --inventory` | does not refuse; needs nothing |
| `:54-57` / `:56-59` | `plot_by_beta.py … --shards … --phi-init-Mp $PHI` | refuses (exit 2); needs `--ray-address 127.0.0.1:6380`; then read-only, `--shards` ignored |

**The plot loop's options against the build's**, read in `main.py` (`:905-940`), `exponential.yaml`
and `config/argument_parser.py`: both pass `--config exponential.yaml` (`Exponential`, `abs-tol` and
`rel-tol` 1e-8, `Lambda-values-eV 1E-3`), neither passes `--T-init-GeV` or `--T-stop-GeV`; M comes
from `read_table`; every φ* the loop draws is stored if C5 ran. As the orchestrator found.

**4. The index.** `grep -c '^- \`\[' .documents/OPEN_ISSUES.md` = **26** before and after, the
header's "26 open"; §1.12 has no row and says so. `prompts/INDEX.md`: "8 campaigns: 0 planned, 0 live,
8 closed", dated 2026-10-14; the `read-only-plots` row says "§1.12 (empty); 1 assigned … closed by 02;
none opened, none open; 26 open in the whole index", which agrees with this board (§3: none) and with
`OPEN_ISSUES.md`.

## The deliberate-breakage record

None: 04 adds no code and no test.

## Observations not acted on

1. `plot_by_beta.py`'s `ProfileAgent` label still reads `plot_ScalarModel` (log 02, Observation 1);
   cosmetic, offered to the user as housekeeping, not done.
2. The orchestrator's amendment 1 corrects counts in `CLAUDE.md` only; `prompts/INDEX.md`'s older
   rows (`dk-and-rpk-migration`: "18 / 114 / 97") are dated records of their own close and are
   unchanged.
3. `architecture-summary.md` §9 step 1 (`ray.init(address=args.ray_address)`) is still true and was
   not touched.
4. `§4.12`'s own text is unedited; it still reads as it did, and §4.13 supersedes it by statement.

## State handed to the next prompt

The campaign is closed; there is no next prompt.
- **Left to the user:** the merge of `read-only-plots` into `main` (P5); 03's store,
  `~/ScienceData/ChamPBH-rehearsals/read-only-plots/` (kept); the run scripts' lines (§4.13 point 1:
  `--ray-address 127.0.0.1:6380` on `run()`'s `main.py` call and on the plot line, the cluster started
  and stopped with `ray_cluster.sh`); the science rebuild, held until `jordan-normalization` lands.
- **Recorded and not opened:** with `tags=[]`, two validated rows for one model that differ only by
  tag raise `MultipleResultsFound`; `AdiabaticHistory.build` and `BBNData.build` do not filter on
  `validated` (`dk-and-rpk-migration` log 09, Observation 3).
- Suites at this commit: CosmologyModels 18, ComputeTargets 152 (61 + 91), ObjectFactories 63,
  tools 34. `.documents/OPEN_ISSUES.md` holds 26; no issue opened.
