# Log 08 — close

**Subject:** Close the `one-loop-drain` campaign: the law and the measures for the paper, the reading guides, `CLAUDE.md`, the
handover for the rebuild of record, the issues and the indexes
**Commit:** `a1fc384`, on branch `one-loop-drain`, on top of `d0d8918`
**Date:** 2026-11-07, by Claude Opus 5.5
**Result:** **COMPLETE.** C1–C6 shipped; no stop condition was met; no code changed. Opened none, closed none.
- `one-loop-drain-verification.md`: §8 (six subsections) and §9; `numerical-strategies.md`: §6.12 (two subsections);
  `architecture-summary.md`: five dated notes; `review-remediation-verification.md`: §4.20 (seven points). **No existing line of
  the four documents is deleted or reworded** (`git diff --numstat`: 0 deleted lines in each).
- Suites: 26 / 665 (219 + 446) / 163 / 259 / 72, 07's exactly, all passing. `main.py --inventory`: the reference store exit 0,
  25.3 s, `problems: none`, its 18 files byte-identical before and after; a copy of the base store exit 1 in 1.6 s, refused by the
  schema check, its 18 files byte-identical before and after.
- The index stays at 29 open; the board and `prompts/INDEX.md` close the campaign.

Orchestrator note 08 (its Gate, §0 and rulings 1–11) governed where it differs from the prompt; each ruling is classified in §2.

## 1. What shipped

- **C1, the law and the measures for the paper:** `.documents/one-loop-drain-verification.md` §8 (`:2226`–`:2524`):
  - §8.1 the two laws as coded (the fixed law; `one-loop-measured-v1` with f by regime as a table, each constant with its value,
    `file:line` and source; S(L); ℓ, ln R and L, L the model's with no default; the regimes; the gate; the key; the domain);
  - §8.2 what the law did on 02d's sample (outcomes, regime shares, the gap and R ≤ 1, the law's edges, the gate, parking and the
    first impact, the cap, ΔN_eff, Y_p and D/H, against the old and the fixed law, the cost), every number with its section;
  - §8.3 the stored measures (the bounce rows, the drain record, the all-generation margins and how they differ from the weighted
    ones, the sample masses), each with its columns, unit and the law it is written under;
  - §8.4 the derived measures (the spectrum `U4-U17-v1` with its constants; n_g, n̄_j, δ_stim, E(N), the phase slip, k_ex/H, the
    backreaction ratio, each with its formula and `tools/quanta_measures.py` line; the assumptions; what was checked against what);
  - §8.5 what they say on the sample; §8.6 where the reading does not hold and what is not modelled, each item with its issue.
- **C2, the reading guides:**
  - `numerical-strategies.md` §6.12 (`:1901`–`:1977`): §6.12.1 a history under `one-loop-measured-v1` in eight steps, each with its
    code and the dated note (01 `:317`, 02c `:358`, 03 `:394`, 05 `:417`); §6.12.2 a table of nine statements of §6.1–§6.11 that hold
    only for the fixed law or that this campaign superseded, and a line on §6.11.3's two stale code statements (corrected in
    `4fefdf8`, not by this campaign). No other note in the file: no statement outside §6 was found made false (§3 below).
  - `architecture-summary.md`, five dated notes, each 2026-11-07: after §2's tree (`:322`, the tree gains seven files); after §4.2's
    registry and JN 07's note (`:516`, 23 entries, `alpha_value`'s own factory and `ScalarHistoryBounce`); after §5.2's IR 12 note
    (`:798`, `sharded_tables` gains `ScalarHistoryBounce`, which corrects that note's "`sharded_tables` is unchanged"); the §7.2 map
    after 03's note (`:1560`, nine pieces, pointing to 01, 02c, 03 in §7.2 and 04, 05, 07 in §14 by line at `d0d8918`); and §14 at
    the end (`:2435`, the model's L with no default, and the workers' working directory).
- **C3, `CLAUDE.md`:** the time of `ComputeTargets/tests` and its halves (`:86`–`:87`) and of `tools/tests` (`:111`–`:112`), with
  the loads, from this run. The counts are unchanged (07's), and the parenthesis that dates them is left as it is (ruling 6). The
  Redshift, plot-scripts and science-runs bullets were read against the code and describe it truly; none changed.
- **C4, the handover:** `review-remediation-verification.md` §4.20 (`:2421`–`:2685`): the campaign in order with each commit; the
  "This supersedes" list (seven statements of §4.18 and §4.19); the seven points.
- **C5, the issues:** the board's three §3 entries gain dated lines (2026-11-07, prompt 08): `[00-later-bounces-…]` documented in
  verification §8.5–§8.6 with where the random-phase reading fails; `[00-the-parking-leftover-has-no-spectrum]` narrowed with 02d's
  measured parking share; `[02-one-loop-tunings-fail-at-small-A-…]` pointed to §4.20 points 3 and 6. `.documents/OPEN_ISSUES.md`:
  the date, this board's header clause, §1.23's paragraph and the parking row's hook.
- **C6, the board and the index:** the status line replaced by a short closed line, the run-on moved unchanged under "History of
  the status line" after §5; a dated note beside the opening description (the measured law); the `Code:` line completed; row 08
  and five items; §5's baseline for 08. `prompts/INDEX.md`: the row **complete** in the closed rows' form, the header corrected.
- **The scripts and outputs:** `.documents/one-loop-drain/close-08/run_inventories.sh`, `check_help.py` (`black`-clean), and
  `out/`: `inventory-reference.txt`, `inventory-basecopy.txt`, `check_help.txt`, ten `help-<name>.txt`.
- **This log.**

## 2. Deviations from the prompt, and the rulings of note 08

| Ruling / item | What was done | Class |
|---|---|---|
| 1, additive in the documents' own form | No line of the four documents deleted or reworded; the five arch notes and §6.12 placed as ruled; the dated notes of 01–07 untouched. The prompt's "a dated note at the statement" is applied where the statement is false of the present and no later note says so: arch §2's tree, §4.2's registry and §5.2's IR note | STRUCTURALLY REQUIRED |
| 2, the code is what the documents state | Every formula and constant of verification §8 and §6.12 carries its `file:line` at `d0d8918`, read in the code. The two README readings are history (table §3.3 below). No disagreement between a log, the verification or the README and the code was found on a formula, constant, column or default, so no stop | STRUCTURALLY REQUIRED |
| 3, the inventories | As ruled: the reference in place, the base as an 18-file `cp -p` copy in `<scratch>/basecopy/`, hashes checked before running; the outputs committed with `<scratch>`. Results equal the orchestrator's | IMPLEMENTATION CHOICE (ruled) |
| 4, the `--help` check | `check_help.py` collects every `--name` token of §4.20's final text and checks each against its own commands' help. Its first run failed one item: `--is-ancestor` (point 7's git command) belongs to no `--help`; the script now runs that read-only git command as written and records its exit. Rerun: 0 failures | IMPLEMENTATION CHOICE (ruled; the git entry mine) |
| 5, the bounds and the heartbeat | Point 3 gives the cap (30, `:5453`), the step budget (2 000 000, `:358`–`:381`), neither a wall-clock limit; 160 s; the wrapper's docstring; `runregistrykit list --stale-after SECONDS` with no value prescribed. The projection: ten workers from the 2026.8.0 record ("ten at a time", its README "Timings against the plan"); its basis, block counts and weakest assumption (block C) stated | IMPLEMENTATION CHOICE (ruled) |
| 6, `CLAUDE.md` by description | The times only; the dating parenthesis untouched; the modal grep run (§4.4) | STRUCTURALLY REQUIRED |
| 7, the lines addressed to 08 | Table §3.1; the notes 01–07 and the board's reviews read for it only; note 08's §2 and §3 not read | IMPLEMENTATION CHOICE (ruled) |
| 8, the issues and the index | As ruled; the count stays 29 | NOTED |
| 9, the board and the index | As ruled. The old INDEX header, "**Last updated:** 2026-11-02 · 17 campaigns: 1 planned, 0 live, 16 closed", was wrong: its table had 16 complete rows and this campaign's, live. §5's baseline for 08 is a dated paragraph in §5's form ("**2026-11-07, prompt 08:** …"), not a "### 5.N" heading, since §5 has no numbered subsections. The `Code:` line was checked against `git diff --stat 3b8a553 d0d8918 -- '*.py'`: it lacked `CosmologyConcepts/drain_model.py`, `ObjectFactories/alpha_value.py`, `CosmologyConcepts/alpha_value.py`, `CosmologyConcepts/Models/ExponentialModel.py`, `ComputeTargets/dark_radiation.py` and `RunDrivers/registered_run.py`, now added; it lists nothing the diff does not (`tools/outward_step_scan.py`'s 1 400 lines are mostly `reflection-restart`'s, merged at `1561b60`; `tools/` covers it) | NOTED; the paragraph form an IMPLEMENTATION CHOICE |
| 10, the suites | Once, serially, before the edits; the `test_[g-z]` half and `tools/tests` in the background, each waited on by its notification | IMPLEMENTATION CHOICE (ruled) |
| 11, the log | This file, in `dk-and-rpk-migration` README §5.1's form | STRUCTURALLY REQUIRED |
| §8.2's and §8.5's median shares | The one-loop share's and the parking share's medians over the 230 are not in the verification's sections; they are quoted from README §8.15, which states them from 05's export, and cited there | IMPLEMENTATION CHOICE |
| §4.20 point 2's A2 | That A2's 106 β are among A's 186 was checked by running the 2026.8.0 script's own `G` function (copied, on its specs), not by any build | IMPLEMENTATION CHOICE |
| A slip of mine | A shell command meant to patch a scratch draft started a bare `./venv/bin/python` reading stdin; it was stopped (`TaskStop`) after the Bash call's timeout, having done nothing, and the patch run again | UNINTENDED DRIFT (no effect) |

## 3. The lines addressed to 08 (§2 item 2; ruling 7), the greps, and the README's history

### 3.1 The lines addressed to 08

Searched in logs 01–07 (with 02b, 02c, 02d), README §8.11–§8.16 (and §8.3, §8.7, which name 08), orchestrator notes 01–07 and the
board, for "08", "close", "handover", "made false", "for the documents".

| Where | The line | Done / left | Where, or why |
|---|---|---|---|
| README §8.3; board §3 `[00-later-bounces-…]` ("documented by 08, and closed there if negligible") | document δ_stim; close if negligible | done; left open | verification §8.5–§8.6; board §3 dated line. The user's ruling (README §8.16, P71) keeps it open |
| board §4 `[00-weighted-margins-…]` (twice: "Under the fixed law the record stays as it is; 08 documents that") | document the fixed law's weighted-only record | done | verification §8.3 (the difference) and §8.6 (the fixed law has no instrumentation) |
| log 05 §6; board, 05's review | "08's documentation may want to say that the first-order, random-phase reading of δ_stim does not hold there" | done | verification §8.6; §4.20 point 6 |
| log 05 §6 | the backreaction ratio above 1; E negligible | done | verification §8.5–§8.6 |
| log 06 §7 "For 08"; note 06 `:280`; board 06's review | the stores, the gates, the costs | done | §4.20 points 1, 3, 5 |
| log 06 §6 (for 08's documentation) | histories.csv's marker rows and the ΔN_eff legend | done | §4.20 point 6 |
| log 06 §6 | the shard warning | done | §4.20 point 3 |
| log 06 §6 | "Reflections (elastic model)" | done by 07 (P64) | `extract_common.add_ScalarHistory_labels`; nothing for 08 |
| log 06 §6; README §8.15 ("08's handover notes it") | the disk | done | §4.20 point 3 |
| log 06 §6; board 06's review ("worth a line in 08's handover") | the workers' working directory | done | §4.20 point 3; arch §14 note |
| README §8.15, §8.16 | the shard warning is DatastoreKit's (`befa7f3`) | done | §4.20 point 3 |
| log 07 §6 (State handed to 08); note 07 `:261` | the commands, exits, figures, costs, what the demonstration shows | done | §4.20 points 2 and 4 |
| log 07 §5 item 1; note 07 ruling 8 | `reflection_count`'s docstring "elastic reflections" (`extract_common.py:248`) | left: code, outside 08's files; for the orchestrator's housekeeping | ruling 7 |
| log 07 §5 items 2, 5 | `drained_share_classical` 1.0000000000000007; no unassessed point | done | §4.20 point 6; verification §8.5 |
| log 07 §5 item 7 | the tools are for a handful of histories | done | §4.20 point 4 |
| log 07 §5 item 8 | the shard warning and the legend's α with no point are not 07's | done | §4.20 points 3, 6 |
| board, 07's review; README §8.16 | the two weak readings | done | §4.20 point 4 |
| README §8.7 | the handover for the rebuild at U14's A, Fig 6 at laboratory M first | done | §4.20 points 2, 4 |
| README §8.16 P66–P73 | the close as proposed | done | §1 above |
| README §8.16; board header ("under U2, the rebuild of record waits for this campaign's 08") | §4.19 point 4's condition | done | §4.20 "This supersedes" |
| logs 01, 02c, 03, 05, 07 | `CLAUDE.md`'s counts and dated sentence | done by each prompt; this run's counts equal | §4.2 below |

### 3.2 The greps for statements made false (prompt §1.2)

Over `git show d0d8918:<file>` of `architecture-summary.md`, `numerical-strategies.md` and `CLAUDE.md`, outside this campaign's own
dated notes (arch `:1430`–`:1535`, `:2281`–`:2362`; NS `:317`–`:454`), for `α_eff`, `alpha_eff`, `alpha_value`,
`ScalarHistoryBounce`, `FINGERPRINT_FORMAT`, `VERSION_LABEL`, `elastic`, `histories.csv`, `fields.csv`, `2.7`, `drain_law`, `drain
model`, `fixed fraction`, and the registry and table listings. Dispositions:
- **Noted at the statement (new dated notes):** arch §2's tree (it lists none of the seven new files); arch §4.2's registry table
  and its notes (21 entries, `alpha_value` on the dimensionless factory, no `ScalarHistoryBounce`); arch §5.2's IR 12 note
  ("`sharded_tables` is unchanged").
- **Already said by a later dated note, left:** arch §8.2's IR note (`:1634`, `alpha_value(store_id, value)`) and §7.2's IR 09/10
  notes on α as the key and one pipeline per α: said by 03's note (`:1485`) and 04's §14 note (`:2281`); arch `:980` (`first_bounce_reflected`
  "an elastic reflection"): IR 12's note at `:1003`; `FINGERPRINT_FORMAT`/`VERSION_LABEL` in JN, IR and RR notes (`:501`–`:502`,
  `:1284`–`:1286`, `:1414`–`:1418`): 03's note gives 7 and 2026.10.0.
- **Records of their day, left:** every hit in another campaign's dated note or section (arch `:247`, `:597`, `:818`, `:968`,
  `:1110`–`:1159`, `:1298`–`:1338`; NS `:202`, `:292`, `:568`–`:576`, `:695`–`:696`, `:948`, §7). NS §6.1–§6.11's hits
  (`:1344`–`:1463`, `:1827`–`:1881`) are §6.12.2's table.
- **True of the present, left:** `elastic` in NS §3.5.3 (`:188`, `:217`, `:262`–`:281`: elastic at α = 0, as IR 12's note there
  says; unchanged by the one-loop law); `2.7` hits are other quantities (NS `:603`, `:959`, `:1008`–`:1017`, `:2112`, arch `:1606`:
  times, densities, exponents) or §6.10's record (`:1782`); `CLAUDE.md`'s hits (`:103`, `:105` `drain_law` test names; `:149`–`:156`
  `alpha_value`, true).

### 3.3 Superseded within the README (ruling 2)

| README | Statement | Superseded by |
|---|---|---|
| §8.2 (and §8.1's "zero at R ≤ 1"; P26) | one-loop α_eff = min(α_cap, A·2.7/ℓ); the `zero`/`edge` gap prescriptions; classical drains zero | §8.10 (the measured law; decisions 2 and 3); §8.16 calls these the plan's history; the code (`drain_fraction`, `:1831`) |
| §8.10 | "L = M/φ_t and ln R are the assessment's" | U15 (b), ruled later in §8.10: L = ln(1 + E/V₀), the model's `_drain_L`, equal to M/φ_t at n = 1 (`ExponentialModel.py:81`) |

## 4. Verification performed

### 4.1 The Gate

Re-checked at 20:38 GMT before anything else (verification §9.1): every item of note 08's Gate holds as the note gives it.

### 4.2 The suites

Serially, once, before any edit, from the repository root with `CLAUDE.md`'s commands; `unittest`'s time; `uptime` at the start
and the end (outputs in the agent's scratchpad):

| suite | count | time | `uptime` load at start → end |
|---|---|---|---|
| CosmologyModels | 26 OK | 66.8 s | 3.51 5.02 4.75 (20:38) → 5.32 5.26 4.86 (20:39) |
| ComputeTargets `test_[a-f]` | 219 OK | 148.1 s | 5.21 5.23 4.85 (20:39) → 4.84 5.40 5.00 (20:42) |
| ComputeTargets `test_[g-z]` (background) | 446 OK | 491.1 s | 5.02 5.42 5.01 (20:42) → 5.52 8.09 7.33 (20:50) |
| ObjectFactories | 163 OK | 125.9 s | 5.08 7.96 7.28 (20:50) → 2.50 5.88 6.54 (20:52) |
| tools (background) | 259 OK | 266.5 s | 2.30 5.78 6.50 (20:52) → 12.90 6.62 6.49 (20:57) |
| RunDrivers | 72 OK | 25.0 s | 15.47 7.36 6.76 (20:57) → 13.89 7.80 6.94 (20:57) |

07's counts exactly: 26, 665 (219 + 446), 163, 259, 72. None failed; nothing was re-run. No `.py` file outside `close-08/` changed
after, so this is the final run.

### 4.3 The inventories, the `--help` check and the twenty numbers

Verification §9.3, §9.4 and §9.5. The inventories (`close-08/run_inventories.sh <scratch> <scratch>/inv-out`, copied into
`close-08/out/` with the scratchpad written `<scratch>`):
- the reference store: exit 0, 25.3 s, 75,121 records in 16 classes, `problems: none`, 2026.10.0, `alpha_value` 6; 18 of 18 hashes
  identical before and after (primary `848435c3a521fb93842aa39b52466116c85da66402f6411b96a924199b2251e8`, manifest
  `1f7f59bb508654af2041d02ac8b0dee88279b4daaec9325431524040d9c8f7ee`);
- the copy of the base store: the copy's 18 hashes equal to the originals' before; exit 1 in 1.6 s, refused ("differs from the
  tables this code declares … so nothing was read"); 18 of 18 hashes identical after (primary
  `64516736f27f111bfdbce3d8886c3e95dd30dac831013e2302dcb7bc74cd1f87`, manifest
  `4d1b3bbbca0c429ba58cfdee5cc0cfb6783845a064b31405782ea5665582679f`). No byte of the reference changed, and the copy did not
  change, so neither the stop nor the issue of prompt §2 item 4 arises.

`check_help.py`: 10 commands exit 0; 30 options, 63 checks, and the git command; 0 failures (after the one fix of §2).

The twenty numbers:


| # | number, as written | where written | source line |
|---|---|---|---|
| 1 | α_eff equal at 616 270 candidates | §8.2 | this document `:1099` (§2d.2) |
| 2 | ℓ_min 1.03–2.05 at A = 0.1 (P, W) | §8.2 | `:1168` (§2d.5) |
| 3 | S held at 37 922 classical candidates | §8.2 | `:1183` (§2d.5) |
| 4 | f₁ = 0.0823–0.115 at ℓ 22.8–32.3 | §8.2 | `:1195` (§2d.6) |
| 5 | P-2197 ΔN_eff today 0.06009 at A = 1 | §8.2 | `:1215` (§2d.7) |
| 6 | one-loop fit within 0.074 of 27 points | §8.1 | `source/crossover/out/fit_law.txt:30` |
| 7 | κ = 3.9657820823215935 | §8.4 | `:1663` (§5.2) |
| 8 | I = 0.09300715800293864 at c_a = 1 | §8.4 | `:1674` (§5.2) |
| 9 | δ_stim median 3.5 × 10⁻¹⁷ | §8.5 | `:1772` (§5.7) |
| 10 | least producing phase slip 5.1 × 10⁻¹⁰ rad (C-3873-A1) | §8.5, §8.6 | `:1782` (§5.7) |
| 11 | 2 524 629 values equal (Bitwise) | §4.20 point 1 | `:1947` (§6.4) |
| 12 | C-3873 at A = 1: 28 integrations, 3324 s | §4.20 point 3 | `measure-02d/out/cost.txt:243` |
| 13 | L-M5e-20 at A = 0.1: 1 962 144 steps, 1115 s | §4.20 point 3 | `:1270` (§2d.10) |
| 14 | `--tuning-max-integrations` default 30 | §4.20 point 3 | `ComputeTargets/ScalarHistory.py:5453` |
| 15 | step budget 2 000 000 accepted steps | §4.20 point 3 | `ComputeTargets/ScalarHistory.py:380` |
| 16 | longest heartbeat silence 160 s | §4.20 point 3 | `:1902` (§6.2) |
| 17 | 44.6 → 39.3 GB free | §4.20 point 3 | `:1899` (§6.2) |
| 18 | reference store 137 352 KiB, `store_id` `2b6fd51c…` | §4.20 point 5 | `prompts/one-loop-drain/logs/06-rehearse.md:236` |
| 19 | 75,121 records, 25.3 s | §4.20 points 1, 5 | `close-08/out/inventory-reference.txt:28`, `:153` |
| 20 | block C, 836.6 s per (M, β) over the eight A (the medians 112.3, 112.4, 112.3, 110.3, 94.2, 110.3, 92.4, 92.4) | §4.20 point 3 | `measure-02d/out/cost.txt:289`–`:296` ("C A=…") |


### 4.4 The `CLAUDE.md` modal grep (ruling 6)

```
$ git diff -U0 d0d8918 -- CLAUDE.md | grep '^+' | grep -n -i -E '\b(must|never|always|do not|don.t|should|shall)\b'
5:+  one-minute load of 2.3 at its start; run it alone.) They **must not
```

One hit: "They **must not" is an unchanged clause carried inside an edited line (the line's load changed from 25.0 to 2.3; the
clause begins the existing sentence "They **must not need a Ray cluster or a persistent datastore**", which continues on the next,
unchanged line). The other added text is times, dates and loads. `CLAUDE.md` gains no rule.

### 4.5 The board and the index agree (§2 item 6)

`.documents/OPEN_ISSUES.md` has 29 rows in its §1 and says 29 open, dated 2026-11-07; §1.23 lists exactly the board's three §3
entries; the board's §3.1 says "None" assigned and §1.23 says none assigned. `prompts/INDEX.md`'s row: "§1.23; 3 open; 29 open in the
whole index"; its header "17 campaigns: 0 planned, 0 live, 17 closed", and its 17 rows are all **complete**.

### 4.6 `git diff --stat d0d8918` before the commit

```
 .documents/OPEN_ISSUES.md                          |  11 +-
 .documents/architecture-summary.md                 |  88 +++++
 .documents/numerical-strategies.md                 |  76 ++++
 .documents/one-loop-drain-verification.md          | 397 +++++++++++++++++++++
 .documents/one-loop-drain/close-08/check_help.py   | 163 +++++++++
 .../one-loop-drain/close-08/out/check_help.txt     |  79 ++++
 .../close-08/out/help-bbn_from_store.txt           |  52 +++
 .../close-08/out/help-history_and_bbn.txt          |  61 ++++
 .../one-loop-drain/close-08/out/help-main.txt      | 172 +++++++++
 .../close-08/out/help-plot_ScalarHistory.txt       | 176 +++++++++
 .../close-08/out/help-plot_by_beta.txt             | 177 +++++++++
 .../close-08/out/help-plot_quanta_grid.txt         |  19 +
 .../close-08/out/help-plot_quanta_history.txt      |  22 ++
 .../close-08/out/help-quanta_measures.txt          |  14 +
 .../close-08/out/help-registered_run.txt           |  20 ++
 .../close-08/out/help-runregistrykit_list.txt      |   9 +
 .../close-08/out/inventory-basecopy.txt            |  74 ++++
 .../close-08/out/inventory-reference.txt           | 174 +++++++++
 .../one-loop-drain/close-08/run_inventories.sh     |  73 ++++
 .documents/review-remediation-verification.md      | 265 ++++++++++++++
 CLAUDE.md                                          |   8 +-
 prompts/INDEX.md                                   |   4 +-
 prompts/one-loop-drain/IMPLEMENTATION_STATE.md     | 103 ++++--
 prompts/one-loop-drain/logs/08-close.md            | 263 ++++++++++++++
 24 files changed, 2456 insertions(+), 44 deletions(-)
```

The files of prompt §4 and nothing else; no Python file but `close-08/check_help.py` (`black --check`: unchanged). Deleted lines:
0 in each of the four documents.

## 5. The deliberate-breakage record

Not applicable: no code or test was written. The checks' sensitivity: `check_help.py` failed its first run on one token it could
not place (§2), so an option §4.20 named and no command listed would be reported; the inventory script compares the hash lists with
`cmp` and prints any difference.

## 6. Observations not acted on

1. **Three statements in code still call a reflection elastic** where at α > 0 it drains: `reflection_count`'s docstring
   (`extract_common.py:248`, log 07 §5 item 1), the first-bounce legend label "first bounce, elastic reflection"
   (`extract_common.py:1199`) and the comment of `reflected` (`ComputeTargets/ScalarHistory.py:3379`). Code, outside 08's files;
   for the orchestrator's housekeeping (ruling 7).
2. **§6.11.2's own row on §6.4** ("a root within the rounding width raises") is itself superseded by 01's
   `_turning_point_unresolvable`; §6.12.2 lists it. §6.11 is left as a record.
3. **The projection's block C** rests on three histories at 10⁻¹² M_P, and on whether C-3873's tuning cost recurs elsewhere: no
   history of 02d ran at 10⁻¹⁵ or 10⁻¹³ M_P. A few histories there before the rebuild would measure it. A proposal for the user, not
   acted on.
4. **A run given only `--A-values` also builds α = 0**, which at laboratory M is natural mode, never run there (README §8.14 P52),
   at an unmeasured cost. Under `--A-values`, A2's grid adds nothing that A does not build. Both are stated in §4.20; what the run
   scripts do is the user's.
5. **`--stale-after`:** no value is proposed. The facts are in §4.20 point 3.
6. **`plot_ScalarHistory.py` in the 2026.8.0 script reads the reflection store** (`$DB_R`), not the main store; §4.20 point 2 says
   "with the same options", which is true of the options, and the store is the script's.

## 7. State handed on, to the user

- **The merge.** `git merge-base --is-ancestor main one-loop-drain` exits 0: `main` (`0209aac`) is an ancestor, so the merge is a
  fast-forward. `reflection-restart` (`25b1242`) is contained (merged at `1561b60`). `github/main` (`7b35dc8`) is four commits behind
  local `main` (`626b04f`, `4249829`, `cc7fe75`, `0209aac`), and an ancestor of this branch; `one-loop-drain` has no upstream.
  Merging and pushing are the user's.
- **The rebuild of record.** `review-remediation-verification.md` §4.20 is the plan's source: the build's changes (point 1), the run
  scripts' changes against 2026.8.0's with U14's A and the post-build commands (point 2), the costs, failures, projection, disk,
  workers' directory, shard warning and heartbeat (point 3), the figures of record (point 4), the stores (point 5), what stays open
  (point 6) and the merge (point 7). The fixed-α blocks' fate, the scripts and the grid are the user's.
- **The stores.** `one-loop-drain-06-base` (format 6, 2026.9.0; the tip refuses it), `-reference` and `-resumed` (format 7,
  2026.10.0), and `inelastic-reflection-11-reference` (format 6, 2026.8.0) stay under `var/datastores/`; retiring any is the
  user's. The scratch copy of the base store made for §4.3 was deleted from the scratchpad after its hashes were taken.
- **The open issues:** the board's three, none assigned; `[02-one-loop-tunings-fail-at-small-A-in-the-transition-window]` bears on
  the rebuild.
