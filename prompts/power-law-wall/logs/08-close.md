# Log 08 — close

**Subject:** Close the `power-law-wall` campaign: the two walls as they stand, for the paper; the reading guides; `CLAUDE.md`'s
descriptions; the handover for a science run of record of both potentials; the issues and the indexes (P135–P148, README
§6.17; orchestrator note 08).
**Commit:** `8f7a77e`, on branch `power-law-wall`, on top of `278c58d`
**Date:** 2026-11-19, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** C1–C6 shipped; no stop condition was met; no code changed. Opened none, closed none.
- `power-law-wall-verification.md`: §12 (seven subsections) and §13; `numerical-strategies.md`: §6.13 (two subsections);
  `architecture-summary.md`: six dated notes (§2, §4.2, §7.2, §8.4a, §9, §14); `review-remediation-verification.md`: §4.22 (eight
  points). **No existing line of the four documents is deleted or reworded** (`git diff 278c58d`: insertions only in each).
- `CLAUDE.md`: the counts, the times and the plot-scripts bullet; no rule added (the modal grep finds nothing).
- Suites, once before the edits: 26, 809 (226 + 583), 175, 312, 79, with the two worktree failures and no other.
- Inventories: a 2026.14.0 store read in place, unchanged; a 2026.12.0 copy read, a 2026.11.0 copy refused at the schema, both
  copies and both originals unchanged. The per-n roots: the unsplit power-law root refused by `plot_quanta_grid`, each n's root
  drawn. Every option of §4.22 in its command's `--help`, 0 failures.
- The index stays at 36 open; the board and `prompts/INDEX.md` close the campaign.

Orchestrator note 08 (its Gate, §0 and rulings 1–14), and the dispatch brief's corrections (the dispatch commit `278c58d`; the
verification document ending with §11.13), governed where they differ from the prompt; each ruling is classified in §2.

## 1. What shipped

- **C1, the two walls for the paper:** `.documents/power-law-wall-verification.md` §12 (`:3018`–`:3306`):
  - §12.1 the two models as coded, side by side (V, A, n, the normalization, the hooks in log space, the floor,
    `reflects_at_origin`, φ ≤ 0, the seed, the type ids, the tables, the option values), each with its `file:line`;
  - §12.2 one k_ex: the definition and both pulse rates in closed form, φ_t, ω_t, ln R, ℓ_t, the turnaround, ℓ and L as the
    code forms them; the check against Erickcek et al. Eqs. (64), (66), (71); the range on each wall and the least margins
    measured; `ONE_LOOP_MAX_ITERATIONS` and the slowest point;
  - §12.3 the laws as coded: the fixed law, the one-loop law and its dispatch, the regimes and what each selects under each
    law (a table), f on the power wall and on the exponential with every constant at the code's precision, S(L), the domain
    (n = 1, 2; v2's ln(V₀/M⁴) ≤ 10 and 07d's 47 candidates outside it), the drain models with the two retired names;
  - §12.4 the spectrum as coded, the three walls side by side (`SPECTRUM_MODELS`, the tables, x_lo, x_hi, x_p, κ_hi, the
    shares), k² below, x_* and the associated piece flagged as unmeasured on the power wall, `G_Y_MIN`, `OMEGA_U_MAX`;
  - §12.5 one table: each constant, its value, where it was measured, the file at full precision, its fit's errors and
    residuals as recorded there;
  - §12.6 what the sample and the rehearsals showed, by section; §12.7 what is not measured and what is not modelled, each
    with its issue where one exists, and against `one-loop-drain-verification.md` §8.6.
- **C2, the reading guides:**
  - `numerical-strategies.md` §6.13 (`:2020`–`:2094`): a history in order on either wall (§6.13.1, steps 0–7, each with its
    code at `278c58d` and the verification section), and the statements of §3–§6.12 this campaign superseded (§6.13.2, a
    table of ten, by statement, each checked against the code);
  - `architecture-summary.md`: dated notes at the end of §2 (the tree gains four files, `:333`), §4.2 (the registry has 24
    entries, `:537`), §7.2 (the drain on either wall, `:1632`), §8.4a (two models and the hooks every model now has,
    `:1998`), §9 (the potentials of a run, `:2237`) and §14 (three gotchas, `:2612`).
- **C3, `CLAUDE.md`, by description:** `ComputeTargets/tests` 783 → 809 and the `test_[g-z]` half 557 → 583; the
  `ComputeTargets` time and the halves' loads from this run, "in a worktree"; the parenthesis dating the counts replaced by one
  sentence (07c2, 2026-11-19, and what 07c and 07c2 added), followed by the `tools/tests` time sentence it held (its clause
  "run it alone" unchanged); the plot-scripts bullet names `--models` and the root `<label>/<model>/` for every output. The
  Redshift bullet already names both drain models (07c); the science-runs bullet holds.
- **C4, the handover:** `.documents/review-remediation-verification.md` §4.22 (`:2957`–`:3263`): what the campaign did, in
  order, each prompt with its commit and gate and the stops the user ruled; the labels; "This supersedes" (seven statements
  of §4.20 and §4.21); points 1–8 as the prompt lists them, with the inventories (point 1), the per-n measurement (point 3),
  the five analyses' changes read on `main` (point 3), the grid block by block and a labelled projection (point 4), the
  stores and the worktree's outputs (point 6) and the merge facts from commands run at the time of writing (point 8).
- **C5, the issues:** none opened, none closed, none assigned. `[06-plot-quanta-grid-refuses-two-n-at-one-cell]` gains a dated
  line with the per-n measurement; the other three gain nothing (§4.22 adds no fact to them).
- **C6, the board and the indexes:** the board closed (the status line short, its run-on moved unchanged into the history as
  its first entry; the opening description's dated note; the `Code:` line's check; row 08; the gate Close; the baseline; the
  issue's line); `prompts/INDEX.md` row **complete** and header 0 live, 19 closed; `.documents/OPEN_ISSUES.md` header clause
  and §1.26 closed, the `[06-…]` hook.
- **Scripts and outputs:** `.documents/power-law-wall/close-08/check_help.py`, `split_by_n.py` (`black`-formatted) and `out/`
  (the three inventories, `split-n1.txt`, `split-n2.txt`, `per_n.txt`, `check_help.txt`, thirteen `help-<name>.txt`).

## 2. Deviations from the prompt

**The orchestrator's rulings** (note 08 §0), each followed:
1. *The worktree, not the main checkout* — STRUCTURALLY REQUIRED. Every edit in the worktree; `main`'s files read by `git show
   main:<path>`; no tracked file of the main checkout opened. In the main checkout's `var/` only the three stores the ruling
   names were read (one in place, two copied).
2. *Additive, in the documents' own form* — STRUCTURALLY REQUIRED. Insertions only in the four documents; corrections as dated
   notes `> **Note, 2026-11-19 (`power-law-wall`, prompt 08): …**`; placement as ruled, §12 and §13 after §11.13 (the brief's
   correction). A statement inside an earlier campaign's dated note or dated section is a record and gets no note: in
   `numerical-strategies.md` every superseded statement is of that kind, so §6.13.2 names them by statement and no one-line note
   was added; in `architecture-summary.md` the two undated present-tense statements this campaign made false (the tree listing,
   the registry table) take the §2 and §4.2 notes, as earlier campaigns' notes did.
3. *The code is what the documents state* — STRUCTURALLY REQUIRED. Every formula and constant of §12 and §6.13 read in the code
   with its `file:line` at `278c58d`. No disagreement beyond wording was found between a log, the verification or the README and
   the code; the README's superseded readings are in §5's table as "superseded within the README".
4. *Symmetry* — STRUCTURALLY REQUIRED. §12's items are side-by-side tables or paired formulas; §6.13 reads one history on either
   wall; §4.22 gives each change for both potentials, and says where one has nothing (the exponential's seed at n ≠ 1; L on the
   power wall; x_* measured on the exponential only).
5. *The inventories* — IMPLEMENTATION CHOICE, ruled. As ruled; the outcomes in §3.3.
6. *The `--help` check* — IMPLEMENTATION CHOICE, ruled. As ruled, on §4.22's final text, re-run after its last edit.
7. *The per-n roots* — IMPLEMENTATION CHOICE, ruled. As ruled; `all` refused, `n1` and `n2` drawn, as expected.
8. *The projection* — IMPLEMENTATION CHOICE, ruled. Labelled; basis, ratios, the number of n and the weakest assumption (C)
   stated; nothing run. See D5 for how the ratios were formed.
9. *`CLAUDE.md`, descriptions only* — STRUCTURALLY REQUIRED. The modal grep finds nothing (§3.4).
10. *The lines addressed to 08* — IMPLEMENTATION CHOICE, ruled. The table is §5; statements in code that only a code change would
    correct are "left: code, outside 08's files" and named in §4.22 point 7.
11. *The issues and the index* — NOTED. As found.
12. *The board and `prompts/INDEX.md`* — NOTED. As found, with D4.
13. *The suites* — IMPLEMENTATION CHOICE, ruled. Once, before the edits, serially; the two worktree failures only (§3.1).
14. *The log* — STRUCTURALLY REQUIRED. This file.

**Deviations of the agent's own:**
- **D1 — a suite launched with no tests** (*UNINTENDED DRIFT*). The first launch of the `test_[a-f]` half (12:53:26) held
  `-p 'test_[a-f]*.py'` in a shell variable, which zsh passed as one word: `unittest` ran 0 tests in 2 s. It was run again at
  once in full (12:56:10, 226 OK); the CosmologyModels run before it was unaffected. No count is taken from the first launch.
- **D2 — the same quoting in the per-n first pass** (*UNINTENDED DRIFT*). At 13:22:15–13:22:25 `--assoc-scale 1` reached
  `quanta_measures` as one argument: exit 2 on usage, nothing written; `plot_quanta_grid` then read 07d's quanta files on `all`
  (refused 3) and none on `n1`, `n2` (refused, nothing written). The pass that replaced it is the measurement; `per_n.txt` says so.
- **D3 — one command contacted the remote** (*UNINTENDED DRIFT*). `git ls-remote --heads github power-law-wall`, run while
  gathering the merge facts, reads the remote (note 08's header: "Network: none"). It is read-only, wrote nothing and printed
  nothing; §4.22's "no upstream" is from `git rev-parse --abbrev-ref power-law-wall@{upstream}` ("no upstream configured").
- **D4 — the board's §5 entry is a bullet** (*IMPLEMENTATION CHOICE*). Ruling 12 says "`### 5.N After prompt 08` in §5's form";
  §5 has no numbered subsections, only one bullet per prompt, so 08's is a bullet in that form. The gate list's intro sentence
  "The last is not yet met." became "All hold.", the old sentence kept beside it in italics.
- **D5 — the projection's ratios are sums I formed** (*IMPLEMENTATION CHOICE*). §11.10 gives ranges, not ratios; the ratios are
  the sums of the 07d-r2 tuning walls in 07d's committed `rehearse-07d/out/07d-07d-costs.txt`, per block's nearest M, written
  out in §4.22 point 4 so that each can be checked. No script was committed for them.
- **D6 — a store read refused before it ran** (*UNINTENDED DRIFT*). A `sqlite3` read of every campaign store's version label,
  meant for §4.22 point 6, was refused by the session's permission check before it ran; it would have opened stores the brief
  does not let 08 open. The labels in point 6 are the records' (log 07 §7, log 07d §7, README §6.16) and the formats the
  sidecars' `fingerprint_format`, read as files.
- **D7 — `check_help.py`'s table** (*IMPLEMENTATION CHOICE*). Beside the prompt's list it runs `tools/surfing_scan.py`,
  `tools/compare_stores.py` and `runregistrykit store create` with `--help` (§4.22 names them), the three read-only git commands of
  point 8 as written, and checks `--potential-type`, which §4.22 point 7 quotes from `REMOVED_OPTIONS`, against that dict.

## 3. Verification performed

### 3.1 The suites

Once, before the edits, serially, in the worktree (verification §13.2):

| suite | count | result | `unittest` | wall | load at start |
|---|---|---|---|---|---|
| CosmologyModels | 26 | OK | 68.0 s | 70 s | 7.50 |
| ComputeTargets `test_[a-f]*` | 226 | OK | 150.4 s | 154 s | 8.49 |
| ComputeTargets `test_[g-z]*` (background) | 583 | OK | 559.7 s | 563 s | 3.89 |
| ObjectFactories | 175 | OK | 155.8 s | 159 s | 55.09 |
| tools (background) | 312 | 1 failure (worktree) | 376.2 s | 378 s | 46.66 |
| RunDrivers | 79 | 1 failure (worktree) | 81.4 s | 85 s | 45.41 |

`uptime` at each start and end is in the scratch `suites/*.txt`; the ends were 6.23, 4.05, 68.74, 48.55, 44.08 and 40.08. The two
failures are `test_roots_and_distributions` and `test_the_script_is_main_py_with_its_hash_and_this_repositorys_head`; no other.
The counts equal 07d's. No `.py` file outside `close-08/` changed, so the run is the final one (§3.8).

### 3.2 The Gate

As verification §13.1; every item held, with the brief's correction (the document ended with §11.13).

### 3.3 The inventories

As verification §13.3: in place on `power-law-wall-07d-r2-exp`, exit 0 in 27 s, `problems: none`, 2026.14.0, the 18 files' hashes
identical before and after (list `4e09e63d75aed0ad0e3f9091899949f9108080632ba9eee0e4a8a0f6174f76e2`); the copy of
`power-law-wall-07-exp`, exit 0 in 26 s, `problems: none`, 2026.12.0, its retired drain-model rows listed (list
`b16661285bbf92071f664e94765c2a33eac284ad7af781402a9ceb566db8d29d`, equal to the original's, before and after); the copy of
`power-law-wall-07-base`, exit 1 in 2 s, refused: "it lacks the table(s) "InversePowerModel"" (list
`09f3baeda61fc60a0248e2bdc93816d6567d2f2a3818db022c02b10b471f5109`, equal to the original's, before and after). The originals
were hashed again at the end, unchanged. No copy changed, so no issue.

### 3.4 `CLAUDE.md`'s modal grep

`git diff -U0 278c58d -- CLAUDE.md | grep '^+' | grep -n -i -E '\b(must|never|always|do not|don.t|should|shall)\b'` prints
nothing (exit 1). The two imperatives near the edited lines, "so run it in two halves" and "run it alone", are on lines the diff
does not touch (unchanged clauses, not in any `+` line).

### 3.5 The `--help` check and the per-n measurement

`check_help.py`: 13 commands, exit 0 each; 30 tokens of §4.22, 0 failures (verification §13.4). The per-n roots: `all` refused
(exit 1, 0 written, 3 refused), `n1` and `n2` drawn (exit 0, 3 written each), the measures equal to the unsplit root's of each n
(verification §13.5).

### 3.6 The twenty numbers

Verification §13.6, each with its source line: S₁ (`InversePowerModel.py:97`, §2.4 `:342`); ln R_×,1 (`:103`, §3.1 `:522`); A_f
(`ExponentialModel.py:74`, README `:884`); A_f and ℓ₀ with errors (§8.4 `:2078`); x_* (`quanta_spectrum.py:188`, §7.7 `:1873`);
k_ex/ω_t at n = 1 (`quanta_spectrum.py:736`, §7.7 `:1847`); 45.0 decades (§10.5 `:2455`); 195 iterations (§9.2 `:2218`); 47
candidates up to 27.31 (§11.11 `:2914`); f_n's residual (§3.1 `:533`); block A 4 h 00 min (run record `:76`); 2292 histories (run
script `:140`); 165.5 s (`07d-07d-costs.txt` `:6`–`:14`, `:17`–`:22`); 301.7 s (`:23`); 54 s (§11.2 `:2631`); the `store_id`
(sidecar `:218`); −0.36% to −6.70% (§11.5 `:2732`); 16 291 536 (§11.4 `:2671`–`:2674`); 75,097 records
(`inventory-07d-r2-exp.txt:32`); 12 h 28 min (run record `:90`). Each equals its source.

### 3.7 The board and the indexes agree

The board's four open issues and §3.1 "None"; `.documents/OPEN_ISSUES.md` 36 open, its clause and §1.26 the same four, the
campaign closed; `prompts/INDEX.md`'s row complete, "§1.26; 4 open …; 36 open in the whole index", its header 0 live, 19 closed.

### 3.8 What moved

`git diff --stat 278c58d`, before the commit, with the new files: `.documents/OPEN_ISSUES.md`, `.documents/architecture-summary.md`,
`.documents/numerical-strategies.md`, `.documents/power-law-wall-verification.md`, `.documents/review-remediation-verification.md`,
`CLAUDE.md`, `prompts/INDEX.md`, `prompts/power-law-wall/IMPLEMENTATION_STATE.md`, and new `.documents/power-law-wall/close-08/`
and this log: exactly the prompt's §4 list. No Python file outside `close-08/`; `black` leaves its two scripts unchanged. The
Ray token is in none of the committed files (`grep -r -F -l -f ~/.ray/auth_token` over them, exit 1).

## 4. The gate Close, item by item (README §2.1)

1. **Documents written:** verification §12 and §13, `numerical-strategies.md` §6.13, six dated notes in `architecture-summary.md`,
   `CLAUDE.md`'s descriptions — holds.
2. **Issues:** none opened or closed; `[06-…]`'s dated line; the index at 36 — holds.
3. **Indexes:** the board closed, `prompts/INDEX.md` and `.documents/OPEN_ISSUES.md` closed and in agreement — holds.
4. **The handover written:** §4.22, its commands checked by `--help`, its merge facts from commands run at the time — holds.
5. **The suites at their counts:** 26, 809, 175, 312, 79 — holds.

## 5. Every line addressed to 08 (prompt §2 item 2; ruling 10)

Searched: logs 01–07d, the README, the board (its reviews and "For 08" paragraphs) and the orchestrator notes 01–07d, for "08",
"close", "handover", "the science run", "made false", "for the documents".

| Where | What it asks of 08 | Done, or left and why |
|---|---|---|
| README §2, row 08 | the documents, `CLAUDE.md`, the handover with the grid's inputs from 04, the issues, the indexes | done: C1–C6 |
| README §2 (after the table, 2026-11-17) | 08 needs 07d's stores in place of 07's | done: the inventory on `07d-r2-exp`; §4.22 point 6 |
| README §4, "08 → the science run" | what a power-law run of record needs, the grid's inputs from 04, the standard analyses and what each needs | done: §4.22 points 2–4 |
| README §6.10 P55 | the 2026.11.0 analyses' readers (06 checked; the export path moved) | done: §4.22 point 3 reads all five on `main` |
| README §6.12 P89 | the standard analyses repeated for both potentials | done: §4.22 point 2 |
| README §6.14 P108 | `architecture-summary.md` and `numerical-strategies.md` are 08's (additive) | done: C2 |
| README §6.15 P124 | 08 reads 07d's stores; retiring 07's is the user's | done: §4.22 point 6 |
| README §6.16 P132, P134 | the documents left to 08; the first attempt's stores read by nothing | done; the two stores are listed and not read |
| README §6.17 P135–P148 | the close's form and parts | done, each (P135 C1–C6; P136 ruling 1; P137 §12; P138 C2; P139 C3; P140, P141 §4.22; P142 §3.3; P143 §3.5; P144 the projection; P145 C5; P146 point 8; P147 C6; P148 §3.1) |
| README §7 | the tree and the measurements from which the user plans the run | done: §12, §4.22 |
| README §0.3's estimates; P4, P11, P16–P17 as amended; `one-loop-measured-pl-v1` (renamed by U11); v1's law and plateau (replaced by U13) | — | superseded within the README; §12 states what replaced each |
| log 03 §7 | "Relativistic quanta" of the exponential moves by verification §3.6 when repeated | superseded within the campaign by 07c's tables; §4.22 point 2 says it moves with v2, the one k_ex and the new tables |
| log 04 §7 | the grid's inputs for the handover: costs, the n = 1 natural-mode failures, M_crit, the surfing edges, the T_max/`n_crossings` difference | done: §4.22 points 4, 5 and 7 |
| log 05 §6 | `outward_step_scan` re-runs exponential rows at n = 1; the hand-written imitation stores of two tests | the first named in §4.22 point 7 (left: code); the second left: tests, outside 08's files, and not for the run |
| log 06 §6 | `changes.py`'s key without `n`; `REMOVED_OPTIONS["--potential-type"]`'s message; `--n-values` shared; `plot_inputs.exponential_models` with no caller; `/tmp/ray` from D4 | `changes.py`: §4.22 point 3; `--n-values`: §4.22 points 1, 3 and §14's note; the message and `exponential_models`: left: code, outside 08's files, named in §4.22 point 7; `/tmp/ray`: named in point 7, the user's |
| log 06 §7 "Open for 08 / the science run" | `plot_quanta_grid` on two n; `changes.py` | done: measured (§3.5), §4.22 point 3 |
| log 07 §7; board "For 08" (07's review) | the stores, the costs, `--n-values`, `plot_quanta_grid`, `compare_stores` and a failure | done: §4.22 points 3–7 |
| log 07c §6 | the exponential's first two hooks raise a raw `OverflowError` | left: code, outside 08's files; §4.22 point 7 |
| log 07c2 §6 | `prompts/INDEX.md` named 07c2 "not dispatched" | done before 08, by the orchestrator (the row says landed); 08's row rewrite keeps it |
| log 07d §6, §7; board "For the user" (07d's review) | the stores 08 reads; the others the user's; v2's domain; the margins; the reader's traceback | done: §4.22 points 1, 4, 6, 7; §12.3, §12.6 |
| board §3 `[06-…]`, "08's handover may assign it" | assign or not | not assigned (ruling 11); measured and named in §4.22 points 3 and 7 |
| orchestrator notes 07 and 07d, "Its §7 gives 08 …", "the costs, for the science run's handover", "08 … written only when the user asks" | — | done; the user asked for 08 |
| orchestrator note 02 (`:268`), notes 05, 06 ("any observation that bears on … the science run") | — | the logs' §6 and §7 carry them; done through those rows |
| `CLAUDE.md` (made false since 06) | the counts, the plot-scripts bullet | done: C3 |
| code comments made false (found by 08) | `ExponentialModel.py:39`–`:40`, `:169`–`:170` ("≤ 3.3"); `ScalarHistory.py:2143` ("define the law's L, or its own fraction"); `drain_model.py:74`–`:76` ("refused wherever a drain model is read … the store tools") | left: code, outside 08's files; the first two in §4.22 point 7, the third in §6 below |

## 6. Observations not acted on

- **`main.py --inventory` lists a 2026.12.0 store's retired drain models.** On the copy of `power-law-wall-07-exp` it exits 0 with
  `problems: none` and prints its `alpha_value` rows as `A=… (one-loop-measured-v1)`; it interprets no drain model, so nothing
  refuses them there. `CosmologyConcepts/drain_model.py:74`–`:76` says the retired names are "refused wherever a drain model is
  read (law_of, and through it … the drivers, the plot scripts and the store tools)". A read of what is stored, not a misreading;
  no issue opened.
- **Code comments that this campaign made false**, outside 08's files: `ExponentialModel`'s docstring and its law's docstring,
  "every history of record has ln(V_0/M^4) <= 3.3" (07d found 27.31 at 10⁻³³ M_P); `integrate_scalar_history`'s "the model must
  define the law's L, or its own fraction" (`ScalarHistory.py:2143`), where since 07c the fraction is required; and
  `REMOVED_OPTIONS["--potential-type"]`'s "there is one model" (log 06 §6).
- **The architecture summary's tree listing and registry table were stale before this campaign** (`ExponentialPotential`,
  `Lambda_value`, "27 entries"); the notes of this date correct only what this campaign changed, as earlier campaigns' notes did.
- **The exponential at n = 2** has a one-loop law (v2's rise at n = 2) but no history of the campaign ran it, and its analytic seed
  is n = 1 only (plain at n = 2); a two-potential run at `--n-values 1 2` builds it (§14's note; §4.22 point 3).
- **`relativistic.py` of 2026.11.0** drops `n` from its quanta join, and `quanta_histories.csv` has no `model` column, so its key
  collides on a power-law root's two n (§4.22 point 3).
- **The load** rose from 3.9 to 68.7 during the `test_[g-z]` half and stayed above 40 to the end of the suites and the
  measurements, from processes outside ChamPBH.

## 7. State handed on (to the user)

- **The merge.** `power-law-wall` is not merged into `main`, and the merge is not a fast-forward: `main` (`9df9e13`) has 15 commits
  since the merge base `a7e0978`, all under `science-runs/`; `git merge-tree --write-tree main power-law-wall` finds no conflict.
  Local `main` is 16 commits ahead of `github/main` (`84a2494`); `power-law-wall` has no upstream. Merging and pushing are the
  user's (§4.22 point 8). After the merge, the `2026.11.0` run record needs a dated note: its one-loop rows are under the retired
  `one-loop-measured-v1`, and the tip cannot read its stores.
- **The science run.** Planned in `science-runs/` by the user from §4.22: both potentials, every standard analysis for each;
  the exponential rebuilt at 2026.14.0, not re-exported; the five 2026.11.0 analyses need the changes of point 3 (the retired
  `ONE_LOOP`, keys without `n` or `model`, the root per potential); `plot_quanta_grid` on a power-law root per n; the projection of
  point 4 (about 23 h 37 min for the power law at both n, a projection) with its weakest assumption, block C.
- **The stores.** In the main checkout's `var/datastores/`: 07d's four (`power-law-wall-07d-r2-*`, 2026.14.0), 07's five
  (`power-law-wall-07-*`, `-base` 2026.11.0 format 7, the rest 2026.12.0 format 8) and the first attempt's two
  (`power-law-wall-07d-exp`, `-pl`, 2026.13.0; `-pl` without a sidecar, so its retirement needs a `store create` first).
  Retiring any of them is the user's. Nothing of 08 is in `var/`; its copies and split roots are in the session scratchpad.
- **The worktree.** `.claude/worktrees/power-law-wall` holds the ignored `var/power-law-wall/` (06 77 328 KiB, 07 669 280 KiB, 07d
  788 KiB, 07d-r2 526 524 KiB); removing the worktree deletes them, and not the stores. Removing it is the user's.
- **Nothing is running**: no Ray was started; the registry has 96 runs, all finished.
