# Log 10 — close

**Subject:** Close the `jordan-normalization` campaign: the documents, `CLAUDE.md`, the handover, the indexes
**Commit:** `d638738`, on branch `jordan-normalization`, on top of `dbfa31f`
**Date:** 2026-10-26, by Claude Sonnet 5.5
**Result:** **COMPLETE.** C1–C5 shipped; no stop condition was met; no code changed.
- `architecture-summary.md`: 15 dated notes; `numerical-strategies.md`: §3.7 (ten sub-subsections) and ten one-line
  notes; `review-remediation-verification.md`: §4.16 (seven points); `CLAUDE.md`: one parenthesis. **No existing line
  of the three documents is deleted or reworded** (`git diff` shows 0 deleted lines in them).
- Suites before and after: 26 / 76 + 167 / 93 / 115 / 64, all passing. `main.py --inventory` on 09's reference store:
  exit 0, 29 s, all 18 files byte-identical before and after.
- Closed none, opened none; the index stays at 25 open; the board and `prompts/INDEX.md` close the campaign.

Orchestrator note 10 (its Gate, §0 and eleven rulings) governed where it differs from the prompt; each ruling is
classified below.

## What shipped

- **C1 — `architecture-summary.md`.** Notes, each directly after the block it corrects, headed `> **Note, 2026-10-26
  (`jordan-normalization`, prompt 10; …)`: §1 (grids of (M, n, β)); §2 (the tools, `plot_inputs.py`, `histories.csv`);
  §4.3 (parameter tables, sharded list, the history's key); §5.1 (the axis options and defaults, the three tuning
  options, the removed options and `RemovedOption`); §5.3 (`DEFAULT_REDSHIFT_PRECISION` gone,
  `DEFAULT_LOG_ONE_PLUS_Z_PRECISION`); §7.2 twice (`SampleValues`' 18th field; the two layers, the tools, the
  outputs, the policies); §8.2 (`Lambda_value`, `redshift`); §8.3, §8.4 (`Potentials/` and `ConformalCouplings/`
  deleted; the warning); §8.4a (`from_rows` deleted); §9 (options, warning, scope); §11 (tuning failures); §12 twice.
- **C1 — `numerical-strategies.md`.** `### 3.7 Added 2026-10-26 …` after §3.6.2, with `#### 3.7.1`–`3.7.10`: the model and
  the normalization; the two layers; the conditions and residuals; the update (seed, plain step, secant, guards,
  cap) with 06's and README §0.3's measurements; the failures; the progress line; the stored outputs; what was
  measured (06, 09, the φ* dependence); the axis; the Planck mass. Ten one-line notes: §1 intro, §1.1, §3.5.3 (G1),
  §3.5.7 (the region properties; found by my grep, not on the note's list), §3.6.1, §4.1, §6, §7.6.1, §7.6.5, §9.
- **C2 — `CLAUDE.md`.** The parenthesis on the counts now reads "after `jordan-normalization` 10, which changed no
  test", rewrapped. "Units", "Redshift" and the plot-scripts bullets checked against the code (below): true, unchanged.
- **C3 — `review-remediation-verification.md` §4.16**, after §4.15 and before `---`/`## 5`: the first paragraph and
  the "supersedes" list in §4.15's form; then (1) the rebuild unheld; (2) the run scripts by line, the options
  gone and new, `--drop scalar-history`, `plot_ScalarHistory.py`; (3) the cost table per block, the slowest
  histories, the heartbeat; (4) S1 and S5 on the tuned tool, each to a new `--out`; (5) the RayPoolKit wait (P19);
  (6) what the campaign deferred; (7) left to the user.
- **C4.** Board: row 10 ✅ "`d638738`", C1–C5 ✅, status line CLOSED, §3's paragraph (closed with six open), §5.11 and the
  closing paragraph, header's `plot_ScalarModel.py` → `plot_ScalarHistory.py`. `prompts/INDEX.md`: the row complete, header
  "2026-10-26 · 13 campaigns: 0 planned, 0 live, 13 closed". `.documents/OPEN_ISSUES.md`: header clause, §1.15, §1.16.
- **C5.** Suites below.

## Deviations from the prompt

1. **`STRUCTURALLY REQUIRED`: additive, in the documents' form** (ruling 1). "Corrected where it describes the present"
   is done by dated notes; no existing line deleted or reworded in `architecture-summary.md`,
   `numerical-strategies.md` or `review-remediation-verification.md`.
2. **`IMPLEMENTATION CHOICE`: what C1 writes** (ruling 2), each item from the code at `dbfa31f` with `file:line`.
3. **`IMPLEMENTATION CHOICE`: the greps and the audit** (ruling 3): below. The two paper notes and the `peakewsurf-*`
   reports were read and not edited.
4. **`NOTED`: the "made false" lines** (ruling 4): the table below.
5. **`NOTED`: `CLAUDE.md`** (ruling 5): one parenthesis changed.
6. **`IMPLEMENTATION CHOICE`: the cost** (ruling 6): §4.16 point 3. My figures equal the note's in every block
   (about 88, 20, 92, 8, 42–101, 145–193 minutes; 394–502 minutes = 6.6–8.4 h in all, against 103.9 min;
   adiabatic 12.2 min, BBN 42.0 min). Computed by summing the `ALL WORK ITEMS COMPLETE` lines of
   `science-runs/2026-10-03-science-2026.6.0/logs/science/`. Said to be read and not measured.
7. **`IMPLEMENTATION CHOICE`: §4.16's points** (ruling 7). Two readings: (a) ruling 7 counts RayPoolKit's wait among
   "three of the six open issues on this board"; it is `rrk-adoption`'s, so §4.16 point 7 says two of the board's six
   and the RayPoolKit wait; (b) the surfing commands name `<NEW>` and `mkdir -p`, since I did not verify that the tool
   creates directories.
8. **`NOTED`: the board closed** (ruling 8). The status line says "10 awaiting its review", as earlier agents' lines did.
   The orchestrator-notes list is not edited.
9. **`NOTED`: the indexes** (ruling 9). The `ray-sqla-align` row not moved or edited (Observations).
10. **`NOTED`: the suites** (ruling 10): no failure, no re-run needed.
11. **`STRUCTURALLY REQUIRED`: this log** (ruling 11).
12. **`IMPLEMENTATION CHOICE`: one note beyond the note's list.** The grep found §3.5.7's "`ExponentialPotential`'s
    `bounce_region_*` … are still defined and nothing reads them", false since 04 and 07; a one-line note was added.
13. **`UNINTENDED DRIFT`: two "before" records lost their counts.** My first "before" runs of CosmologyModels and of
    the two ComputeTargets halves printed only `OK` (my `tail -4` cut the `Ran N tests` line); I re-ran the two
    ComputeTargets halves with the output in a file (counts below) and did not re-run CosmologyModels before the
    edits. Its count, 26, is from the after-run on identical code (only documents changed). The first
    ComputeTargets runs passed too (1:55.6 and 4:13.0 wall).
14. **`NOTED`: the interruption.** The work was interrupted by an API rate limit after the INDEX and OPEN_ISSUES edits and
    the "after" suites and resumed on the same tree; the coordinator's message said what was applied. On resuming I
    checked each of the edits was applied once (INDEX header and row, OPEN_ISSUES three clauses, board §5.11, §4.16),
    that the three documents have 0 deleted lines, and that `HEAD` was still `dbfa31f`. Not a deviation of the work.

## Verification performed

**Suites**, serially, in the foreground, `CLAUDE.md`'s commands; unittest's time, the one-minute load at the start:

| Suite | Before (HEAD `dbfa31f`, clean) | After (documents edited) |
|---|---|---|
| CosmologyModels | OK, count line lost (deviation 13), 1:21.9 wall, load 5.67 | 26 OK, 78.1 s, load 3.48 |
| ComputeTargets `test_[a-f]*` | 76 OK, 113.4 s, load 1.86 | 76 OK, 115.9 s, load 8.70 |
| ComputeTargets `test_[g-z]*` | 167 OK, 264.9 s | 167 OK, 254.5 s, load 7.89 |
| ObjectFactories | 93 OK, 16.6 s | 93 OK, 16.6 s, load 9.99 |
| tools | 115 OK, 133.8 s, load 9.15 | 115 OK, 136.8 s, load 12.36 |
| RunDrivers | 64 OK, 25.7 s | 64 OK, 26.5 s, load 8.89 |

`uptime`: 21:51 (load 5.67 5.25 5.43) at the start; 22:09 (6.29 8.02 6.83) after the before-runs; 22:33–22:39
(9.37 8.54 6.55, then 2.44 5.79 6.20) after. Both golden histories are in the ComputeTargets runs. `black` has no `.py` file to
check: none changed.

**The Gate** (at `dbfa31f`): `git log -1` `dbfa31f`, tree clean; `runregistrykit list`: 11 runs, all finished (none running,
none stale); no Ray started, `ray stop` never run. 

**`--help`, each exit 0:** `RunDrivers/registered_run.py`, `main.py`, `plot_by_beta.py`, `plot_ScalarHistory.py`,
`tools/surfing_scan.py`, `tools/history_and_bbn.py`, `tools/plot_surfing_histories.py`,
`tools/plot_surfing_temperature.py`, `ray_cluster.sh`, `runregistrykit list`, `store create`, `end`, `store retire`,
`raypoolkit.rehearsal_lock`.

**`main.py --database var/datastores/jordan-normalization-09-reference.db --inventory`:** exit 0, 29 s, no Ray; 75,050
records in 15 classes, `problems: none`, 6 `ExponentialModel`, 7 `ScalarHistory` (7 validated), 7 `BBNData`
(one failure), label 2026.7.0, 75,001 redshift points to 690.78. `shasum -a 256` of the 18 files of the store
(primary, 16 shards, sidecar) before and after: `cmp` identical (e.g. the first line before: `a9a4222d2300b2cc14ba…`;
the lists are `var/jordan-normalization-10/store-{before,after}.sha`).

**`CLAUDE.md` checked against the code.** "Units": `Units/base.py:25,31`, `Planck_units.py:46`, `Mpc_units.py:47`,
`GeV_units.py:38`; PRyMordial's `Mpl = 1./np.sqrt(GN)` (`PRyM/PRyM_init.py:137`): true. "Redshift":
`CosmologyConcepts/redshift.py:74-84`, `ScalarHistory.py:1327`, `log_T_Jordan` a field: true. Plot scripts:
`plot_inputs.parameters` (`:108`), `exponential_models` (`:183`): true. The counts: those of the suites above.

**Statements and sources.** Every statement in the notes, §3.7 and §4.16 is a `file:line` read at `dbfa31f`, a measurement named
with its log or record, or an estimate said to be one.

## The greps (ruling 3)

Commands: for each pattern, `grep -n -F -e PATTERN FILE` over `.documents/architecture-summary.md`,
`.documents/numerical-strategies.md` and `CLAUDE.md`, the working files as committed; classified by
`var/jordan-normalization-10/audit_grep.py` (a hit inside a blockquote note is "a dated record, left"; `d638738`'s
own notes are `P10`; undated text has a hand disposition). The patterns: `Lambda`, `Λ`, `T_stop`, `T-stop`, `potential-type`,
`Potentials/`, `ConformalCouplings`, `ExponentialPotential`, `ExponentialCoupling`, `from_rows`, `z_serial`,
`REDSHIFT_PRECISION`, `logspace`, `exp(N)`, `2.436`, `ScalarModel`, `scale out`, `A(0)`, `Ω(0)`, `2βφ`, `Planck units`.
Format: file | pattern | hits | dispositions with line numbers. `P10` = `d638738`'s own text (the notes, §3.7).
`CLAUDE.md` has no hit for any pattern. **`scale out`, `A(0)`, `Ω(0)`, `2βφ`, `z_serial`: no hit outside this
commit's own text (§3.7.1's description of the old state): the R6 audit finds nothing to correct.** The statement that z is stored
(`exp(N) - 1`, `largest_z`, `DEFAULT_REDSHIFT_PRECISION`) has notes. Extra greps (`largest_z`, `z_sample`, `compute_scalar_model`,
`Lambda_eV`, `fixed Λ`, `1 meV`): every hit in a dated record or `d638738`'s text.

```
architecture-summary.md | Lambda | 22 | P10 6: 293,305,656,657,658,1097; a dated record, left 8: 271,484,720,721,722,724,726,727; already noted at :269 (prompt 07's note on the tree) 1: 87; already noted at :484 (prompt 07's note on the registry) 1: 463; already noted at :720 (prompt 07's note on the options and sharding) 2: 694,713; noted at :1097 (§8.2) 1: 1092; noted at :642 (§5.1) 1: 633; true (LambdaCDM, the cosmology; not the potential's Λ) 2: 106,481
architecture-summary.md | Λ | 14 | P10 7: 12,13,294,554,1159,1400,1419; already noted at :1386 (prompt 07's grid note), and noted at :1399 (§9) 1: 1290; noted at :11 (§1) 1: 9; noted at :1152, :1185 and :1246 (adapter removed by prompt 07) 1: 1147; noted at :278 (§2) 2: 166,203; noted at :550 (§4.3) 1: 545; true (prompt 07's note says there is no Λ grid and no T_stop) 1: 1388
architecture-summary.md | T_stop | 6 | P10 3: 293,298,991; already noted at :914 (integrator-remediation's note on steps 1-4) 1: 909; noted at :278 (§2) 1: 166; true (prompt 07's note says there is no Λ grid and no T_stop) 1: 1388
architecture-summary.md | T-stop | 11 | P10 5: 285,287,293,656,661; a dated record, left 1: 722; noted at :278 (§2) 2: 154,172; noted at :642 (§5.1) 3: 627,680,681
architecture-summary.md | potential-type | 3 | P10 1: 656; noted at :642 (--potential-type refused since prompt 08) 1: 1128; noted at :642 (§5.1) 1: 630
architecture-summary.md | Potentials/ | 6 | P10 2: 1153,1531; a dated record, left 1: 270; already noted at :269 (prompt 07's note on the tree) 1: 95; noted at :1152 (§8.3) 1: 1108; noted at :1530 (§12) 1: 1522
architecture-summary.md | ConformalCouplings | 6 | P10 2: 1186,1531; a dated record, left 1: 270; already noted at :269 (prompt 07's note on the tree) 1: 99; noted at :1185 (§8.4) 1: 1161; noted at :1530 (§12) 1: 1523
architecture-summary.md | ExponentialPotential | 14 | P10 2: 1153,1154; a dated record, left 4: 271,485,486,726; already noted at :269 (prompt 07's note on the tree) 2: 55,97; already noted at :484 (prompt 07's note on the registry) 1: 467; already noted at :720 (prompt 07's note on the options and sharding) 1: 696; noted at :1152 (the classes are deleted) 2: 1124,1148; noted at :1152 (§8.3) 1: 1126; noted at :642 (--potential-type refused since prompt 08) 1: 1128
architecture-summary.md | ExponentialCoupling | 14 | P10 4: 552,1186,1187,1496; a dated record, left 3: 272,485,726; already noted at :269 (prompt 07's note on the tree) 2: 56,101; already noted at :484 (prompt 07's note on the registry) 1: 468; already noted at :720 (prompt 07's note on the options and sharding) 1: 702; noted at :1185 (§8.4) 1: 1177; noted at :1495 (§12) 1: 1482; noted at :550 (§4.3) 1: 503
architecture-summary.md | from_rows | 4 | P10 1: 1246; noted at :1152, :1185 and :1246 (adapter removed by prompt 07) 2: 1146,1181; noted at :1246 (§8.4a) 1: 1244
architecture-summary.md | z_serial | 0 | 
architecture-summary.md | REDSHIFT_PRECISION | 2 | P10 1: 743; noted at :743 (§5.3) 1: 734
architecture-summary.md | logspace | 1 | already noted at :1386 (prompt 07's grid note), and noted at :1399 (§9) 1: 1290
architecture-summary.md | exp(N) | 0 | 
architecture-summary.md | 2.436 | 1 | a dated record, left 1: 1079
architecture-summary.md | ScalarModel | 19 | a dated record, left 18: 163,195,220,666,670,767,917,939,944,947,954,1045,1130,1338,1341,1345,1355,1359; noted at :642 (--potential-type refused since prompt 08) 1: 1128
architecture-summary.md | scale out | 0 | 
architecture-summary.md | A(0) | 0 | 
architecture-summary.md | Ω(0) | 0 | 
architecture-summary.md | 2βφ | 0 | 
architecture-summary.md | Planck units | 3 | a dated record, left 1: 972; true (quantities are stored in Planck units; the R6 audit finds no statement that Planck units scale M_P out) 2: 1054,1087
numerical-strategies.md | Lambda | 2 | P10 1: 534; true (LambdaCDM_GenericEOS, the cosmology class) 1: 957
numerical-strategies.md | Λ | 21 | P10 18: 18,19,297,298,420,523,526,529,533,641,667,675,676,743,747,750,801,1081; noted at :1077 (§6) 1: 1065; noted at :17 (§1) 1: 13; noted at :293 (§3.5.3) 1: 286
numerical-strategies.md | T_stop | 9 | P10 3: 599,604,1691; already noted at :197 (§3.5's 'What this supersedes') 1: 150; noted at :1690 (§9) 3: 1674,1685,1687; true (integrate_scalar_history's own parameter log_T_stop, ComputeTargets/ScalarHistory.py:567) 2: 211,229
numerical-strategies.md | T-stop | 10 | P10 7: 530,605,607,1292,1298,1410,1413; noted at :1292 (§7.6.1) 1: 1287; noted at :1410 (§7.6.5) 2: 1405,1407
numerical-strategies.md | potential-type | 0 | 
numerical-strategies.md | Potentials/ | 0 | 
numerical-strategies.md | ConformalCouplings | 0 | 
numerical-strategies.md | ExponentialPotential | 4 | P10 2: 388,389; noted at :293 (§3.5.3) 1: 284; noted at :386 (§3.5.7) 1: 381
numerical-strategies.md | ExponentialCoupling | 0 | 
numerical-strategies.md | from_rows | 0 | 
numerical-strategies.md | z_serial | 0 | 
numerical-strategies.md | REDSHIFT_PRECISION | 0 | 
numerical-strategies.md | logspace | 0 | 
numerical-strategies.md | exp(N) | 3 | P10 2: 40,524; noted at :37 (§1.1) 1: 33
numerical-strategies.md | 2.436 | 2 | P10 2: 535,792
numerical-strategies.md | ScalarModel | 14 | a dated record, left 14: 121,122,123,213,345,393,399,412,413,414,499,871,1320,1639
numerical-strategies.md | scale out | 0 | 
numerical-strategies.md | A(0) | 1 | P10 1: 544
numerical-strategies.md | Ω(0) | 0 | 
numerical-strategies.md | 2βφ | 0 | 
numerical-strategies.md | Planck units | 1 | P10 1: 726
CLAUDE.md | Lambda | 0 | 
CLAUDE.md | Λ | 0 | 
CLAUDE.md | T_stop | 0 | 
CLAUDE.md | T-stop | 0 | 
CLAUDE.md | potential-type | 0 | 
CLAUDE.md | Potentials/ | 0 | 
CLAUDE.md | ConformalCouplings | 0 | 
CLAUDE.md | ExponentialPotential | 0 | 
CLAUDE.md | ExponentialCoupling | 0 | 
CLAUDE.md | from_rows | 0 | 
CLAUDE.md | z_serial | 0 | 
CLAUDE.md | REDSHIFT_PRECISION | 0 | 
CLAUDE.md | logspace | 0 | 
CLAUDE.md | exp(N) | 0 | 
CLAUDE.md | 2.436 | 0 | 
CLAUDE.md | ScalarModel | 0 | 
CLAUDE.md | scale out | 0 | 
CLAUDE.md | A(0) | 0 | 
CLAUDE.md | Ω(0) | 0 | 
CLAUDE.md | 2βφ | 0 | 
CLAUDE.md | Planck units | 0 |
```

## The "made false" lines (ruling 4)

| Log | The line | Fixed by | Why |
|---|---|---|---|
| 01 | the dated notes of `architecture-summary.md` describe the potentials | left | invariant 6; the notes at §8.3, §5.1 say what changed |
| 02 | `run_readers.sh` names `plot_ScalarModel.py`; the dated outputs say `ScalarModel` | left | records |
| 02 | `[02-the-scalar-history-parent-is-still-called-model]` | 07 narrowed it | open on the board |
| 03 | `architecture-summary.md` `DEFAULT_REDSHIFT_PRECISION` | `d638738` (§5.3 note) | C1 |
| 03 | `ScalarHistory._create_functions` | noted (num.-strat. §4.1) | open issue |
| 03 | `--samples-log10-z` help "per log10(z)" | left | a help string in code outside 10's files, imprecise before 03 too (the grid was a `logspace` in 1 + z): not a stop; noted in §5.1's note |
| 03 | `extract_common.py:421` | fixed as housekeeping (`807f175`, 2026-10-22) | the review of 10 corrected this row, which said "left" |
| 04 | `architecture-summary.md` §7.2 `ODERHS` "passed to `solve_ivp`" | already noted at the integrator-remediation note (§7.2) | not made false by 04 |
| 04 | `integrator-audit-2026-09-30/harness.py` reads deleted properties | left | dated record |
| 04 | `default_abs_tol`/`default_rel_tol`, the diagnostic's "potential" label | left | code, outside 10 |
| 05 | stored `2.436e24 eV` literals in two test fixtures | left | tests, outside 10 |
| 06 | the commented-out `print` names `compute_scalar_history` | left | a code comment |
| 07 | `test_golden_history.py` docstring | fixed by 07's review (`f8fced8`) | — |
| 07 | `config/sharding.py:51` "mostly independent of (M, Lambda)" | left | a statement of physics (07's reason) |
| 07 | `bbn_from_store` `_FIND_QUERY` has no `model_type` check | left | code, one model type |
| 08 | the 2026.6.0 run script is not refused | §4.16 point 2 | handover |
| 08 | plot scripts accept `--tuning-max-integrations` | true | `CLAUDE.md` says so |
| 08 | surfing CSV's `accepted_steps` is now to T_CMB | §4.16 point 4 context | measurement note |
| 08 | "The prose": README, `architecture-summary.md`, `numerical-strategies.md` are 10's | `d638738` | C1 |
| 09 | none made false | — | observations are in §4.16 and §3.7.6 |

None needs a code change; the prompt's second stop does not arise.

## The paper notes (ruling 3)

Read and **not edited**. What this campaign makes false, by line (as read on 2026-10-26):

`.documents/numerical-methods-for-paper.md`
- `:29` names `ComputeTargets/ScalarModel.py`: it is `ComputeTargets/ScalarHistory.py` (02).
- `:167-184` (§3): "A stored sample's `z` is the Einstein-frame redshift": the stored label is ln(1 + z_E) (`z` is derived,
  `inf` past 709.78 e-folds), and the Jordan-frame ln(1 + z_J) is now stored on every sample (`log_one_plus_z_Jordan`);
  `:170`'s "`main.py`, where the z grid is built": `log_one_plus_z_grid`, `main.py:998`, default 300 decades.
- `:283-285`: "a history must reach 20 eV": every stored history ends at T_CMB, so the pre-check always passes; `--T-stop-GeV` is gone.
- `:465`, `:478-485`: initial field "hard-coded" (superseded by its own addendum); `plot_ScalarModel.py` is `plot_ScalarHistory.py`;
  the warning is on models at the seed's φ_ref (`pipeline_selection.py:341`), not on couplings at φ_ref = 0; "none for `recliner.yaml`",
  `starobinsky.yaml`: both files deleted (01).
- every roster value and baseline in §4, §4.1, §4.2 (e.g. `:307-311`) was measured with Λ = 1 meV, φ_ref = 0 and the old electronvolt; a record of that tree.

`.documents/paper-corrections-numerical-section.md`
- `:114-117`: "the other four potentials do not declare …" and `[00-declare-reflects-at-origin-for-the-other-potentials]`: the four potentials are deleted (01), the issue closed as moot.
- `:178-180`: "Couplings … are warned about", "93 of the 125 values … from β = 6.526" (φ_ref = 0): models, at the seed's φ_ref; at M = 10⁻³ the threshold is β > 6.506 at φ* = 5 (08); `starobinsky.yaml`/`recliner.yaml` gone.
- `:36` (row 8) "the potential must declare that it reflects at the origin": the model does (`AbstractModel.reflects_at_origin`, 04); true in substance.
- `:6`, `:11-12`: names and labels of their day (`ScalarModel`, `VERSION_LABEL 2026.5.0`): now `ScalarHistory`, `2026.7.0`.

## The deliberate-breakage record

None: no code changed.

## Observations not acted on

1. **`prompts/INDEX.md`'s `ray-sqla-align` row** sits after the closing paragraph, outside the table, and says "not yet merged or pushed", though `main` is at its review commit `396526b` and `jordan-normalization` contains it. Another campaign's row: not moved or edited; for the orchestrator to offer as housekeeping.
2. **`--samples-log10-z`'s help** says "per log10(z)" (`config/argument_parser.py`, `main.py --help`); left (code).
3. **`config/sharding.py:51`'s comment** still names (M, Lambda); left.
4. **The cost estimate** takes a tuned integration to cost a 2026.6.0 history; the 2026.6.0 run's machine and cores are unrecorded in its logs.
5. **`exponential.yaml`** still carries `Yp-axis-limits`, `D-axis-limits`, `Li7-axis-limits` that the shared parser does not list in the sections I read; not examined.
6. **Ruling 7's "three of the six"** counted RayPoolKit's wait, which is `rrk-adoption`'s (deviation 7).

## State handed to the next prompt

The campaign is closed; there is no next prompt. For the user:
- **Tree:** `d638738`, on `jordan-normalization`, on top of `dbfa31f`; nothing pushed; `main` not merged (P15, the user's).
- **The rebuild** is unheld and described in `.documents/review-remediation-verification.md` §4.16: the run scripts' changes by line, the cost (about 6.6–8.4 hours of scalar stage, estimated), the heartbeat (no `--stale-after`), the surfing scans S1 (about 55 min) and S5 (about 35 min) to new `--out`s, the RayPoolKit wait.
- **Stores and runs:** `var/datastores/jordan-normalization-09-reference.db` (16 shards, sidecar), unchanged by `--inventory`; the resumed store retired by the user (sidecar a tombstone); the eleven runs under `var/runs/`, all finished.
- **Open issues:** six on this board (`.documents/OPEN_ISSUES.md` §1.16); 25 open in the index. For the user to rule: `[08-a-history-that-did-not-converge-is-never-retried]`, `[09-ray-deduplicates-the-tune-lines-of-parallel-histories]`, and RayPoolKit's wait.
- **The paper notes:** their false statements are listed above for the authors.
- **Counts:** CosmologyModels 26, ComputeTargets 243 (76 + 167), ObjectFactories 93, tools 115, RunDrivers 64.
