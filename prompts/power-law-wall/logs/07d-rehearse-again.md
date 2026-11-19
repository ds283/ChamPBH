# Log 07d — the rehearsal again, at one k_ex

**Subject:** 07's grid rebuilt at the tip (2026.14.0) in four fresh registered stores on the isolated cluster, one build
interrupted on purpose and resumed; every history whose law reads no ℓ compared with 07's, only P114's k_ex columns
moving; the exponential's one-loop histories under `one-loop-exponential-v2` measured against 07's under v1; the readers
on every store, U12's guard on a 07 store last (P111–P124, as P133 amends them for the restart).
**Commit:** this commit, on branch `power-law-wall`, on top of `00fb718`
**Date:** 2026-11-19, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`); the restart of
the first attempt of 2026-11-18 (README §6.16, P133, P134)
**Result:** **COMPLETE.** The gate R2 holds (§4), with the user's ruling (a) on the stop at the suites before (D1).
- **Thirteen registered runs into four stores** in the main checkout's `var/datastores/`, `power-law-wall-07d-r2-*`: exp
  (3 runs, 20 histories), pl (3, 40), pl-resumed (4: run a killed by `SIGTERM` inside its window and resumed with
  `--prune-unvalidated`, then b and c; 40), mixed (3, 40). Every manifest names `00fb718` and the tip's `main.py` hash.
- **The probe:** every process imports this worktree's code at 2026.14.0 and format 8, in its root, reading 07's data
  files.
- **The first attempt's five histories** (power wall, n = 1, β = 2.9, M = 4 × 10⁻³¹ M_P) build in pl, pl-resumed and
  mixed, equal to 07's but the k_ex columns.
- **(i)** P114 on exp (9), pl (40), pl-resumed (40), mixed (29): 16 291 536 values, every one outside P114's list equal by
  `float.hex`; no candidate assessed by 07 unassessed here; the k_ex columns' moves listed. **(ii)** the exponential's
  eleven one-loop histories measured under v2 (ΔN_eff today −0.4% to −6.7%). **(iii)** pl-resumed equal to pl (one
  fingerprint, 5 538 777 values, the run label as 07's ruling (a)); mixed equal to exp and to pl at n = 1. **(iv)** every
  export root against 07's: no difference outside the timing, masked-label, mapped and P114 columns. **(v)** four
  inventories `problems: none`, five refusals exit 2, R5's `R8-<wall>-v2`, R6 equal at ten digits, R8 refuses 07's
  store writing nothing; no store's `shasum` changed.
- **Issues:** none opened or closed. No code, test, config or golden changed.
- **Suites:** before 26, 809 (226 + 583), 175, 312, 79, with the two worktree failures and one timing failure ruled (a)
  (D1); after, by discovery, the same counts (§3).

## 1. What shipped

- **`.documents/power-law-wall/rehearse-07d/`** (new). The first attempt's uncommitted scripts
  (`var/power-law-wall/07d/rehearse-07d-attempt1/`), read as data, were the starting point; each was checked and changed
  as D2 lists. Each has a docstring saying what it does, how it is run, what it reads, and that it writes nothing but
  stdout (or, for the recorder and the check, files under `var/power-law-wall/07d-r2/`); every store is read through
  `sqlite3` `mode=ro` URIs only:
  - the helpers and `histories.py`, copied from `rehearse-07/` with their paths changed: `wait_for_line.py`,
    `beat_times.py`, `ray_state.py`, `silences.py`, `check.sh`, `suite.sh`;
  - `code_probe.py`: 07's, holding every process also to 2026.14.0, format 8 and 07's data-file hashes;
  - `compare_histories.py`: 07's, with `--across-k-ex` (P114: the drain-model map applied to the key and to the stored
    string; P114's columns reported per column and group, not failed; every other column as 07's; the power wall's
    drained-share sum and its candidates' assessment checked) and `--include-exponential-one-loop`;
  - `compare_exports.py`: 07's, with `--across-k-ex` (P118: 07's directories mapped; each differing column classed
    timing, masked label, mapped, P114, measured or DIFFERENCE);
  - `compare_v2.py` (P115), `margins.py` (P119), `domain.py` (P120, the third hook against 07c2's range), new.
- **`out/`** (178 files, 4.0 MB, text only, no PDF or PNG): the thirteen runs' logs (ANSI codes removed), recorders,
  manifests and status files, the interruption's log and the checks; the four sidecars; the probe; the clusters' start,
  stop, status and lock records; the Ray states; the delay; the commands beside 07's; the comparisons; each store's
  `histories.py` output (and pl-resumed's after run a); each export's `histories.csv` and `quanta_histories.csv`; every
  reader's log, `07d-readers.txt`, `07d-shasums.txt`; the loads, costs, silences, stage counts, runs table, the P115
  table, the domain; the suites; the adaptations' diffs against `rehearse-07/` (`07d-*.diff`).
- **`.documents/power-law-wall-verification.md` §11** (new; §1–§10 unchanged).
- **The board:** row 07d, the gate R2 in §2, §5's entry, the status line and its history. `.documents/OPEN_ISSUES.md` is
  unchanged (no issue opened or closed).

## 2. Deviations from the prompt

**The orchestrator's rulings** (note 07d §0, with the restart's names), each followed:
1. *The tree and the main checkout* — STRUCTURALLY REQUIRED. In the main checkout only
   `var/datastores/power-law-wall-07d-r2-*` and `var/runs/power-law-wall-07d-r2-*` were created (by `main.py`, the wrapper
   and `runregistrykit`); no tracked file there read or written. 07's stores were opened only by `sqlite3` `mode=ro`, by
   `store fingerprint` without `--write` (the Gate) and by R8. Nothing under `var/` or `venv` is staged;
   `/tmp/ray-cpbh` named in no command; nothing written to `/tmp` but the scratchpad.
2. *The cluster* — IMPLEMENTATION CHOICE, ruled. `start` alone, from this worktree, `PYTHONPATH` unset, the default 10 CPUs;
   no exit 3 or 4. Started a second time for R8 (D4).
3. *The manifests and sidecars* — STRUCTURALLY REQUIRED. Every manifest `00fb718` and `27056364…fb87` (= `git show
   00fb718:main.py`); every sidecar format 8, its fingerprint written with `store fingerprint --write` after `store create`.
4. *P114, exactly* — as README §6.15 says. P114's list and no other; the map applied to the key and the stored string;
   the failure-reason mask of 07's ruling (a), in the stores and the exports. No column outside the list differed.
5. *The interruption* — IMPLEMENTATION CHOICE, ruled. Window from `r2-pl-a`, its opening from the store's timestamps (07's
   D5), opening + 2 s below 10 s, both loads recorded.
6. *The load* — as the prompt says. Every wall with its load; nothing delayed or repeated for load (loads up to 238.7,
   mostly outside ChamPBH).
7. *The two worktree failures* — IMPLEMENTATION CHOICE, ruled. Expected in RunDrivers and tools; a third failure before was
   a stop (D1).
8. *A finding about production* — IMPLEMENTATION CHOICE, ruled. R8's refusal by a traceback is recorded (§6), not fixed;
   no issue opened.
9. *Files* — STRUCTURALLY REQUIRED. Prompt §7's list as the dated block amends it, and nothing else; the README is not
   edited.

**Deviations:**
- **D1 — the stop at the suites before, ruled (a).** STRUCTURALLY REQUIRED (ruled by the user, 2026-11-19: "This test is
  of no importance anyway, and has nothing to do with the science code. Please just do (a)."). `tools/tests` before ran
  312 tests with 2 failures: the expected `test_roots_and_distributions`, and
  `tools.tests.test_outward_step_scan.TestTheBatch.test_a_task_past_its_limit_is_a_timeout` (`'timeout' != 'done'`: the
  batch's quick task overran its 3 s limit). The machine slept during that suite ("Clamshell Sleep" 07:57:16 to the full
  wake at 09:04:57; the suite's wall 4367 s against `unittest`'s 433.5 s), and ended at a load of 39.8 / 55.7. Alone at
  09:09 the test passed (1 test, 3.458 s), and its module (24 tests, 29.829 s). I stopped with nothing running. The ruling:
  accept it as an artefact of the sleep and the load, do not re-run `tools/tests` before, and record it; if the same test
  fails after with the count unchanged and nothing else failing, the ruling covers it.
- **D2 — the first attempt's scripts, checked and changed.** Each change below is in the scripts as committed, and each
  adaptation of a `rehearse-07/` script is shown by its diff in `out/`.
  - Paths `var/power-law-wall/07d` → `07d-r2` and example store names: STRUCTURALLY REQUIRED (P133's names).
  - `code_probe.py`'s expected label 2026.13.0 → 2026.14.0 (and `compare_histories.py`'s docstring): STRUCTURALLY
    REQUIRED (the dated block).
  - `compare_v2.py` read the reflections as extra_data's `reflections`, a key the store does not hold (it is
    `number_reflections`), with a default of 0, so every history would have shown 0 → 0: corrected before any run.
    UNINTENDED DRIFT (the first attempt's; caught on reading).
  - `domain.py`'s third hook: reported against 07c2's range (V″/V leaving float range; 07c2's measured ends on the power
    wall, and the end at the history's M by bisection on the exponential), in place of |(ln V)‴| against the largest
    float. STRUCTURALLY REQUIRED (the dated block). Its first run overflowed in `exp` on the exponential's branch; the
    term was rewritten with `exp(−ln u)` before any result was recorded; the bisection reproduces 07c2's 1.054769e-154 and
    1.826913e-154 M_P. UNINTENDED DRIFT (mine), fixed before use.
- **D3 — comparisons run between the builds.** IMPLEMENTATION CHOICE. The note's order puts the comparisons after the
  readers. I ran the read-only P114 comparison on exp, pl and pl-resumed, `compare_v2`, `margins`, `domain` and
  `compare_stores`/`compare_histories` on pl and pl-resumed as each store closed, so that a P114 stop (the first attempt's)
  would show before the next build. They read only the closed stores through `mode=ro`; their outputs are the recorded
  ones; nothing in the order of the builds or the readers changed.
- **D4 — the cluster started a second time, for R8.** IMPLEMENTATION CHOICE. The note's order stops the cluster after R7
  and runs R8 last, after the comparisons; the prompt's §1.4 stops it "after the last reader that needs it". R8 is a
  `plot_by_beta.py` run, which calls `ray.init` (line 144) before it opens the store, so it needs a cluster. I stopped the
  cluster after R7 as the order says, then started it again for R8 alone (`start "power-law-wall 07d restart (rehearsal,
  tip): R8"`, gcs pid 93241), and stopped it after (exit 0, lock released, status free).
- **D5 — R8's exit.** IMPLEMENTATION CHOICE (recorded, not judged). The prompt asks that R8 "exit without writing any
  export file, and leave the store's `shasum` unchanged", and makes an export of 07's rows a stop. It exits 1 with a
  `ReadOnlyMiss` traceback naming both labels and "Nothing was written"; no file under its `--output`, the 18 files' shasums
  unchanged, no new file beside them. I take the item as holding on the prompt's terms, and record the form of the refusal
  in §6.
- **D6 — R1's start times not recorded by the wrapper.** UNINTENDED DRIFT. My scratch reader wrapper formatted the start
  time with `date -r`, which the `date` on the shell's path read as a file; the four R1 lines of `07d-readers.txt` have an
  empty time. The epochs it printed give 10:04:51, 10:05:17, 10:05:43, 10:06:10 UTC. Fixed for every later reader; exits,
  walls, loads and shasums were recorded for R1 as for the rest.
- **D7 — R4's `--models` refusal run twice.** IMPLEMENTATION CHOICE (07's D8). With the store's alpha rows it is refused for
  `--A-values 0.05 (one-loop-inverse-power-v1)`; with the fixed rows alone for `--models inverse-power`. Both exit 2.
- **D8 — `plot_quanta_grid.py` with `--output`.** IMPLEMENTATION CHOICE (07's D13), so that pl's root stays as R2 and R5 left
  it.
- **D9 — scratch wrappers, not committed.** IMPLEMENTATION CHOICE (07's D12). `<scratch>/launch.sh` (the launch, the
  recorder, the one check), `reader.sh` (the shasums, exit, wall and load), `commands.py` (the commands beside 07's),
  `runs_table.py` and `costs.py`. Their outputs are committed; verification §11.12 gives the commands they issue.
- **D10 — the helpers formatted by `black`.** IMPLEMENTATION CHOICE. `black` left all eleven Python scripts unchanged.
- **D11 — doubled prefixes in `out/`.** UNINTENDED DRIFT, cosmetic. Files already named `07d-…` under `$L` were copied with
  the `07d-` prefix again (`07d-07d-costs.txt`, `07d-07d-silences.txt`, …); the verification document links them by
  those names.

## 3. Verification performed

- **The Gate** at 07:38 (load 3.46): `HEAD` `00fb718`; `bd8d648` and `00fb718` ancestors; `VERSION_LABEL = "2026.14.0"`,
  `FINGERPRINT_FORMAT = 8`; `git status --porcelain` `?? venv` only; two worktrees (main at `9df9e13`); `ray_cluster.sh
  status` exit 0, the lock free, no process, ports 6380, 6381, 8267, 52367 free; the lock's `status` free; `runregistrykit
  list` 83 runs (75 done, 5 killed, 2 failed, 1 lost), none running or stale; `store fingerprint` without `--write` on 07's
  exp `2092b200…6086`, pl and pl-resumed `9d4fcf02…ff7a`, mixed `7d7a1a4c…57c7`, each format 8, `problems: none`,
  matching its recorded fingerprint; `var/power-law-wall/07/` holds the four exports; none of the restart's stores, runs,
  `$L` or `rehearse-07d/` existed.
- **The suites before** (`out/07d-suites.txt`; `unittest`'s time; the one-minute load at the start): CosmologyModels 26 OK
  (63.2 s; 7.19), ComputeTargets `test_[a-f]*` 226 OK (146.2 s; 6.35), `test_[g-z]*` 583 OK (532.1 s; 3.39),
  ObjectFactories 175 OK (154.8 s; 2.95), tools 312 with 2 failures (433.5 s; 8.72; wall 4367 s with the sleep; D1),
  RunDrivers 79 with 1 failure (24.7 s; 6.45). The failures: `test_roots_and_distributions`,
  `test_the_script_is_main_py_with_its_hash_and_this_repositorys_head`, and D1's.
- **The suites after**, by discovery (P123; 10:25 onward): CosmologyModels 26 OK (63.5 s; 17.81 at 10:24),
  ComputeTargets `test_[a-f]*` 226 OK (152.2 s; 13.45), `test_[g-z]*` 583 OK (545.9 s; 11.79), ObjectFactories 175 OK
  (154.9 s; 5.25), tools 312 with 1 failure (315.3 s; 7.50), RunDrivers 79 with 1 failure (23.8 s; 6.61): the counts of
  before. The failures: the two worktree failures and no other (D1's timing test passed).
- **The probe**, the runs, the interruption, the readers and the costs: verification §11.1–§11.10.
- `git status` shows only prompt §7's files; nothing under `var/` is committed; `git worktree list` shows the main checkout
  and this worktree.
- No ChamPBH Ray process outlives the last `stop`; `status` and the lock's `status` free. `runregistrykit list`: 96 runs,
  all finished; this prompt's 13, 12 `done` and `r2-pl-resumed-a` `killed`. No pid under `var/power-law-wall/07d-r2/`
  answers `kill -0`.
- `grep -r -F -l -f ~/.ray/auth_token` over every committed file finds nothing (exit 1). `black --check` passes on the
  scripts.

## 4. The gate R2, item by item (prompt §2)

- **§2.1 — holds.** `compare_histories.py --across-k-ex`, 07's store as A: exp 9 of 9 matched each way (1 330 907 values),
  pl 40 of 40 (5 540 349), pl-resumed 40 of 40 (5 540 349), mixed 29 of 29 (3 879 931); `DIFFERENCES: none` in each; the
  three n = 1 natural-mode failures in both with equal masked reasons. Under the power wall's law every drained-share sum
  and every candidate's `delta_K_Mp4` bitwise; no candidate 07 assessed is `unassessed` here (1142 in pl, 579 in mixed);
  `regime` unmoved. What moved: the exponential's fixed law, the three worst margins (6 of 9, up to 0.190); the power
  wall's fixed law, the three worst margins (6 of 9; up to 1.104 at n = 1, 0.698 at n = 2); its one-loop law, the worst
  and `all_worst` margins, `ell_min`, the bounce rows' `ell`, `birth_margin`, `log_k_ex_Mp`, `log_q_Mp`, `log_m_Mp`,
  `reflection_margin`, `reflection_margin_N`, `tracker_margin`, and `worst_reflection_margin_N` in 1 of 11 (largest:
  1.267 at n = 1, 0.802 at n = 2; `log_m_Mp` 0.075 and 0.047). Verification §11.4 tabulates them.
- **§2.2 — measured.** The eleven exponential one-loop histories of exp and of mixed matched each way under the map, no
  failure in either; mixed's equal exp's. ΔN_eff today falls in 10 (−0.36% to −6.70%), holds at 10⁻¹⁵ M_P (no one-loop
  candidate); the candidates rise in 8. Verification §11.5.
- **§2.3 — holds under 07's ruling (a).** `compare_stores.py` on pl and pl-resumed: exit 1 as it is, `problems: none`, its
  only differences the three failure rows (and their tags), whose reasons carry the run label;
  `InversePowerModel` and `ScalarHistoryBounce` in its table list. `compare_histories.py` 5 538 777 values equal, the three
  reasons equal masked. `store fingerprint` on both `152f04f7…50be`, format 8. Exports: 104 byte-identical, 16 timing or
  masked-label only. mixed/exp `--model exponential`: 20 matched, 2 986 808 values equal; mixed/pl `--model inverse-power
  --n 1`: 20 matched, 2 548 230 equal; their exports as 07's §2.4 (55 + 8 timing; the n = 1 subset).
- **§2.4 — holds.** `compare_exports.py --across-k-ex` on the five roots against 07's: `DIFFERENCE` 0 in each; where the law
  reads no ℓ only P114's columns, the timing columns, the mapped `drain_model` and the masked `failure_reasons` differ;
  the exponential's v2 files and every quanta file measured (verification §11.7).
- **§2.5 — holds.** R1 `problems: none` on all four; R4's four refusals (five calls) exit 2, each naming its option and
  value, nothing written, no store created, the two `main.py` refusals at parse; R5's `spectrum_model`s `R8-exponential-v2`,
  `R8-power-n1-v2`, `R8-power-n2-v2` by potential and n; R6's four re-solves equal the stored values to ten digits; R8 on
  07's exp writes no export and leaves its `shasum` unchanged (exit 1, D5); no reader changed any store's `shasum`.

**The gate R2 holds**: §2.1 and §2.3–§2.5 hold item by item, and §2.2 is measured.

## 5. The deliberate-breakage record

- **The interruption.** `interrupt_when.py --after 16` sent `SIGTERM` to `r2-pl-resumed-a` at 09:47:10, about +16.2 s after
  the A = 0.05 banner, inside the window [+14.994, +24.182] s: recorded `killed`, exit 143; A = 0.05 n = 1 β = 2, n = 2
  β = 1.2 and n = 2 β = 2 stored unvalidated; the resume pruned them and recomputed; pl-resumed's fingerprint equals pl's.
- **The refusals.** `--models inverse-power` on exp (twice), `--A-values 0.5` on pl, `main.py --n-values 3 --A-values 1` on
  each potential against a path that does not exist, and the tip's reader on 07's exp store: nothing written, no store
  created or changed.

## 6. Observations not acted on

- **R8 refuses by a traceback.** U12's guard on a 2026.12.0 store holds at datastorekit's read-only open, but
  `plot_by_beta.py` does not catch the `ReadOnlyMiss`: the reader exits 1 with a Ray traceback ending `… a lookup of
  "version" matched no row; a read-write store would have inserted one with payload {'label': '2026.14.0'} (the store holds
  the version label(s) ['2026.12.0']). Nothing was written`, rather than 2 with a `!! plot:` line. Nothing is written and
  the message names both labels. It is akin to `[07-a-refused-plot-leaves-ray-errors-unretrieved]` (a refusal that reads
  like a crash). No issue opened; for the orchestrator to judge.
- **The exponential's history at 10⁻³³ M_P lies outside v2's fitted domain**: 47 of its candidates have ln(V₀/M⁴) > 10, up to
  27.31 (P109's premise of ≤ 3.3 does not hold there); every other exponential one-loop candidate of the grid has
  ln(V₀/M⁴) ≤ 3.345. Its ΔN_eff today moves by −5.1% under v2.
- **The third hook against 07c2's range, over the candidates** (P120): the least stored φ_t is 10^−79.05 M_P at n = 1 and
  10^−56.03 at n = 2 on the power wall (10⁻³³ M_P, β = 2, A = 1), 74.92 and 97.71 decades above 07c2's ends; on the
  exponential 10^−35.03, 58.54 decades above the end at that M. 07c2's 45.0 decades over every assessment of 07's grid
  is the smaller: the assessments at the ρ_kin peak are not drain candidates.
- **The power wall's k_ex columns move by more than 07c's tool route showed** at other points of the grid: ℓ by up to
  +1.267 at n = 1 (A = 0.05, β = 1.2), against +1.103 at β = 2; the rate's factor (n + 2)/(ns) gives ln 3 or ln 2 in the
  logarithm only where s → 1.
- **The machine slept** during the suites before (D1). No build, reader or cluster ran while it slept; the user: "Sometimes
  the machine has to move between locations, and when that happens the lid has to be closed. That's just life."
- **The shard warning** in every plot script's log (`… (=16) does not match specified number of shards (=10)`), and the
  `Delta_N_eff` legend covering A = 10's point at β = 2.9, as in 07.
- **`plot_quanta_grid.py` refuses every figure of pl's root** (two n at one cell), exit 1, `figures written 0, refused 3`:
  `[06-plot-quanta-grid-refuses-two-n-at-one-cell]`, measured.

## 7. State handed to the next prompt

- **The stores**, kept, in `<main checkout>/var/datastores/`, each with its sidecar and a fingerprint written with
  `--write`, all format 8 at 2026.14.0, built at `00fb718`:
  - `power-law-wall-07d-r2-exp.db`: 20 exponential histories, `17b288e7…c14c`, 124 712 KiB;
  - `power-law-wall-07d-r2-pl.db`: 40 power-law histories at n = 1, 2 (37 tuned, the three G2 failures), `152f04f7…50be`,
    156 988 KiB;
  - `power-law-wall-07d-r2-pl-resumed.db`: the same, built interrupted and resumed, `152f04f7…50be`, 164 772 KiB;
  - `power-law-wall-07d-r2-mixed.db`: both potentials at n = 1, 40 histories, `477755a5…b8b8`, 164 772 KiB.
  Their run records are `<main checkout>/var/runs/power-law-wall-07d-r2-*` (13; `r2-pl-resumed-a` killed, the rest done).
  07's stores and the first attempt's (`power-law-wall-07d-{exp,pl}*`, five run records) are where they were, untouched;
  retiring them is the user's (P124, P134).
- **The exports and figures**, raw, under this worktree's `var/power-law-wall/07d-r2/`: `export-{exp,pl,pl-resumed,mixed}/`
  (CSV, with R5's quanta files), `plot-exp/` (215 PDF and 215 PNG: the exponential under v2), `quanta-grid-pl/` (empty).
- **The gate R2 holds** (§4).
- **What moved** against 07, where the law reads no ℓ: only P114's columns (§4 §2.1); under the exponential's one-loop law
  everything that follows from the integration (P115's table, verification §11.5).
- **The costs** (verification §11.10): at laboratory M every history tunes in 1 integration (3.5–17.7 s on the
  exponential, the power law within 2.6 s of 07's); natural mode at 10⁻³ M_P: the exponential 4 integrations in 39 s, the
  power law at n = 2 4 in 91–92 s, n = 1 fails at G2; one-loop at 10⁻³³ and 10⁻¹⁵ M_P 1 integration; a store of twenty: exp
  5 min 57 s over three runs, pl at two n 7 min 17 s, mixed 8 min 19 s; bounce rows 999 (exponential, v2) against 917.
- **For the science run:**
  - the exponential's one-loop law under v2 lowers ΔN_eff today by up to 6.7% at laboratory M and 5.1% at 10⁻³³ M_P,
    where every candidate lies outside v2's fitted domain (ln(V₀/M⁴) up to 27.3); Figure 6 and Relativistic quanta move
    with it (P89);
  - the power wall's relativistic margins all rise (by up to +1.13 in the worst margins), and no failing share moved;
  - `plot_quanta_grid` still refuses a power-law root with n = 1 and 2 at one cell; `compare_stores` still cannot certify
    two builds holding failures (`[07-failure-reason-carries-the-run-label]`);
  - a tip reader refuses an older label's store by a traceback (§6).
- **Nothing is running**: the cluster stopped, the lock free, no pid live.
