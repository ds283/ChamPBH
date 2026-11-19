# Orchestrator — prompt 07d, the rehearsal again

**Written** 2026-11-18 with the prompt, at the user's request: "Please write 07d (and an orchestration note, if needed)
now." Not dispatched. P111–P124 (README §6.15) stand unless the user rules otherwise; none needs a ruling first. 07d is
dispatched at the user's word.

**Used for 07d's first attempt**, dispatched 2026-11-18 at 19:04 on the Gate as measured again by the orchestrator;
it stopped in run `pl-a` (README §6.16) and was ruled (a) by the user. **Amended 2026-11-19 for the restart** (P133):
the prompt's dated block governs the Gate, the names and the section, and this note's rulings and conventions bind
the restart as they bound the first attempt, with `power-law-wall-07d-r2-*`, `var/runs/power-law-wall-07d-r2-*` and
`$L = var/power-law-wall/07d-r2` in place of the first attempt's names wherever they appear, and the counts of 07c2's
review in §3 check 2. The planner's §0 stays as it was written; its third-hook paragraph is superseded by §6.16. The
Gate below is the first attempt's; the restart's is re-measured by the orchestrator at dispatch. 07c2 landed `bd8d648`,
reviewed 2026-11-19: the restart's suites are 26, 809 (226 + 583), 175, 312, 79.

Read [`../README.md`](../README.md) first: §2.1 (the gate R2), §5, §6.12 (P87–P89), §6.14 (P109) and §6.15 (P111–P124).
Then read the prompt. This note binds 07d only, and governs where it differs from the prompt. The conventions of earlier
notes bind here as well: a tolerance is a number with its reason, timing is taken under load, and the suites run serially.

A note is written for 07d, where 07c had its rulings in its own §5, because 07d is 07 again: real builds under Ray, the
rehearsal lock, an interruption, and a review that re-reads stores. 07 had a note for the same reasons.

**You do not write code.** You may:
- run the suites, the scripts the log names and the tools the prompt names, read-only on the stores;
- run a probe from the session scratchpad, never committing one;
- fix small residue in a follow-up commit of your own (§4).

**The prompt:** [`07d-rehearse-again.md`](../07d-rehearse-again.md) · **Gate:** "R2" · **Closes:** nothing · **Opens:**
only what the work finds · **Model:** Opus · **Network:** none. Nothing is pushed.

**Gate**, as measured by the planner on 2026-11-18 between 15:51 and 16:00 local, before this note was committed:
- **The worktree.** `.claude/worktrees/power-law-wall` was on `power-law-wall` at `2e3c551` (review of 07c), on top of
  `c8461ab` (07c). `git status --porcelain` showed `?? venv`, plus the planning files before their commit.
  `git worktree list`: the main checkout (on `main` at `9df9e13`) and this worktree.
- **The label.** `config/version.py`: `VERSION_LABEL = "2026.13.0"`; `FINGERPRINT_FORMAT` 8.
- **The registry.** `runregistrykit list`: 78 runs, all finished; none `running`, stale or unknown.
- **The cluster.** `tools/ray_cluster.sh status` exits 0: the lock free (its directory does not exist), no Ray process of
  ChamPBH's, ports 6380, 6381, 8267 and 52367 free. `raypoolkit.rehearsal_lock status`: free.
- **07's stores.** `runregistrykit store fingerprint` (no `--write`) on `$S/power-law-wall-07-{exp,pl,pl-resumed,mixed}.db`:
  `2092b20090988edbc7e3d75c5909f30aeb095803b8c2cce16b861b3675f56086` (exp),
  `9d4fcf022a4968fb59f891f701f978c013d14e8cebc0fc4d8f26039c9fd3ff7a` (pl and pl-resumed),
  `7d7a1a4ce1b79cd735e18fa1f630f68529b9a09b4c71c5fe669d8683e99357c7` (mixed): each equal to 07's sidecar in
  `rehearse-07/out/`. 07's exports are under the worktree's `var/power-law-wall/07/`.
- **The paths.** No `$S/power-law-wall-07d-*`, no `var/power-law-wall/07d/`, no `.documents/power-law-wall/rehearse-07d/`.
- **The machine.** Load 28.10 / 59.65 / 45.74 at 15:51, from processes outside ChamPBH (at 12:36 the same day it was
  about 5). `PYTHONPATH` is unset in this session's shell.
- **The suites.** Not re-run by the planner. 07c's review left them at 26, 805 (226 + 579), 175, 312 and 79 by discovery,
  with only the two worktree failures, and nothing has touched code since.

## 0. What the planner found before writing 07d

A survey of the tree at `2e3c551`, of 07's records and stores (read only), and of 07c's.

- **07's build tree is 07c's base.** 07's stores were built at `945bf5d` (their sidecars' `git_head`), and `git diff
  945bf5d c4258ef` outside `prompts/`, `.documents/` and `science-runs/` is empty. So 07c's tool route, whose base was
  `c4258ef`, ran 07's code, and its table (verification §9.7) is what 07d should see in the stores.
- **What 07c's tool route moved**, at 4 × 10⁻³¹ M_P and β = 2 (07c log §7):
  - the power wall at n = 1: ℓ_t by exactly ln 3; ℓ, the birth and reflection margins by up to +1.103; ln k_ex, ln q and
    the tracker margin by up to +1.094; the one-loop peak's ln m by up to 0.013; `ell_min` 21.411 → 22.514;
  - at n = 2: ℓ_t by ln 2; ℓ and the birth margin by up to +0.697; ln k_ex and ln q by +0.691; ln m by 0.008;
  - the exponential under the fixed law (α 0.1): the generations' ln k_ex, ln q and margins by up to +0.21, ln m by
    0.0028, the worst margins by about +0.18;
  - natural mode: nothing;
  - the exponential under the one-loop law at A = 1: ΔN_eff today 0.04462 → 0.04163, 45 → 46 candidates.
  07's grid adds 10⁻³ M_P (natural mode, no candidates), 10⁻³³ and 10⁻¹⁵ M_P (one-loop, A = 1), and A = 0.05 and 10.
- **Where k_ex is stored.** `ScalarHistoryBounce` holds `regime`, `ell`, `L`, `log_R`, `log_E_Mp4`, `log_k_ex_Mp`,
  `log_m_Mp`, `log_q_Mp`, `share`, `weighted` and the margins and their N (`ObjectFactories/ScalarHistory.py:1375–`).
  `ScalarHistory` holds the worst margins, their N, the failing shares and `unassessed_share` (`:475–`), and the drain
  record's `DrainHistoryValues` (`ComputeTargets/ScalarHistory.py:4957`): drained shares and candidates per regime,
  `ell_min`, `log_R_min_drain`, the two counts and the `all_*` margins. `build_extra_data` writes a `margins` dictionary.
  P114's list is the subset k_ex defines: the regime label, ℓ, ln k_ex, the peak's ln m (the one-loop fixed point reads
  k_ex), ln q, the margins and what is counted or summed by regime. `L`, `log_R`, `log_E_Mp4`, `share`, `weighted`,
  `log_R_min_drain` and `parking_share` are not in it: k_ex does not define them.
- **The exports.** `extract_common.CSV_COLUMNS` holds the three failing shares, `unassessed_share` and `drain_model`. A
  one-loop row's export directory is named by its drain model (`<label>/<model>/<drain_model>/A=<A>/`, `CLAUDE.md`), so
  07's one-loop directories carry `one-loop-measured-v1` and `one-loop-measured-pl-v1`; hence P118's map.
- **The banner.** `main.py:180` prints `RUNNING PIPELINE FOR MODEL <label>, <alpha_label(...)>`, and `alpha_label` gives
  `A = 0.05 (one-loop-inverse-power-v1)` for a power-wall A row (`CosmologyConcepts/drain_model.py:142`).
- **The third hook** (`[07c-the-third-hook-leaves-float-range-below-phi-1e-103-mp]`). 07's least power-wall M is
  10⁻³³ M_P, where 07c's estimate puts φ_t near 10⁻⁹⁷ M_P, about 10¹⁷ inside the float range of (ln V)‴. A candidate
  below it would be `unassessed`, its f 0 under the power wall's law, and its history's drain would change: P114 makes
  that a stop, and P120 measures the margin.
- **U12's guard, not probed.** `drain_model.py:57` says a lookup under a new `VERSION_LABEL` finds none of the old label's
  rows. The planner did not run a tip reader on a 07 store; R8 does, last, under a `shasum` guard.
- **07's lessons** (log 07 §2): the export's `failure_reasons` need 07's ruling-(a) mask (D2); exports hold timing
  columns, named by `compare_exports.py` (D3); `store create` writes no fingerprint, so 07 ran `store fingerprint
  --write` on each sidecar (D6); the quanta files sit in the export roots and were excluded where two trees' roots were
  compared (D7); three reader calls were malformed by a shell loop (D9); one throwaway file went to `/tmp` (D15).
- **The cost.** 07's stores took 5–8 minutes each over three runs, and its longest history 93 s. With the suites (about
  25 minutes before, by discovery after), the readers and the comparisons, 07d should take two to three hours of the
  agent's time, the builds under an hour.

**Rulings.** Each is recorded in the log, with its class.
1. **The tree and the main checkout.**
   - The agent works in this worktree, from its root, with `./venv/bin/python` and `PYTHONPATH=.`.
   - In the main checkout, only `var/datastores/power-law-wall-07d-*` and `var/runs/power-law-wall-07d-*` are created, by
     `main.py`, the wrapper and `runregistrykit`. No tracked file there is read or written, and nothing else under its
     `var/`. 07's stores are opened only by `sqlite3` `mode=ro` URIs, by `store fingerprint` without `--write`, and by R8.
   - Never stage `venv` or anything under `var/`. Never name `/tmp/ray-cpbh`. Nothing goes in `/tmp`.

   *STRUCTURALLY REQUIRED* (README §5; P61, P111).
2. **The cluster.** `start` alone in its Bash call, from this worktree's root, with `PYTHONPATH` unset, through
   `tools/ray_cluster.sh`, with its default of 10 CPUs; `stop` the same script's. A stop: the probe's mismatch.
   *IMPLEMENTATION CHOICE*, ruled here (P112).
3. **The manifests and sidecars.** Each run's manifest records a `git_head` of this branch at or after the prompt's commit
   and the hash of that commit's `main.py`; every store's sidecar is format 8, its fingerprint written with `store
   fingerprint --write` after `store create`, as 07's D6. A mismatch is a stop. *STRUCTURALLY REQUIRED.*
4. **P114, exactly.** The k_ex columns are P114's list and no other. The agent does not add a column to it: a column
   outside it that differs is a stop, reported with the column, the histories and the values. The drain-model map is
   applied to the key only; the stored string itself is compared under the map. The failure-reason mask is 07's
   ruling (a), in the stores and in the exports (07's D2). *As README §6.15 says; made exact.*
5. **The interruption.** As the prompt says, with 07's ruling 5 and its D5 (the window's opening from the store's
   timestamps where the beats are coarser). *IMPLEMENTATION CHOICE*, ruled here.
6. **The load.** The machine is shared, and on the planning day the load reached 60. Each build's and reader's wall is
   recorded with the load at its start. The 60-minute bound per run stands. No run is delayed or repeated for load.
   *As the prompt says.*
7. **The two worktree failures** are expected in the suites. Any other failure is a stop. *IMPLEMENTATION CHOICE*, ruled
   here.
8. **A finding about production** is recorded and reported, not fixed. A defect found in a driver or reader is a stop
   (prompt §6). Anything else may be opened as a §3 issue, with `.documents/OPEN_ISSUES.md` in the same commit.
   *IMPLEMENTATION CHOICE*, ruled here.
9. **Files.** Prompt §7's list, and nothing else. The README is not edited. *STRUCTURALLY REQUIRED.*

**Conventions.** All of these bind the agent.
- **Staging and reporting.** Stage by explicit path only, and show `git diff --cached --name-status` before committing.
  Count, then paste. A log states only what the outputs show.
- **The SHA.** The agent writes "this commit", and the orchestrator substitutes it.
- **Order.**
  1. The Gate, and the five suites, before anything else: serially, ComputeTargets in its two halves, the `test_[g-z]`
     half and `tools/tests` in the background, waiting for each notification.
  2. The scripts of §1.5; the run commands beside 07's (§1.2).
  3. The cluster and the probe; `exp`, `pl`, `pl-resumed` (with the interruption), `mixed`, each with `store create` and
     `store fingerprint --write`.
  4. The readers R1–R4 and R7; the cluster stopped; R5 and R6.
  5. The comparisons of §2.1–§2.4, and the measurements of §3.
  6. R8, last, with its `shasum`s.
  7. The counts by discovery; the documents; the records; the commit.
- **Scratch.** Run outputs go under `$L`. Probes and suite outputs go under `<scratch>`, the agent's subdirectory of the
  session scratchpad. Nothing goes in `/tmp` but the cluster's own directory, which only `ray_cluster.sh` names.
- **Ray.** Through `tools/ray_cluster.sh` alone. No `ray stop`, ever. A Ray CLI command takes `RAY_AUTH_MODE=token`. The
  token is never printed.
- **Waiting.** Launch, one check, then one background wait per run for its end line. No polling from the turn.
- **Stop conditions are real:** prompt §6's, whole; ruling 3's mismatch; ruling 4's columns; a suite count that differs.
  A stop goes to the user through the orchestrator, and the agent resumes with `SendMessage`. At a stop the agent leaves
  the cluster up and the stores where they are, and reports what is running, unless the stop is the lock's.
- **Other sessions** may land commits on this branch. The agent does not assume `HEAD` is its own.

## 1. Before you dispatch

1. **The Gate above**, on the commit that adds this note, or a later one.
2. **`runregistrykit list`**, `tools/ray_cluster.sh status`, the rehearsal lock
   (`./venv/bin/python -m raypoolkit.rehearsal_lock status`) and the load average. If another project holds the lock, do
   not dispatch: tell the user.

## 2. Dispatch

Start one fresh-context subagent, on **Opus**, in the worktree, in the background. Give it:
- the prompt, the README and the board;
- `HEAD`, and the Gate as measured;
- §0 of this note, up to "## 1. Before you dispatch", as a copy in the session scratchpad.

The agent does not read `orchestrator/`.

Tell it plainly:
- **One commit, at the end**, with exactly prompt §7's files. Its log is `logs/07d-rehearse-again.md`, in
  `dk-and-rpk-migration` README §5.1's form.
- **§0's rulings bind it**, each recorded with its class.
- **Nothing it starts outlives its turn** but the four stores (five, after a miss) and their sidecars. The cluster is
  stopped, the lock is released, and no pid recorded under `$L` answers `kill -0`.
- **Stop and ask** on any of the conventions' stop conditions. A stop is a reply headed **STOP**, with the work left
  uncommitted.
- **At the end it replies with:**
  - the log's path, the message as committed, and `git show --stat` of its commit;
  - the suites before, and the counts after, with times and `uptime`;
  - the probe's output;
  - the per-run table, and each manifest's `git_head`, `git_dirty` and script hash;
  - the interruption: the window, the delay, when the signal fell, what was stored and pruned;
  - the gate R2 item by item (§2.1–§2.5), with the values compared per table and each k_ex column's count and largest
    change, by store, potential, n and law;
  - P115's table; P119's and P120's results;
  - the readers R1–R8, with exits, walls and `shasum`s;
  - the costs beside 07's;
  - every deviation with its class;
  - "Observations not acted on" and "State handed to the next prompt", verbatim.

## 3. The review — seven checks

1. **Scope.**
   - `git show --stat <agent>` lists only prompt §7's files. Its parent is this note's commit, or a later commit that is
     not the agent's.
   - `git status --porcelain` shows `?? venv` only; `git worktree list` the main checkout and this worktree. Nothing is
     pushed.
   - No line of code, test, config file, `CLAUDE.md`, `.gitignore` or `PRyM/` changes; nothing under `var/` is tracked; no
     PDF or PNG is committed; `rehearse-07/` and the verification document's §1–§9 unchanged.
   - `$MAIN/var` gained only `datastores/power-law-wall-07d-*` and `runs/power-law-wall-07d-*`. 07's four stores still give
     the Gate's fingerprints, by my own `store fingerprint`.
2. **The counts,** by discovery, myself: 26, 805 (226 + 579), 175, 312, 79.
3. **The cluster, the registry and the tree.** `ray_cluster.sh status` and the lock free, no process; `runregistrykit
   list` every run of 07d `done` or `killed`, as the log says; each manifest's `git_head` and script hash against `git
   show <sha>:main.py` by my own `shasum`; the probe output as committed; every sidecar format 8.
4. **No ℓ, no change, independently.** A read-only script of my own (`sqlite3` `mode=ro`) over 07's and 07d's `pl`, and
   over the fixed-law and natural-mode histories of `exp`: the histories matched under the map; every sample of
   `ScalarHistoryValue`, the BBN values and every column outside P114's list by `float.hex`; the k_ex columns' largest
   changes against the agent's; my counts against the agent's.
5. **The tip's identities and the exponential under v2.** `tools/compare_stores.py` on 07d's `pl` and `pl-resumed`, and
   `store fingerprint` on each, by me; P115's table at three histories of my choosing (one at each of A = 0.05, 1 and 10)
   re-read from both stores: ΔN_eff, Y_p, D/H, the drained share, `ell_min`.
6. **The readers and the exports.**
   - One `compare_exports.py` result re-made by `cmp` and `diff` of my own: 07's `export-pl` against 07d's, the one-loop
     directory mapped.
   - `tools/quanta_measures.py` at c_a = 1 re-run by me on 07d's `export-exp` root, byte-identical to the agent's, timing
     fields aside, with every row's `spectrum_model` `R8-exponential-v2`.
   - R4's and R8's logs and the absence of their outputs; R6's lines against the stored values; the `shasum`s unchanged.
   - One of `plot-exp`'s figures looked at myself.
7. **The records.**
   - The log is in §5.1's form, every deviation classed and §0's rulings among them. Its §7 gives 08 the stores, the
     exports, the gate, what moved and the costs.
   - The verification document's §1–§9 are unchanged, and §10 follows prompt §3 with the commands verbatim.
   - The board: row 07d, its items, the gate R2 in §2, an entry in §5, and **Last updated**. `.documents/OPEN_ISSUES.md`
     matches anything opened.
   - The token grep over the commit finds nothing.

## 4. After the review

If it passes, in one commit of the orchestrator's:
- "this commit" → the SHA, wherever it appears;
- the board's row 07d landed, the gate R2 held, and this note marked "used for 07d";
- an *Orchestrator review of prompt 07d* paragraph on the board, holding what each check found;
- `prompts/INDEX.md`'s campaign row.

Residue is fixed in that commit or a follow-up of its own. Anything that is not residue is opened on the board's §3 and in
`.documents/OPEN_ISSUES.md`, in the same commit.

**Report to the user:**
- the agent's commit and the review commit;
- the gate R2: what moved in the histories whose law reads no ℓ, by store, potential and n, and that nothing else did;
- the exponential's one-loop histories under v2 beside v1: ΔN_eff, Y_p, D/H and the drained share;
- the margins, the domain and the third hook;
- the readers, the refusals (U14's and U12's guard) and the quanta tools;
- the costs, for the science run's handover;
- anything opened, and the stores left in `$MAIN/var/datastores`, 07's beside 07d's;
- that 08 (the close and the handover) is written only when the user asks for it.
