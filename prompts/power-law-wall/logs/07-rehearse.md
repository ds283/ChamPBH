# Log 07 — the rehearsal

**Subject:** registered builds under real Ray from two trees: the exponential at the campaign's base `a7e0978` and at the
tip, bitwise; the power law at n = 1, 2 under both laws, straight and interrupted and resumed, by value and against 04's
tool route; both potentials in one store; the readers on every store (P59–P72).
**Commit:** `bbd99f4`, on branch `power-law-wall`, on top of `945bf5d`
**Date:** 2026-11-16, by Claude Opus 5.5 (one agent, in the worktree `.claude/worktrees/power-law-wall`)
**Result:** **COMPLETE.** The gate R holds (§4), with the user's ruling (a) on the stop at §2.2.
- **Sixteen registered runs into five stores** in the main checkout's `var/datastores/`: base (3 runs, 20 exponential
  histories, format 7, from `a7e0978`), exp (3, 20), pl (3, 40), pl-resumed (4: run a killed by `SIGTERM` inside its
  window and resumed with `--prune-unvalidated`, then b and c; 40), mixed (3, 40, both potentials at n = 1). Every run's
  manifest names its tree's head and `main.py`'s hash.
- **Two clusters, one per tree**, each probed: every process imports its own tree's code at its tree's labels, runs in
  its tree's root, and reads data files whose hashes are equal in both trees.
- **(i)** base and exp: 2 984 336 values bitwise equal. **(ii)** pl and pl-resumed: one fingerprint at format 8; 5 538 777
  values equal; the three failure rows' reasons differ only by main.py's run label, the stop (D1). **(iii)** every one of
  the forty, in both stores, equal to 04 bit for bit and at every printed digit, Y_p and D/H at ten digits; exp's three too.
  **(iv)** mixed equal to exp and to pl at n = 1. **(v)** five inventories `problems: none`, every reader as asked, no store
  changed.
- **The stop** (D1): `compare_stores` on pl and pl-resumed exits 1 because a stored `failure_reason` carries the run's
  label. Ruled (a) by the user: the label a run-dependent fragment, compared masked and to the shorter cut;
  `compare_stores.py` unchanged; one issue opened.
- **Issues:** one opened (§6, board §3, `[07-failure-reason-carries-the-run-label]`). No code, test, config or golden
  changed.
- **Suites:** at the base 26, 783 (226 + 557), 175, 312, 79, in full; after, the same by discovery. Only the two worktree
  failures of note 01's ruling 7 failed.

## 1. What shipped

- **`.documents/power-law-wall/rehearse-07/`** (new), each script with a docstring saying what it does, how it is run,
  what it reads, and that it writes nothing but stdout (or, for the recorder and the check, files under
  `var/power-law-wall/07/`); every store read through `sqlite3` `mode=ro` URIs only, never through `datastorekit` or Ray:
  - `code_probe.py` (§1.2): `one-loop-drain` 06's probe with `electroweak-eos` 02's data-file hash and `PRyMrates/`
    (its files sorted, each relative path and SHA-256 digested together); exits 1 on a worker's files, labels or hashes
    differing from the driver's, a module file outside the tree, or a working directory not the tree's root;
  - the helpers copied from `one-loop-drain` 06 and adapted to this prompt's paths: `wait_for_line.py`, `beat_times.py`
    (its `--runs` defaulting to the registry's own runs root), `ray_state.py`, `silences.py`, `check.sh`, `suite.sh`;
  - `histories.py`: 06's reader with the potential, read through `model_type` from its own table;
  - `compare_histories.py A B [--model] [--n]` (§2.1, §2.4): 06's `compare_base.py`, matching by (potential, M, n, β, φ*,
    α, drain model) through each history's `model_type`, comparing every shared column by `float.hex`, `extra_data` key by
    key and `ScalarHistoryBounce` row by row; a NULL reference only to NULL; `failure_reason` under ruling (a) (D1);
  - `compare_fingerprints.py` (§2.1): two sidecars' fingerprints class by class;
  - `compare_04.py STORE [--exp]` (§2.3): each history against 04's output, with the formats named from the source;
  - `compare_exports.py` (§2.2, §2.4, §2.5): file by file; `--base` (06's rule); `--n1-subset`; `--exclude`; timing
    columns and masked failure reasons named.
- **`out/`** (182 files, 2.9 MB, small text only, no PDF or PNG): the runs' logs with ANSI codes removed, the recorders,
  the checks, the manifests and status files of the sixteen runs, the five sidecars, the probes, the clusters' start, stop,
  status and lock records, the Ray states, the delay, the comparisons, each store's `histories.py` output, each export's
  `histories.csv` and each tip root's `quanta_histories.csv`, the readers' logs, `07-readers.txt`, `07-shasums.txt`, the
  loads, the costs, the silences, the stage counts, the runs table, the worktree record, the map of 04's outputs, the
  suites, and the adaptations' diffs (`07-*.diff`).
- **`.documents/power-law-wall-verification.md` §6** (new; §1–§5 unchanged): the gate, the measurement, the commands
  verbatim and "Observed, not acted on".
- **The board** (row 07, the gate R, §3's new issue, the baselines, the status line and its history) and
  **`.documents/OPEN_ISSUES.md`** (the issue's row; 34 open).

## 2. Deviations from the prompt

**The orchestrator's rulings** (note 07 §0), each followed:
1. *The trees and the main checkout* — STRUCTURALLY REQUIRED. The base worktree at `<scratch>/base-a7e0978` with the
   `venv` link alone, nothing under it edited; removed without `--force` (the link unlinked first). In the main checkout
   only `var/datastores/power-law-wall-07-*` and `var/runs/power-law-wall-07-*` were created. Nothing under `var/` or
   `venv` is staged; `/tmp/ray-cpbh` is named in no command.
2. *The clusters* — IMPLEMENTATION CHOICE, ruled. Each `start` alone in its call, from its tree's root, `PYTHONPATH` unset,
   the default 10 CPUs; the base's stopped (status: no process, lock free) before the tip's started; no exit 3 or 4.
3. *The manifests and sidecars* — STRUCTURALLY REQUIRED. Base manifests `a7e0978` and `7b93fae4…` (= `git show
   a7e0978:main.py`), tip manifests `945bf5d` and `27056364…`; base sidecar format 7, the others 8.
4. *§2.3, exactly* — as the prompt says, made exact. The map recorded with each `# command:` line; the tuning by name and
   `float.hex`; the history counts the store holds; every other value through the tool's format; cand fields through
   `to_column` (kind None for every compared field); Y_p and D/H at full precision; counts per line kind, per history and
   in total.
5. *The interruption* — IMPLEMENTATION CHOICE, ruled. Window from `pl-a`, opening + 2 s below 10 s, both loads recorded.
6. *The load* — as the prompt says. Every wall with its load; nothing delayed or repeated for load.
7. *The two worktree failures* — IMPLEMENTATION CHOICE, ruled. Expected; no other failure.
8. *A finding about production* — IMPLEMENTATION CHOICE, ruled. The run label in a failure reason (D1) is recorded and
   opened as a §3 issue; nothing fixed.
9. *Files* — STRUCTURALLY REQUIRED. Prompt §7's list and nothing else; the README is not edited.

**Deviations:**
- **D1 — the stop at §2.2 and the user's ruling (a).** STRUCTURALLY REQUIRED (ruled by the user, 2026-11-16, "(a)
  please"). `tools/compare_stores.py` on pl and pl-resumed printed `RESULT: the stores differ, or a problem was named`
  (exit 1, `problems: none`): `ScalarHistory` 37 matched, 3 and 3 on one side only, `ScalarHistory_tags` the same. The six
  rows are the three n = 1 natural-mode failures at 10⁻³ M_P in each store. Their `failure_reason` embeds main.py's
  history label (job name and submission time), which `compare_stores.py` compares as content; production cuts the reason
  to 256 characters with the label inside, so the longer label of `pl-resumed-b` keeps 8 characters fewer of the reason.
  I stopped with the tip cluster up and the four stores in place. The ruling: the label is a run-dependent fragment for 07;
  §2.2's `compare_stores` item and §2.4's comparisons hold when their only differences are failure rows whose reasons are
  equal once the label is masked and compared to the shorter cut; `compare_stores.py` is not edited, and its raw output
  is recorded; `compare_histories.py` masks the label and names the mask; one §3 issue is opened. All three pairs of
  reasons are equal under the mask (157 of 165 and 157; 159 of 167 and 159; 157 of 165 and 157 masked characters), and
  every other value of the two stores is equal.
- **D2 — the export's `failure_reasons` under the same mask.** STRUCTURALLY REQUIRED by D1's ruling. Each export's
  `histories.csv` carries the same reasons in `failure_reasons`; `compare_exports.py` treats a differing cell of that column
  as equal when it matches with the label masked, to the shorter cut, and names it as a masked label (§2.2's exports, 3
  rows; §2.4's n = 1 subset, 3 cells).
- **D3 — timing columns in the exports.** STRUCTURALLY REQUIRED. §2.4 asks `mixed`'s exponential exports "byte-identical"
  to exp's and §2.5 every base file "equal byte for byte" but `histories.csv`. Each plot run's per-(row, M) `data.csv`
  holds `scalar_compute_time`, `BBN_compute_time` and `NP_compute_time`, which two builds never share (06's base rule
  compared one store exported by two trees). `compare_exports.py` accepts a pair that differs only in those columns, as
  §2.2 allows and §1.6 asks it to name them: mixed/exp 55 byte-identical and 8 `data.csv` timing-only; base/exp 31 and 8;
  mixed/pl's n = 1 subset 50 and 7. No other column differed.
- **D4 — `compare_04.py` compares a failure's reason to the column's width.** IMPLEMENTATION CHOICE. Production stores
  the reason cut to `DEFAULT_STRING_LENGTH` = 256 characters, so the tool's reason, with the stored history's name in place
  of the tool's, is cut the same way and compared character for character; the 39 characters beyond the cut are listed as
  not held. Without it the three failures' masked reasons differ only by that tail.
- **D5 — the window's opening from the store's timestamps.** IMPLEMENTATION CHOICE (`one-loop-drain` 06, ruling 7's
  practice). `pl-a`'s beats hold no line per stored history, so the opening is the first A = 0.05 row's `timestamp` in the
  pl store (+16.303 s); the close is the stage's completion line (+24.693 s). The 8.39 s window gives the opening + 2 s =
  18.303 s, rounded down to `--after 18` as the midpoint rule rounds.
- **D6 — `store fingerprint --write` on every sidecar.** IMPLEMENTATION CHOICE. `store create` writes no fingerprint, and
  `compare_fingerprints.py` reads the sidecars, so each was fingerprinted with `--write` from its own tree after
  `store create`, before its readers (whose shasums include the sidecar). Only the sidecar's fingerprint field changes.
- **D7 — `--exclude 'quanta_*'` in §2.5's base rule.** IMPLEMENTATION CHOICE. The quanta tools run on the tip's roots only
  (R5), after the exports; their 23 files in exp's root are left out by name, and the rule is taken over the plot scripts'
  files.
- **D8 — R4's `--models` refusal run twice.** IMPLEMENTATION CHOICE. With the store's alpha rows it is refused for
  `--A-values 0.05 (one-loop-measured-pl-v1)` (the power law's A rows are looked up under its own drain model, which exp
  does not hold, and that check comes first); with the fixed rows alone it is refused for `--models inverse-power`. Both
  exit 2 and write nothing.
- **D9 — three malformed R1 calls.** UNINTENDED DRIFT. A shell loop of mine passed `--n-values 1 2` and `--models
  exponential inverse-power` as one word (zsh does not split an unquoted variable); main.py refused them at parse (exit 2,
  nothing opened, shasums unchanged). Re-run one command each; `07-readers.txt` records both.
- **D10 — bugs in my own scripts, fixed before their results were recorded.** UNINTENDED DRIFT. `compare_04.py` first
  selected a key table's value as the column `"value"` (SQLite read the quoted unknown name as a string literal for
  `M_value.value_eV`); then its name mask stopped at the parentheses inside main.py's label; and it counted cand rows for
  the plain-tool outputs, which print none. `compare_histories.py` failed on a failure row's NULL `solver_serial`. Each
  failed loudly on its first run; the recorded outputs are the fixed scripts'.
- **D11 — the helpers formatted by `black`.** IMPLEMENTATION CHOICE (`CLAUDE.md`: format the files you change). The six
  copied helpers are new files and were formatted, so their diffs against 06's show black's changes beside the adaptation.
- **D12 — two scratch wrappers, not committed.** IMPLEMENTATION CHOICE. `<scratch>/launch.sh` issued one-loop-drain 06's
  launch command (verification §6.10 gives its expansion) for each run, and `<scratch>/reader.sh` took the shasums, the load,
  the wall and the exit around each reader.
- **D13 — `plot_quanta_grid.py` with `--output`.** IMPLEMENTATION CHOICE. So that pl's export root, compared in §2.2 and
  §2.4, stays as R2 and R5 left it.
- **D14 — `compare_histories.py` drops 06's masses and new-column checks.** IMPLEMENTATION CHOICE. Those were 06's own
  (its tip's new columns); here every shared column is compared, `ScalarHistoryBounce` included.
- **D15 — one file in `/tmp`.** UNINTENDED DRIFT. A throwaway redirect of mine wrote and immediately deleted
  `/tmp/claude-35086-dummy` (empty), against the conventions' "nothing in `/tmp`". Nothing else was written there.

## 3. Verification performed

- **The Gate** at 18:41: `945bf5d`, `fda2df2` and `c7ba5ca` ancestors; `git status` `?? venv` only; two worktrees; lock
  free, no ChamPBH Ray process, the four ports free; 62 runs, all finished; none of `$S/power-law-wall-07-*`, `$L`,
  `rehearse-07/`; `sample-04/runs/` 41 entries, `tool-route/` the three `exp-*.txt`.
- **The suites before**, serially (`out/07-suites.txt`; wall, one-minute load at the start): CosmologyModels 26 OK (66 s;
  2.73); ComputeTargets `test_[a-f]*` 226 OK (149 s; 4.40), `test_[g-z]*` 557 OK (546 s; 6.23); ObjectFactories 175 OK
  (154 s; 4.12); tools 312, 1 failure (315 s; 2.07); RunDrivers 79, 1 failure (26 s; 2.98). The failures:
  `test_roots_and_distributions` and `test_the_script_is_main_py_with_its_hash_and_this_repositorys_head`, note 01's ruling 7.
- **The suites after**, by discovery (P70; 20:40, load 5.88): 26, 226, 557, 175, 312, 79, equal.
- **The probes**, the runs, the interruption, the readers and the costs: verification §6.1–§6.9.
- `git status` shows only prompt §7's files; nothing under `var/` is committed; `git worktree list` shows the main checkout
  and this worktree.
- No ChamPBH Ray process outlives the tip's `stop`; `status` and the lock's `status` say free. `runregistrykit list`: 78
  runs, all finished; this prompt's 16, 15 `done` and `pl-resumed-a` `killed`. No pid under `var/power-law-wall/07/`
  answers `kill -0`.
- `grep -r -F -l -f ~/.ray/auth_token` over every committed file finds nothing (exit 1). `black --check` passes on the
  scripts.

## 4. The gate R, item by item (prompt §2)

- **§2.1 — holds.** `compare_histories.py` base/exp: 20 of 20 matched each way; 2 984 336 values bitwise equal
  (`ScalarHistory` 1 560, `extra_data` 134, `ScalarHistoryValue` 2 290 092, `ScalarHistoryBounce` 27 510, `AdiabaticHistory` 120,
  `AdiabaticHistoryValue` 545 260, `BBNData` 200, `BBNDataValue` 119 060, the key tables 400); no failure. `compare_fingerprints.py`:
  every difference explained from the specs (`version`'s label leaf; `ScalarHistory`, `AdiabaticHistory`, `BBNData` name
  it as a parent at both trees); the twelve other classes equal; `InversePowerModel` the tip's alone, count 0.
- **§2.2 — holds under ruling (a).** `compare_stores.py` exit 1 as it is, its only differences the three failure rows
  (and their tags), whose reasons are equal masked to the shorter cut; `InversePowerModel` and `ScalarHistoryBounce` in its
  table list, `problems: none`. `store fingerprint` on both `9d4fcf02…ff7a`, format 8. `compare_histories.py` 5 538 777
  values equal. `compare_exports.py` on the two exports and their quanta outputs: 104 byte-identical, 15 `data.csv`
  timing-only, `histories.csv` masked reasons only.
- **§2.3 — holds.** `compare_04.py` on pl and pl-resumed: 40 of 40 matched, 11 635 values each (outcome 43, history 185,
  tuning 555 bit for bit, bounce 148, dissipation 816, drain 308, fixed_T 148, cand 9 173, bbn 259), no difference, no
  Y_p or D/H beyond ten digits; on exp's three plain-tool outputs 185 values, no difference. The measurement: exp's export
  equals the 2026.11.0 counterparts in all 61 shared columns, 20 of 20 rows.
- **§2.4 — holds under ruling (a).** mixed/exp `--model exponential`: 20 matched, 2 984 336 values equal. mixed/pl
  `--model inverse-power --n 1`: 20 matched, 2 548 230 values equal, three reasons masked. Exports: mixed's exponential root
  equals exp's (55 identical, 8 timing-only); mixed's inverse-power root equals pl's n = 1 files, `histories.csv` and
  `quanta_histories.csv` equal pl's n = 1 rows in order, pl's other 61 files all n = 2.
- **§2.5 — holds.** R1 `problems: none` on all five; the base's exports equal exp's by 06's rule (`histories.csv`
  identical once `model`, after `n` and `exponential` throughout, is removed; 31 identical, 8 timing-only); R4's three
  refusals exit 2, each naming its option and value and writing nothing (D8); R6's four re-solves equal the stored values
  to ten digits; no reader changed a store's `shasum`.

## 5. The deliberate-breakage record

- **The interruption.** `interrupt_when.py --after 18` sent `SIGTERM` to `pl-resumed-a` at 19:33:44, about 17.7 s after the
  A = 0.05 banner, inside the window [+16.3, +24.7] s: recorded `killed`, exit 143; three A = 0.05 histories stored
  unvalidated; the resume pruned them and recomputed; `pl-resumed`'s fingerprint equals `pl`'s.
- **The refusals.** `--models inverse-power` on exp, `--A-values 0.5` on pl, `main.py --n-values 3 --A-values 1` against a
  path that does not exist: each exit 2, nothing written, no store created.

## 6. Observations not acted on

- **`[07-failure-reason-carries-the-run-label]` (opened, board §3).** A stored `ScalarHistory.failure_reason` embeds main.py's
  history label; `compare_stores.py` has no mask for it, so two builds of one failing history never match; and production's
  256-character cut, taken with the label inside, makes how much of the reason survives depend on the job name's length
  (the tool route's reason 39 characters longer than `pl`'s stored one; `pl-resumed` 8 fewer than `pl`, `mixed` 3 fewer).
- **`histories.csv`'s grid rows** (as `one-loop-drain` 06): the three runs' grids are not a rectangle, so each export holds
  48 rows per potential and n, 28 of them markers, which `quanta_measures` counts as "failed histories" (13 per potential
  and n).
- **`plot_quanta_grid.py` refuses every figure of pl's root** (two n at one cell), exit 1, `figures written 0, refused 3`:
  the existing issue `[06-plot-quanta-grid-refuses-two-n-at-one-cell]`, measured, not a stop.
- **The shard warning** in every plot script's log (`… (=16) does not match specified number of shards (=10)`), though
  given `--shards 16`, as before.
- **R4's order of refusals** (D8): on an exponential store, the power law's A rows are refused before the potential.
- **The `Delta_N_eff` legend** covers A = 10's point at β = 2.9 (n = 1, upper panel).
- **Equal model digests** in mixed: `ExponentialModel` and `InversePowerModel` share the digest `132ac557…` (one spec and
  the same rows, P33's design), so the fingerprint does not tell the two tables apart.
- **The n = 2 natural mode at 10⁻³ M_P is the costliest history**: 4 integrations in 92–93 s against the exponential's 41–42
  s; at laboratory M the power law costs about what the exponential does (1 integration, 4–23 s).

## 7. State handed to the next prompt

- **The stores**, kept, in `<main checkout>/var/datastores/`, each with its sidecar and a fingerprint written with
  `--write`:
  - `power-law-wall-07-base.db`: `a7e0978`, format 7, 20 exponential histories, `2b80c50c…`, 123 096 KiB;
  - `power-law-wall-07-exp.db`: tip, format 8, the base's 20, `2092b200…`, 126 720 KiB;
  - `power-law-wall-07-pl.db`: 40 power-law histories at n = 1, 2 (37 tuned, the three R5 failures), `9d4fcf02…`, 159 408 KiB;
  - `power-law-wall-07-pl-resumed.db`: the same, built interrupted and resumed, `9d4fcf02…`, 159 656 KiB;
  - `power-law-wall-07-mixed.db`: both potentials at n = 1, 40 histories, `7d7a1a4c…`, 164 772 KiB.
  Their run records are `<main checkout>/var/runs/power-law-wall-07-*` (16; `pl-resumed-a` killed, the rest done).
- **The exports and figures**, raw, under this worktree's `var/power-law-wall/07/`: `export-{base,exp,pl,pl-resumed,mixed}/`
  (CSV, with R5's quanta files in the tip's roots), `plot-pl/` (397 PDF and 397 PNG: the power law's first figures under
  Ray), `quanta-grid-pl/` (empty: every figure refused).
- **The gate R holds** (§4), under the user's ruling (a) on the run label in a failure reason.
- **The costs, for the science run's handover** (tuning wall per history, the queueing included; verification §6.9):
  - laboratory M (4 × 10⁻³¹ M_P): the power law 1 integration at both n, 4.2–22.5 s per history (A = 0.05 the longest),
    against the exponential's 5.0–15.2 s; bounce rows 13–339 per one-loop history;
  - 10⁻³ M_P natural mode: exponential 4 integrations, 41–42 s; power law n = 2 4 integrations, 92–93 s; n = 1 fails at G2
    in 13 s;
  - 10⁻³³ and 10⁻¹⁵ M_P one-loop: 5.2–9.7 s, 1 integration;
  - a store of the twenty: exp 5 min 25 s over three runs, pl at two n 7 min 12 s, mixed (two potentials at n = 1) 7 min
    56 s; on disk 123 096–164 772 KiB for 20–40 histories.
- **For the science run:**
  - `--n-values` is one list for every potential of a run, so the power law at n = 1, 2 beside the exponential at n = 1 is
    two runs, as 07 built them (or the exponential at both n), and a plot run over a mixed store at different n is one per
    potential;
  - `plot_quanta_grid` refuses a power-law root with n = 1 and 2 at one (β, M) (`[06-plot-quanta-grid-refuses-two-n-at-one-cell]`);
    `quanta_measures` and `plot_quanta_history` are not affected;
  - `compare_stores` cannot by itself certify two builds that hold failures (`[07-failure-reason-carries-the-run-label]`);
    a long job name keeps less of a failure's reason;
  - the n = 2 natural mode at large M costs about twice the exponential's.
- **Nothing is running**: both clusters stopped, the lock free, the base worktree removed, no pid live.
