# Log 06 — the rehearsal

**Subject:** Rehearse the one-loop law under Ray: the gates Bitwise and R hold
**Commit:** this commit, on branch `one-loop-drain`, on top of `41d18c4`
**Date:** 2026-11-07, by Claude Opus 5.5
**Result:** **COMPLETE.** The gates Bitwise and R hold (`.documents/one-loop-drain-verification.md` §6.4, §6.5).
- **The probe** (§6.1): from the base worktree every process, driver, task and actor, imported the worktree's code at
  2026.9.0 and format 6; from the main checkout, before and after the base build, the main checkout's at 2026.10.0 and format 7.
  Workers run in the cluster's working directory, the main checkout, whatever the driver's tree (ruling 1: not a stop).
- **The builds** (§6.2): ten registered runs, nine `done` and `resumed-a` `killed` (143); base (17 histories) from a worktree of
  `1561b60`, its manifests at `1561b60` with the base `main.py`'s hash, its store fingerprint format 6; reference and resumed (26
  each: the same 17 and nine of 02d's one-loop tasks) from the main checkout at `41d18c4`, format 7.
- **Bitwise** (§6.4): the base's 17 histories matched once each in the reference store under `fixed`; **2 524 629 values, every
  one equal**; the drain record NULL, no bounce row and `drain_model` = `fixed` at all 17; the sample masses filled at all 101 335
  samples, each equal bit for bit to `sample_masses` recomputed from the stored state; no exception (no history failed at either
  commit).
- **R** (§6.5): `compare_stores` `RESULT: equal` with `ScalarHistoryBounce` 2 562 rows matched; both stores fingerprint to
  `7c7641d2…dd04`, format 7; against 02d's tool route, on both stores, the 15 tuning keys, the 13 drain values, 14 135 candidate
  values (by `candidate_index`, through `to_column`) and the 24 dissipation values per history bitwise, and Y_p and D/H **exactly
  equal** in all nine; the exports byte-identical but for timing columns, the nine bounces.csv byte-identical to 05's, and the
  nine quanta_histories rows equal to 05's at all three scales.
- **The interruption** (§6.3): `--after 41` (the window 11.28–71.05 s after the A = 0.1 banner); the signal fell at +40.8 s with
  A = 0.1 β = 1.06, 2 and 1.185 stored and validated and β = 10 not; the resume computed β = 10 alone; one attempt.
- **The readers** (§6.6): R1–R7 as the prompt and ruling 4 give them, the store's `shasum` unchanged by each; R4 exits 2 and
  writes nothing; R7's three re-solves print the stored Y_p and D/H to ten digits, the format-6 store included.
- **Suites:** before, 26, 665 (219 + 446), 163, 191, 72, all passing; after, the same.
- **Issues:** none opened or closed. Five observations recorded (§6), none a defect of 04's or 05's work.

Orchestrator note 06 (§0, rulings 1–9 and its conventions) governed where it differs from the prompt. Each ruling is classified
in §2. No code, test, `CLAUDE.md`, `.gitignore`, `PRyM/` or package changed; `ray stop` was never run.

## 1. What shipped

- **`.documents/one-loop-drain/rehearse-06/`** (new), each script with a docstring saying what it does, how it is run, what it
  reads and that it writes only stdout (or, for the recorder, `check.sh` and `suite.sh`, their files under
  `var/one-loop-drain/06/` and the scratchpad); every store read through `sqlite3` `mode=ro` URIs:
  - `code_probe.py` (§1.2; ruling 1): from the driver, a task and an actor, the two module files, the two labels, `sys.path[:5]`,
    the working directory and `PYTHONPATH`, and the job's `py_driver_sys_path`; exit 1 on a mismatch.
  - `histories.py`: one line per history (key, tuning, integrations, compute time, tuning wall, ΔN_eff, Y_p and D/H; for a one-loop
    history the drain record and the bounce rows by source and regime), adapted from `inelastic-reflection` 11's.
  - `compare_base.py BASE_STEM TIP_STEM` (§2.1; P53), adapted from `compare_jn09.py`; the masses' check through
    `sample_masses`.
  - `compare_02d.py STORE_STEM` (§2.2 (ii); rulings 2, 3), adapted from 11's `compare_tool.py` and `run_05.py`'s comparison.
  - `compare_exports.py ROOT_A ROOT_B [--subset] [--names …] [--key-rows FILE]` (§2.2 (iii)).
  - The helpers copied from `inelastic-reflection` 11: `wait_for_line.py`, `beat_times.py`, `ray_state.py`, `silences.py` (a
    docstring paragraph added), `check.sh` (paths), `suite.sh` (its output directory from `$SUITES`).
  - `out/`: 100 small text files, the six helpers' diffs (`06-*.diff`) among them; no PDF or PNG. The four CSV copies
    (histories.csv, quanta_histories at three scales) are written with CRLF line endings and committed with LF (`core.autocrlf
    input`), as at `inelastic-reflection` 11; the comparisons were made on the files as written.
- **`.documents/one-loop-drain-verification.md` §6** (new; §1–§5 unchanged).
- **This log; the board** (row 06, its item, the gates Bitwise and R in §2, an entry in §5, **Last updated**).
- **Kept, not committed** (P57): the stores `var/datastores/one-loop-drain-06-{base,reference,resumed}` (16 shards and a primary
  each) with their sidecars; the run records under `var/runs/one-loop-drain-06-*`; `var/one-loop-drain/06/` (the launch logs, pid
  files, recorders, the readers' logs, `plot-reference/` and `plot-resumed/` with the exports, the tool's outputs and the figures).

## 2. Deviations from the prompt

The note's rulings, each as followed:
1. **The probe and the cluster's environment** (ruling 1). *IMPLEMENTATION CHOICE*, ruled. The cluster started from the main
   checkout with `PYTHONPATH` unset, alone in its call. The probe ran as `PYTHONPATH=. ./venv/bin/python -u <abs>/code_probe.py`
   from each tree's root and printed, beyond §1.2's list, each process's working directory and `PYTHONPATH` and the job's
   `py_driver_sys_path`. The workers' working directory was the main checkout for both trees: not a stop, recorded. No module
   file lay outside its driver's tree, and every label was its tree's. The base manifests record `git_head` `1561b60` and
   `92117838…acc0` (= `git show 1561b60:main.py | shasum -a 256`); the tip manifests `41d18c4` and `2b8956ff…c0e9`. The base
   store's `store create` and `store fingerprint` ran from the worktree with `PYTHONPATH=.`: format 6. The sidecar itself
   records no format; its history records `git_head` `1561b60`, and the fingerprint taken by the base's code at that time is format 6.
2. **The candidates, exactly** (ruling 2). *STRUCTURALLY REQUIRED*; followed. Candidate rows matched by `candidate_index` (indices
   0, 1, … and counts equal to both lists in every history); the eight `DrainCandidate` fields and the injection's three compared
   against `to_column(value, kind, Planck_units())`; all 14 135 also equal the raw values (reported). The 1 277 `plateau` rows
   counted, not compared; no `wall` row occurred. The tuning keys compared through `_tuning_unit`, the columns' own conversion.
   Not compared in (ii), as the ruling names them: `K_before`, `log_k_ex`, `log_m`, the generation's fields, `log_T_Jordan`,
   `log_rho_kin`, `nonadiabaticity`; (iii)'s byte comparison of the nine bounces.csv with 05's covers them.
3. **The values to their digits** (ruling 3). *As the prompt says; made exact*; followed. `drain_values`, the 24 dissipation
   values and both ΔN_eff bit for bit; Y_p and D/H at full precision, with a 5 × 10⁻¹⁰ allowance that was not needed: the
   differences are 0 in all nine histories in both stores. (The build's BBN stage reads the samples back from the store; in
   `Planck_units` that round trip is exact here, which is why the values agree to the bit.)
4. **R7** (ruling 4). *STRUCTURALLY REQUIRED*; followed. `--small-network --variant prod --phi-init-Mp 5 --beta 1.185 --M-Mp 4e-31`,
   from the main checkout; each `stored:` line read and each `prod` line equal to it at ten digits, the format-6 base store included.
5. **The commands** (ruling 5). *As the prompt says; made exact*; followed: 11's §5 with this prompt's paths, slugs and options,
   `tools/ray_cluster.sh`, the base launched from the worktree through its own wrapper, the readers with `--config exponential.yaml
   --database <stem>.db --shards 16` and the plot scripts with `--ray-address`. Every command is in verification §6.8.
6. **The base worktree** (ruling 6). *IMPLEMENTATION CHOICE*, ruled. At `<scratch>/base-1561b60` with the two links; nothing in it
   edited; its `git status` before the first base run and after the last showed the two links only. **Removed without `--force`**:
   `git worktree remove` refused while the two untracked links were there, so I unlinked them with `rm` (a link, not its target)
   and removed the worktree plainly; then `git worktree prune`. `git worktree list` shows the main checkout only. The worktree was
   removed at the note's step 6, after the comparisons, not at once after the base sidecar (prompt §1.1); nothing ran from it after
   the sidecar. *IMPLEMENTATION CHOICE* (the note's order governs).
7. **The interruption's delay** (ruling 7). *IMPLEMENTATION CHOICE*, ruled, with one departure. The ruling reads the window's
   opening from `reference-a`'s beats; **the beats hold no per-history line** (the heartbeat changes only at stage banners, and the
   only other lines of that stage are the raylet's disk warnings). The opening was read from the reference store's
   `ScalarHistory.timestamp` instead, as `inelastic-reflection` 11 read its window; the close from the beats as ruled. Window 11.280
   to 71.048 s after the banner, midpoint 41.164 s, `--after 41`; not short. Loads 13.87 (`reference-a`) and 10.20 (`resumed-a`).
   The departure is *STRUCTURALLY REQUIRED*: the beats cannot give the opening.
8. **The load** (ruling 8). *As the prompt says*; followed: every build's and reader's wall with the load at its start; no run
   delayed or repeated for load; no run near the 60-minute bound (the longest 5 min 46 s).
9. **Files** (ruling 9). *STRUCTURALLY REQUIRED*; followed: prompt §7's files and nothing else.

Other deviations:
10. **The suites' outputs** went to the session scratchpad (`$SUITES`), as the note's "Scratch" says, and their summary was copied
    to `out/06-suites.txt`. `suite.sh` takes the directory from `$SUITES` instead of naming it. *IMPLEMENTATION CHOICE.*
11. **Black.** The five scripts I wrote were formatted with `black` after the runs (layout only); each was re-run afterwards on the
    same inputs and its output compared with the one used here (§4). The four copied Python helpers were not black-clean at 11
    either (11 committed them as `.txt`) and are left as copied, so that their diffs show only the added docstring paragraph.
    *IMPLEMENTATION CHOICE.*
12. **Four bugs of my own scripts, fixed as I met them** (none touches the repository's code):
    - `code_probe.py` and `compare_base.py` first took `import ComputeTargets.ScalarHistory as SM`, which gives the class
      `ScalarHistory` that `ComputeTargets/__init__.py` exports under that name; both now use `importlib.import_module`. The first
      probe call from each tree failed with `AttributeError` before printing anything; the probe was re-run from both trees.
    - `compare_base.py` took `cosmology_type`/`model_type` as table names; they are integer type ids. It now names the store's two
      tables, `QCD_Cosmology` and `ExponentialModel`, and still compares the ids as `ScalarHistory` columns.
    - Shell quoting (zsh does not split an unquoted variable): four R6 calls passed `--assoc-scale <c>` as one word and were
      refused by argparse (exit 2), and three R7 calls from a loop were malformed (an empty store name, exit 2 at argument parsing,
      nothing opened; their record files had spaces in their names and were deleted). Each was re-run as an explicit call. The
      readers' record (`out/06-readers.txt`) keeps the failed calls with a note.
    *IMPLEMENTATION CHOICE* (fixes of this prompt's own tooling, recorded).
13. **An early `compare_base.py`** on the base and reference stores at 08:51, after B1 and before B2 (the note puts the comparisons
    at step 6): read-only, to stop early if the base differed. The official run at step 6 gave the same output byte for byte.
    Likewise a read-only `compare_02d.py` on the reference store while `reference-b` ran (eight histories then, all equal).
    *IMPLEMENTATION CHOICE.*
14. **histories.csv has 54 rows, not 26** (prompt §3). The plot script writes a row for every (row, M, β) of the stored grid and
    marks the 28 that have no history `no ScalarHistory row in the store` (`plot_by_beta.py:882`, since `abccc23`). The 26 stored
    histories are there in P36's sort. This is the plot script's long-standing behaviour on a non-rectangular grid, not a
    failure of a reader: recorded (§6), not a stop. *UNINTENDED DRIFT* of the prompt's expectation, not of the code.

## 3. The gates

**Bitwise** (verification §6.4), item by item:
- **Matched:** 17 of 17 base histories, each once under `fixed` in the reference store; 17 of 17 of the tip's fixed histories
  matched once; every sample matched by ln(1 + z) (101 335), none on one side only, no duplicate.
- **Compared bitwise:** `ScalarHistory` 935 (55 columns × 17), `extra_data` 107, `ScalarHistoryValue` 1 925 365 (19 × 101 335),
  `AdiabaticHistory` 102, `AdiabaticHistoryValue` 506 675, `BBNData` 170, `BBNDataValue` 90 952, key tables 323: **2 524 629, all
  equal**.
- **Left out, by name:** `serial`, `*_serial`, `version`, `label`, `timestamp`, and the six timing columns.
- **The new columns:** `drain_model` = `fixed` 17/17; the 23 drain-record columns NULL 17/17; no bounce row 17/17; `log_m_eff_Mp`
  and `log_m_tracker_Mp` filled at 101 335 of 101 335 samples, NULL at none; recomputed by `sample_masses` on the read-back state,
  0 NULL-where-value, 0 value-where-None, 202 670 of 202 670 filled values equal bit for bit.
- **Exceptions:** none.
- **Holds.**

**R** (verification §6.5), item by item:
- **(i)** `compare_stores` exit 0, `RESULT: equal`, `problems: none`, `ScalarHistoryBounce` 2 562 matched, 0 on either side only;
  `store fingerprint`: both `7c7641d2aade2d926319113781d8515ea8488b826d2f4b488b3c48105636dd04`, format 7, problems none. **Holds.**
- **(ii)**, on each store: nine histories matched; tuning 135 values, `drain_values` 117, candidate values 14 135 (1 285 rows ×
  11; 14 135 also raw-equal), dissipation 216 (ΔN_eff included): **all equal**. Y_p's and D/H's largest relative differences:
  **0 and 0** (every one of the nine exactly equal). **Holds.**
- **(iii)** reference against resumed exports: 101 pairs, 93 byte-identical, 8 `data.csv` differing only in the timing columns
  `scalar_compute_time`, `BBN_compute_time`, `NP_compute_time`, 0 otherwise; the nine bounces.csv byte-identical to 05's; the
  quanta_histories rows equal to 05's at c_a = 1, 0 and 10 (27 of 27, a measurement). **Holds.**

## 4. Verification performed

**The suites**, serially, ComputeTargets in its two halves, the `test_[g-z]` half and tools in the background, never with the
cluster up (`out/06-suites.txt`; `unittest`'s time; the one-minute load at the start). Before, 2026-11-07 08:06–08:24:

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 68.5 s | 8.68 24.11 35.96 (08:06) |
| ComputeTargets `test_[a-f]` | 219 OK | 144.1 s | 6.82 20.01 33.40 (08:07) |
| ComputeTargets `test_[g-z]` | 446 OK | 487.9 s | 6.35 14.32 28.89 (08:10) |
| ObjectFactories | 163 OK | 133.7 s | 40.99 18.30 22.65 (08:18) |
| tools | 191 OK | 211.5 s | 18.57 22.65 24.23 (08:20) |
| RunDrivers | 72 OK | 23.3 s | 12.40 17.60 21.63 (08:24) |

After, 2026-11-07 09:17–09:39, after the cluster's stop and the worktree's removal, the machine heavily loaded by other work:

| suite | count | time | load at start |
|---|---|---|---|
| CosmologyModels | 26 OK | 76.9 s | 107.02 55.21 32.61 (09:17) |
| ComputeTargets `test_[a-f]` | 219 OK | 162.9 s | 130.27 74.59 41.92 (09:18) |
| ComputeTargets `test_[g-z]` | 446 OK | 545.8 s | 149.46 101.68 58.85 (09:21) |
| ObjectFactories | 163 OK | 159.1 s | 199.91 185.56 125.59 (09:31) |
| tools | 191 OK | 273.4 s | 188.09 186.97 137.06 (09:33) |
| RunDrivers | 72 OK | 33.3 s | 151.10 178.99 149.10 (09:38) |

No count fell; no code changed, so none was expected to move. `uptime` after the last: `9:39 up 7 days, 12 hrs, 1 user, load averages: 162.69 178.34 150.22`.

**The runs and readers:** §6.2–§6.6 of the verification document. Each run launched detached through `sh`, checked once, its end
waited on in the background; nothing polled. **The scripts after `black`:** `compare_base.py`, `compare_02d.py` (both stores),
`compare_exports.py` (the three forms) and `histories.py` (three stores) re-run, their outputs byte-identical to those of §6;
`code_probe.py` needs the cluster and was not re-run (its change is layout only). **Nothing outlives the prompt** but the three stores
and their sidecars: the cluster stopped (`ray_cluster.sh stop`, then `status`: lock free, no process, ports free); the lock free;
the worktree removed and pruned; every pid under `var/one-loop-drain/06/` answers `no such process`; `runregistrykit list`: 33
runs, all finished, this prompt's nine `done` and `resumed-a` `killed`. **The token:** `grep -r -F -l -f ~/.ray/auth_token` over
every committed file: no output, exit 1. `git status` before staging: only this prompt's files.

## 5. The deliberate-breakage record

Not applicable: no code or test was written. The comparisons' sensitivity rests on their counts (each compares every value it
names, and a single differing value is reported) and on two checks made in passing: `compare_02d.py` reported `MATCH FAILED` for
C-3233 when run on the reference store before run c had stored it, and `compare_base.py`'s masses' check found every NULL/None
case and every filled value equal on 101 335 samples (a NULL would have been checked against `sample_masses`; none occurred).

## 6. Observations not acted on

- **histories.csv's grid rows.** `plot_by_beta.py` writes a row for every (row, M, β) of the stored grid and marks those with no
  stored history `no ScalarHistory row in the store` (`plot_by_beta.py:882`). On this prompt's non-rectangular grid that is 54 rows,
  28 of them markers; `quanta_measures.py` counts the 9 one-loop markers as "failed histories", and `Delta_N_eff.pdf` lists α = 0.15
  in the legend at laboratory M with no point. The rebuild of record's grid is a rectangle (U14), so it will not show there. 07's
  figure tools should skip the marker rows (empty `normalization`), as the tool does.
- **The shard warning.** Each plot script prints `!! WARNING: number of shards read from database (=16) does not match specified
  number of shards (=10)` although given `--shards 16`; `inelastic-reflection` 11's plot logs carry the same line. The stores open
  read-only with their 16 shards and nothing is written. Cosmetic.
- **"Reflections (elastic model)"** in the footer of a one-loop history's figures (seen on P-2197 at A = 1's `dark_radiation.png`):
  the label predates the drain laws.
- **The disk.** The raylet warned every 10 s that the volume holding `/tmp/ray-cpbh` was over 95% full (44.6 → 39.3 GB free of
  926 GB). Nothing failed. Ray refuses object creation past its threshold, so the rebuild of record on this machine could stop if
  the free space falls further; it is to be run elsewhere (`CLAUDE.md`), but a rehearsal here would meet it.
- **The workers' working directory** is the cluster's (the main checkout) whatever tree the driver runs from (§6.1). Harmless here,
  since the data files read by relative path are identical at both commits; a worktree driver on a shared cluster reads
  `PRyMrates/` and the EOS table from the cluster's start directory, not its own.

## 7. State handed to the next prompt

**For 07 (the figure tools):**
- **The exports:** `var/one-loop-drain/06/plot-reference/QCD_Cosmology/` (the reference store's) and
  `var/one-loop-drain/06/plot-resumed/QCD_Cosmology/` (byte-identical but for three timing columns of `data.csv`).
  - `histories.csv`: 54 rows, the 26 stored histories and 28 marker rows (`failure_reasons` = `no ScalarHistory row in the store`,
    every value column blank). The 26: 17 fixed (α = 0 at 10⁻³ M_P, β ∈ {0.5, 1.5, 3, 5.5}; α = 0.01 and 0.1 at 4 × 10⁻³¹ M_P,
    β ∈ {1.06, 1.185, 2, 10}; α = 0.1 at 10⁻³ M_P, the same four β; α = 0.15 at 10⁻¹² M_P, β = 1) and 9 one-loop
    (`one-loop-measured-v1`, A = 0.1 and 1 at 4 × 10⁻³¹ M_P, β ∈ {1.06, 1.185, 2, 10}; A = 1 at 10⁻¹² M_P, β = 1). The two
    histories at 10⁻³ M_P, β = 0.5 have a BBN failure (T below the spline's minimum) and blank Y_p and D/H.
  - Per history: `fields.csv` (26), `bounces.csv` (9, one-loop only, under `one-loop-measured-v1/A=<A>/csv/beta=<β>/M=<M>eV_n=1/`),
    and the tool's `quanta_bounces.csv`, `quanta_samples.csv` at c_a = 1 (and `-assoc0`, `-assoc10`); at `<label>/` the three
    `quanta_histories*.csv` (9 rows each).
  - Figures (PDF and PNG) under each row's `plots/`, `Delta_N_eff.pdf` per M under `<label>/plots/`.
- **What differs from 05's exports:** the nine one-loop histories are 02d tasks also in 05's export of 230: their bounces.csv are
  byte-identical to 05's, and their quanta_histories rows equal 05's at all three scales. What 05's export lacks and this one has:
  the 17 fixed-law histories (fields.csv, no bounces.csv), BBN and adiabatic values behind histories.csv and fields.csv, the plot
  scripts' figures, and the marker rows. 05's export remains the source for the grid of 230.

**For 08 (the close and the rebuild's handover):**
- **The stores** (kept for the user, P57), each 16 shards and a primary under `var/datastores/`, with a `store create` sidecar:
  - `one-loop-drain-06-base` — the fixed law at `1561b60` (2026.9.0, format 6), 17 histories, `store_id`
    `2f6e8a7c74e54965865a781e8430f66c`, fingerprint `ff38053c…f4a7` (format 6, by the base's code); 117 344 KiB.
  - `one-loop-drain-06-reference` — the tip (2026.10.0, format 7), 26 histories, 2 562 bounce rows, `store_id`
    `2b6fd51c22ad445abaf2cf3e5573582a`, fingerprint `7c7641d2…dd04`; 137 352 KiB.
  - `one-loop-drain-06-resumed` — the same grid with run a interrupted and resumed, `store_id` `23db39c668b4486cba95c7006cdffa65`,
    the same fingerprint; 137 868 KiB.
- **The gates:** Bitwise holds (2 524 629 values, no exception); R holds (stores equal, fingerprints equal, 02d's route bitwise
  including Y_p and D/H, exports byte-identical but for timings).
- **The costs**, at loads of 9–37 on 10 cores: each run 58 s – 5 min 46 s; the three builds 9 min 18 s (base), 13 min 7 s
  (reference), 14 min 33 s (resumed); a fixed-law tuning 1–5 integrations and 6.7–128 s per history (queueing included); a one-loop
  history at laboratory M 1 integration and 4.2–59 s, C-3233 at A = 1 3 integrations and 89 s; the scalar stage of the A = 0.1 row
  (four laboratory-M histories) 61–71 s, of A = 1 16 s; adiabatic ≈ 1 s and BBN ≈ 16–20 s per four histories. Bounce rows 9–1 071
  per one-loop history (R-611 at A = 0.1 the most). The tip's store is 17% larger than the base's for 9 more histories (26 against
  17). The longest heartbeat silence 160 s of the 900 s window.
- **Under Ray, the drivers, readers and tool behave as 04's and 05's logs say**, the refusal of an unstored A included; the
  observations of §6 are for 08's documentation, none a defect.
